// The fuse-gdrive command makes your Google Drive files accessible as a local mount point.
// It implements a user space filesystem, using the Fuse and Google Drive APIs,
// to allow you to access your files in Google Drive just like a regular local
// filesystem.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
  "sync/atomic"

  drive "code.google.com/p/google-api-go-client/drive/v2"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"
)

var nextInode uint64 = 0

var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s MOUNTPOINT\n", os.Args[0])
	flag.PrintDefaults()
}

// FS implements Root() to pass the 'tree' to Fuse
type FS struct{
  root Node
}

func (s FS) Root() (fs.Node, fuse.Error) {
       return s.root, nil
}

// Node represents a file (or folder) in Drive.
type Node struct {
  drive.File
  Children []*Node
  Inode uint64  // TODO: how do we define an inode?  global incrementer?
}

// https://developers.google.com/drive/web/folder
func (n Node) IsDir() bool {
  if n.MimeType == "application/vnd.google-apps.folder" {
    return true
  }
  return false
}

func (n Node) Attr() fuse.Attr {
  if n.IsDir() {
    return fuse.Attr{Inode: n.Inode, Mode: os.ModeDir | 0555}
  } else {
    return fuse.Attr{Inode: n.Inode, Mode: 0444}
  }
}

func (n Node) ReadDir(intr fs.Intr) ([]fuse.Dirent, fuse.Error) {
  var dirs []fuse.Dirent
  var childType fuse.DirentType
  for _, child := range n.Children {
    if child.IsDir() {
      childType = fuse.DT_Dir
    } else {
      childType = fuse.DT_File
    }
    entry := fuse.Dirent{Inode: n.Inode, Name: n.Title, Type: childType}
    dirs = append(dirs, entry)
  }
	return dirs, nil
}

func (n Node) Lookup(name string, intr fs.Intr) (Node, fuse.Error) {
  for _, child := range n.Children {
    if child.Title == name {
      return *child, nil
    }
  }
  return Node{}, fuse.ENOENT
}

func (n Node) Read(req *fuse.ReadRequest, resp *fuse.ReadResponse, intr fs.Intr) fuse.Error {
  /* TODO: fix this
  if n.DownloadUrl == "" { // If there is no downloadUrl, there is no body
    return nil
  }
  req, err := http.NewRequest("GET", downloadUrl, nil)
  if err != nil {
    return err
  }
  spec = fmt.Sprintf("%s-%s", req.Offset, req.Size)
  req.Header.Add("byte", spec)

  resp, err := t.RoundTrip(req)
  // Make sure we close the Body later
  // maybe in Release()?
  defer resp.Body.Close()
  if err != nil {
    fmt.Printf("An error occurred: %v\n", err)
    return "", err
  }
  body, err := ioutil.ReadAll(resp.Body)
  if err != nil {
    fmt.Printf("An error occurred: %v\n", err)
    return "", err
  }
  return string(body), nil
  copy(resp.Data, resp)  //[]byte
  */
	return nil
}

func main() {
	flag.Usage = Usage
	flag.Parse()

	if flag.NArg() != 1 {
		Usage()
		os.Exit(2)
	}
	mountpoint := flag.Arg(0)

  client := getOAuthClient(drive.DriveReadonlyScope)
  service, _ := drive.New(client)
  files, err := AllFiles(service)
  if err != nil {
    log.Fatal("failed to list files in drive: ", err)
  }

  about, err := service.About.Get().Do()
  if err != nil {
    log.Fatal("drive.service.About.Get.Do: %v\n", err)
  }
  rootId := about.RootFolderId


  // TODO: build a tree representation of nodes in a filesystem, for fuse
  fileById := make(map[string]Node, len(files))
  for _, f := range files {
    inode := atomic.AddUint64(&nextInode, 1)
    fileById[f.Id] = Node{*f, nil, inode}
  }
  for _, f := range fileById {
    for _, p := range f.Parents {
      var parent = fileById[p.Id]  // can't assign to field of a map, so...
      parent.Children = append(parent.Children, &f)
      fileById[p.Id] = parent
    }
  }
  tree := FS{fileById[rootId]}

	c, err := fuse.Mount(
		mountpoint,
		fuse.FSName("GoogleDrive"),
		fuse.Subtype("gdrive"),
		fuse.LocalVolume(),
		fuse.VolumeName("TODO: insert account name here."),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	err = fs.Serve(c, tree)
	if err != nil {
		log.Fatal(err)
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		log.Fatal(err)
	}
}

