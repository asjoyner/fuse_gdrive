// The fuse-gdrive command makes your Google Drive files accessible as a local mount point.
// It implements a user space filesystem, using the Fuse and Google Drive APIs,
// to allow you to access your files in Google Drive just like a regular local
// filesystem.
package main

import (
	"flag"
	"fmt"
  "io/ioutil"
  "net/http"
	"log"
	"os"
  "sync/atomic"

  drive "code.google.com/p/google-api-go-client/drive/v2"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"
)

var port = flag.String("port", "12345", "HTTP Server port; your browser will send credentials here.  Must be accessible to your browser, and authorized in the developer console.")

var nextInode uint64 = 0
var driveFolderMimeType string = "application/vnd.google-apps.folder"

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
  Children map[string]*Node
  Inode uint64
  client *http.Client
}

// https://developers.google.com/drive/web/folder
func (n Node) IsDir() bool {
  if n.MimeType == driveFolderMimeType {
    return true
  }
  return false
}

func (n Node) Attr() fuse.Attr {
  if n.IsDir() {
    return fuse.Attr{Inode: n.Inode, Mode: os.ModeDir | 0555}
  } else {
    return fuse.Attr{Inode: n.Inode, Size: uint64(n.FileSize), Mode: 0444}
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
    entry := fuse.Dirent{Inode: child.Inode, Name: child.Title, Type: childType}
    dirs = append(dirs, entry)
  }
	return dirs, nil
}

func (n Node) Lookup(name string, intr fs.Intr) (fs.Node, fuse.Error) {
  if child, ok := n.Children[name]; ok {
    return child, nil
  }
  return Node{}, fuse.ENOENT
}

func (n Node) Read(req *fuse.ReadRequest, resp *fuse.ReadResponse, intr fs.Intr) fuse.Error {
  if n.DownloadUrl == "" { // If there is no downloadUrl, there is no body
    return nil
  }
  dlReq, err := http.NewRequest("GET", n.DownloadUrl, nil)
  if err != nil {
    return err
  }
  // See http://tools.ietf.org/html/rfc2616#section-14.35  (.1 and .2)
  // https://developers.google.com/drive/web/manage-downloads#partial_download
  spec := fmt.Sprintf("bytes=%s-%s", req.Offset, req.Size)
  dlReq.Header.Add("Range", spec)
  log.Println("Requesting partial size: ", spec)

  dlResp, err := n.client.Do(dlReq)
  // Make sure we close the Body later
  defer dlResp.Body.Close()
  if err != nil {
    return err
  }
  log.Println("HTTP status response: ", dlResp.StatusCode)
  body, err := ioutil.ReadAll(dlResp.Body)
  resp.Data = body
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

  http.HandleFunc("/", RootHandler)
  go http.ListenAndServe(fmt.Sprintf(":%s", *port), nil)

  client := getOAuthClient(drive.DriveReadonlyScope)
  service, _ := drive.New(client)
  files, err := AllFiles(service)
  log.Println("Num files in Drive: ", len(files))
  if err != nil {
    log.Fatal("failed to list files in drive: ", err)
  }

  about, err := service.About.Get().Do()
  if err != nil {
    log.Fatal("drive.service.About.Get.Do: %v\n", err)
  }
  rootId := about.RootFolderId


  // build a tree representation of nodes in a filesystem, for fuse
  fileById := make(map[string]*Node, len(files))
  rootInode := atomic.AddUint64(&nextInode, 1)
  rootFile := drive.File{Title: "/", MimeType: driveFolderMimeType}
  rootNode := Node{rootFile, nil, rootInode, client}
  fileById[rootId] = &rootNode // synthesize the root of the drive tree

  for _, f := range files {
    inode := atomic.AddUint64(&nextInode, 1)
    fileById[f.Id] = &Node{*f, nil, inode, client}
  }
  for _, f := range fileById {
    for _, p := range f.Parents {
      parent, ok := fileById[p.Id]
      if !ok {
        log.Printf("parent of %s not found, expected %s", f.Title, p.Id)
      }
      if parent.Children == nil {
        parent.Children = make(map[string]*Node)
      }
      parent.Children[f.Title] = f
    }
  }
  tree := FS{*fileById[rootId]}

  http.Handle("/files", FilesPage{files})
  http.Handle("/tree", TreePage{*fileById[rootId]})

	c, err := fuse.Mount(
		mountpoint,
		fuse.FSName("GoogleDrive"),
		fuse.Subtype("gdrive"),
		fuse.LocalVolume(),
		fuse.VolumeName(about.User.EmailAddress),
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

