// The fuse-gdrive command makes your Google Drive files accessible as a local mount point.
// It implements a user space filesystem, using the Fuse and Google Drive APIs,
// to allow you to access your files in Google Drive just like a regular local
// filesystem.
package main

import (
	"flag"
	"fmt"
  "net/http"
  _ "net/http/pprof"
	"log"
	"os"
  "os/signal"
  "sync/atomic"

  drive "code.google.com/p/google-api-go-client/drive/v2"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"

  "github.com/asjoyner/fuse_gdrive/cache"
)

var port = flag.String("port", "12345", "HTTP Server port; your browser will send credentials here.  Must be accessible to your browser, and authorized in the developer console.")
var allowOther = flag.Bool("allow_other", false, "If other users are allowed to view the mounted filesystem.")

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
  b, err := cache.Read(n.DownloadUrl, req.Offset, int64(req.Size), n.FileSize)
  if err != nil {
    return fmt.Errorf("cache.Read (..%v..): %v", req.Offset, err)
  }
  resp.Data = b
	return nil
}

func sanityCheck(mountpoint string) error {
  fileInfo, err := os.Stat(mountpoint);
  if os.IsNotExist(err) {
    if err := os.MkdirAll(mountpoint, 0777); err != nil {
      return fmt.Errorf("mountpoint does not exist, attempting to create it.")
    }
    return nil
  }
  if err != nil {
    return fmt.Errorf("error stat()ing mountpoint: %s", err)
  }
  if !fileInfo.IsDir() {
    return fmt.Errorf("the mountpoint is not a directory")
  }
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

  if err := sanityCheck(mountpoint); err != nil {
    fmt.Printf("sanityCheck failed: %s\n", err)
    os.Exit(1)
  }

  http.HandleFunc("/", RootHandler)
  go http.ListenAndServe(fmt.Sprintf(":%s", *port), nil)

  client := getOAuthClient(drive.DriveReadonlyScope)

  cache.Configure("/tmp", client)

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
	rootNode.Children[f.Title] = f
	continue
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

  options := []fuse.MountOption{
		fuse.FSName("GoogleDrive"),
		fuse.Subtype("gdrive"),
		fuse.LocalVolume(),
		fuse.VolumeName(about.User.EmailAddress),
  }

  if *allowOther {
    options = append(options, fuse.AllowOther())
  }

	c, err := fuse.Mount(mountpoint, options...)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

  // Trap control-c (sig INT) and unmount
  sig := make(chan os.Signal, 1)
  signal.Notify(sig, os.Interrupt)
  go func(){
    for _ = range sig {
      fuse.Unmount(mountpoint)
    }
  }()

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

