// The fuse-gdrive command makes your Google Drive files accessible as a local mount point.
// It implements a user space filesystem, using the Fuse and Google Drive APIs,
// to allow you to access your files in Google Drive just like a regular local
// filesystem.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"

	drive "code.google.com/p/google-api-go-client/drive/v2"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"

	"github.com/asjoyner/fuse_gdrive/cache"
)

var port = flag.String("port", "12345", "HTTP Server port; your browser will send credentials here.  Must be accessible to your browser, and authorized in the developer console.")
var allowOther = flag.Bool("allow_other", false, "If other users are allowed to view the mounted filesystem.")
var debugGdrive = flag.Bool("gdrive.debug", true, "print debug statements from the fuse_gdrive package")

// https://developers.google.com/drive/web/folder
var driveFolderMimeType string = "application/vnd.google-apps.folder"
var account string
var rootId string
var rootChildren map[string]*Node
var childLock sync.RWMutex

var debug debugging

type debugging bool

func (d debugging) Printf(format string, args ...interface{}) {
	if d {
		log.Printf(format, args...)
	}
}

var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s MOUNTPOINT\n", os.Args[0])
	flag.PrintDefaults()
}

// FS implements Root() to pass the 'tree' to Fuse
type FS struct {
	root Node
}

func (s FS) Root() (fs.Node, fuse.Error) {
	return s.root, nil
}

// don't think this does anything?  still don't see async reads  :-/
func (fs FS) Init(req *fuse.InitRequest, resp *fuse.InitResponse, intr fs.Intr) fuse.Error {
	debug.Printf("Init flags: %+v", req.Flags.String())
	resp.MaxWrite = 128 * 1024
	resp.Flags = fuse.InitBigWrites & fuse.InitAsyncRead
	return nil
}

// Node represents a file (or folder) in Drive.
type Node struct {
	Id          string
	Children    map[string]*Node
	Parents     []string
	Inode       uint64
	Title       string
	isDir       bool
	FileSize    int64
	DownloadUrl string
	isRoot      bool // lookups handled differently, because fuse takes a copy of it
}

func (n Node) Attr() fuse.Attr {
	if n.isDir {
		return fuse.Attr{Inode: n.Inode, Mode: os.ModeDir | 0555}
	} else {
		return fuse.Attr{Inode: n.Inode, Size: uint64(n.FileSize), Mode: 0444}
	}
}

func (n Node) ReadDir(intr fs.Intr) ([]fuse.Dirent, fuse.Error) {
	var dirs []fuse.Dirent
	var childType fuse.DirentType
	children := n.Children
	if n.isRoot {
		childLock.RLock()
		defer childLock.RUnlock()
		children = rootChildren
	}
	for _, child := range children {
		if child.isDir {
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
	children := n.Children
	if n.isRoot {
		childLock.RLock()
		defer childLock.RUnlock()
		children = rootChildren
	}
	if child, ok := children[name]; ok {
		return child, nil
	}
	return Node{}, fuse.ENOENT
}

func (n Node) Read(req *fuse.ReadRequest, resp *fuse.ReadResponse, intr fs.Intr) fuse.Error {
	if n.DownloadUrl == "" { // If there is no downloadUrl, there is no body
		return nil
	}
	debug.Printf("Read(title: %s, offset: %d, size: %d)\n", n.Title, req.Offset, req.Size)
	b, err := cache.Read(n.DownloadUrl, req.Offset, int64(req.Size), n.FileSize)
	if err != nil {
		return fmt.Errorf("cache.Read (..%v..): %v", req.Offset, err)
	}
	resp.Data = b
	return nil
}

func sanityCheck(mountpoint string) error {
	fileInfo, err := os.Stat(mountpoint)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(mountpoint, 0777); err != nil {
			return fmt.Errorf("mountpoint does not exist, could not create it.")
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

	if *debugGdrive {
		debug = true
	}

	if err := sanityCheck(mountpoint); err != nil {
		log.Fatalf("sanityCheck failed: %s\n", err)
	}

	http.HandleFunc("/", RootHandler)
	go http.ListenAndServe(fmt.Sprintf(":%s", *port), nil)

	client := getOAuthClient(drive.DriveReadonlyScope)

	cache.Configure("/tmp", client)

	service, _ := drive.New(client)
	about, err := service.About.Get().Do()
	if err != nil {
		log.Fatalf("drive.service.About.Get.Do: %v\n", err)
	}
	rootId = about.RootFolderId
	account = about.User.EmailAddress

	// Populate the initial filesystem as a single empty node
	rootNode := Node{Id: rootId,
		Children: make(map[string]*Node),
		Inode:    1, // The root of the tree is always 1
		Title:    "/",
		isDir:    true,
		isRoot:   true,
	}
	tree := FS{root: rootNode}

	// periodically refresh the FS with the list of files from Drive
	go updateFS(service, tree)

	//http.Handle("/files", FilesPage{files})
	//http.Handle("/tree", TreePage{tree})

	options := []fuse.MountOption{
		fuse.FSName("GoogleDrive"),
		fuse.Subtype("gdrive"),
		fuse.LocalVolume(),
		fuse.VolumeName(account),
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
	go func() {
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
