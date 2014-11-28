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
	"os/user"
	"strconv"
	"sync"
	"time"

	drive "code.google.com/p/google-api-go-client/drive/v2"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"

	"github.com/asjoyner/fuse_gdrive/cache"
)

var port = flag.String("port", "12345", "HTTP Server port; your browser will send credentials here.  Must be accessible to your browser, and authorized in the developer console.")
var readOnly = flag.Bool("readonly", false, "Mount the filesystem read only.")
var allowOther = flag.Bool("allow_other", false, "If other users are allowed to view the mounted filesystem.")
var debugGdrive = flag.Bool("gdrive.debug", true, "print debug statements from the fuse_gdrive package")

var client *http.Client
var service *drive.Service

// https://developers.google.com/drive/web/folder
var driveFolderMimeType string = "application/vnd.google-apps.folder"
var uid uint32                    // uid of the user who mounted the FS
var gid uint32                    // gid of the user who mounted the FS
var account string                // email address of the mounted google drive account
var rootId string                 // Drive Id of the root of the FS
var rootChildren map[string]*Node // children of the root node of the FS

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
	root *Node
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
	Mu          sync.Mutex
	Children    map[string]*Node
	Parents     []string
	Inode       uint64
	Title       string
	isDir       bool
	FileSize    int64
	DownloadUrl string
	Atime       time.Time
	Mtime       time.Time
	Ctime       time.Time
	isRoot      bool // lookups handled differently, because fuse takes a copy of it
}

func (n *Node) Attr() fuse.Attr {
	a := fuse.Attr{Inode: n.Inode,
		Uid:   uid,
		Gid:   gid,
		Atime: n.Atime,
		Mtime: n.Mtime,
		Ctime: n.Ctime,
	}
	if n.isDir {
		a.Mode = os.ModeDir | 0555
		return a
	} else {
		a.Mode = 0444
		a.Size = uint64(n.FileSize)
		return a
	}
}

func (n *Node) ReadDir(intr fs.Intr) ([]fuse.Dirent, fuse.Error) {
	var dirs []fuse.Dirent
	n.Mu.Lock()
	defer n.Mu.Unlock()
	for filename, child := range n.Children {
		childType := fuse.DT_File
		if child.isDir {
			childType = fuse.DT_Dir
		}
		dirs = append(dirs, fuse.Dirent{Inode: child.Inode, Name: filename, Type: childType})
	}
	return dirs, nil
}

func (n *Node) Lookup(name string, intr fs.Intr) (fs.Node, fuse.Error) {
	n.Mu.Lock()
	defer n.Mu.Unlock()
	if child, ok := n.Children[name]; ok {
		return child, nil
	}
	return &Node{}, fuse.ENOENT
}

func (n *Node) Read(req *fuse.ReadRequest, resp *fuse.ReadResponse, intr fs.Intr) fuse.Error {
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

func (n *Node) Mkdir(req *fuse.MkdirRequest, intr fs.Intr) (fs.Node, fuse.Error) {
	// req: Mkdir [ID=0x12 Node=0x1 Uid=13040 Gid=5000 Pid=50632] "test" mode=drwxr-xr-x
	// TODO: if allow_other, require uid == invoking uid to allow writes
	p := []*drive.ParentReference{&drive.ParentReference{Id: n.Id}}
	f := &drive.File{Title: req.Name, MimeType: driveFolderMimeType, Parents: p}
	f, err := service.Files.Insert(f).Do()
	if err != nil {
		return &Node{}, fmt.Errorf("Insert failed: %v", err)
	}
	// TODO: update the FS sooner, rather than later
	node, err := nodeFromFile(f)
	if err != nil {
		return &Node{}, fmt.Errorf("created dir, but failed to parse response: %v", err)
	}
	return node, nil
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

	userCurrent, err := user.Current()
	if err != nil {
		log.Fatalf("unable to get UID/GID of current user: %v", err)
	}
	uidInt, err := strconv.Atoi(userCurrent.Uid)
	if err != nil {
		log.Fatalf("unable to get UID/GID of current user: %v", err)
	}
	uid = uint32(uidInt)
	gidInt, err := strconv.Atoi(userCurrent.Gid)
	if err != nil {
		log.Fatalf("unable to get UID/GID of current user: %v", err)
	}
	gid = uint32(gidInt)

	if err = sanityCheck(mountpoint); err != nil {
		log.Fatalf("sanityCheck failed: %s\n", err)
	}

	http.HandleFunc("/", RootHandler)
	go http.ListenAndServe(fmt.Sprintf(":%s", *port), nil)

	if *readOnly {
		client = getOAuthClient(drive.DriveReadonlyScope)
	} else {
		client = getOAuthClient(drive.DriveScope)
	}

	cache.Configure("/tmp", client)

	service, _ = drive.New(client)
	about, err := service.About.Get().Do()
	if err != nil {
		log.Fatalf("drive.service.About.Get().Do: %v\n", err)
	}
	rootId = about.RootFolderId
	account = about.User.EmailAddress

	rootNode := rootNode()
	tree := FS{root: &rootNode}

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

	// TODO: if *readOnly { .. add an option to the fuse library for that
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
