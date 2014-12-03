// The fuse-gdrive command makes your Google Drive files accessible as a local mount point.
// It implements a user space filesystem, using the Fuse and Google Drive APIs,
// to allow you to access your files in Google Drive just like a regular local
// filesystem.
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"math"
	"os"
	"os/signal"
	"os/user"
	"path"
	"runtime"
	"strconv"
	"sync"
	"time"

	drive "code.google.com/p/google-api-go-client/drive/v2"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"bazil.org/fuse/fuseutil"
	_ "bazil.org/fuse/fs/fstestutil"

	"github.com/asjoyner/fuse_gdrive/cache"
	"github.com/asjoyner/fuse_gdrive/drive_db"
)

var port = flag.String("port", "12345", "HTTP Server port; your browser will send credentials here.  Must be accessible to your browser, and authorized in the developer console.")
var readOnly = flag.Bool("readonly", false, "Mount the filesystem read only.")
var allowOther = flag.Bool("allow_other", false, "If other users are allowed to view the mounted filesystem.")
var debugGdrive = flag.Bool("gdrive.debug", true, "print debug statements from the fuse_gdrive package")
var driveMetadataLatency = flag.Duration("metadatapoll", time.Minute, "How often to poll Google Drive for metadata updates")

var client *http.Client
var service *drive.Service
var driveCache cache.Reader
var startup = time.Now()

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

// FuseServe receives and dispatches Requests from the kernel
func FuseServe(c *fuse.Conn, db *drive_db.DriveDB) error {
	sc := serveConn{db: db,
		handles: make(map[fuse.HandleID]string),
		launch:  time.Unix(1335225600, 0),
	}
	for {
		req, err := c.ReadRequest()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		/* TODO: see c.req[hdr.ID] != nil in fs/serve.go
		if req.Header.ID != nil {
			intr = nil
		}
		*/
		fuse.Debug(fmt.Sprintf("%+v", req))
		go sc.serve(req)
	}
	return nil

}

// serveConn holds the state about the fuse connection
type serveConn struct {
	sync.Mutex
	db      *drive_db.DriveDB
	// handles maps a kernel fuse filehandle id to a drive file id
	// held open by the kernel from OpenRequest() until it sends ForgetRequest()
	// This is a sparse map, presence of a HandleID indicates the kernel has it open
	handles map[fuse.HandleID]string
	launch  time.Time
	nodeGen uint64
}

func (sc *serveConn) serve(req fuse.Request) {
	switch req := req.(type) {
	default:
		// ENOSYS means "this server never implements this request."
		//done(fuse.ENOSYS)
		fuse.Debug(fmt.Sprintf("ENOSYS: %+v", req))
		req.RespondError(fuse.ENOSYS)

	case *fuse.InitRequest:
		resp := fuse.InitResponse{MaxWrite: 128 * 1024,
			Flags: fuse.InitBigWrites & fuse.InitAsyncRead,
		}
		req.Respond(&resp)

	case *fuse.StatfsRequest:
		req.Respond(&fuse.StatfsResponse{})

	// Return Attr for the given inode
	case *fuse.GetattrRequest:
		inode := uint64(req.Header.Node)
		resp := &fuse.GetattrResponse{}
		resp.AttrValid = *driveMetadataLatency
		var attr fuse.Attr
		if inode == 1 {
			attr.Inode = 1
			attr.Mode = os.ModeDir | 0755
			attr.Atime = sc.launch
			attr.Mtime = sc.launch
			attr.Ctime = sc.launch
			attr.Crtime = sc.launch
			attr.Uid = uid
			attr.Gid = gid
		} else {
			id, err := sc.db.FileIdByInode(inode)
			if err != nil {
				fuse.Debug(fmt.Sprintf("inode lookup failure: %v", err))
				req.RespondError(fuse.ENOENT)
				break
			}
			f, err := sc.db.FileById(id)
			if err != nil {
				fuse.Debug(fmt.Sprintf("file lookup failure: %v", err))
				req.RespondError(fuse.EIO)
				break
			}

			attr = AttrFromFile(f, inode)
		}
		resp.Attr = attr
		fuse.Debug(resp)
		req.Respond(resp)

	// Retrieve all children of inode
	// Return a Dirent for child "xxx" of an inode, or ENOENT
	case *fuse.LookupRequest:
		inode := uint64(req.Header.Node)
		resp := &fuse.LookupResponse{}
		var childFileIds []string
		var err error
		if inode == 1 {
			childFileIds, err = sc.db.RootFileIds()
			if err != nil {
				fuse.Debug(fmt.Sprintf("RootFileIDs lookup failure: %v", err))
				req.RespondError(fuse.ENOENT)
				break
			}
		} else {
			fileId, err := sc.db.FileIdByInode(inode)
			if err != nil {
				fuse.Debug(fmt.Sprintf("FileIdByInode lookup failure for %d: %v", inode, err))
				req.RespondError(fuse.ENOENT)
				break
			}
			childFileIds, err = sc.db.ChildFileIds(fileId)
			if err != nil {
				fuse.Debug(fmt.Sprintf("child id lookup failure: %v", err))
				req.RespondError(fuse.ENOENT)
				break
			}
		}

		for _, childId := range childFileIds {
			cf, err := sc.db.FileById(childId)
			if err != nil {
				fuse.Debug(fmt.Sprintf("FileById lookup failure on childId : %v", childId, err))
				req.RespondError(fuse.EIO)
				break
			}
			if cf.Title == req.Name {
				inode, err := sc.db.InodeByFileId(childId)
				if err != nil {
					fuse.Debug(fmt.Sprintf("InodeByFileId lookup failure on fileId %v: %v", childId, err))
					req.RespondError(fuse.EIO)
					break
				}
				resp.Node = fuse.NodeID(inode)
				sc.Lock()
				resp.Generation = sc.nodeGen
				sc.nodeGen++
				sc.Unlock()
				resp.EntryValid = *driveMetadataLatency
				resp.AttrValid = *driveMetadataLatency
				resp.Attr = AttrFromFile(cf, inode)
				fmt.Printf("%+v\n", resp)
				req.Respond(resp)
				break
			}
		}
		req.RespondError(fuse.ENOENT)

	// Ack that the kernel has forgotten the metadata about an inode
	case *fuse.ForgetRequest:
		req.Respond()

	// Allocate a file handle for an inode, if it's a valid fileId
	case *fuse.OpenRequest:
		inode := uint64(req.Header.Node)
		var fileId string
		var err error
		if inode == 1 {
			fileId = rootId
		} else {
			fileId, err = sc.db.FileIdByInode(inode)
			if err != nil {
				fuse.Debug(fmt.Sprintf("failure looking up inode %d: %v", inode, err))
				req.RespondError(fuse.ENOENT)
				break
			}
		}
		// Allocate a handle
		sc.Lock()
		var resp fuse.OpenResponse
		for i := 1; i<math.MaxInt32; i++ {
			if _, ok := sc.handles[fuse.HandleID(i)]; !ok {
				sc.handles[fuse.HandleID(i)] = fileId
				resp.Handle = fuse.HandleID(i)
				break
			}
		}
		sc.Unlock()
		if resp.Handle == 0 {
			fuse.Debug(fmt.Sprintf("failure allocating a file handle for open on inode %v", inode))
			req.RespondError(fuse.EIO)
		} else {
			req.Respond(&resp)
		}

	// Return Dirent
	case *fuse.ReadRequest:
		// Lookup which fileId this request refers to
		sc.Lock()
		fileId, ok := sc.handles[req.Handle]
		sc.Unlock()
		if !ok {
			req.RespondError(fuse.ESTALE)
			break
		}
		resp := &fuse.ReadResponse{Data: make([]byte, 0, req.Size)}
		var data []byte
		var err error
		if req.Dir {
			data, err = sc.ReadDir(fileId)
		} else {
			data, err = sc.Read(fileId, req.Offset, req.Size)
		}
		if err != nil {
			fuse.Debug(fmt.Sprintf("read failure: %v", err))
			req.RespondError(fuse.EIO)
		} else {
			fuseutil.HandleRead(req, resp, data)
			req.Respond(resp)
		}

	// Ack that the kernel has forgotten the metadata about an inode
	case *fuse.FlushRequest:
		req.Respond()

	// Ack release of a file handle the kernel held mapping an inode->fileId
	case *fuse.ReleaseRequest:
		sc.Lock()
		delete(sc.handles, req.Handle)
		sc.Unlock()
		req.Respond()

	case *fuse.DestroyRequest:
		req.Respond()
	}
}

func AttrFromFile(file *drive.File, inode uint64) fuse.Attr {
	var atime, mtime, crtime time.Time
	if err := atime.UnmarshalText([]byte(file.LastViewedByMeDate)); err != nil {
		atime = startup
	}
	if err := mtime.UnmarshalText([]byte(file.ModifiedDate)); err != nil {
		mtime = startup
	}
	if err := crtime.UnmarshalText([]byte(file.CreatedDate)); err != nil {
		crtime = startup
	}
	attr := fuse.Attr{
		Inode:  inode,
		Atime:  atime,
		Mtime:  mtime,
		Ctime:  mtime,
		Crtime: crtime,
		Uid:    uid,
		Gid:    gid,
		Mode:   0755,
		Size:   uint64(file.FileSize),
		Blocks: uint64(file.FileSize),
	}
	if file.MimeType == driveFolderMimeType {
		attr.Mode = os.ModeDir | 0755
	}
	return attr
}

// FS implements Root() to pass the 'tree' to Fuse
type FS struct {
	root *Node
}

func (s *FS) Root() (fs.Node, fuse.Error) {
	return s.root, nil
}

// don't think this does anything?  still don't see async reads  :-/
func (s *serveConn) Init(req *fuse.InitRequest, resp *fuse.InitResponse, intr fs.Intr) fuse.Error {
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
	Crtime      time.Time
}

func (n *Node) Attr() fuse.Attr {
	a := fuse.Attr{Inode: n.Inode,
		Uid:    uid,
		Gid:    gid,
		Atime:  n.Atime,
		Mtime:  n.Mtime,
		Ctime:  n.Ctime,
		Crtime: n.Crtime,
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

func (sc *serveConn) ReadDir(fileId string) ([]byte, error) {
	var dirs []fuse.Dirent
	var children []string
	var err error
	if fileId == rootId {
		children, err = sc.db.RootFileIds()
		if err != nil {
			return nil, fmt.Errorf("RootFileIds %d: %v", fileId, err)
		}
	} else {
		children, err = sc.db.ChildFileIds(fileId)
		if err != nil {
			return nil, fmt.Errorf("ChildFileIds on fileId %d: %v", fileId, err)
		}
	}

	for _, cId := range children {
		f, err := sc.db.FileById(cId)
		if err != nil {
			return nil, fmt.Errorf("FileById on cId %d: %v", cId, err)
		}
		inode, err := sc.db.InodeByFileId(cId)
		if err != nil {
			return nil, fmt.Errorf("InodeByFileId on cId %d: %v", cId, err)
		}
		childType := fuse.DT_File
		if f.MimeType == driveFolderMimeType {
			childType = fuse.DT_Dir
		}
		dirs = append(dirs, fuse.Dirent{Inode: inode, Name: f.Title, Type: childType})
	}
	fuse.Debug(fmt.Sprintf("%+v", dirs))
	var data []byte
	for _, dir := range dirs {
		data = fuse.AppendDirent(data, dir)
	}
	return data, nil
}

func (sc *serveConn) Read(fileId string, offset int64, size int) ([]byte, error) {
	f, err := sc.db.FileById(fileId)
	if err != nil {
		return nil, fmt.Errorf("FileById on fileId %d: %v", fileId, err)
	}
	if f.DownloadUrl == "" { // If there is no downloadUrl, there is no body
		return nil, io.EOF
	}
	debug.Printf("Read(title: %s, offset: %d, size: %d)\n", f.Title, offset, size)
	b, err := driveCache.Read(f.DownloadUrl, offset, int64(size), f.FileSize)
	if err != nil {
		return nil, fmt.Errorf("driveCache.Read (..%v..): %v", offset, err)
	}
	return b, nil
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
	node, err := nodeFromFile(f, 0)
	if err != nil {
		return &Node{}, fmt.Errorf("created dir, but failed to parse response: %v", err)
	}
	n.Children[node.Title] = node
	return n, nil
}

// TODO: Implement remove (doubles as rmdir)
/*
func (n *Node) Remove(req *fuse.RemoveRequest, intr Intr) fuse.Error {
	// TODO: if allow_other, require uid == invoking uid to allow writes
}
*/

// TODO: Implement Write
/*
func (n *Node) Write(req *fuse.WriteRequest, resp *fuse.WriteResponse, intr fs.Intr) fuse.Error
	// TODO: if allow_other, require uid == invoking uid to allow writes
}
*/

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
	runtime.SetBlockProfileRate(1)

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

	driveCache = cache.NewCache("/tmp", client)

	service, _ = drive.New(client)
	about, err := service.About.Get().Do()
	if err != nil {
		log.Fatalf("drive.service.About.Get().Do: %v\n", err)
	}

	// Create and start the drive syncer.
	dbpath := path.Join(os.TempDir(), "fuse-gdrive", about.User.EmailAddress)
	log.Printf("using drivedb: %v", dbpath)
	db, err := drive_db.NewDriveDB(service, dbpath)
	if err != nil {
		log.Fatalf("could not open leveldb: %v", err)
	}
	defer db.Close()
	db.WaitUntilSynced()
	log.Printf("synced!")

	rootId = about.RootFolderId
	account = about.User.EmailAddress

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

	err = FuseServe(c, db)
	if err != nil {
		log.Fatalln("fuse server failed: ", err)
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		log.Fatal(err)
	}
}
