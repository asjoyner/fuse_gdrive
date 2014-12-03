// a thin layer of glue between the bazil.org/fuse kernel interface, and Google Drive.
package main

import (
	"fmt"
	"io"
	_ "net/http/pprof"
	"os"
	"sync"
	"time"

	drive "code.google.com/p/google-api-go-client/drive/v2"

	"bazil.org/fuse"
	_ "bazil.org/fuse/fs/fstestutil"
	"bazil.org/fuse/fuseutil"

	"github.com/asjoyner/fuse_gdrive/cache"
	"github.com/asjoyner/fuse_gdrive/drive_db"
)

// https://developers.google.com/drive/web/folder
var driveFolderMimeType string = "application/vnd.google-apps.folder"

// serveConn holds the state about the fuse connection
type serveConn struct {
	sync.Mutex
	db         *drive_db.DriveDB
	driveCache cache.Reader
	launch  time.Time
	rootId  string // the fileId of the root
	uid     uint32 // uid of the user who mounted the FS
	gid     uint32 // gid of the user who mounted the FS
	conn    *fuse.Conn
}

// FuseServe receives and dispatches Requests from the kernel
func (sc *serveConn) Serve() error {
	for {
		req, err := sc.conn.ReadRequest()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		fuse.Debug(fmt.Sprintf("%+v", req))

		// TODO: paralellize this, after I figure out why every parallel request
		// causes a panic on null pointer dereference when responding to the one or
		// the other request.
		//go sc.serve(req)
		sc.serve(req)
	}
	return nil
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
			attr.Uid = sc.uid
			attr.Gid = sc.gid
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

			attr = sc.AttrFromFile(f, inode)
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
				sc.Unlock()
				resp.EntryValid = *driveMetadataLatency
				resp.AttrValid = *driveMetadataLatency
				resp.Attr = sc.AttrFromFile(cf, inode)
				fmt.Printf("%+v\n", resp)
				req.Respond(resp)
				break
			}
		}
		req.RespondError(fuse.ENOENT)

	// Ack that the kernel has forgotten the metadata about an inode
	case *fuse.ForgetRequest:
		req.Respond()

	// Hand back the inode as the HandleID
	case *fuse.OpenRequest:
		/* // TODO: Remove Me.  The kernel should never ask to open an inode that
		* it hasn't already called Lookup on... so we don't need this check...
		if _, err := fileId, err := sc.db.FileIdByInode(inode); err != nil {
			req.RespondError(fuse.ENOENT)
			break
		}
		*/
		req.Respond(&fuse.OpenResponse{Handle: fuse.HandleID(req.Header.Node)})

	// Return Dirent
	case *fuse.ReadRequest:
		// Lookup which fileId this request refers to
		inode := uint64(req.Header.Node)
		fileId, err := sc.db.FileIdByInode(inode)
		if err != nil {
			fuse.Debug(fmt.Sprintf("inode lookup failure: %v", err))
			req.RespondError(fuse.ENOENT)
			break
		}
		resp := &fuse.ReadResponse{Data: make([]byte, 0, req.Size)}
		var data []byte
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

	// Ack release of the kernel's mapping an inode->fileId
	case *fuse.ReleaseRequest:
		req.Respond()

	case *fuse.DestroyRequest:
		req.Respond()
	}
}

func (sc *serveConn) ReadDir(fileId string) ([]byte, error) {
	var dirs []fuse.Dirent
	var children []string
	var err error
	if fileId == sc.rootId {
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
	b, err := sc.driveCache.Read(f.DownloadUrl, offset, int64(size), f.FileSize)
	if err != nil {
		return nil, fmt.Errorf("driveCache.Read (..%v..): %v", offset, err)
	}
	return b, nil
}

func (sc *serveConn) AttrFromFile(file *drive.File, inode uint64) fuse.Attr {
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
		Uid:    sc.uid,
		Gid:    sc.gid,
		Mode:   0755,
		Size:   uint64(file.FileSize),
		Blocks: uint64(file.FileSize),
	}
	if file.MimeType == driveFolderMimeType {
		attr.Mode = os.ModeDir | 0755
	}
	return attr
}

/*
func (sc *serveConn) Mkdir(req *fuse.MkdirRequest) {
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
*/

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
