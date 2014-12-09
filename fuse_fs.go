// a thin layer of glue between the bazil.org/fuse kernel interface, and Google Drive.
package main

import (
	"fmt"
	"io"
	_ "net/http/pprof"
	"os"
	"time"

	"bazil.org/fuse"
	_ "bazil.org/fuse/fs/fstestutil"
	"bazil.org/fuse/fuseutil"

	drive "code.google.com/p/google-api-go-client/drive/v2"

	"github.com/asjoyner/fuse_gdrive/cache"
	"github.com/asjoyner/fuse_gdrive/drive_db"
)

// https://developers.google.com/drive/web/folder
var driveFolderMimeType string = "application/vnd.google-apps.folder"

// serveConn holds the state about the fuse connection
type serveConn struct {
	db         *drive_db.DriveDB
	service    *drive.Service
	driveCache cache.Reader
	launch     time.Time
	rootId     string // the fileId of the root
	uid        uint32 // uid of the user who mounted the FS
	gid        uint32 // gid of the user who mounted the FS
	conn       *fuse.Conn
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
			f, err := sc.db.FileByInode(inode)
			if err != nil {
				fuse.Debug(fmt.Sprintf("FileByInode(%v): %v", inode, err))
				req.RespondError(fuse.EIO)
				break
			}

			attr = sc.AttrFromFile(*f)
		}
		resp.Attr = attr
		fuse.Debug(resp)
		req.Respond(resp)

	// Retrieve all children of inode
	// Return a Dirent for child "xxx" of an inode, or ENOENT
	case *fuse.LookupRequest:
		inode := uint64(req.Header.Node)
		resp := &fuse.LookupResponse{}
		var childInodes []uint64
		var err error
		if inode == 1 {
			childInodes, err = sc.db.RootInodes()
			if err != nil {
				fuse.Debug(fmt.Sprintf("RootInodes lookup failure: %v", err))
				req.RespondError(fuse.ENOENT)
				break
			}
		} else {
			file, err := sc.db.FileByInode(inode)
			if err != nil {
				fuse.Debug(fmt.Sprintf("FileByInode lookup failure for %d: %v", inode, err))
				req.RespondError(fuse.ENOENT)
				break
			}
			for _, cInode := range file.Children {
				// TODO: optimize this to not call FileByInode twice for non-root children
				child, err := sc.db.FileByInode(cInode)
				if err != nil {
					fuse.Debug(fmt.Sprintf("child inode %v lookup failure: %v", cInode, err))
					req.RespondError(fuse.ENOENT)
					break
				}
				childInodes = append(childInodes, child.Inode)
			}
		}

		var found bool
		for _, cInode := range childInodes {
			cf, err := sc.db.FileByInode(cInode)
			if err != nil {
				fuse.Debug(fmt.Sprintf("FileByInode(%v): %v", cInode, err))
				req.RespondError(fuse.EIO)
				break
			}
			if cf.Title == req.Name {
				resp.Node = fuse.NodeID(cInode)
				resp.EntryValid = *driveMetadataLatency
				resp.AttrValid = *driveMetadataLatency
				resp.Attr = sc.AttrFromFile(*cf)
				fuse.Debug(fmt.Sprintf("Lookup(%v in %v): %v", req.Name, inode, cInode))
				req.Respond(resp)
				found = true
				break
			}
		}
		if !found {
			fuse.Debug(fmt.Sprintf("Lookup(%v in %v): ENOENT", req.Name, inode))
			req.RespondError(fuse.ENOENT)
		}

	// Ack that the kernel has forgotten the metadata about an inode
	case *fuse.ForgetRequest:
		req.Respond()

	// Hand back the inode as the HandleID
	case *fuse.OpenRequest:
		/* // TODO: Remove Me.  The kernel should never ask to open an inode that
		* it hasn't already called Lookup on... so we don't need this check...
		if _, err := fileId, err := sc.db.FileIdForInode(inode); err != nil {
			req.RespondError(fuse.ENOENT)
			break
		}
		*/
		req.Respond(&fuse.OpenResponse{Handle: fuse.HandleID(req.Header.Node)})

	// Return Dirent
	case *fuse.ReadRequest:
		// Lookup which fileId this request refers to
		inode := uint64(req.Header.Node)
		resp := &fuse.ReadResponse{Data: make([]byte, 0, req.Size)}
		var data []byte
		var err error
		if req.Dir {
			data, err = sc.ReadDir(inode)
		} else {
			data, err = sc.Read(inode, req.Offset, req.Size)
		}
		if err != nil {
			fuse.Debug(fmt.Sprintf("read failure: %v", err))
			req.RespondError(fuse.EIO)
		} else {
			fuseutil.HandleRead(req, resp, data)
			req.Respond(resp)
		}

	// Return MkdirResponse (it's LookupResponse, essentially) of new dir
	case *fuse.MkdirRequest:
		sc.Mkdir(req)

	// Return MkdirResponse (it's LookupResponse, essentially) of new dir
	case *fuse.RemoveRequest:
		sc.Remove(req)

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

func (sc *serveConn) ReadDir(inode uint64) ([]byte, error) {
	var dirs []fuse.Dirent
	var children []uint64
	var err error
	if inode == 1 {
		children, err = sc.db.RootInodes()
		if err != nil {
			return nil, fmt.Errorf("RootInodes: %v", err)
		}
	} else {
		file, err := sc.db.FileByInode(inode)
		if err != nil {
			return nil, fmt.Errorf("FileByInode on inode %d: %v", inode, err)
		}
		children = file.Children
	}

	for _, inode := range children {
		f, err := sc.db.FileByInode(inode)
		if err != nil {
			return nil, fmt.Errorf("FileByInode on child inode %d: %v", inode, err)
		}
		childType := fuse.DT_File
		if f.MimeType == driveFolderMimeType {
			childType = fuse.DT_Dir
		}
		dirs = append(dirs, fuse.Dirent{Inode: f.Inode, Name: f.Title, Type: childType})
	}
	fuse.Debug(fmt.Sprintf("%+v", dirs))
	var data []byte
	for _, dir := range dirs {
		data = fuse.AppendDirent(data, dir)
	}
	return data, nil
}

func (sc *serveConn) Read(inode uint64, offset int64, size int) ([]byte, error) {
	f, err := sc.db.FileByInode(inode)
	if err != nil {
		return nil, fmt.Errorf("FileByInode on inode %d: %v", inode, err)
	}
	url := sc.db.FreshDownloadUrl(f)
	if url == "" { // If there is no url, the file has no body
		return nil, io.EOF
	}
	debug.Printf("Read(title: %s, offset: %d, size: %d)\n", f.Title, offset, size)
	b, err := sc.driveCache.Read(url, offset, int64(size), f.FileSize)
	if err != nil {
		return nil, fmt.Errorf("driveCache.Read (..%v..): %v", offset, err)
	}
	return b, nil
}

func (sc *serveConn) AttrFromFile(file drive_db.File) fuse.Attr {
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
		Inode:  file.Inode,
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

func (sc *serveConn) Mkdir(req *fuse.MkdirRequest) {
	// TODO: if allow_other, require uid == invoking uid to allow writes
	pInode := uint64(req.Header.Node)
	pId, err := sc.db.FileIdForInode(pInode)
	if err != nil {
		debug.Printf("failed to get parent fileid: %v", err)
		req.RespondError(fuse.EIO)
	}
	p := []*drive.ParentReference{&drive.ParentReference{Id: pId}}
	file := &drive.File{Title: req.Name, MimeType: driveFolderMimeType, Parents: p}
	file, err = sc.service.Files.Insert(file).Do()
	if err != nil {
		debug.Printf("Insert failed: %v", err)
		req.RespondError(fuse.EIO)
	}
	debug.Printf("Child of %v created in drive: %+v", file.Parents[0].Id, file)
	f, err := sc.db.UpdateFile(nil, file)
	if err != nil {
		debug.Printf("failed to update levelDB for %v: %v", f.Id, err)
		// The write has happened to drive, but we can't update the kernel yet.
		// The Changes API will update Fuse, and when the kernel metadata for
		// the parent directory expires, the new dir will become visible.
		req.RespondError(fuse.EIO)
	}
	sc.db.FlushCachedInode(pInode)
	resp := &fuse.MkdirResponse{}
	resp.Node = fuse.NodeID(f.Inode)
	resp.EntryValid = *driveMetadataLatency
	resp.AttrValid = *driveMetadataLatency
	resp.Attr = sc.AttrFromFile(*f)
	fuse.Debug(fmt.Sprintf("Mkdir(%v): %+v", req.Name, f))
	req.Respond(resp)
}

// Remove also covers rmdir
func (sc *serveConn) Remove(req *fuse.RemoveRequest) {
	// TODO: if allow_other, require uid == invoking uid to allow writes
	// TODO: consider disallowing deletion of directories with contents.. but what error?
	pInode := uint64(req.Header.Node)
	parent, err := sc.db.FileByInode(pInode)
	if err != nil {
		debug.Printf("failed to get parent file: %v", err)
		req.RespondError(fuse.EIO)
	}
	for _, cInode := range parent.Children {
		child, err := sc.db.FileByInode(cInode)
		if err != nil {
			debug.Printf("failed to get child file: %v", err)
		}
		if child.Title == req.Name {
			sc.service.Files.Delete(child.Id).Do()
			sc.db.RemoveFileById(child.Id, nil)
			sc.db.FlushCachedInode(pInode)
			req.Respond()
			return
		}
	}
	req.RespondError(fuse.ENOENT)
}

// TODO: Implement Write
/*
func (n *Node) Write(req *fuse.WriteRequest, resp *fuse.WriteResponse, intr fs.Intr) fuse.Error
	// TODO: if allow_other, require uid == invoking uid to allow writes
}
*/
