package main

// This is a thin layer of glue between the bazil.org/fuse kernel interface
// and Google Drive.

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
const driveFolderMimeType string = "application/vnd.google-apps.folder"

// serveConn holds the state about the fuse connection
type serveConn struct {
	db         *drive_db.DriveDB
	service    *drive.Service
	driveCache cache.Reader
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
		go sc.serve(req)
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

	case *fuse.GetattrRequest:
		sc.getattr(req)

	case *fuse.LookupRequest:
		sc.lookup(req)

	// Ack that the kernel has forgotten the metadata about an inode
	case *fuse.ForgetRequest:
		req.Respond()

	// Hand back the inode as the HandleID
	case *fuse.OpenRequest:
		sc.open(req)

	case *fuse.CreateRequest:
		// TODO: if allow_other, require uid == invoking uid to allow writes
		sc.create(req)

	// Return Dirents for directories, or requested portion of file
	case *fuse.ReadRequest:
		if req.Dir {
			sc.readDir(req)
		} else {
			sc.read(req)
		}

	// Return MkdirResponse (it's LookupResponse, essentially) of new dir
	case *fuse.MkdirRequest:
		sc.mkdir(req)

	// Removes the inode described by req.Header.Node
	// Respond() for success, RespondError otherwise
	case *fuse.RemoveRequest:
		sc.remove(req)

	// req.Header.Node describes the current parent directory
	// req.NewDir describes the target directory (may be the same)
	// req.OldName and req.NewName describe any (or no) change in name
	case *fuse.RenameRequest:
		sc.rename(req)

	// Responds with the number of bytes written on success, RespondError otherwise
	case *fuse.WriteRequest:
		sc.write(req)

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

// gettattr returns fuse.Attr for the inode described by req.Header.Node
func (sc *serveConn) getattr(req *fuse.GetattrRequest) {
	inode := uint64(req.Header.Node)
	resp := &fuse.GetattrResponse{}
	resp.AttrValid = *driveMetadataLatency
	f, err := sc.db.FileByInode(inode)
	if err != nil {
		fuse.Debug(fmt.Sprintf("FileByInode(%v): %v", inode, err))
		req.RespondError(fuse.EIO)
		return
	}

	resp.Attr = sc.attrFromFile(*f)
	fuse.Debug(resp)
	req.Respond(resp)
}

// Return a Dirent for all children of an inode, or ENOENT
func (sc *serveConn) lookup(req *fuse.LookupRequest) {
	inode := uint64(req.Header.Node)
	resp := &fuse.LookupResponse{}
	var err error
	file, err := sc.db.FileByInode(inode)
	if err != nil {
		fuse.Debug(fmt.Sprintf("FileByInode lookup failure for %d: %v", inode, err))
		req.RespondError(fuse.ENOENT)
		return
	}
	for _, cInode := range file.Children {
		cf, err := sc.db.FileByInode(cInode)
		if err != nil {
			fuse.Debug(fmt.Sprintf("FileByInode(%v): %v", cInode, err))
			req.RespondError(fuse.EIO)
			return
		}
		if cf.Title == req.Name {
			resp.Node = fuse.NodeID(cInode)
			resp.EntryValid = *driveMetadataLatency
			resp.AttrValid = *driveMetadataLatency
			resp.Attr = sc.attrFromFile(*cf)
			fuse.Debug(fmt.Sprintf("Lookup(%v in %v): %v", req.Name, inode, cInode))
			req.Respond(resp)
			return
		}
	}
	fuse.Debug(fmt.Sprintf("Lookup(%v in %v): ENOENT", req.Name, inode))
	req.RespondError(fuse.ENOENT)
}

func (sc *serveConn) readDir(req *fuse.ReadRequest) {
	inode := uint64(req.Header.Node)
	resp := &fuse.ReadResponse{make([]byte, 0, req.Size)}
	var dirs []fuse.Dirent
	var err error
	file, err := sc.db.FileByInode(inode)
	if err != nil {
		fuse.Debug(fmt.Sprintf("FileByInode(%d): %v", inode, err))
		req.RespondError(fuse.EIO)
		return
	}

	for _, inode := range file.Children {
		f, err := sc.db.FileByInode(inode)
		if err != nil {
			fuse.Debug(fmt.Sprintf("child: FileByInode(%d): %v", inode, err))
			req.RespondError(fuse.EIO)
			return
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
	fuseutil.HandleRead(req, resp, data)
	req.Respond(resp)
}

func (sc *serveConn) read(req *fuse.ReadRequest) {
	inode := uint64(req.Header.Node)
	resp := &fuse.ReadResponse{}
	// Lookup which fileId this request refers to
	f, err := sc.db.FileByInode(inode)
	if err != nil {
		debug.Printf("FileByInode(%d): %v", inode, err)
		req.RespondError(fuse.EIO)
		return
	}
	debug.Printf("Read(title: %s, offset: %d, size: %d)\n", f.Title, req.Offset, req.Size)
	resp.Data, err = sc.db.ReadFiledata(f.Id, req.Offset, int64(req.Size), f.FileSize)
	if err != nil && err != io.EOF {
		debug.Printf("driveCache.Read (..%v..): %v", req.Offset, err)
		req.RespondError(fuse.EIO)
		return
	}
	req.Respond(resp)
}

func (sc *serveConn) attrFromFile(file drive_db.File) fuse.Attr {
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

// Hand back the inode as the HandleID
func (sc *serveConn) open(req *fuse.OpenRequest) {
	// This will be cheap, Lookup always preceeds Open, so the cache is warm
	/* TODO: uncomment this after FileByInode handles inode 1
	if _, err := sc.db.FileByInode(uint64(req.Header.Node)); err != nil {
		req.RespondError(fuse.ENOENT)
		return
	}
	*/
	if *readOnly && !req.Flags.IsReadOnly() {
		req.RespondError(fuse.EPERM)
		return
	}
	// TODO: if allow_other, require uid == invoking uid to allow writes
	// TODO: when implementing writes, allocate a kernel handle here, id
	// allocation scheme TBD...
	req.Respond(&fuse.OpenResponse{Handle: fuse.HandleID(req.Header.Node)})
}

// TODO: Implement create
func (sc *serveConn) create(req *fuse.CreateRequest) {
	if *readOnly && !req.Flags.IsReadOnly() {
		req.RespondError(fuse.EPERM)
		return
	}
	req.RespondError(fuse.EIO)
}

func (sc *serveConn) mkdir(req *fuse.MkdirRequest) {
	if *readOnly {
		req.RespondError(fuse.EPERM)
		return
	}
	// TODO: if allow_other, require uid == invoking uid to allow writes
	pInode := uint64(req.Header.Node)
	pId, err := sc.db.FileIdForInode(pInode)
	if err != nil {
		debug.Printf("failed to get parent fileid: %v", err)
		req.RespondError(fuse.EIO)
		return
	}
	p := []*drive.ParentReference{&drive.ParentReference{Id: pId}}
	file := &drive.File{Title: req.Name, MimeType: driveFolderMimeType, Parents: p}
	file, err = sc.service.Files.Insert(file).Do()
	if err != nil {
		debug.Printf("Insert failed: %v", err)
		req.RespondError(fuse.EIO)
		return
	}
	debug.Printf("Child of %v created in drive: %+v", file.Parents[0].Id, file)
	f, err := sc.db.UpdateFile(nil, file)
	if err != nil {
		debug.Printf("failed to update levelDB for %v: %v", f.Id, err)
		// The write has happened to drive, but we can't update the kernel yet.
		// The Changes API will update Fuse, and when the kernel metadata for
		// the parent directory expires, the new dir will become visible.
		req.RespondError(fuse.EIO)
		return
	}
	sc.db.FlushCachedInode(pInode)
	resp := &fuse.MkdirResponse{}
	resp.Node = fuse.NodeID(f.Inode)
	resp.EntryValid = *driveMetadataLatency
	resp.AttrValid = *driveMetadataLatency
	resp.Attr = sc.attrFromFile(*f)
	fuse.Debug(fmt.Sprintf("Mkdir(%v): %+v", req.Name, f))
	req.Respond(resp)
}

// Removes the inode described by req.Header.Node (doubles as rmdir)
// Nota bene: this simply moves files into the Google Drive "Trash", it does not
// delete them permanently.
// Nota bene: there is no check preventing the removal of a directory which
// contains files.
func (sc *serveConn) remove(req *fuse.RemoveRequest) {
	if *readOnly {
		req.RespondError(fuse.EPERM)
		return
	}
	// TODO: if allow_other, require uid == invoking uid to allow writes
	// TODO: consider disallowing deletion of directories with contents.. but what error?
	pInode := uint64(req.Header.Node)
	parent, err := sc.db.FileByInode(pInode)
	if err != nil {
		debug.Printf("failed to get parent file: %v", err)
		req.RespondError(fuse.EIO)
		return
	}
	for _, cInode := range parent.Children {
		child, err := sc.db.FileByInode(cInode)
		if err != nil {
			debug.Printf("failed to get child file: %v", err)
		}
		if child.Title == req.Name {
			sc.service.Files.Delete(child.Id).Do()
			sc.db.RemoveFileById(child.Id, nil)
			req.Respond()
			return
		}
	}
	req.RespondError(fuse.ENOENT)
}

// rename renames a file or directory, optionally reparenting it
func (sc *serveConn) rename(req *fuse.RenameRequest) {
	if *readOnly {
		debug.Printf("attempt to rename while fs in readonly mode")
		req.RespondError(fuse.EPERM)
		return
	}
	// TODO: if allow_other, require uid == invoking uid to allow writes
	oldParent, err := sc.db.FileByInode(uint64(req.Header.Node))
	if err != nil {
		debug.Printf("can't find the referenced inode: %v", req.Header.Node)
		req.RespondError(fuse.ENOENT)
		return
	}
	var f *drive_db.File
	for _, i := range oldParent.Children {
		c, err := sc.db.FileByInode(uint64(i))
		if err != nil {
			debug.Printf("error iterating child inodes: %v", err)
			continue
		}
		if c.Title == req.OldName {
			f = c
		}
	}
	if f == nil {
		debug.Printf("can't find the old file '%v' in '%v'", req.OldName, oldParent.Title)
		req.RespondError(fuse.ENOENT)
		return
	}

	newParent, err := sc.db.FileByInode(uint64(req.NewDir))
	if err != nil {
		debug.Printf("can't find the new parent by inode: %v", req.NewDir)
		req.RespondError(fuse.ENOENT)
		return
	}

	// did the name change?
	if req.OldName != req.NewName {
		f.Title = req.NewName
	}

	// did the parent change?
	var sameParent bool
	var numParents int
	var oldParentId string
	for _, o := range f.Parents {
		numParents++
		oldParentId = o.Id
		if o.Id == newParent.Id {
			sameParent = true
		}
	}
	if !sameParent && numParents > 1 {
		// TODO: Figure out how to identify which of the multiple parents the
		// file is being moved from, so we can call RemoveParents() correctly
		debug.Printf("can't reparent file with multiple parents: %v", req.OldName)
		req.RespondError(fuse.ENOSYS)
		return
	}

	u := sc.service.Files.Update(f.Id, f.File)
	if !sameParent {
		debug.Printf("moving from %v to %v", oldParentId, newParent.Id)
		u = u.AddParents(newParent.Id)
		u = u.RemoveParents(oldParentId)
	}
	r, err := u.Do()
	if err != nil {
		debug.Printf("failed to update '%v' in drive: %v", req.OldName, err)
		req.RespondError(fuse.EIO)
		return
	}

	if _, err := sc.db.UpdateFile(nil, r); err != nil {
		debug.Printf("failed to update leveldb and cache: ", err)
		req.RespondError(fuse.EIO)
		return
	}
	debug.Printf("rename complete")
	req.Respond()
	return
}

// TODO: Implement write
func (sc *serveConn) write(req *fuse.WriteRequest) {
	if *readOnly {
		req.RespondError(fuse.EPERM)
		return
	}
	// TODO: if allow_other, require uid == invoking uid to allow writes
	req.RespondError(fuse.EIO)
}
