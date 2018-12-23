package main

// This is a thin layer of glue between the bazil.org/fuse kernel interface
// and Google Drive.

import (
	"flag"
	"fmt"
	"io"
	"log"
	_ "net/http/pprof"
	"os"
	"sync"
	"time"

	"bazil.org/fuse"
	_ "bazil.org/fuse/fs/fstestutil"
	"bazil.org/fuse/fuseutil"

	drive "google.golang.org/api/drive/v2"

	"github.com/asjoyner/fuse_gdrive/drive_db"
)

// https://developers.google.com/drive/web/folder
const driveFolderMimeType string = "application/vnd.google-apps.folder"
const blockSize uint32 = 4096

var (
	numWorkers = flag.Int("numWorkers", 20, "The number of goroutines to service fuse requests.")
)

// serveConn holds the state about the fuse connection
type serveConn struct {
	db      *drive_db.DriveDB
	service *drive.Service
	uid     uint32 // uid of the user who mounted the FS
	gid     uint32 // gid of the user who mounted the FS
	conn    *fuse.Conn
	handles []handle              // index is the handleid, inode=0 if free
	writers map[int]io.PipeWriter // index matches fh
	sync.Mutex
}

type handle struct {
	inode    fuse.NodeID
	writer   *io.PipeWriter
	lastByte int64
}

// FuseServe receives and dispatches Requests from the kernel
func (sc *serveConn) Serve() error {
	// Create a pool of goroutines that service Fuse requests
	// Each goroutine consumes a small amount of memory, but allows more
	// parallelisim in handling kernel fuse requests.
	workRequests := make(chan fuse.Request)
	for w := 1; w <= *numWorkers; w++ {
		go func(reqs chan fuse.Request) {
			for req := range reqs {
				fuse.Debug(fmt.Sprintf("%+v", req))
				sc.serve(req)
			}
			return
		}(workRequests)
	}

	for {
		req, err := sc.conn.ReadRequest()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		fuse.Debug(fmt.Sprintf("%+v", req))
		workRequests <- req
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
		resp := fuse.InitResponse{
			MaxWrite: 128 * 1024,
			MaxReadahead: 1<<32 - 1,
			Flags: fuse.InitBigWrites & fuse.InitAsyncRead,
		}
		req.Respond(&resp)

	case *fuse.StatfsRequest:
		var numfiles uint64
		if f, err := sc.db.AllFileIds(); err != nil {
			numfiles = uint64(len(f))
		}
		req.Respond(
			&fuse.StatfsResponse{
				Files: numfiles,
				Bsize: blockSize,
			},
		)

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

	// Silently ignore attempts to change permissions
	case *fuse.SetattrRequest:
		inode := uint64(req.Header.Node)
		f, err := sc.db.FileByInode(inode)
		if err != nil {
			fuse.Debug(fmt.Sprintf("FileByInode(%v): %v", inode, err))
			req.RespondError(fuse.EIO)
			return
		}
		req.Respond(&fuse.SetattrResponse{Attr: sc.attrFromFile(*f)})

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
		sc.release(req)

	case *fuse.DestroyRequest:
		req.Respond()
	}
}

// gettattr returns fuse.Attr for the inode described by req.Header.Node
func (sc *serveConn) getattr(req *fuse.GetattrRequest) {
	inode := uint64(req.Header.Node)
	f, err := sc.db.FileByInode(inode)
	if err != nil {
		fuse.Debug(fmt.Sprintf("FileByInode(%v): %v", inode, err))
		req.RespondError(fuse.EIO)
		return
	}

	/* TODO: getattr during upload must return current file size
	sc.Lock()
	sc.Unlock()
	*/

	resp := &fuse.GetattrResponse{}
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
	resp.Data, err = sc.db.ReadFiledata(f, req.Offset, int64(req.Size), f.FileSize)
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
	blocks := file.FileSize / int64(blockSize)
	if r := file.FileSize % int64(blockSize); r > 0 {
		blocks += 1
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
		Nlink:  uint32(file.LinkCount),
		Size:   uint64(file.FileSize),
		Blocks: uint64(blocks),
	}
	if file.MimeType == driveFolderMimeType {
		attr.Mode = os.ModeDir | 0755
	}
	return attr
}

// Allocate a file handle, held by the kernel until Release
func (sc *serveConn) open(req *fuse.OpenRequest) {
	// This will be cheap, Lookup always preceeds Open, so the cache is warm
	f, err := sc.db.FileByInode(uint64(req.Header.Node))
	if err != nil {
		req.RespondError(fuse.ENOENT)
		return
	}

	sc.db.OpenFile(f.Id)

	var hId uint64
	if !req.Flags.IsReadOnly() { // write access requested
		if *readOnly {
			// TODO: if allow_other, require uid == invoking uid to allow writes
			req.RespondError(fuse.EPERM)
			return
		}

		r, w := io.Pipe() // plumbing between WriteRequest and Drive
		go sc.updateInDrive(f.File, r)
		hId = sc.allocHandle(req.Header.Node, w)
	} else {
		hId = sc.allocHandle(req.Header.Node, nil)
	}

	resp := fuse.OpenResponse{Handle: fuse.HandleID(hId)}
	fuse.Debug(fmt.Sprintf("Open Response: %+v", resp))
	req.Respond(&resp)
}

// allocate a kernel file handle for the requested inode
func (sc *serveConn) allocHandle(inode fuse.NodeID, w *io.PipeWriter) uint64 {
	var hId uint64
	var found bool
	h := handle{inode: inode, writer: w}
	sc.Lock()
	defer sc.Unlock()
	for i, ch := range sc.handles {
		if ch.inode == 0 {
			hId = uint64(i)
			sc.handles[hId] = h
			found = true
			break
		}
	}
	if !found {
		hId = uint64(len(sc.handles))
		sc.handles = append(sc.handles, h)
	}
	return hId
}

// Lookup an inode by its NodeID
func (sc *serveConn) handleById(id fuse.HandleID) (handle, error) {
	sc.Lock()
	defer sc.Unlock()
	if int(id) >= len(sc.handles) {
		return handle{}, fmt.Errorf("handle %v has not been allocated", id)
	}
	return sc.handles[id], nil
}

// Prepare to upload the content of the file.
// Any insert or update w/ Media() blocks until the Reader closes.
func (sc *serveConn) updateInDrive(f *drive.File, r *io.PipeReader) {
	_, err := sc.service.Files.Update(f.Id, f).Media(r).Do()
	if err != nil {
		log.Printf("failed uploading %v to drive: %v", f.Title, err)
	}
	debug.Printf("finished uploading to drive: %v", f.Title)
}

// Acknowledge release of file handle by kernel
func (sc *serveConn) release(req *fuse.ReleaseRequest) {
	f, err := sc.db.FileByInode(uint64(req.Header.Node))
	if err == nil {
		sc.db.CloseFile(f.Id)
	}
	sc.Lock()
	defer sc.Unlock()
	h := sc.handles[req.Handle]
	if h.writer != nil {
		h.writer.Close()
		/*
			fileId, err := sc.db.FileIdForInode(uint64(h.inode))
			if err != nil {
				log.Printf("failed to lookup inode for close: %v\n", err)
				req.RespondError(fuse.EIO)
				return
			}

			l := &drive.FileLabels{Hidden: false}
			f := drive.File{Id: fileId, Labels: l}
			if _, err = sc.service.Files.Update(fileId, &f).Do(); err != nil {
				log.Printf("failed to mark inode %v not hidden: %v\n", h.inode, err)
				req.RespondError(fuse.EIO)
				return
			}
		*/
	}
	h.inode = 0
	req.Respond()
}

// Create file in drive, allocate kernel filehandle for writes
func (sc *serveConn) create(req *fuse.CreateRequest) {
	if *readOnly && !req.Flags.IsReadOnly() {
		req.RespondError(fuse.EPERM)
		return
	}

	pInode := uint64(req.Header.Node)
	parent, err := sc.db.FileByInode(pInode)
	if err != nil {
		debug.Printf("failed to get parent file: %v", err)
		req.RespondError(fuse.EIO)
		return
	}
	p := &drive.ParentReference{Id: parent.Id}

	f := &drive.File{Title: req.Name}
	f.Parents = []*drive.ParentReference{p}
	f, err = sc.service.Files.Insert(f).Do()
	if err != nil {
		debug.Printf("Files.Insert(f).Do(): %v", err)
		req.RespondError(fuse.EIO)
		return
	}
	inode, err := sc.db.InodeForFileId(f.Id)
	if err != nil {
		debug.Printf("failed creating inode for %v: %v", req.Name, err)
		req.RespondError(fuse.EIO)
		return
	}

	r, w := io.Pipe() // plumbing between WriteRequest and Drive
	h := sc.allocHandle(fuse.NodeID(inode), w)

	go sc.updateInDrive(f, r)

	// Tell fuse and the OS about the file
	df, err := sc.db.UpdateFile(nil, f)
	if err != nil {
		debug.Printf("failed to update levelDB for %v: %v", f.Id, err)
		// The write has happened to drive, but we failed to update the kernel.
		// The Changes API will update Fuse, and when the kernel metadata for
		// the parent directory expires, the new file will become visible.
		req.RespondError(fuse.EIO)
		return
	}

	resp := fuse.CreateResponse{
		// describes the opened handle
		OpenResponse: fuse.OpenResponse{
			Handle: fuse.HandleID(h),
			Flags:  fuse.OpenNonSeekable,
		},
		// describes the created file
		LookupResponse: fuse.LookupResponse{
			Node:       fuse.NodeID(inode),
			EntryValid: *driveMetadataLatency,
			Attr:       sc.attrFromFile(*df),
		},
	}
	fuse.Debug(fmt.Sprintf("Create(%v in %v): %+v", req.Name, parent.Title, resp))

	req.Respond(&resp)
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
		// The write has happened to drive, but we failed to update the kernel.
		// The Changes API will update Fuse, and when the kernel metadata for
		// the parent directory expires, the new dir will become visible.
		req.RespondError(fuse.EIO)
		return
	}
	sc.db.FlushCachedInode(pInode)
	resp := &fuse.MkdirResponse{}
	resp.Node = fuse.NodeID(f.Inode)
	resp.EntryValid = *driveMetadataLatency
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

// Pass sequential writes on to the correct handle for uploading
func (sc *serveConn) write(req *fuse.WriteRequest) {
	if *readOnly {
		req.RespondError(fuse.EPERM)
		return
	}
	// TODO: if allow_other, require uid == invoking uid to allow writes
	h, err := sc.handleById(req.Handle)
	if err != nil {
		fuse.Debug(fmt.Sprintf("inodeByNodeID(%v): %v", req.Handle, err))
		req.RespondError(fuse.ESTALE)
		return
	}
	if h.lastByte != req.Offset {
		fuse.Debug(fmt.Sprintf("non-sequential write: got %v, expected %v", req.Offset, h.lastByte))
		req.RespondError(fuse.EIO)
		return
	}
	n, err := h.writer.Write(req.Data)
	if err != nil {
		req.RespondError(fuse.EIO)
		return
	}
	sc.Lock()
	h.lastByte += int64(n)
	sc.handles[req.Handle] = h
	sc.Unlock()
	req.Respond(&fuse.WriteResponse{n})
}
