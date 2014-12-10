package drive_db

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/kr/pretty"
)

var driveDBLinks string = `<a href=fileids>FileIDs</a><br>
<a href=checkpoint>Check Point</a><br>
<a href=inodes>Inodes</a><br>
<a href=tree>Tree</a><br>
`

func (d *DriveDB) fileIdsHandler(w http.ResponseWriter, req *http.Request) {
	ids, err := d.AllFileIds()
	if err != nil {
		fmt.Fprintf(w, "Failed to list fileids: %v", err)
		return
	}
	fmt.Fprintf(w, "%v fileids in LevelDB:\n", len(ids))
	for _, i := range ids {
		fmt.Fprintf(w, "%v\n", i)
	}
}

func (d *DriveDB) checkpointHandler(w http.ResponseWriter, req *http.Request) {
	var cpt CheckPoint
	err := d.get(internalKey("checkpoint"), &cpt)
	if err != nil {
		fmt.Fprintf(w, "Failed to read checkpoint: %v", err)
		return
	}
	fmt.Fprintf(w, "Checkpoint: %+v\n", cpt)
}

func (d *DriveDB) inodesHandler(w http.ResponseWriter, req *http.Request) {
	var cpt CheckPoint
	err := d.get(internalKey("checkpoint"), &cpt)
	if err != nil {
		fmt.Fprintf(w, "Failed to read checkpoint: %v", err)
		return
	}
	for i:=2; uint64(i)<=cpt.LastInode; i++ {
		var fileId string
		err := d.get(inodeToFileIdKey(uint64(i)), &fileId)
		if err != nil {
			fmt.Fprintf(w, "%v: Missing\n", i)
		} else {
			fmt.Fprintf(w, "%v: %v\n", i, fileId)
		}
	}
}

func (d *DriveDB) fileIdHandler(w http.ResponseWriter, req *http.Request) {
	// This handles strings of the format /drivedb/fileid/<fileid>
	// tip: there's a leading slash... watch those indexes...
	splitUrl := strings.SplitN(req.URL.Path, "/", 4)
	if len(splitUrl) < 4 {
		fmt.Fprintf(w, "wanted url which splits on / with a len(/drivedb/fileid/<fileid>) >= 4, found %v with len(%v): ", splitUrl, len(splitUrl))
		return
	}
	fileId := splitUrl[3]
	file, err := d.FileById(fileId)
	if err != nil {
		fmt.Fprintf(w, "Failed to read file '%v': %v", fileId, err)
		return
	}
	fmt.Fprintf(w, "%# v", pretty.Formatter(file))
}

func (d *DriveDB) fileInodeHandler(w http.ResponseWriter, req *http.Request) {
	// This handles strings of the format /drivedb/fileinode/<int>
	// tip: there's a leading slash... watch those indexes...
	splitUrl := strings.SplitN(req.URL.Path, "/", 4)
	if len(splitUrl) < 4 {
		fmt.Fprintf(w, "wanted url which splits on / with a len(/drivedb/fileid/<fileid>) >= 4, found %v with len(%v): ", splitUrl, len(splitUrl))
		return
	}
	inode, err := strconv.Atoi(splitUrl[3])
	if err != nil {
		fmt.Fprintf(w, "int must be an inode: %v", err)
		return
	}
	file, err := d.FileByInode(uint64(inode))
	if err != nil {
		fmt.Fprintf(w, "Failed to read inode %v: %v", inode, err)
		return
	}
	fmt.Fprintf(w, "%# v", pretty.Formatter(file))
}

// flushInodeHandler flushes the File object at the provided inode from the cache
func (d *DriveDB) flushInodeHandler(w http.ResponseWriter, req *http.Request) {
	// This handles strings of the format /drivedb/flushinode/<int>
	// tip: there's a leading slash... watch those indexes...
	splitUrl := strings.SplitN(req.URL.Path, "/", 4)
	if len(splitUrl) < 4 {
		fmt.Fprintf(w, "wanted url which splits on / with a len(/drivedb/fileid/<fileid>) >= 4, found %v with len(%v): ", splitUrl, len(splitUrl))
		return
	}
	inode, err := strconv.Atoi(splitUrl[3])
	if err != nil {
		fmt.Fprintf(w, "int must be an inode: %v", err)
		return
	}
	d.FlushCachedInode(uint64(inode))
	fmt.Fprintf(w, "Flushed.")
}

func registerDebugHandles(d DriveDB) {
	http.HandleFunc("/drivedb/fileids", d.fileIdsHandler)
	http.HandleFunc("/drivedb/checkpoint", d.checkpointHandler)
	http.HandleFunc("/drivedb/inodes", d.inodesHandler)
	http.HandleFunc("/drivedb/fileid/", d.fileIdHandler)
	http.HandleFunc("/drivedb/fileinode/", d.fileInodeHandler)
	http.HandleFunc("/drivedb/flushinode/", d.flushInodeHandler)
	// TODO: Implement /tree printing of FS
	http.HandleFunc("/drivedb/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, driveDBLinks)
	})
}
