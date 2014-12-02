package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/asjoyner/fuse_gdrive/drive_db"

	"code.google.com/p/google-api-go-client/drive/v2"
)

var driveCacheRead = flag.Duration("forcerefresh", time.Minute, "how often to force a full refresh of the list of files and directories from the metadata cache.")
var query = flag.String("query", "trashed=false", "Search parameters to pass to Google Drive, which limit the files mounted.  See http://goo.gl/6kSu3E")
var maxFilesList = flag.Int64("maxfileslist", 1000, "Maximum number of files to query Google Drive for metata at a time.  Range: 1-1000")

// the root node starts out as a single empty node
func rootNode() Node {
	return Node{Id: rootId,
		Children: make(map[string]*Node),
		Inode:    1, // The root of the tree is always 1
		Title:    "/",
		isDir:    true,
		Atime:    time.Unix(1335225600, 0),
		Mtime:    time.Unix(1335225600, 0),
		Ctime:    time.Unix(1335225600, 0),
		Crtime:   time.Unix(1335225600, 0),
	}
}

var (
	startup = time.Now()
)

func nodeFromFile(f *drive.File, inode uint64) (*Node, error) {
	var isDir bool
	if f.MimeType == driveFolderMimeType {
		isDir = true
	}
	node := &Node{Id: f.Id,
		Inode:       inode,
		Title:       f.Title,
		isDir:       isDir,
		FileSize:    f.FileSize,
		DownloadUrl: f.DownloadUrl,
	}
	// Note: on error, the time.Time remains uninitialized; we set it to startup time.
	if err := node.Atime.UnmarshalText([]byte(f.LastViewedByMeDate)); err != nil {
		node.Atime = startup
	}
	if err := node.Ctime.UnmarshalText([]byte(f.ModifiedDate)); err != nil {
		node.Ctime = startup
	}
	node.Mtime = node.Ctime
	if err := node.Crtime.UnmarshalText([]byte(f.CreatedDate)); err != nil {
		node.Crtime = startup
	}
	return node, nil
}

// getNodes returns a map of unique IDs to the Node it describes
func getNodes(db *drive_db.DriveDB) (map[string]*Node, error) {
	ids, err := db.AllFileIds()
	if err != nil {
		return nil, fmt.Errorf("failed to get ids from cache: %v", err)
	}

	var files []*drive.File
	for _, id := range ids {
		f, err := db.FileById(id)
		if err != nil {
			return nil, fmt.Errorf("failed to get id %s from cache: %v", id, err)
		}
		files = append(files, f)
	}

	// synthesize the root of the drive tree
	rootNode := rootNode()
	fileById := make(map[string]*Node, len(files)+1)
	fileById[rootId] = &rootNode

	for _, f := range files {
		inode, err := db.InodeByID(f.Id)
		if err != nil {
			log.Printf("failed to lookup inode for %s: %v", f.Id, err)
			continue
		}
		node, err := nodeFromFile(f, inode)
		if err != nil {
			log.Printf("failed to interpret node %s: %v", f.Id, err)
			continue
		}
		if len(f.Parents) > 0 {
			if node.Parents == nil {
				node.Parents = make([]string, len(f.Parents))
			}
			for i := range f.Parents {
				node.Parents[i] = f.Parents[i].Id
			}
		} else {
			debug.Printf("%s has no parents, making it accessible at /.", node.Title)
			node.Parents = []string{rootId}
		}
		fileById[f.Id] = node
	}
	return fileById, nil
}

// updateFS polls the Changes cache for the list of files, and updates the fuse FS
func updateFS(db *drive_db.DriveDB, fs *FS) (Node, error) {
	start := make(chan int)
	go func() { start <- 1 }()
	http.HandleFunc("/refresh", func(w http.ResponseWriter, r *http.Request) {
		start <- 1
		fmt.Fprintf(w, "Refresh request accepted.")
	})
	timeToStart := time.Tick(*driveCacheRead)

	for {
		select {
		case <-timeToStart:
			go func() { start <- 1 }()

		case <-start:
			fileById, err := getNodes(db)
			if err != nil {
				log.Printf("error updating filesystem, getNodes: %s", err)
				continue
			}
			newRootNode, ok := fileById[rootId]
			if !ok {
				log.Printf("can not refresh tree: fileById[rootId] for (%v) not found", rootId)
				continue
			}

			var missingParents int
			dupes := make(map[string]*Node)
			for _, f := range fileById {
				missingParents = 0
				for _, pId := range f.Parents {
					parent, ok := fileById[pId]
					if !ok {
						missingParents++
						continue
					}
					if parent.Children == nil {
						parent.Children = make(map[string]*Node)
					}
					if conflict, ok := parent.Children[f.Title]; ok {
						dupes[f.Title] = parent
						conflictWithDocid := fmt.Sprintf("%s.%s", conflict.Title, conflict.Id)
						parent.Children[conflictWithDocid] = conflict
						fWithDocid := fmt.Sprintf("%s.%s", f.Title, f.Id)
						parent.Children[fWithDocid] = f
						debug.Printf("Found conflicting file (%s/%s), added additional dir entries: %s, %s", parent.Title, conflict.Title, conflictWithDocid, fWithDocid)
					} else {
						parent.Children[f.Title] = f
					}
				}
				if missingParents == len(f.Parents) && f.Id != rootId {
					log.Printf("Could not find any parents for '%s' in %v, placing at root.", f.Title, f.Parents)
					newRootNode.Children[f.Title] = f
				}
			}

			for conflict, parent := range dupes {
				delete(parent.Children, conflict)
			}

			debug.Printf("Refreshing fuse filesystem with new view: %d files\n", len(fileById))
			fs.root.Mu.Lock()
			fs.root.Children = newRootNode.Children
			debug.Printf("Updating root node to: %+v\n", fs.root)
			fs.root.Mtime = time.Now()
			fs.root.Mu.Unlock()
		}
	}
	return Node{}, fmt.Errorf("unexpectedly reached end of updateFS")
}
