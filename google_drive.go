package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"code.google.com/p/google-api-go-client/drive/v2"
)

var driveFullRefresh = flag.Duration("forcerefresh", time.Hour, "how often to force a full refresh of the list of files and directories from Google Drive.")
var driveCheckChanges = flag.Duration("checkchanges", time.Minute, "how often to do a quick check to see if it's necessary to refresh the list of files and directories from Google Drive.")
var query = flag.String("query", "trashed=false", "Search parameters to pass to Google Drive, which limit the files mounted.  See http://goo.gl/6kSu3E")
var maxFilesList = flag.Int64("maxfileslist", 1000, "Maximum number of files to query Google Drive for metata at a time.  Range: 1-1000")

// The root of the tree is always one, we increment from there.
var nextInode uint64 = 1

// AllFiles fetches and returns all files in Google Drive
func AllFiles(d *drive.Service) ([]*drive.File, error) {
	var fs []*drive.File
	pageToken := ""
	for {
		list := d.Files.List()
		list.MaxResults(1000)

		if len(*query) > 0 {
			list = list.Q(*query)
		}

		// Limit the data we get back from the API to what we use
		list = list.Fields("nextPageToken", "items(createdDate,downloadUrl,fileSize,id,lastViewedByMeDate,mimeType,modifiedDate,parents/id,title)")

		// If we have a pageToken set, apply it to the query
		if pageToken != "" {
			list = list.PageToken(pageToken)
		}
		querystart := time.Now()
		r, err := list.Do()
		querytime := time.Now().Sub(querystart).Nanoseconds() / 1e6
		debug.Printf("Files.List() of %d items took %dms", len(r.Items), querytime)
		if err != nil {
			return fs, err
		}
		fs = append(fs, r.Items...)
		pageToken = r.NextPageToken
		if pageToken == "" {
			break
		}
	}
	return fs, nil
}

// the root node starts out as a single empty node
func rootNode() Node {
	return Node{Id: rootId,
		Children: make(map[string]*Node),
		Inode:    1, // The root of the tree is always 1
		Title:    "/",
		isDir:    true,
		isRoot:   true,
		Atime:    time.Unix(1335225600, 0),
		Mtime:    time.Unix(1335225600, 0),
		Ctime:    time.Unix(1335225600, 0),
	}
}

// TODO: reuse inodes; don't generate a whole new set every getNodes
func nodeFromFile(f *drive.File) (*Node, error) {
	var isDir bool
	if f.MimeType == driveFolderMimeType {
		isDir = true
	}
	/* TODO: Figure out time vs. JSON
	var atime, ctime, mtime time.Time
	if err := ctime.UnmarshalJSON(f.CreatedDate); err != nil {
	  return &Node{}, fmt.Errorf("CreatedDate.UnmarshalJSON: %v", err)
	}
	if len(cbytes) > 0 {
	  if err := atime.UnmarshalJSON([]byte(f.LastViewedByMeDate)); err != nil {
	    return &Node{}, fmt.Errorf("LastViewedByMeDate.UnmarshalJSON: %v", err)
	  }
	} else {
	  atime = ctime
	}
	if len(cbytes) > 0 {
	  if err := mtime.UnmarshalJSON([]byte(f.ModifiedDate)); err != nil {
	    return &Node{}, fmt.Errorf("ModifiedDate.UnmarshalJSON: %v", err)
	  }
	} else {
	  mtime = ctime
	}
	*/
	node := &Node{Id: f.Id,
		Inode:       atomic.AddUint64(&nextInode, 1),
		Title:       f.Title,
		isDir:       isDir,
		FileSize:    f.FileSize,
		DownloadUrl: f.DownloadUrl,
		/* TODO: Figure out time vs JSON
		   Atime:       atime,
		   Mtime:       mtime,
		   Ctime:       ctime,
		*/
	}
	return node, nil
}

// getNodes returns a map of unique IDs to the Node it describes
func getNodes(service *drive.Service) (map[string]*Node, error) {
	files, err := AllFiles(service)
	if err != nil {
		return nil, fmt.Errorf("failed to list files in drive: %v", err)
	}

	// synthesize the root of the drive tree
	rootNode := rootNode()
	fileById := make(map[string]*Node, len(files))
	fileById[rootId] = &rootNode

	for _, f := range files {
		node, err := nodeFromFile(f)
		if err != nil {
			log.Printf("Failed to interpret node \"%s\": %v", f.Title, err)
			continue
		}
		if len(f.Parents) > 0 {
			node.Parents = make([]string, len(f.Parents))
			for i := range f.Parents {
				node.Parents[i] = f.Parents[i].Id
			}
		} else {
			node.Parents = []string{rootId}
		}
		fileById[f.Id] = node
	}
	return fileById, nil
}

// updateFS polls Google Drive for the list of files, and updates the fuse FS
func updateFS(service *drive.Service, fs FS) (Node, error) {
	peek := make(chan int)
	start := make(chan int)
	go func() { peek <- 1 }() // first peek always triggers start
	http.HandleFunc("/refresh", func(w http.ResponseWriter, r *http.Request) {
		start <- 1
		fmt.Fprintf(w, "Refresh request accepted.")
	})
	http.HandleFunc("/check", func(w http.ResponseWriter, r *http.Request) {
		peek <- 1
		fmt.Fprintf(w, "Checking Drive for updates since last poll.")
	})
	timeToStart := time.Tick(*driveFullRefresh)
	timeToPeek := time.Tick(*driveCheckChanges)

	// TODO: Investigate also supporting the Watch service, eventually, but it
	// requires a valid SSL cert, and lots of "registration" hoops:
	// https://developers.google.com/drive/web/push
	var currentLargestID, lastLargestID int64
	l := service.Changes.List()
	l.IncludeDeleted(true).IncludeSubscribed(true).MaxResults(1)

	for {
		select {
		case <-timeToStart:
			go func() { start <- 1 }()
		case <-timeToPeek:
			go func() { peek <- 1 }()
		case <-peek:
			querystart := time.Now()
			c, err := l.Do()
			querytime := time.Now().Sub(querystart).Nanoseconds() / 1e6
			if err != nil {
				log.Printf("failed calling Changes.List(): %v", err)
				continue
			}
			currentLargestID = c.LargestChangeId
			if currentLargestID > lastLargestID {
				lastLargestID = currentLargestID
				go func() { start <- 1 }()
			} else {
				debug.Printf("No updates since last check. (took %dms)", querytime)
			}
		case <-start:
			fileById, err := getNodes(service)
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
				if missingParents == len(f.Parents) && !f.isRoot {
					log.Printf("Could not find any parents for '%s' in %v, placing at root.", f.Title, f.Parents)
					newRootNode.Children[f.Title] = f
				}
			}

			for conflict, parent := range dupes {
				delete(parent.Children, conflict)
			}

			log.Printf("Refreshing fuse filesystem with new view: %d files\n", len(fileById))
			fs.root.Mu.Lock()
			fs.root.Children = newRootNode.Children
			fs.root.Mu.Unlock()
		}
	}
	return Node{}, fmt.Errorf("unexpectedly reached end of updateFS")
}
