package main

import (
	"flag"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"code.google.com/p/google-api-go-client/drive/v2"
)

var driveRefresh = flag.Duration("refresh", 5 * time.Minute, "how often to refresh the list of files and directories from Google Drive.")

// The root of the tree is always one, we increment from there.
var nextInode uint64 = 1

// AllFiles fetches and returns all files in Google Drive
func AllFiles(d *drive.Service) ([]*drive.File, error) {
	var fs []*drive.File
	pageToken := ""
	for {
		q := d.Files.List()
		// If we have a pageToken set, apply it to the query
		if pageToken != "" {
			q = q.PageToken(pageToken)
		}
		r, err := q.Do()
		if err != nil {
			fmt.Printf("An error occurred: %v\n", err)
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

// getNodes returns a map of unique IDs to the Node it describes
func getNodes(service *drive.Service) (map[string]*Node, error) {
	files, err := AllFiles(service)
	if err != nil {
		return nil, fmt.Errorf("failed to list files in drive: ", err)
	}

	// synthesize the root of the drive tree
	rootNode := Node{Children: make(map[string]*Node),
									 Inode: 1,  // The root of the tree is always 1
									 Title: "/",
									 isDir: true,
									 isRoot: true,
	}

	fileById := make(map[string]*Node, len(files))
	fileById[rootId] = &rootNode

	for _, f := range files {
		var isDir bool
		if f.MimeType == driveFolderMimeType {
			isDir = true
		}
		// TODO: reuse inodes; don't generate a whole new set every getNodes
		node := &Node{Id: f.Id,
									Inode: atomic.AddUint64(&nextInode, 1),
									Title: f.Title,
									isDir: isDir,
									FileSize: f.FileSize,
									DownloadUrl: f.DownloadUrl,
		}
		if len(f.Parents) > 0 {
			node.Parents = make([]string, len(f.Parents))
			for i, p := range f.Parents {
				node.Parents[i] = p.Id
			}
		}
		fileById[f.Id] = node
	}
	return fileById, nil
}

// updateFS polls Google Drive for the list of files, and updates the fuse FS
func updateFS(service *drive.Service, fs FS) (Node, error) {
	// TODO: setup a http.Handler to allow forcing a refresh
	for {
		fileById, err := getNodes(service)
		if err != nil {
			log.Printf("failed to get Nodes from Drive: %s", err)
		}
		newRootNode, ok := fileById[rootId]
		if !ok {
			log.Printf("can not refresh tree: fileById[rootId] for (%v) not found", rootId)
			continue
		}
		for _, f := range fileById {
			for _, pId := range f.Parents {
				parent, ok := fileById[pId]
				if !ok {
					log.Printf("parent of %s not found, expected %s", f.Title, pId)
					newRootNode.Children[f.Title] = f
					continue
				}
				if parent.Children == nil {
					parent.Children = make(map[string]*Node)
				}
				parent.Children[f.Title] = f
			}
		}
		fmt.Printf("Refreshing fuse filesystem with new view: %d files\n", len(fileById))
		childLock.Lock()
		rootChildren = make(map[string]*Node, len(newRootNode.Children))
		for _, c := range newRootNode.Children {
			rootChildren[c.Title] = c
		}
		childLock.Unlock()
		time.Sleep(*driveRefresh)
	}
}
