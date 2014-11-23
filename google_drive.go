package main

import (
	"fmt"
	"net/http"
	"sync/atomic"

	"code.google.com/p/google-api-go-client/drive/v2"
)

var nextInode uint64 = 0

// AllFiles fetches and returns all files
// TODO(asjoyner): optimize later, request only what we end up using:
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
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


func getNodes(oauthClient *http.Client) (Node, map[string]*Node, string, error) {
	service, _ := drive.New(oauthClient)
	files, err := AllFiles(service)
	fmt.Println("Number of files in Drive: ", len(files))
	if err != nil {
		return Node{}, nil, "", fmt.Errorf("failed to list files in drive: ", err)
	}

	about, err := service.About.Get().Do()
	if err != nil {
		return Node{}, nil, "", fmt.Errorf("drive.service.About.Get.Do: %v\n", err)
	}
	rootId := about.RootFolderId
	account := about.User.EmailAddress

	// build a tree representation of nodes in a filesystem, for fuse
	fileById := make(map[string]*Node, len(files))
	// synthesize the root of the drive tree
	rootNode := Node{Children: make(map[string]*Node),
									 Inode: atomic.AddUint64(&nextInode, 1),
									 Title: "/",
									 isDir: true,
	}
	fileById[rootId] = &rootNode

	for _, f := range files {
		var isDir bool
		if f.MimeType == driveFolderMimeType {
			isDir = true
		}
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
	return rootNode, fileById, account, nil
}
