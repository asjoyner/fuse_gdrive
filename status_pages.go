package main

import (
	"fmt"
	"net/http"

	"code.google.com/p/google-api-go-client/drive/v2"
)

type FilesPage struct {
	files []*drive.File
}

func (t FilesPage) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	for _, f := range t.files {
		fmt.Fprintf(w, "%+v %+v\n  Parents: ", f.Title, f.Id)
		for _, p := range f.Parents {
			fmt.Fprintf(w, "%+v ", p.Id)
		}
		fmt.Fprintf(w, "\n")
	}
}

type TreePage struct {
	tree Node
}

func (n Node) PrintChildren(w http.ResponseWriter, depth int) {
	indent := ""
	for i := 0; i < depth; i++ {
		indent += "  "
	}
	fmt.Fprintf(w, "%+v\n", n.Title)
	for _, c := range n.Children {
		c.PrintChildren(w, depth+1)
	}
}

func (t TreePage) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	t.tree.PrintChildren(w, 0)
}

func RootHandler(w http.ResponseWriter, req *http.Request) {
	// The "/" pattern matches everything, so we need to check
	// that we're at the root here.
	if req.URL.Path != "/" {
		http.NotFound(w, req)
		return
	}
	fmt.Fprintln(w, "<html><body>Check out the <a href=/files>files</a> and <a href=/tree>tree</a> pages!")
}
