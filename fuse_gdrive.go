// The fuse-gdrive command makes your Google Drive files accessible as a local mount point.
// It implements a user space filesystem, using the Fuse and Google Drive APIs,
// to allow you to access your files in Google Drive just like a regular local
// filesystem.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"

  drive "code.google.com/p/google-api-go-client/drive/v2"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"
)

var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s MOUNTPOINT\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	flag.Usage = Usage
	flag.Parse()

	if flag.NArg() != 1 {
		Usage()
		os.Exit(2)
	}
	mountpoint := flag.Arg(0)

  client := getOAuthClient(drive.DriveReadonlyScope)
  service, _ := drive.New(client)
  files, err := AllFiles(service)
  if err != nil {
    log.Fatal("failed to list files in drive: ", err)
  }

  for f := range files {
    fmt.Println(f)
  }

	c, err := fuse.Mount(
		mountpoint,
		fuse.FSName("GoogleDrive"),
		fuse.Subtype("gdrive"),
		fuse.LocalVolume(),
		fuse.VolumeName("TODO: insert account name here."),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	err = fs.Serve(c, FS{})
	if err != nil {
		log.Fatal(err)
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		log.Fatal(err)
	}
}

// FS implements the hello world file system.
type FS struct{}

func (FS) Root() (fs.Node, fuse.Error) {
	return Dir{}, nil
}

// Dir implements both Node and Handle for the root directory.
type Dir struct{}

func (Dir) Attr() fuse.Attr {
	return fuse.Attr{Inode: 1, Mode: os.ModeDir | 0555}
}

func (Dir) Lookup(name string, intr fs.Intr) (fs.Node, fuse.Error) {
	if name == "hello" {
		return File{}, nil
	}
	return nil, fuse.ENOENT
}

var dirDirs = []fuse.Dirent{
	{Inode: 2, Name: "hello", Type: fuse.DT_File},
}

func (Dir) ReadDir(intr fs.Intr) ([]fuse.Dirent, fuse.Error) {
	return dirDirs, nil
}

// File implements both Node and Handle for the hello file.
type File struct{}

const greeting = "hello, world\n"

func (File) Attr() fuse.Attr {
	return fuse.Attr{Inode: 2, Mode: 0444, Size: uint64(len(greeting))}
}

func (File) ReadAll(intr fs.Intr) ([]byte, fuse.Error) {
	return []byte(greeting), nil
}
