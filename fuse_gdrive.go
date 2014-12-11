// The fuse-gdrive command makes your Google Drive files accessible as a local mount point.
// It implements a user space filesystem, using the Fuse and Google Drive APIs,
// to allow you to access your files in Google Drive just like a regular local
// filesystem.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"os/user"
	"path"
	"runtime"
	"strconv"
	"time"

	drive "code.google.com/p/google-api-go-client/drive/v2"

	"bazil.org/fuse"

	"github.com/asjoyner/fuse_gdrive/cache"
	"github.com/asjoyner/fuse_gdrive/drive_db"
)

var port = flag.String("port", "12345", "HTTP Server port; your browser will send credentials here.  Must be accessible to your browser, and authorized in the developer console.")
var readOnly = flag.Bool("readonly", false, "Mount the filesystem read only.")
var allowOther = flag.Bool("allow_other", false, "If other users are allowed to view the mounted filesystem.")
var debugGdrive = flag.Bool("gdrive.debug", false, "print debug statements from the fuse_gdrive package")
var driveMetadataLatency = flag.Duration("metadatapoll", time.Minute, "How often to poll Google Drive for metadata updates")

var startup = time.Now()

var debug debugging

type debugging bool

func (d debugging) Printf(format string, args ...interface{}) {
	if d {
		log.Printf(format, args...)
	}
}

var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s MOUNTPOINT\n", os.Args[0])
	flag.PrintDefaults()
}

func sanityCheck(mountpoint string) error {
	fileInfo, err := os.Stat(mountpoint)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(mountpoint, 0777); err != nil {
			return fmt.Errorf("mountpoint does not exist, could not create it.")
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("error stat()ing mountpoint: %s", err)
	}
	if !fileInfo.IsDir() {
		return fmt.Errorf("the mountpoint is not a directory")
	}
	return nil
}

func main() {
	runtime.SetBlockProfileRate(1)

	flag.Usage = Usage
	flag.Parse()

	if flag.NArg() != 1 {
		Usage()
		os.Exit(2)
	}
	mountpoint := flag.Arg(0)

	if *debugGdrive {
		debug = true
	}

	userCurrent, err := user.Current()
	if err != nil {
		log.Fatalf("unable to get UID/GID of current user: %v", err)
	}
	uidInt, err := strconv.Atoi(userCurrent.Uid)
	if err != nil {
		log.Fatalf("unable to get UID/GID of current user: %v", err)
	}
	uid := uint32(uidInt)
	gidInt, err := strconv.Atoi(userCurrent.Gid)
	if err != nil {
		log.Fatalf("unable to get UID/GID of current user: %v", err)
	}
	gid := uint32(gidInt)

	if err = sanityCheck(mountpoint); err != nil {
		log.Fatalf("sanityCheck failed: %s\n", err)
	}

	http.HandleFunc("/", RootHandler)
	go http.ListenAndServe(fmt.Sprintf("localhost:%s", *port), nil)

	var client *http.Client
	if *readOnly {
		client = getOAuthClient(drive.DriveReadonlyScope)
	} else {
		client = getOAuthClient(drive.DriveScope)
	}

	driveCache := cache.NewCache("/tmp", client)

	// TODO: retries
	service, _ := drive.New(client)
	about, err := service.About.Get().Do()
	if err != nil {
		log.Fatalf("drive.service.About.Get().Do: %v\n", err)
	}

	// Create and start the drive metadata syncer.
	dbpath := path.Join(os.TempDir(), "fuse-gdrive", about.User.EmailAddress)
	log.Printf("using drivedb: %v", dbpath)
	db, err := drive_db.NewDriveDB(service, dbpath, *driveMetadataLatency)
	if err != nil {
		log.Fatalf("could not open leveldb: %v", err)
	}
	defer db.Close()
	db.WaitUntilSynced()
	log.Printf("synced!")

	// fileId of the root of the FS (aka "My Drive")
	rootId := about.RootFolderId
	// email address of the mounted google drive account
	account := about.User.EmailAddress

	options := []fuse.MountOption{
		fuse.FSName("GoogleDrive"),
		fuse.Subtype("gdrive"),
		fuse.VolumeName(account),
	}

	if *allowOther {
		options = append(options, fuse.AllowOther())
	}
	/* TODO: uncomment when upstream fuse.ReadOnly is accepted
	if *readOnly {
		options = append(options, fuse.ReadOnly())
	}
	*/
	c, err := fuse.Mount(mountpoint, options...)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	// Trap control-c (sig INT) and unmount
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	go func() {
		for _ = range sig {
			if err := fuse.Unmount(mountpoint); err != nil {
				log.Printf("fuse.Unmount failed: %v", err)
			}
		}
	}()

	sc := serveConn{db: db,
		driveCache: driveCache,
		service:    service,
		//handles:    make(map[fuse.HandleID]string),
		launch: time.Unix(1335225600, 0),
		uid:    uid,
		gid:    gid,
		rootId: rootId,
		conn:   c,
	}
	err = sc.Serve()
	if err != nil {
		log.Fatalln("fuse server failed: ", err)
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		log.Fatal(err)
	}
}
