fuse\_gdrive
==============

A Fuse filesystem using the Google Drive API v2 in Go.

For the impatient
-----------------
  1. install the fuse library for your operating system:
    * debian/ubuntu: apt-get install fuse
    * osxfuse: [download](https://osxfuse.github.io/) and install
    * freebsd: cd /usr/ports/sysutils/fusefs-libs/ && make install
    * from src: [http://fuse.sourceforge.net/](http://fuse.sourceforge.net/)
  2. [install go](http://golang.org/dl), at least version 1.3, for your OS
  3. Ensure you [set $GOPATH](https://golang.org/doc/code.html#GOPATH) (~/go
     will work fine), then add $GOPATH/bin to your traditional $PATH
     environment variable.
  4. use go to fetch and install this code and its dependencies:

    $ go install github.com/asjoyner/fuse_gdrive

  5. make a mount point on your system

    $ mkdir /mnt/gdrive

  6. request to mount the filesystem

    $ fuse_gdrive /mnt/gdrive

  7. A browser window will open for you to grant permission to your files in
     Drive.
  8. Once you accept, the client will sync locally the metadata about your
     files in drive, and make those files accessible at the mount point you
     specified.

But that didn't work?
---------------------
The steps above assume that you're running the fuse client on the same computer
the web browser is running on.  If you get an error about failing to launch a
web browser, and have to copy/paste the URL by hand, then you'll also have
to ensure when your web browser attempts to HTTP post to http://localhost:12345 that
it gets connected to the fuse\_gdrive client.  Typically, you want a command like
this:

    $ ssh -L12345:localhost:12345 <remote_host>
