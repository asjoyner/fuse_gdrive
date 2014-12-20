// The cache package provides efficient small reads of large remote HTTP files.
package cache

// TODO: Exponentially increasing read-ahead, up to limit

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/asjoyner/fuse_gdrive/lru"
	"github.com/dustin/go-humanize"
)

var chunkSize = flag.Int64("chunksize", 20*1024*1024, "Size of each chunk read from Google Drive.")
var numChunks = flag.Int64("numchunks", 2, "The number of chunks to keep in ram at a time.")
var enablePrefetch = flag.Bool("prefetch", false, "If true, prefetch chunks on read")

type Reader interface {
	Read(url string, offset int64, size int64, max int64) ([]byte, error)
}

type chunk struct {
	url   string
	n     int64
	start int64
	end   int64
}

type readRequest struct {
	c     chunk
	data  []byte
	err   error
	ready chan struct{}
}

type driveCache struct {
	path      string
	client    *http.Client
	chunkSize int64
	maxChunks int32
	lru       *lru.Cache
	sync.RWMutex
	pending map[chunk]*readRequest
}

func NewCache(path string, client *http.Client) Reader {
	return &driveCache{
		path:      path,
		client:    client,
		chunkSize: *chunkSize,
		lru:       lru.New(int(*numChunks)),
		pending:   make(map[chunk]*readRequest),
	}
}

func (d *driveCache) Read(url string, offset int64, size int64, max int64) ([]byte, error) {
	if offset > max {
		return nil, fmt.Errorf("%v: read past EOF : (offset: %v size: %v max: %v)", url, offset, size, max)
	}
	var eof bool
	if (offset + size) > max {
		eof = true
		size = max - offset
	}
	// figure out which chunks we need in order to satisfy the read
	startChunk := offset / d.chunkSize
	endChunk := (offset + size) / d.chunkSize
	var chunks []chunk
	for n := startChunk; n <= endChunk; n++ {
		chunks = append(chunks, d.chunkN(url, n, max))
	}
	var retBytes []byte
	// optimization: if we only need a single chunk (likely), then we can avoid allocating
	// retBytes at all, and just use the slice returned by readChunk
	if len(chunks) == 1 {
		r, err := d.readChunk(chunks[0])
		if err != nil {
			return nil, err
		}
		retBytes = r
	} else {
		retBytes = make([]byte, 0, int64(len(chunks))*d.chunkSize)
		for _, chunk := range chunks {
			r, err := d.readChunk(chunk)
			if err != nil {
				return nil, err
			}
			retBytes = append(retBytes, r...)
		}
	}
	// return the slice of the read chunks corresponding to the original read.
	retStart := offset % d.chunkSize
	retEnd := retStart + size
	// optionally issue a prefetch for the next chunk if prefetching is enabled and
	// the read ended less than 2x|size| bytes from the end of the last chunk
	if *enablePrefetch && (int64(len(retBytes))-retEnd) <= (2*size) {
		nextChunk := d.chunkN(url, endChunk+1, max)
		if nextChunk.start < max {
			log.Printf("prefetching chunk: %s(%d)", nextChunk.url, nextChunk.n)
			go d.readChunk(nextChunk)
		}
	}
	var err error
	if eof {
		err = io.EOF
	}
	return retBytes[retStart:retEnd], err
}

func (d *driveCache) chunkN(url string, n int64, max int64) chunk {
	s := n * d.chunkSize
	if s > max {
		s = max
	}
	e := s + d.chunkSize
	if e > max {
		e = max
	}
	return chunk{url: url, n: n, start: s, end: e}
}

func (d *driveCache) readChunk(c chunk) ([]byte, error) {
	// check the cache.
	d.Lock()
	cacheBytes, ok := d.lru.Get(c)
	if ok {
		// cache hit
		d.Unlock()
		return cacheBytes.([]byte), nil
	}
	// not in cache, possibly pending
	req, ok := d.pending[c]
	if !ok {
		// no request pending, create one
		req = &readRequest{c: c, ready: make(chan struct{})}
		d.pending[c] = req
		go func() {
			req.data, req.err = d.getChunk(req.c)
			d.Lock()
			// remove ourselves from the pending list.
			delete(d.pending, req.c)
			// if we got data, add it to the LRU for future requests.
			if req.err == nil {
				d.lru.Add(req.c, req.data)
			}
			d.Unlock()
			// notify waiters
			close(req.ready)
		}()
	}
	d.Unlock()
	<-req.ready
	return req.data, req.err
}

func (d *driveCache) getChunk(c chunk) ([]byte, error) {
	req, err := http.NewRequest("GET", c.url, nil)
	if err != nil {
		return nil, err
	}
	// See http://tools.ietf.org/html/rfc2616#section-14.35  (.1 and .2)
	// https://developers.google.com/drive/web/manage-downloads#partial_download
	spec := fmt.Sprintf("bytes=%d-%d", c.start, c.end)
	req.Header.Add("Range", spec)

	getRate := MeasureTransferRate()
	resp, err := d.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("client.Do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 206 && resp.StatusCode != 200 {
		err := fmt.Errorf("for %s got HTTP status %v, want 206 or 200: %v", spec, resp.StatusCode, resp.Status)
		return nil, err
	}
	chunkBytes, err := ioutil.ReadAll(resp.Body)
	log.Printf("Chunk %d transferred at %v", c.n, getRate(int64(len(chunkBytes))))
	if err != nil {
		return nil, fmt.Errorf("ioutil.ReadAll: %v", err)
	}
	return chunkBytes, nil
}

// Credit to github.com/prasmussen/gdrive/cli for inspiration
func MeasureTransferRate() func(int64) string {
	start := time.Now()

	return func(bytes int64) string {
		seconds := time.Now().Sub(start).Seconds()
		kbps := int64((float64(bytes) / seconds) / 1024)
		return fmt.Sprintf("%s KB/s", humanize.Comma(kbps))
	}
}
