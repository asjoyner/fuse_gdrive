// The cache package provides efficient small reads of large remote HTTP files.
package cache

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/golang/groupcache/lru"
)

var chunkSize = flag.Int64("chunksize", 20*1024*1024, "Size of each chunk read from Google Drive.")
var numChunks = flag.Int64("numchunks", 2, "The number of chunks to keep in ram at a time.")

var dc = DriveCache{
	path:      "/tmp",
	client:    nil,
	chunkSize: *chunkSize,
	lru:       lru.New(2),
	request:   make(chan *fetchRequest),
}

// Configure sets the cache dir and oauth client.
// It starts the goroutine to fetch chunks; it must be called before Read().
func Configure(path string, client *http.Client) {
	dc.path = path
	dc.client = client
	go Fetcher(dc.request)
}

type DriveCache struct {
	path      string
	client    *http.Client
	chunkSize int64
	maxChunks int32
	lru       *lru.Cache
	request   chan *fetchRequest
	sync.RWMutex
}

type chunk struct {
	url  string
	size int64
	max  int64
	n    int64
}

type fetchRequest struct {
	c          chunk
	find       sync.WaitGroup
	fill       sync.WaitGroup
	chunkBytes *[]byte
	err        error
}

func Fetcher(in chan *fetchRequest) {
	var queueLock sync.RWMutex
	queue := make(map[chunk]*fetchRequest)
	for {
		fr := <-in
		queueLock.RLock()
		inProgressFr, ok := queue[fr.c]
		queueLock.RUnlock()
		if ok {
			// fr.find needs to be separate, so we can copy fr.fill in place before
			// the caller fr.fill.Wait()s
			fr.fill = inProgressFr.fill
			fr.find.Done()
			continue
		}
		fr.fill.Add(1)
		queueLock.Lock()
		queue[fr.c] = fr
		queueLock.Unlock()
		go func() {
			chunkBytes, err := getChunk(fr.c)
			if err != nil {
				fr.err = fmt.Errorf("getChunk(%+v) failed: %v", fr.c, err)
			} else {
				dc.Lock()
				dc.lru.Add(fr.c, chunkBytes)
				dc.Unlock()

				queueLock.Lock()
				delete(queue, fr.c)
				queueLock.Unlock()
			}
			fr.find.Done()
			fr.fill.Done()
		}()
	}
}

func Read(url string, offset int64, size int64, max int64) ([]byte, error) {
	var copied int = 0
	var chunkBytes []byte
	n := offset / dc.chunkSize
	response := make([]byte, size)
	for {
		c := chunk{url: url, size: size, max: max, n: n} // uniquely identify the chunk
		dc.RLock()
		cb, ok := dc.lru.Get(c) // look in cache
		dc.RUnlock()
		if ok {
			cb, ok := cb.([]byte)
			if ok {
				chunkBytes = cb
			} else {
				return nil, fmt.Errorf("cache error, expected []byte, got %v", cb)
			}
		} else {
			fr := fetchRequest{c: c}
			fr.find.Add(1)
			dc.request <- &fr // request cache fill
			fr.find.Wait()    // block on cache lookup
			fr.fill.Wait()    // block on cache fill completion
			if fr.err != nil {
				return nil, fr.err
			}
			dc.RLock()
			cb, ok := dc.lru.Get(c) // get from cache, now that it's filled
			dc.RUnlock()
			if ok {
				cb, ok := cb.([]byte)
				if ok {
					chunkBytes = cb
				} else {
					return nil, fmt.Errorf("cache error, expected []byte, got %v", cb)
				}
			} else {
				return nil, fmt.Errorf("Cache miss immediately after fill: %s", url)
			}
		}
		chunkOffset := (offset + int64(copied)) % dc.chunkSize
		copied += copy(response[copied:], chunkBytes[chunkOffset:])

		if n == (offset+size)/dc.chunkSize { // read satisfied by this chunk
			return response, nil
		} else { // ugh, the read extends into the next chunk
			n++
		}
	}

}

func getChunk(c chunk) ([]byte, error) {
	req, err := http.NewRequest("GET", c.url, nil)
	if err != nil {
		return nil, err
	}
	// See http://tools.ietf.org/html/rfc2616#section-14.35  (.1 and .2)
	// https://developers.google.com/drive/web/manage-downloads#partial_download
	cs := c.n * dc.chunkSize
	if cs > c.max {
		return nil, fmt.Errorf("chunk requested that starts past EOF (%v)", cs)
	}
	ce := cs + dc.chunkSize
	if ce > c.max {
		ce = c.max
	}
	spec := fmt.Sprintf("bytes=%d-%d", cs, ce)
	req.Header.Add("Range", spec)

	getRate := MeasureTransferRate()
	resp, err := dc.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("client.Do: %v", err)
	}
	if resp.StatusCode != 206 && resp.StatusCode != 200 {
		return nil, fmt.Errorf("Failed to retrieve file, got HTTP status %v, want 206 or 200, asked for: %v", resp.StatusCode, spec)
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
