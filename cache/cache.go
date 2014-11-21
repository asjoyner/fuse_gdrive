// The cache package provides efficient small reads of large remote HTTP files.
package cache

import (
  "fmt"
  "io/ioutil"
  "log"
  "net/http"
  "sync"
  "time"

  "github.com/dustin/go-humanize"
)

var dc = DriveCache{
  path: "/tmp",
  client: nil,
  chunkSize: 10*1024*1024,
  maxChunks: 2,
  files: make(map[string]*File),
  req: make(chan int)
}

// Configure sets the cache dir and oauth client.
// It also starts a goroutine to fetch chunks, so it must be called before
// Read().
func Configure(path string, client *http.Client) {
  dc.path = path
  dc.client = client
  go Fetcher(req)
}

type DriveCache struct {
  path string
  client *http.Client
  chunkSize int64
  maxChunks int32
  files map[string]*File
}

func Fetcher(req chan int)

func Read(url string, offset int64, size int64, max int64) ([]byte, error) {
  f, ok := dc.files[url]
  if !ok {
    f = NewFile(url, max)
    dc.files[url] = f
  }
  b, err := f.Read(offset, size)
  return b, err
}

// Find the oldest timestamp and remove it
// TODO: This is racy, maybe sync DropOldest runs against the DriveCache struct?
func DropOldest() {
  var numEntries int32
  var oT = time.Now()
  var oF *File
  var oN int64
  for _, f := range dc.files {
    //f.RLock()
    //defer f.RUnlock()
    for n, t := range f.atime {
      numEntries++
      if t.Before(oT) {
        oT = t
        oF = f
        oN = n
      }
    }
  }
  //oF.Lock()
  //defer oF.Unlock()
  if numEntries >= dc.maxChunks {
    delete(oF.atime, oN)
    delete(oF.local, oN)
  } else if numEntries > dc.maxChunks {
    log.Printf("Too many chunks!", numEntries)
  }
}

type File struct {
  url string
  size int64
  local map[int64][]byte
  atime map[int64]time.Time
  sync.RWMutex
}

func NewFile(url string, size int64) *File {
  return &File{
    // Maps, because even a sparse slice would still use a lot more ram
    url: url,
    size: size,
    local: make(map[int64][]byte),
    atime: make(map[int64]time.Time),
  }
}

func (f *File) Read(offset int64, size int64) ([]byte, error) {
  if size > dc.chunkSize {
    log.Fatal("You're doing it wrong.")
  }
  n := offset / dc.chunkSize
  chunk, err := f.GetChunk(n)
  if err != nil {
    return nil, err
  }
  response := make([]byte, size)
  chunkOffset := offset % dc.chunkSize
  copied := copy(response, chunk[chunkOffset:])
  if n == (offset+size) / dc.chunkSize {  // read satisfied by this chunk
    /*
    if chunkOffset == 0 {
      log.Printf("Warming chunk %d into the cache.", n+1)
      go f.GetChunk(n+1) }
    */
    return response, nil
  }

  // ugh, the read extends into the next chunk
  chunk, err = f.GetChunk(n+1)
  if err != nil {
    return nil, fmt.Errorf("GetChunk: %v", err)
  }
  copy(response[copied:], chunk)
  return response, nil
}

func (f *File) GetChunk(n int64) ([]byte, error) {
  //f.Lock()
  f.atime[n] = time.Now()
  if chunk, ok := f.local[n]; ok {
    //f.Unlock()
    return chunk, nil
  }
  //f.Unlock()
  req, err := http.NewRequest("GET", f.url, nil)
  if err != nil {
    return nil, err
  }
  // See http://tools.ietf.org/html/rfc2616#section-14.35  (.1 and .2)
  // https://developers.google.com/drive/web/manage-downloads#partial_download
  cs := n * dc.chunkSize
  if cs > f.size {
    return nil, fmt.Errorf("chunk requested that starts past EOF (%v)", cs)
  }
  ce := cs+dc.chunkSize
  if ce > f.size {
    ce = f.size
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
  chunk, err := ioutil.ReadAll(resp.Body)
  log.Printf("Chunk %d transferred at %v", n, getRate(int64(len(chunk))))
  if err != nil {
    return nil, fmt.Errorf("ioutil.ReadAll: %v", err)
  }
  //f.Lock()
  f.local[n] = chunk
  //f.Unlock()
  go DropOldest()
  return chunk, nil
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
