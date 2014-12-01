// drive_sync syncs Google Drive metadata to a local LevelDB database and provides methods to query it.

package drive_db

import (
	"bytes"
	"encoding/json"
	"log"
	"sync"
	"time"

	gdrive "code.google.com/p/google-api-go-client/drive/v2"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type DriveDB struct {
	service *gdrive.Service
	db      *leveldb.DB
	syncmu  sync.Mutex
	synced  *sync.Cond
	iters   sync.WaitGroup
}

// NewDriveDB creates a new DriveDB and starts syncing.
func NewDriveDB(svc *gdrive.Service, filepath string) (*DriveDB, error) {
	o := &opt.Options{
		Filter: filter.NewBloomFilter(10),
		Strict: opt.StrictAll,
	}
	db, err := leveldb.OpenFile(filepath, o)
	if err != nil {
		if _, ok := err.(*errors.ErrCorrupted); ok {
			log.Printf("recovering leveldb: %v", err)
			db, err = leveldb.RecoverFile(filepath, o)
			if err != nil {
				log.Printf("failed to recover leveldb: %v", err)
				return nil, err
			}
		} else {
			log.Printf("failed to open leveldb: %v", err)
			return nil, err
		}
	}

	d := &DriveDB{
		service: svc,
		db:      db,
	}
	d.synced = sync.NewCond(&d.syncmu)

	go d.sync()
	return d, nil
}

type CheckPoint struct {
	LastChangeID int64
	LastInode    int64
}

// AllFileIDs returns the IDs of all Google Drive file objects currently stored.
func (d *DriveDB) AllFileIDs() ([]string, error) {
	var ids []string
	// We can't Close() until all iterators are released.
	// TODO: this can still be racy with Close(), fix that.
	d.iters.Add(1)
	iter := d.db.NewIterator(util.BytesPrefix(fileKey("")), nil)
	for iter.Next() {
		ids = append(ids, deKey(string(iter.Key())))
	}
	iter.Release()
	d.iters.Done()
	return ids, iter.Error()
}

// RootFileIDs returns the IDs of all Google Drive file objects at the root.
func (d *DriveDB) RootFileIDs() ([]string, error) {
	var ids []string
	d.iters.Add(1)
	iter := d.db.NewIterator(util.BytesPrefix(rootKey("")), nil)
	for iter.Next() {
		ids = append(ids, deKey(string(iter.Key())))
	}
	iter.Release()
	d.iters.Done()
	return ids, iter.Error()
}

// ChildFileIDs returns the IDs of all Files that have parent refs to the given file.
func (d *DriveDB) ChildFileIDs(fileID string) ([]string, error) {
	var ids []string
	d.iters.Add(1)
	batch := new(leveldb.Batch)
	iter := d.db.NewIterator(util.BytesPrefix(childKey(fileID)), nil)
	for iter.Next() {
		pidcid := deKey(string(iter.Key()))
		cid := pidcid[len(fileID)+1:]
		found, err := d.db.Has(fileKey(cid), nil)
		if err == nil && found {
			ids = append(ids, cid)
		} else {
			batch.Delete(iter.Key())
		}
	}
	iter.Release()
	d.iters.Done()
	err := d.db.Write(batch, nil)
	if err != nil {
		log.Printf("error writing to db: %v", err)
	}
	return ids, iter.Error()
}

// FileByID returns a File, given its ID.
func (d *DriveDB) FileByID(fileID string) (*gdrive.File, error) {
	var res gdrive.File
	err := d.get(fileKey(fileID), &res)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

// InodeByID returns a File's inode number, given its ID.
func (d *DriveDB) InodeByID(fileID string) (int64, error) {
	var inode int64
	ik := inodeKey(fileID)
	err := d.get(ik, &inode)
	if err != nil {
		return -1, err
	}
	return inode, nil
}

func (d *DriveDB) get(key []byte, item interface{}) error {
	data, err := d.db.Get(key, nil)
	if err != nil {
		return err
	}
	buf := bytes.NewBuffer(data)
	dec := json.NewDecoder(buf)
	return dec.Decode(item)
}

func internalKey(key string) []byte {
	return []byte("int:" + key)
}

func inodeKey(key string) []byte {
	return []byte("ind:" + key)
}

func fileKey(key string) []byte {
	return []byte("fid:" + key)
}

func childKey(key string) []byte {
	return []byte("kid:" + key)
}

func rootKey(key string) []byte {
	return []byte("rtf:" + key)
}

func pathKey(key string) []byte {
	return []byte("pth:" + key)
}

func deKey(key string) string {
	return key[4:]
}

// sync is a background goroutine to sync drive data.
func (d *DriveDB) sync() {
	log.Printf("starting drive sync")

	var (
		cpt          CheckPoint
		lastChangeID int64
		buf          bytes.Buffer
	)
	enc := json.NewEncoder(&buf)

	// Get saved checkpoint.
	err := d.get(internalKey("checkpoint"), &cpt)
	if err != nil {
		log.Printf("error reading checkpoint: %v", err)
	}

	l := d.service.Changes.List().IncludeDeleted(true).IncludeSubscribed(true).MaxResults(1000)
	if cpt.LastChangeID > 0 {
		log.Printf("resuming sync from %v", cpt.LastChangeID)
		l.StartChangeId(cpt.LastChangeID + 1)
	} else {
		log.Printf("starting sync from scratch")
	}

	for {
		c, err := l.Do()
		if err != nil {
			log.Printf("sync error: %v", err)
			d.pollSleep()
			continue
		}

		// Already synced
		if cpt.LastChangeID >= c.LargestChangeId {
			d.synced.Broadcast()
			d.pollSleep()
			continue
		}

		if len(c.Items) == 0 {
			d.synced.Broadcast()
			d.pollSleep()
			continue
		}

		batch := new(leveldb.Batch)

		for _, i := range c.Items {
			lastChangeID = i.Id
			fileId := i.FileId
			fkey := fileKey(fileId)
			ikey := inodeKey(fileId)
			ckey := childKey(fileId)

			// Delete file
			if i.Deleted || i.File.Labels.Trashed || i.File.Labels.Hidden {
				batch.Delete(fkey)
				batch.Delete(ikey)
				// delete any "root object" ref
				batch.Delete(rootKey(fileId))
				// also delete all of its child refs
				d.iters.Add(1)
				iter := d.db.NewIterator(util.BytesPrefix(ckey), nil)
				for iter.Next() {
					batch.Delete(iter.Key())
				}
				iter.Release()
				d.iters.Done()
				continue
			}

			// Add/Update file
			buf.Reset()
			err := enc.Encode(i.File)
			if err != nil {
				log.Printf("error encoding file %v: %v", fileId, err)
				continue
			}
			batch.Put(fkey, buf.Bytes())

			// Check for, and allocate an inode number if needed.
			found, err := d.db.Has(ikey, nil)
			if err == nil && found {
				continue
			}
			cpt.LastInode++
			buf.Reset()
			err = enc.Encode(cpt.LastInode)
			if err != nil {
				log.Printf("error encoding inode %v for %v: %v", cpt.LastInode, fileId, err)
				continue
			}
			batch.Put(ikey, buf.Bytes())

			// Child references
			for _, pr := range i.File.Parents {
				if pr.IsRoot {
					batch.Put(rootKey(fileId), []byte{}) // we care only about the key
				} else {
					batch.Put(childKey(pr.Id+":"+fileId), []byte{}) // we care only about the key
				}
			}
		}

		cpt.LastChangeID = lastChangeID
		log.Printf("%d changes; new checkpoint (change, inode): %v", len(c.Items), cpt)
		buf.Reset()
		err = enc.Encode(cpt)
		if err != nil {
			// TODO: figure out how to recover from the error.
			log.Printf("error encoding checkpoint: %v", err)
			batch.Reset()
			d.pollSleep()
			continue
		}
		batch.Put(internalKey("checkpoint"), buf.Bytes())

		err = d.db.Write(batch, nil)
		if err != nil {
			// TODO: figure out how to recover from the error.
			log.Printf("error writing to db: %v", err)
			d.pollSleep()
			continue
		}

		// Get the next page.
		if c.NextPageToken != "" {
			l.PageToken(c.NextPageToken)
			continue
		}

		// Start at the new change ID next time
		l = d.service.Changes.List().
			IncludeDeleted(true).IncludeSubscribed(true).MaxResults(1000).
			StartChangeId(cpt.LastChangeID + 1)

		// Signal we're synced, if we are.
		if cpt.LastChangeID >= c.LargestChangeId {
			d.synced.Broadcast()
		}

		d.pollSleep()
	}
}

func (d *DriveDB) pollSleep() {
	// TODO: make this an option or parameter.
	time.Sleep(time.Minute)
}

func (d *DriveDB) WaitUntilSynced() {
	d.synced.L.Lock()
	d.synced.Wait()
	d.synced.L.Unlock()
}

func (d *DriveDB) Close() {
	d.iters.Wait()
	d.db.Close()
	d.db = nil
}
