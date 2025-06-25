package db

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/sayuyere/bcask/internal/consts"
	"github.com/sayuyere/bcask/internal/index"
	"github.com/sayuyere/bcask/internal/item"
	"github.com/sayuyere/bcask/internal/segment"
)

type DB interface {
	// Get retrieves a value by key.
	Get(key string) (string, error)

	// Put stores a key and value in the datastore.
	Put(key, value string) error

	// Delete removes a key from the datastore.
	Delete(key string) error

	// ListKeys lists all keys in the datastore.
	ListKeys() ([]string, error)

	// Fold applies a function to all key/value pairs, accumulating a result.
	// The function should have the signature: func(key, value string, acc interface{}) interface{}
	Fold(fn func(key, value string, acc interface{}) interface{}, acc interface{}) interface{}

	// Merge compacts the datastore files.
	Merge() error

	// Sync forces any writes to sync to disk.
	Sync() error

	// Close closes the datastore.
	Close() error
}

type Bcask struct {
	Path       string
	DBName     string
	DBSegments []*segment.FileSegment // Assuming Segment is defined in the segment package
	Lock       sync.RWMutex           // Assuming Sync.RWMutex is defined elsewhere
	Index      *index.PrefixTrie
}

func (b *Bcask) Get(key string) (string, error) {
	// Implementation of Get method
	b.Lock.RLock()
	defer b.Lock.RUnlock()
	item, err := b.Index.Get(key)
	if err != nil {
		return "", err
	}
	kv, err := b.DBSegments[item.FileID].Get(item.Offset)
	if err != nil {
		return "", err
	}
	return kv.Value, nil
}
func (b *Bcask) Put(key, value string) error {
	// Implementation of Put method

	b.Lock.Lock()
	defer b.Lock.Unlock()
	v := item.MemoryItem{
		FileID:    int64(len(b.DBSegments)) - 1,
		ValueSize: int64(len(value)),
		Offset:    b.DBSegments[len(b.DBSegments)-1].GetOffset(),
		Timestamp: time.Now().Unix(),
		// To fix this offset stuff ideally when you have written then use the offsett
	}
	dkv := item.DiskKV{
		KeySize:   int64(len(key)),
		ValueSize: int64(len(value)),
		Key:       key,
		Value:     value,
		Timestamp: v.Timestamp,
	}
	if int64(len(dkv.Encode())) > consts.SegmentMaxSize {
		return consts.ErrorDiskKeyValueBigEntry
	}
	err := b.DBSegments[len(b.DBSegments)-1].Write(dkv)
	if err == nil {
		return b.Index.Set(key, &v)
	}

	if err == consts.ErrorSegmentCapacityFull {
		b.AddNewSegment()
		err = b.DBSegments[len(b.DBSegments)-1].Write(dkv)
		if err == nil {
			return b.Index.Set(key, &v)
		}
	}

	return err // Placeholder return
}

func (b *Bcask) AddNewSegment() {
	fmt.Println("Adding a new segment: ", len(b.DBSegments))
	b.DBSegments = append(b.DBSegments, segment.NewFileSegment(b.Path, int64(len(b.DBSegments)), 0))

}

func (b *Bcask) Delete(key string) error {
	// Implementation of Delete method
	b.Lock.Lock()
	defer b.Lock.Unlock()
	item, err := b.Index.Get(key)
	if err != nil {
		return err
	}
	err = b.DBSegments[item.FileID].Delete(*item)
	if err != nil {
		return err
	}
	return b.Index.Delete(key)
}
func (b *Bcask) ListKeys() ([]string, error) {
	// Implementation of ListKeys method
	b.Lock.RLock()
	defer b.Lock.RUnlock()
	return nil, nil // Placeholder return
}
func (b *Bcask) Fold(fn func(key, value string, acc interface{}) interface{}, acc interface{}) interface{} {
	// Implementation of Fold method
	return acc // Placeholder return
}
func (b *Bcask) Merge() error {
	// Implementation of Merge method
	return nil // Placeholder return
}
func (b *Bcask) Sync() error {
	// Implementation of Sync method
	b.Lock.Lock()
	defer func() {
		b.Lock.Unlock()
		for _, v := range b.DBSegments {
			// v.Close()
			v.OSFile.Sync()
		}
	}()
	indexfile, err := os.Create(filepath.Join(b.Path, consts.IndexFileName))
	if err != nil {
		return err
	}
	encodedData, err := b.Index.Encode()
	if err != nil {
		return err
	}
	writtenByte, err := indexfile.WriteAt(encodedData, 0)
	if err != nil {
		return err
	}
	if writtenByte != len(encodedData) {
		return consts.ErrorMMapIncompleteWrite
	}
	if err := indexfile.Sync(); err != nil {
		return err
	}
	if err := indexfile.Close(); err != nil {
		return err
	}

	return nil // Placeholder return
}

func (b *Bcask) Close() error {
	// Fix index stuff
	b.Lock.Lock()
	defer func() {
		b.Lock.Unlock()
		for _, v := range b.DBSegments {
			// v.Close()
			v.OSFile.Close()
		}

	}()
	indexfile, err := os.Create(filepath.Join(b.Path, consts.IndexFileName))
	if err != nil {
		return err
	}
	encodedData, err := b.Index.Encode()
	if err != nil {
		return err
	}
	writtenByte, err := indexfile.WriteAt(encodedData, 0)
	if err != nil {
		return err
	}
	if writtenByte != len(encodedData) {
		return consts.ErrorMMapIncompleteWrite
	}
	if err := indexfile.Sync(); err != nil {
		return err
	}
	if err := indexfile.Close(); err != nil {
		return err
	}

	return nil // Placeholder return
}

func NewBcask(path string, dbName string) *Bcask {
	// Use filepath.Join for platform-neutral path construction
	fullPath := filepath.Join(path, dbName)
	fullPath = filepath.Clean(fullPath)
	if err := os.MkdirAll(fullPath, 0755); err != nil {
		panic("failed to create database directory: " + err.Error())
	}
	currentIndex := index.NewPrefixTrie()

	var allSegments []*segment.FileSegment = []*segment.FileSegment{segment.NewFileSegment(fullPath, 0, 0)}
	return &Bcask{
		Path:       fullPath,
		DBName:     dbName,
		DBSegments: allSegments,
		Lock:       sync.RWMutex{},
		Index:      currentIndex,
	}
}
