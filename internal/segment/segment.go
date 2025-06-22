package segment

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	mmap "github.com/edsrzf/mmap-go"
	"github.com/sayuyere/bcask/internal/consts"
	"github.com/sayuyere/bcask/internal/item"
)

type Segment interface {
	// Get retrieves a value by key.
	Get(offset int64) (item.DiskKV, error)
	// Write stores a value by key.
	Write(val item.DiskKV) error
	// Delete removes a value by key.
	Delete(val item.MemoryItem) error
	Sync() error
	// Close closes the segment.
	Close() error
	GetOffset() int64
}
type FileSegment struct {
	Path string
	// FileID is the identifier for the segment file.
	FileID int64
	// Segment is the segment associated with the file.
	File   *mmap.MMap
	Offset int64
	OSFile *os.File
	Lock   sync.RWMutex
}

func (f *FileSegment) Get(offset int64) (item.DiskKV, error) {
	// Implementation of Get method
	f.Lock.RLock()
	defer f.Lock.RUnlock()
	res := item.DiskKV{}
	res.DecodeFromMMapedFile(f.File, offset)
	return res, nil
}
func (f *FileSegment) Write(val item.DiskKV) error {
	f.Lock.Lock()
	defer f.Lock.Unlock()
	data := val.Encode()
	mm := *f.File

	if int(f.Offset)+len(data) > len(mm) {
		return consts.ErrorSegmentCapacityFull
	}

	n := copy(mm[f.Offset:], data)
	if n != len(data) {
		return consts.ErrorMMapIncompleteWrite
	}
	f.Offset += int64(len(data))
	return nil
}

func (f *FileSegment) WriteAt(val item.DiskKV, offset int) error {
	f.Lock.Lock()
	defer f.Lock.Unlock()
	data := val.Encode()
	m := *f.File
	if offset < 0 {
		return consts.ErrorInvalidOffset
	}
	if offset+len(data) > len(m) {
		return consts.ErrorSegmentCapacityFull
	}
	n := copy(m[offset:offset+len(data)], data)
	if n != len(data) {
		return fmt.Errorf("unable to write required bytes: expected %d, wrote %d", len(data), n)
	}
	return nil
}

func (f *FileSegment) Delete(val item.MemoryItem) error {
	// Mark the record as deleted by setting its timestamp to zero
	// f.Lock.RLock()
	// defer f.Lock.RUnlock()
	locationItem := item.DiskKV{}
	locationItem.DecodeFromMMapedFile(f.File, val.Offset)
	locationItem.Timestamp = 0 // Mark as deleted
	// data := locationItem.Encode()

	err := f.WriteAt(locationItem, int(val.Offset))
	if err != nil {
		return fmt.Errorf("failed to write to segment file: %v", err)
	}

	return nil
}

func (f *FileSegment) Sync() error {
	f.Lock.RLock()
	defer f.Lock.RUnlock()

	if err := f.File.Flush(); err != nil {
		return fmt.Errorf("failed to sync segment file: %v", err)
	}
	return nil
}
func (f *FileSegment) GetOffset() int64 {
	f.Lock.RLock()
	defer f.Lock.RUnlock()
	return f.Offset
}
func (f *FileSegment) Close() error {

	err := f.Sync()
	if err != nil {
		fmt.Println(err)
	}
	f.Lock.Lock()
	defer f.Lock.Unlock()
	if err := f.File.Unmap(); err != nil {
		return fmt.Errorf("failed to close segment file: %v", err)
	}
	return nil
}

func NewFileSegment(filepath_ string, fileID int64, offset int64) *FileSegment {
	segmentLocation := filepath.Join(filepath_, consts.SegmentPrefix+strconv.Itoa(int(fileID)))
	fmt.Println(segmentLocation)
	f, err := os.Create(segmentLocation)
	if err != nil {
		panic(err)
	}

	f.Truncate(consts.SegmentMaxSize)
	m, err := mmap.Map(f, os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}
	return &FileSegment{
		Path:   segmentLocation,
		FileID: fileID,
		File:   &m,
		Offset: 0,
		OSFile: f,
		Lock:   sync.RWMutex{},
	}

}
