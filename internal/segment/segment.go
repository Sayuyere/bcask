package segment

import (
	"fmt"

	mmap "github.com/edsrzf/mmap-go"
	"github.com/sayuyere/bcask/internal/item"
)

const SEGMENT string = "segment_file_"
const SEGMENT_SIZE int64 = 1024 * 1024 * 4 //4MB Segment Size

type Segment interface {
	// Get retrieves a value by key.
	Get(offset int64) (item.DiskKV, error)
	// Write stores a value by key.
	Write(key item.DiskKV, value string) error
	// Delete removes a value by key.
	Delete(key item.DiskKV) error
	Sync() error
	// Close closes the segment.
	Close() error
}
type FileSegment struct {
	Path string
	// FileID is the identifier for the segment file.
	FileID int64
	// Segment is the segment associated with the file.
	File   *mmap.MMap
	Offset int64
}

func (f *FileSegment) Get(offset int64) (item.DiskKV, error) {
	// Implementation of Get method
	res := item.DiskKV{}
	res.DecodeFromMMapedFile(f.File, offset)
	return res, nil
}
func (f *FileSegment) Write(val item.DiskKV) error {
	// Write the encoded value at the end of the file (append mode is assumed)
	data := val.Encode()
	mm := *f.File
	n := copy(mm[f.Offset:], data)
	if n != len(data) {
		return fmt.Errorf("incomplete write to segment file: expected %d bytes, got %d", len(data), n)
	}
	f.Offset += int64(len(data))
	return nil
}

func (f *FileSegment) WriteAt(val item.DiskKV, offset int) error {
	data := val.Encode()
	m := *f.File
	if offset < 0 || offset+len(data) > len(m) {
		return fmt.Errorf("offset out of bounds")
	}
	n := copy(m[offset:offset+len(data)], data)
	if n != len(data) {
		return fmt.Errorf("unable to write required bytes: expected %d, wrote %d", len(data), n)
	}
	return nil
}

func (f *FileSegment) Delete(val item.MemoryItem) error {
	// Mark the record as deleted by setting its timestamp to zero
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

	if err := f.File.Flush(); err != nil {
		return fmt.Errorf("failed to sync segment file: %v", err)
	}
	return nil
}

func (f *FileSegment) Close() error {

	err := f.Sync()
	if err != nil {
		fmt.Println(err)
	}
	if err := f.File.Unmap(); err != nil {
		return fmt.Errorf("failed to close segment file: %v", err)
	}
	return nil
}
