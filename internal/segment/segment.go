package segment

import (
	"fmt"

	"codeberg.org/go-mmap/mmap"
	"github.com/sayuyere/bcask/internal/item"
)

const SEGMENT string = "segment_file_"

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
	File *mmap.File
}

func (f *FileSegment) Get(offset int64) (item.DiskKV, error) {
	// Implementation of Get method
	res := item.DiskKV{}
	res.DecodeFromFile(f.File, offset)
	return res, nil
}
func (f *FileSegment) Write(val item.DiskKV) error {
	// Implementation of Write method
	n, err := f.File.Write(val.Encode())
	if err != nil {
		return fmt.Errorf("failed to write to segment file: %v", err)
	}
	if n != len(val.Encode()) {
		return fmt.Errorf("incomplete write to segment file: expected %d bytes, got %d", len(val.Encode()), n)
	}

	return nil // Placeholder return
}
func (f *FileSegment) Delete(val item.MemoryItem) error {
	// Implementation of Delete method
	locationItem := item.DiskKV{}
	locationItem.DecodeFromFile(f.File, val.Offset)
	locationItem.Timestamp = 0 // Mark as deleted by setting timestamp to zero
	byteCount, err := f.File.WriteAt(locationItem.Encode(), val.Offset)
	if err != nil {
		return fmt.Errorf("failed to write to segment file: %v", err)
	}
	if byteCount != len(locationItem.Encode()) {
		return fmt.Errorf("incomplete write to segment file: expected %d bytes, got %d", len(locationItem.Encode()), byteCount)
	}
	return nil

}
func (f *FileSegment) Sync() error {
	// Implementation of Sync method

	if err := f.File.Sync(); err != nil {
		return fmt.Errorf("failed to sync segment file: %v", err)
	}
	// Additional logic for syncing if necessary
	// For example, updating metadata or flushing buffers
	// ...
	// Return nil if sync is successful

	return nil
}
func (f *FileSegment) Close() error {
	// Implementation of Close method
	err := f.File.Sync()
	if err != nil {
		return fmt.Errorf("failed to sync segment file before closing: %v", err)
	}
	err = f.File.Close()
	if err != nil {
		return fmt.Errorf("failed to close segment file: %v", err)
	}
	// Additional cleanup if necessary
	// For example, removing the file from the filesystem or releasing resources
	// ...
	// Return nil if close is successful
	return nil
}
