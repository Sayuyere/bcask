package segment

import "github.com/sayuyere/bcask/internal/item"

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
}

func (f *FileSegment) Get(offset int64) (item.DiskKV, error) {
	// Implementation of Get method
	return item.DiskKV{}, nil // Placeholder return
}
func (f *FileSegment) Write(key item.DiskKV, value string) error {
	// Implementation of Write method
	return nil // Placeholder return
}
func (f *FileSegment) Delete(key item.DiskKV) error {
	// Implementation of Delete method
	return nil // Placeholder return
}
func (f *FileSegment) Sync() error {
	// Implementation of Sync method
	return nil // Placeholder return
}
func (f *FileSegment) Close() error {
	// Implementation of Close method
	return nil // Placeholder return
}
