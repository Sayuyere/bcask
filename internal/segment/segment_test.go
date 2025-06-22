package segment

import (
	"os"
	"testing"

	mmap "github.com/edsrzf/mmap-go"
	"github.com/sayuyere/bcask/internal/consts"
	"github.com/sayuyere/bcask/internal/item"
	"github.com/stretchr/testify/assert"
)

func TestFileSegment(t *testing.T) {
	// Create a new memory-mapped file for testing
	tmpfileName := "./test"

	tempFile, err := os.Create(tmpfileName)
	defer func() {
		if tempFile != nil {
			tempFile.Close()
			os.Remove(tempFile.Name())
		}
	}()

	tempFile.Truncate(int64(consts.SegmentMaxSize))

	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}

	mm, err := mmap.Map(tempFile, mmap.RDWR, 0)
	if err != nil {
		t.Fatalf("failed to create mmap region: %v", err)
	}

	segment := &FileSegment{
		File: &mm,
	}

	t.Run("Write and Get", func(t *testing.T) {
		val := item.DiskKV{
			Key:       "1",
			Value:     "test",
			KeySize:   1,
			ValueSize: 4,
		}
		err := segment.Write(val)
		assert.NoError(t, err)

		got, err := segment.Get(0)
		assert.NoError(t, err)
		assert.Equal(t, val, got)
	})

	t.Run("Delete", func(t *testing.T) {
		val := item.MemoryItem{
			ValueSize: 4,
			Offset:    0,
		}
		err := segment.Delete(val)
		assert.NoError(t, err)
	})

	t.Run("Sync and Close", func(t *testing.T) {
		err := segment.Sync()
		assert.NoError(t, err)
		// Close the segment
		err = segment.Close()
		assert.NoError(t, err)
	})
}
