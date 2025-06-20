package index

import (
	"testing"
	"time"

	"github.com/sayuyere/bcask/internal/item"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {

	NewIndex := func() Index {
		t.Helper()
		index := NewPrefixTrie()
		require.NotNil(t, index)
		return index
	}

	t.Run("GetSetDeleteExists", func(t *testing.T) {
		index := NewIndex()
		require.NotNil(t, index)

		t.Run("Set", func(t *testing.T) {
			err := index.Set("key1", &item.MemoryItem{
				FileID:    1,
				ValueSize: 123,
				Offset:    0,
				Timestamp: time.Now().Unix(),
			})
			require.NoError(t, err)
		})

		t.Run("Get", func(t *testing.T) {
			value, err := index.Get("key1")
			require.NoError(t, err)
			assert.Equal(t, int64(1), value.FileID)
			assert.Equal(t, int64(123), value.ValueSize)
			assert.Equal(t, int64(0), value.Offset)
			assert.Equal(t, time.Now().Unix(), value.Timestamp)
		})

		t.Run("Delete", func(t *testing.T) {
			err := index.Delete("key1")
			require.NoError(t, err)
		})

		t.Run("Exists", func(t *testing.T) {
			exists, err := index.Exists("key1")
			require.NoError(t, err)
			assert.False(t, exists)
		})
	})

	t.Run("IndexEncodingTest", func(t *testing.T) {
		index := NewIndex()
		require.NotNil(t, index)

		// Set some values
		err := index.Set("key1", &item.MemoryItem{
			FileID:    1,
			ValueSize: 123,
			Offset:    0,
			Timestamp: time.Now().Unix(),
		})
		require.NoError(t, err)

		err = index.Set("key2", &item.MemoryItem{
			FileID:    2,
			ValueSize: 456,
			Offset:    100,
			Timestamp: time.Now().Unix(),
		})
		require.NoError(t, err)

		// Encode the index
		encodedData, err := index.Encode()
		require.NoError(t, err)

		// Decode into a new index
		newIndex := NewPrefixTrie()
		err = newIndex.Decode(encodedData)
		require.NoError(t, err)

		// Verify the values in the new index
		value1, err := newIndex.Get("key1")
		require.NoError(t, err)
		assert.Equal(t, int64(1), value1.FileID)
		assert.Equal(t, int64(123), value1.ValueSize)

		value2, err := newIndex.Get("key2")
		require.NoError(t, err)
		assert.Equal(t, int64(2), value2.FileID)
		assert.Equal(t, int64(456), value2.ValueSize)
	})
	t.Run("Close", func(t *testing.T) {
		index := NewIndex()
		require.NotNil(t, index)

		err := index.Close()
		require.NoError(t, err)
	})

}
