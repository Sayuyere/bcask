package index

import (
	"testing"
	"time"

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
			err := index.Set("key1", &IndexValue{
				FileID:    "file1",
				ValueSize: 123,
				Offset:    0,
				Timestamp: time.Now().Unix(),
			})
			require.NoError(t, err)
		})

		t.Run("Get", func(t *testing.T) {
			value, err := index.Get("key1")
			require.NoError(t, err)
			assert.Equal(t, "file1", value.FileID)
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

}
