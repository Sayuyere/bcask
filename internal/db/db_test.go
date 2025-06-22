package db

import (
	"os"
	"path/filepath"
	"testing"
)

// Helper function to create a temporary directory for each test
func createTempDir(t *testing.T) string {
	dir, err := os.MkdirTemp("", "bcask_test_no_mock_")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	return dir
}

// Helper function to clean up the temporary directory
func cleanupTempDir(t *testing.T, dir string) {
	if err := os.RemoveAll(dir); err != nil {
		t.Errorf("Failed to clean up temp dir %s: %v", dir, err)
	}
}

func TestNewBcask(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	dbName := "testdb_nomock"
	fullPath := filepath.Join(tempDir, dbName)
	b := NewBcask(fullPath, dbName)
	t.Run("successful creation", func(t *testing.T) {

		if b == nil {
			t.Fatalf("NewBcask returned nil")
		}
		if b.DBName != dbName {
			t.Errorf("Expected DBName %s, got %s", dbName, b.DBName)
		}
		if len(b.DBSegments) != 1 {
			t.Errorf("Expected 1 segment, got %d", len(b.DBSegments))
		}
		if b.DBSegments[0] == nil {
			t.Errorf("Initial segment is nil")
		}
		if b.Index == nil {
			t.Errorf("Index is nil")
		}
		b.Close()

	})
}

func TestBcaskPutGet(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	dbName := "put_get_db_nomock"
	b := NewBcask(tempDir, dbName)
	defer func() {
		if err := b.Close(); err != nil {
			t.Errorf("Error closing Bcask: %v", err)
		}
	}()

	t.Run("successful put and get", func(t *testing.T) {
		key := "mykey"
		value := "myvalue"

		err := b.Put(key, value)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		retrievedValue, err := b.Get(key)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if retrievedValue != value {
			t.Errorf("Expected value %q, got %q", value, retrievedValue)
		}

		// Put another key-value pair to check multiple entries
		key2 := "anotherkey"
		value2 := "anothervalue"
		err = b.Put(key2, value2)
		if err != nil {
			t.Fatalf("Put of second key failed: %v", err)
		}

		retrievedValue2, err := b.Get(key2)
		if err != nil {
			t.Fatalf("Get of second key failed: %v", err)
		}
		if retrievedValue2 != value2 {
			t.Errorf("Expected value for second key %q, got %q", value2, retrievedValue2)
		}

		// Ensure the first key is still retrievable
		retrievedValue, err = b.Get(key)
		if err != nil {
			t.Fatalf("Get of first key after second put failed: %v", err)
		}
		if retrievedValue != value {
			t.Errorf("Expected value for first key %q, got %q (after second put)", value, retrievedValue)
		}
	})
}
