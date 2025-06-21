package db

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
