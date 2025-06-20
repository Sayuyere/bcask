package item

type MemoryItem struct {
	FileID    int64 `json:"file_id"`
	ValueSize int64 `json:"value_size"`
	Offset    int64 `json:"offset"`
	Timestamp int64 `json:"timestamp"`
}
