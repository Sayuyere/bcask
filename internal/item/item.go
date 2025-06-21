package item

import (
	"encoding/binary"

	"codeberg.org/go-mmap/mmap"
)

type MemoryItem struct {
	FileID    int64 `json:"file_id"`
	ValueSize int64 `json:"value_size"`
	Offset    int64 `json:"offset"`
	Timestamp int64 `json:"timestamp"`
}

type DiskKV struct {
	KeySize   int64  `json:"key_size"`
	ValueSize int64  `json:"value_size"`
	Key       string `json:"key"`
	Value     string `json:"value"`
	Timestamp int64  `json:"timestamp"`
}

func int64ToBytesBigEndian(n int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(n))
	return b
}

func (d *DiskKV) Encode() []byte {
	// CRC | timestamp | key_size | value_size | key | value
	encoded := make([]byte, 0)
	encoded = append(encoded, int64ToBytesBigEndian(d.Timestamp)...)
	encoded = append(encoded, int64ToBytesBigEndian(d.KeySize)...)
	encoded = append(encoded, int64ToBytesBigEndian(d.ValueSize)...)
	encoded = append(encoded, []byte(d.Key)...)
	encoded = append(encoded, []byte(d.Value)...)
	return encoded
}

func (d *DiskKV) Decode(data []byte) {
	if len(data) < 24 { // 8 bytes for timestamp, 8 for key_size, 8 for value_size
		return // Not enough data to decode
	}
	d.Timestamp = int64(binary.BigEndian.Uint64(data[:8]))
	d.KeySize = int64(binary.BigEndian.Uint64(data[8:16]))
	d.ValueSize = int64(binary.BigEndian.Uint64(data[16:24]))

	if len(data) < 24+int(d.KeySize)+int(d.ValueSize) {
		return // Not enough data to decode key and value
	}

	d.Key = string(data[24 : 24+int(d.KeySize)])
	d.Value = string(data[24+int(d.KeySize) : 24+int(d.KeySize)+int(d.ValueSize)])
}

func (m *DiskKV) DecodeToMemoryItem() MemoryItem {
	return MemoryItem{
		FileID:    0, // FileID is not set in DiskKV, so we set it to 0
		ValueSize: m.ValueSize,
		Offset:    0, // Offset is not set in DiskKV, so we set it to 0
		Timestamp: m.Timestamp,
	}
}

func (m *DiskKV) DecodeFromFile(file *mmap.File, offset int64) {
	// Read header (24 bytes: timestamp, key_size, value_size)
	header := make([]byte, 24)
	n, err := file.ReadAt(header, offset)
	if err != nil || n < 24 {
		// Could not read header
		return
	}
	m.Timestamp = int64(binary.BigEndian.Uint64(header[0:8]))
	m.KeySize = int64(binary.BigEndian.Uint64(header[8:16]))
	m.ValueSize = int64(binary.BigEndian.Uint64(header[16:24]))

	// Now read key and value
	key := make([]byte, m.KeySize)
	n, err = file.ReadAt(key, offset+24)
	if err != nil || int64(n) < m.KeySize {
		// Could not read key
		return
	}
	value := make([]byte, m.ValueSize)
	n, err = file.ReadAt(value, offset+24+m.KeySize)
	if err != nil || int64(n) < m.ValueSize {
		// Could not read value
		return
	}

	m.Key = string(key)
	m.Value = string(value)
}
