package lsmtree

import (
	"sync"
	"sync/atomic"
)

const tombstone = "<TOMBSTONE>"

// MemTable represents the in-memory table.
type MemTable struct {
	table   *sync.Map
	size    int64      // 이제 int64로 선언 (atomic으로 업데이트)
	maxSize int64      // int64로 변경 (바이트 단위)
	mu      sync.Mutex // 조건 검사와 테이블 업데이트를 위한 락
}

// NewMemTable creates a new MemTable with the given maximum size.
func NewMemTable(maxSize int) *MemTable {
	return &MemTable{
		table:   new(sync.Map),
		maxSize: int64(maxSize),
	}
}

// Insert inserts or updates a key-value pair atomically.
func (m *MemTable) Insert(key, value string) error {
	addSize := int64(len(key) + len(value))
	m.mu.Lock()
	defer m.mu.Unlock()
	currentSize := atomic.LoadInt64(&m.size)
	if currentSize+addSize > m.maxSize {
		return ErrMemTableFull
	}
	m.table.Store(key, value)
	atomic.AddInt64(&m.size, addSize)
	return nil
}

// Get retrieves a value by key.
func (m *MemTable) Get(key string) (string, bool) {
	v, ok := m.table.Load(key)
	if !ok {
		return "", false
	}
	val := v.(string)
	if val == tombstone {
		return "", false
	}
	return val, true
}

// Delete marks a key as deleted.
func (m *MemTable) Delete(key string) error {
	m.table.Store(key, tombstone)
	return nil
}

// Dump returns all key-value pairs for non-tombstoned entries.
func (m *MemTable) Dump() map[string]string {
	data := make(map[string]string)
	m.table.Range(func(k, v interface{}) bool {
		key := k.(string)
		value := v.(string)
		if value == tombstone {
			return true
		}
		data[key] = value
		return true
	})
	return data
}

// Reset clears the memTable.
func (m *MemTable) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.table = new(sync.Map)
	atomic.StoreInt64(&m.size, 0)
}

// Size returns the current size.
func (m *MemTable) Size() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return atomic.LoadInt64(&m.size)
}

// Swap atomically swaps the current memTable with a new one and returns a snapshot of the old data.
func (m *MemTable) Swap() map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Get snapshot from the current table.
	data := make(map[string]string)
	m.table.Range(func(k, v interface{}) bool {
		key := k.(string)
		value := v.(string)
		if value != tombstone {
			data[key] = value
		}
		return true
	})
	// Swap in a new table and reset size.
	m.table = new(sync.Map)
	atomic.StoreInt64(&m.size, 0)
	return data
}
