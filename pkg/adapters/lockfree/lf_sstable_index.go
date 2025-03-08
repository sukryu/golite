package lockfree

import (
	"sort"
	"sync/atomic"
)

// SSTableIndexEntry represents a single index entry with a key and file offset.
type SSTableIndexEntry struct {
	Key    string
	Offset int64
}

// LockFreeSSTableIndex is an immutable, lock-free index for an SSTable.
// 인덱스는 생성 후 변경되지 않으며, 업데이트가 필요하면 전체 인덱스를 원자적으로 교체합니다.
type LockFreeSSTableIndex struct {
	index atomic.Value // holds []SSTableIndexEntry
}

// NewLockFreeSSTableIndex creates a new lock-free SSTable index with the given entries.
// 입력 배열은 내부에서 정렬됩니다.
func NewLockFreeSSTableIndex(entries []SSTableIndexEntry) *LockFreeSSTableIndex {
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Key < entries[j].Key
	})
	lfi := &LockFreeSSTableIndex{}
	lfi.index.Store(entries)
	return lfi
}

// Get searches for the given key and returns the corresponding index entry if found.
func (lfi *LockFreeSSTableIndex) Get(key string) (SSTableIndexEntry, bool) {
	entries := lfi.index.Load().([]SSTableIndexEntry)
	// 이진 탐색 수행
	i := sort.Search(len(entries), func(i int) bool {
		return entries[i].Key >= key
	})
	if i < len(entries) && entries[i].Key == key {
		return entries[i], true
	}
	var empty SSTableIndexEntry
	return empty, false
}

// Length returns the number of entries in the index.
func (lfi *LockFreeSSTableIndex) Length() int {
	entries := lfi.index.Load().([]SSTableIndexEntry)
	return len(entries)
}

// Update replaces the entire index with a new sorted set of entries.
func (lfi *LockFreeSSTableIndex) Update(newEntries []SSTableIndexEntry) {
	sort.Slice(newEntries, func(i, j int) bool {
		return newEntries[i].Key < newEntries[j].Key
	})
	lfi.index.Store(newEntries)
}
