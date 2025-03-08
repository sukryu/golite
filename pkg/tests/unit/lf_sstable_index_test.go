package unit

import (
	"testing"

	"github.com/sukryu/GoLite/pkg/adapters/lockfree"
)

func TestLockFreeSSTableIndex(t *testing.T) {
	entries := []lockfree.SSTableIndexEntry{
		{"a", 100},
		{"b", 200},
		{"c", 300},
	}
	index := lockfree.NewLockFreeSSTableIndex(entries)

	// Get 테스트
	if entry, ok := index.Get("b"); !ok || entry.Offset != 200 {
		t.Errorf("Expected key 'b' to have offset 200, got %v, %t", entry, ok)
	}
	if _, ok := index.Get("d"); ok {
		t.Error("Expected key 'd' to not be found")
	}

	// Length 테스트
	if index.Length() != 3 {
		t.Errorf("Expected length 3, got %d", index.Length())
	}

	// Update 테스트
	newEntries := []lockfree.SSTableIndexEntry{
		{"x", 1000},
		{"y", 2000},
	}
	index.Update(newEntries)
	if index.Length() != 2 {
		t.Errorf("Expected length 2 after update, got %d", index.Length())
	}
	if entry, ok := index.Get("x"); !ok || entry.Offset != 1000 {
		t.Errorf("Expected key 'x' to have offset 1000, got %v, %t", entry, ok)
	}
}
