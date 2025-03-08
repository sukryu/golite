// Package lockfree provides lock‑free data structures for high‑performance concurrent applications.
package lockfree

import (
	"sync"
	"sync/atomic"
)

// LockFreeCache implements a simple lock‑free cache that satisfies the CacheInterface.
// 이 캐시는 내부적으로 sync.Map을 사용하여 동시성 안전한 조회/삽입을 제공하며,
// 항목 수는 atomic 카운터를 통해 관리됩니다.
type LockFreeCache struct {
	data  sync.Map     // key-value 저장소
	count atomic.Int64 // 저장된 항목 수 (approximate count)
}

// NewLockFreeCache creates and returns a new LockFreeCache.
func NewLockFreeCache() *LockFreeCache {
	return &LockFreeCache{}
}

// Get retrieves a cached value for the given key.
// 존재하면 (value, true), 없으면 ("", false)를 반환합니다.
func (c *LockFreeCache) Get(key string) (string, bool) {
	val, ok := c.data.Load(key)
	if !ok {
		return "", false
	}
	return val.(string), true
}

// Put stores a key-value pair in the cache.
// 이미 존재하는 키인 경우 값을 업데이트합니다.
func (c *LockFreeCache) Put(key, value string) {
	// LoadOrStore를 사용해 새 항목인 경우에만 카운터를 증가시킵니다.
	_, loaded := c.data.LoadOrStore(key, value)
	if loaded {
		// 이미 존재하면 단순히 값을 업데이트.
		c.data.Store(key, value)
	} else {
		c.count.Add(1)
	}
}

// Length returns the approximate number of items in the cache.
func (c *LockFreeCache) Length() int {
	return int(c.count.Load())
}

// Clear removes all entries from the cache.
func (c *LockFreeCache) Clear() {
	// sync.Map의 Range로 모든 키를 삭제합니다.
	c.data.Range(func(key, _ interface{}) bool {
		c.data.Delete(key)
		c.count.Add(-1)
		return true
	})
}
