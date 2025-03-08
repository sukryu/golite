package lsmtree

import (
	"container/list"
	"sync"
)

// Cache implements a simple LRU cache.
type Cache struct {
	capacity int
	mu       sync.Mutex
	items    map[string]*list.Element
	order    *list.List
}

type cacheEntry struct {
	key   string
	value string
}

// NewCache creates a new Cache with the specified capacity in bytes.
// For simplicity, capacity is converted to an approximate number of entries.
func NewCache(capacity int) *Cache {
	return &Cache{
		capacity: capacity / 64, // assume average 64 bytes per entry
		items:    make(map[string]*list.Element),
		order:    list.New(),
	}
}

// Get retrieves a value from the cache.
func (c *Cache) Get(key string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.items[key]; ok {
		c.order.MoveToFront(elem)
		return elem.Value.(*cacheEntry).value, true
	}
	return "", false
}

// Put inserts or updates a key-value pair in the cache.
func (c *Cache) Put(key, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.items[key]; ok {
		c.order.MoveToFront(elem)
		elem.Value.(*cacheEntry).value = value
		return
	}
	entry := &cacheEntry{key: key, value: value}
	elem := c.order.PushFront(entry)
	c.items[key] = elem
	if c.order.Len() > c.capacity {
		// Remove least recently used element.
		lru := c.order.Back()
		if lru != nil {
			c.order.Remove(lru)
			delete(c.items, lru.Value.(*cacheEntry).key)
		}
	}
}
