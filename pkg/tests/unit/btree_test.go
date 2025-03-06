package unit

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/sukryu/GoLite/pkg/adapters/btree"
)

// TestBtreeBasicOperations tests basic B-tree operations: Insert, Get, Delete.
func TestBtreeBasicOperations(t *testing.T) {
	// Setup: Create a temporary file for the B-tree
	file, err := os.CreateTemp("", "btree_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(file.Name())
	defer file.Close()

	// Initialize B-tree with default config
	config := btree.BtConfig{
		Degree:     2, // Small degree for testing splits
		PageSize:   4096,
		ThreadSafe: false,
		CacheSize:  0, // No caching for basic test
	}
	bt := btree.NewBtree(file, config)

	// Test Insert
	err = bt.Insert("key1", "value1")
	assert.NoError(t, err, "Insert should succeed")
	err = bt.Insert("key2", "value2")
	assert.NoError(t, err, "Insert should succeed")
	err = bt.Insert("key3", "value3")
	assert.NoError(t, err, "Insert should succeed")

	// Test Get
	value, err := bt.Get("key1")
	assert.NoError(t, err, "Get should succeed")
	assert.Equal(t, "value1", value, "Get should return correct value")
	value, err = bt.Get("key2")
	assert.NoError(t, err, "Get should succeed")
	assert.Equal(t, "value2", value, "Get should return correct value")
	value, err = bt.Get("nonexistent")
	assert.Error(t, err, "Get should fail for nonexistent key")
	assert.Nil(t, value, "Get should return nil for nonexistent key")

	// Test Delete
	err = bt.Delete("key2")
	assert.NoError(t, err, "Delete should succeed")
	value, err = bt.Get("key2")
	assert.Error(t, err, "Get should fail after delete")
	assert.Nil(t, value, "Get should return nil after delete")
	assert.Equal(t, 2, bt.GetLength(), "Length should decrease after delete")
}

// TestBtreeWithCaching tests B-tree operations with caching enabled.
func TestBtreeWithCaching(t *testing.T) {
	// Setup: Create a temporary file
	file, err := os.CreateTemp("", "btree_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(file.Name())
	defer file.Close()

	// Initialize B-tree with caching
	config := btree.BtConfig{
		Degree:     2,
		PageSize:   4096,
		ThreadSafe: true,
		CacheSize:  2, // Small cache size to test eviction
	}
	bt := btree.NewBtree(file, config)

	// Insert multiple keys to fill nodes and trigger splits
	keys := []string{"key1", "key2", "key3", "key4"}
	values := []string{"value1", "value2", "value3", "value4"}
	for i, key := range keys {
		err := bt.Insert(key, values[i])
		assert.NoError(t, err, "Insert should succeed for %s", key)
	}

	// Test caching: Repeated Get should hit cache
	for i := 0; i < 3; i++ {
		for j, key := range keys {
			value, err := bt.Get(key)
			assert.NoError(t, err, "Get should succeed for %s", key)
			assert.Equal(t, values[j], value, "Get should return correct value for %s", key)
		}
	}

	// Test cache eviction: Insert more keys to exceed cache size
	err = bt.Insert("key5", "value5")
	assert.NoError(t, err, "Insert should succeed")
	assert.True(t, bt.GetCacheSize() <= config.CacheSize, "Cache should not exceed size")

	// Test Get after eviction
	value, err := bt.Get("key1")
	assert.NoError(t, err, "Get should succeed after eviction")
	assert.Equal(t, "value1", value, "Get should return correct value after eviction")
}

// TestBtreeConcurrency tests concurrent access to the B-tree.
func TestBtreeConcurrency(t *testing.T) {
	// Setup: Create a temporary file
	file, err := os.CreateTemp("", "btree_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(file.Name())
	defer file.Close()

	// Initialize B-tree with thread safety and caching
	config := btree.BtConfig{
		Degree:     2,
		PageSize:   4096,
		ThreadSafe: true,
		CacheSize:  2,
	}
	bt := btree.NewBtree(file, config)

	// Concurrent inserts
	var wg sync.WaitGroup
	numGoroutines := 10
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", id)
			value := fmt.Sprintf("value%d", id)
			err := bt.Insert(key, value)
			assert.NoError(t, err, "Concurrent Insert should succeed for %s", key)
		}(i)
	}
	wg.Wait()

	// Verify all keys were inserted
	assert.Equal(t, numGoroutines, bt.GetLength(), "All keys should be inserted")
	for i := 0; i < numGoroutines; i++ {
		key := fmt.Sprintf("key%d", i)
		value, err := bt.Get(key)
		assert.NoError(t, err, "Get should succeed for %s", key)
		assert.Equal(t, fmt.Sprintf("value%d", i), value, "Get should return correct value for %s", key)
	}
}

// TestBtreePersistence tests if data persists across B-tree instances.
func TestBtreePersistence(t *testing.T) {
	// Setup: Create a temporary file
	filePath := "btree_test_persistence.db"
	file, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	file.Close()

	// First instance: Insert data
	config := btree.BtConfig{
		Degree:     2,
		PageSize:   4096,
		ThreadSafe: false,
		CacheSize:  0,
	}
	file, err = os.OpenFile(filePath, os.O_RDWR, 0666)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	bt1 := btree.NewBtree(file, config)
	err = bt1.Insert("key1", "value1")
	assert.NoError(t, err, "Insert should succeed")
	err = bt1.Insert("key2", "value2")
	assert.NoError(t, err, "Insert should succeed")
	file.Close()

	// Second instance: Read data
	file, err = os.OpenFile(filePath, os.O_RDWR, 0666)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer os.Remove(filePath)
	defer file.Close()
	bt2 := btree.NewBtree(file, config)
	value, err := bt2.Get("key1")
	assert.NoError(t, err, "Get should succeed")
	assert.Equal(t, "value1", value, "Get should return persisted value")
	value, err = bt2.Get("key2")
	assert.NoError(t, err, "Get should succeed")
	assert.Equal(t, "value2", value, "Get should return persisted value")
}
