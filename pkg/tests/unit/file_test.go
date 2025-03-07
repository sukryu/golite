package unit

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/sukryu/GoLite/pkg/adapters/file"
)

// TestFileBasicOperations tests basic File adapter operations.
func TestFileBasicOperations(t *testing.T) {
	filePath := "file_test_basic.db"
	defer os.Remove(filePath)

	config := file.FileConfig{
		FilePath:   filePath,
		ThreadSafe: false,
	}
	f, err := file.NewFile(config)
	assert.NoError(t, err, "NewFile should succeed")
	defer f.Close()

	// Test Insert
	err = f.Insert("key1", "value1")
	assert.NoError(t, err, "Insert should succeed")
	err = f.Insert("key2", "value2")
	assert.NoError(t, err, "Insert should succeed")

	// Test Get
	value, err := f.Get("key1")
	assert.NoError(t, err, "Get should succeed")
	assert.Equal(t, "value1", value, "Get should return correct value")
	value, err = f.Get("key3")
	assert.Error(t, err, "Get should fail for nonexistent key")
	assert.Nil(t, value, "Get should return nil for nonexistent key")

	// Test Delete
	err = f.Delete("key2")
	assert.NoError(t, err, "Delete should succeed")
	value, err = f.Get("key2")
	assert.Error(t, err, "Get should fail after delete")
	assert.Nil(t, value, "Get should return nil after delete")
}

// TestFileConcurrency tests concurrent access to the File adapter.
func TestFileConcurrency(t *testing.T) {
	filePath := "file_test_concurrency.db"
	defer os.Remove(filePath)
	defer os.Remove(filePath + ".wal") // Clean up WAL file

	config := file.FileConfig{
		FilePath:   filePath,
		ThreadSafe: true,
	}
	f, err := file.NewFile(config)
	assert.NoError(t, err, "NewFile should succeed")
	defer f.Close()

	var wg sync.WaitGroup
	numGoroutines := 10
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", id)
			value := fmt.Sprintf("value%d", id)
			err := f.Insert(key, value)
			assert.NoError(t, err, "Concurrent Insert should succeed for %s", key)
		}(i)
	}
	wg.Wait() // Ensure all inserts complete before verification

	// Verify all keys
	for i := 0; i < numGoroutines; i++ {
		key := fmt.Sprintf("key%d", i)
		value, err := f.Get(key)
		assert.NoError(t, err, "Get should succeed for %s", key)
		assert.Equal(t, fmt.Sprintf("value%d", i), value, "Get should return correct value for %s", key)
	}
}

// TestFilePersistence tests if data persists across File instances.
func TestFilePersistence(t *testing.T) {
	filePath := "file_test_persistence.db"
	defer os.Remove(filePath)

	config := file.FileConfig{
		FilePath:   filePath,
		ThreadSafe: false,
	}
	f1, err := file.NewFile(config)
	assert.NoError(t, err, "NewFile should succeed")
	err = f1.Insert("key1", "value1")
	assert.NoError(t, err, "Insert should succeed")
	err = f1.Insert("key2", "value2")
	assert.NoError(t, err, "Insert should succeed")
	f1.Close()

	f2, err := file.NewFile(config)
	assert.NoError(t, err, "NewFile should succeed")
	defer f2.Close()

	value, err := f2.Get("key1")
	assert.NoError(t, err, "Get should succeed")
	assert.Equal(t, "value1", value, "Get should return persisted value")
	value, err = f2.Get("key2")
	assert.NoError(t, err, "Get should succeed")
	assert.Equal(t, "value2", value, "Get should return persisted value")
}

// TestFileErrorHandling tests error handling in the File adapter.
func TestFileErrorHandling(t *testing.T) {
	// Test invalid config
	config := file.FileConfig{
		FilePath: "", // Empty path
	}
	f, err := file.NewFile(config)
	assert.Error(t, err, "NewFile should fail with empty file path")
	assert.Nil(t, f, "NewFile should return nil with invalid config")
	assert.Contains(t, err.Error(), "file path is required", "Error message should match")

	// Test invalid value type
	filePath := "file_test_error.db"
	defer os.Remove(filePath)
	config = file.FileConfig{FilePath: filePath}
	f, err = file.NewFile(config)
	assert.NoError(t, err, "NewFile should succeed")
	defer f.Close()

	err = f.Insert("key1", 123) // Non-string value
	assert.Error(t, err, "Insert should fail with non-string value")
	assert.Contains(t, err.Error(), "value must be string", "Error message should match")
}
