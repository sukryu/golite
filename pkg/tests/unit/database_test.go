package unit

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/sukryu/GoLite/pkg/adapters/btree"
	"github.com/sukryu/GoLite/pkg/domain"
)

// mockLogger는 테스트용 간단한 로거입니다.
type mockLogger struct {
	logs []string
}

func (m *mockLogger) Info(msg string)  { m.logs = append(m.logs, "INFO: "+msg) }
func (m *mockLogger) Warn(msg string)  { m.logs = append(m.logs, "WARN: "+msg) }
func (m *mockLogger) Error(msg string) { m.logs = append(m.logs, "ERROR: "+msg) }

// TestDatabaseBasicOperations tests basic Database operations.
func TestDatabaseBasicOperations(t *testing.T) {
	logger := &mockLogger{}
	file, err := os.CreateTemp("", "db_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(file.Name())
	defer file.Close()

	config := domain.DatabaseConfig{
		Name:      "testdb",
		FilePath:  file.Name(),
		BtConfig:  btree.BtConfig{Degree: 2, PageSize: 4096, CacheSize: 0},
		MaxTables: 2,
	}
	db, err := domain.NewDatabase(config, logger)
	assert.NoError(t, err, "NewDatabase should succeed")
	defer db.Close()

	// Test CreateTable
	err = db.CreateTable("users")
	assert.NoError(t, err, "CreateTable should succeed")
	assert.Equal(t, 1, db.GetStatus().TableCount, "Table count should increase")

	// Test Insert
	err = db.Insert("users", "user1", "Alice")
	assert.NoError(t, err, "Insert should succeed")

	// Test Get
	value, err := db.Get("users", "user1")
	assert.NoError(t, err, "Get should succeed")
	assert.Equal(t, "Alice", value, "Get should return correct value")

	// Test Delete
	err = db.Delete("users", "user1")
	assert.NoError(t, err, "Delete should succeed")
	_, err = db.Get("users", "user1")
	assert.Error(t, err, "Get should fail after delete")

	// Test DropTable
	err = db.DropTable("users")
	assert.NoError(t, err, "DropTable should succeed")
	assert.Equal(t, 0, db.GetStatus().TableCount, "Table count should decrease")
}

// TestDatabaseConcurrency tests concurrent access to the Database.
func TestDatabaseConcurrency(t *testing.T) {
	logger := &mockLogger{}
	file, err := os.CreateTemp("", "db_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(file.Name())
	defer file.Close()

	config := domain.DatabaseConfig{
		Name:       "testdb",
		FilePath:   file.Name(),
		BtConfig:   btree.BtConfig{Degree: 2, PageSize: 4096, CacheSize: 2, ThreadSafe: true},
		MaxTables:  10,
		ThreadSafe: true,
	}
	db, err := domain.NewDatabase(config, logger)
	assert.NoError(t, err, "NewDatabase should succeed")
	defer db.Close()

	err = db.CreateTable("users")
	assert.NoError(t, err, "CreateTable should succeed")

	// Concurrent inserts
	var wg sync.WaitGroup
	numGoroutines := 10
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("user%d", id)
			value := fmt.Sprintf("value%d", id)
			err := db.Insert("users", key, value)
			assert.NoError(t, err, "Concurrent Insert should succeed for %s", key)
		}(i)
	}
	wg.Wait()

	// Verify all keys
	for i := 0; i < numGoroutines; i++ {
		key := fmt.Sprintf("user%d", i)
		value, err := db.Get("users", key)
		assert.NoError(t, err, "Get should succeed for %s", key)
		assert.Equal(t, fmt.Sprintf("value%d", i), value, "Get should return correct value for %s", key)
	}
}

// TestDatabasePersistence tests if data persists across Database instances.
func TestDatabasePersistence(t *testing.T) {
	logger := &mockLogger{}
	filePath := "db_test_persistence.db"
	file, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	file.Close()

	config := domain.DatabaseConfig{
		Name:      "testdb",
		FilePath:  filePath,
		BtConfig:  btree.BtConfig{Degree: 2, PageSize: 4096, CacheSize: 0},
		MaxTables: 2,
	}
	db1, err := domain.NewDatabase(config, logger)
	assert.NoError(t, err, "NewDatabase should succeed")
	err = db1.CreateTable("users")
	assert.NoError(t, err, "CreateTable should succeed")
	err = db1.Insert("users", "user1", "Alice")
	assert.NoError(t, err, "Insert should succeed")
	err = db1.Insert("users", "user2", "Bob")
	assert.NoError(t, err, "Insert should succeed")
	db1.Close()

	db2, err := domain.NewDatabase(config, logger)
	assert.NoError(t, err, "NewDatabase should succeed")
	defer db2.Close()
	defer os.Remove(filePath)

	value, err := db2.Get("users", "user1")
	assert.NoError(t, err, "Get should succeed")
	assert.Equal(t, "Alice", value, "Get should return persisted value")
	value, err = db2.Get("users", "user2")
	assert.NoError(t, err, "Get should succeed")
	assert.Equal(t, "Bob", value, "Get should return persisted value")
	assert.Equal(t, 1, db2.GetStatus().TableCount, "Table count should persist")
}

// TestDatabaseLimits tests table creation limits and error handling.
func TestDatabaseLimits(t *testing.T) {
	logger := &mockLogger{}
	file, err := os.CreateTemp("", "db_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(file.Name())
	defer file.Close()

	config := domain.DatabaseConfig{
		Name:      "testdb",
		FilePath:  file.Name(),
		BtConfig:  btree.BtConfig{Degree: 2, PageSize: 4096, CacheSize: 0},
		MaxTables: 1, // Small limit for testing
	}
	db, err := domain.NewDatabase(config, logger)
	assert.NoError(t, err, "NewDatabase should succeed")
	defer db.Close()

	err = db.CreateTable("users")
	assert.NoError(t, err, "CreateTable should succeed")
	err = db.CreateTable("posts")
	assert.Error(t, err, "CreateTable should fail due to limit")
	assert.Equal(t, "max tables limit reached: 1", err.Error(), "Error message should match")
	assert.Equal(t, 1, db.GetStatus().TableCount, "Table count should not exceed limit")
	assert.Equal(t, "max tables limit reached: 1", db.GetStatus().Error, "Status should reflect error")
}
