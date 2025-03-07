package unit

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/sukryu/GoLite/pkg/adapters/btree"
	"github.com/sukryu/GoLite/pkg/application"
	"github.com/sukryu/GoLite/pkg/domain"
)

func setupQueryTest(t *testing.T) (*application.QueryHandler, func()) {
	logger := &mockLogger{}
	file, err := os.CreateTemp("", "query_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	config := domain.DatabaseConfig{
		Name:       "testdb",
		FilePath:   file.Name(),
		BtConfig:   btree.BtConfig{Degree: 2, PageSize: 4096, ThreadSafe: true},
		MaxTables:  10,
		ThreadSafe: true,
	}
	db, err := domain.NewDatabase(config, logger)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	handler := application.NewQueryHandler(db, logger)
	cleanup := func() {
		db.Close()
		os.Remove(file.Name())
	}
	return handler, cleanup
}

func TestQueryHandler_GetValue(t *testing.T) {
	handler, cleanup := setupQueryTest(t)
	defer cleanup()

	handler.DB().CreateTable("users")
	handler.DB().Insert("users", "user1", "Alice")
	query := &application.GetValueQuery{TableName: "users", Key: "user1"}
	result, err := handler.ExecuteQuery(context.Background(), query)
	assert.NoError(t, err, "GetValueQuery should succeed")
	assert.Equal(t, "Alice", result, "Queried value should match")
}

func TestQueryHandler_GetStatus(t *testing.T) {
	handler, cleanup := setupQueryTest(t)
	defer cleanup()

	handler.DB().CreateTable("users")
	query := &application.GetStatusQuery{}
	result, err := handler.ExecuteQuery(context.Background(), query)
	assert.NoError(t, err, "GetStatusQuery should succeed")
	status := result.(domain.DatabaseStatus)
	assert.Equal(t, 1, status.TableCount, "Status should reflect table count")
	assert.True(t, status.Ready, "Database should be ready")
}

func TestQueryHandler_GetSpec(t *testing.T) {
	handler, cleanup := setupQueryTest(t)
	defer cleanup()

	handler.DB().CreateTable("users")
	query := &application.GetSpecQuery{}
	result, err := handler.ExecuteQuery(context.Background(), query)
	assert.NoError(t, err, "GetSpecQuery should succeed")
	spec := result.(domain.DatabaseSpec)
	assert.Contains(t, spec.Tables, "users", "Spec should include created table")
}

func TestQueryHandler_AsyncExecution(t *testing.T) {
	handler, cleanup := setupQueryTest(t)
	defer cleanup()

	handler.DB().CreateTable("users")
	handler.DB().Insert("users", "user1", "Alice")
	query := &application.GetValueQuery{TableName: "users", Key: "user1"}
	resultChan := handler.ExecuteQueryAsync(context.Background(), query)
	res := <-resultChan
	assert.NoError(t, res.Err, "Async GetValueQuery should succeed")
	assert.Equal(t, "Alice", res.Result, "Queried value should match")
	handler.Wait()
}
