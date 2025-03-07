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

func setupCommandTest(t *testing.T) (*application.CommandHandler, func()) {
	logger := &mockLogger{}
	file, err := os.CreateTemp("", "command_test_*.db")
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
	handler := application.NewCommandHandler(db, logger)
	cleanup := func() {
		db.Close()
		os.Remove(file.Name())
	}
	return handler, cleanup
}

func TestCommandHandler_CreateTable(t *testing.T) {
	handler, cleanup := setupCommandTest(t)
	defer cleanup()

	cmd := &application.CreateTableCommand{TableName: "users"}
	err := handler.ExecuteCommand(context.Background(), cmd)
	assert.NoError(t, err, "CreateTableCommand should succeed")
	assert.Equal(t, 1, handler.DB().GetStatus().TableCount, "Table count should increase")
}

func TestCommandHandler_Insert(t *testing.T) {
	handler, cleanup := setupCommandTest(t)
	defer cleanup()

	handler.ExecuteCommand(context.Background(), &application.CreateTableCommand{TableName: "users"})
	cmd := &application.InsertCommand{TableName: "users", Key: "user1", Value: "Alice"}
	err := handler.ExecuteCommand(context.Background(), cmd)
	assert.NoError(t, err, "InsertCommand should succeed")

	value, err := handler.DB().Get("users", "user1")
	assert.NoError(t, err, "Get should succeed after insert")
	assert.Equal(t, "Alice", value, "Inserted value should match")
}

func TestCommandHandler_Delete(t *testing.T) {
	handler, cleanup := setupCommandTest(t)
	defer cleanup()

	handler.ExecuteCommand(context.Background(), &application.CreateTableCommand{TableName: "users"})
	handler.ExecuteCommand(context.Background(), &application.InsertCommand{TableName: "users", Key: "user1", Value: "Alice"})
	cmd := &application.DeleteCommand{TableName: "users", Key: "user1"}
	err := handler.ExecuteCommand(context.Background(), cmd)
	assert.NoError(t, err, "DeleteCommand should succeed")

	_, err = handler.DB().Get("users", "user1")
	assert.Error(t, err, "Get should fail after delete")
}

func TestCommandHandler_AsyncExecution(t *testing.T) {
	handler, cleanup := setupCommandTest(t)
	defer cleanup()

	handler.ExecuteCommand(context.Background(), &application.CreateTableCommand{TableName: "users"})
	cmd := &application.InsertCommand{TableName: "users", Key: "user1", Value: "Alice"}
	handler.ExecuteCommandAsync(context.Background(), cmd)
	handler.Wait()

	value, err := handler.DB().Get("users", "user1")
	assert.NoError(t, err, "Get should succeed after async insert")
	assert.Equal(t, "Alice", value, "Inserted value should match")
}
