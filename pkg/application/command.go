package application

import (
	"context"
	"fmt"
	"sync"

	"github.com/sukryu/GoLite/pkg/domain"
	"github.com/sukryu/GoLite/pkg/utils"
)

// CommandHandler handles execution of commands against the database.
type CommandHandler struct {
	db     *domain.Database
	logger utils.Logger
	wg     sync.WaitGroup // For async command execution tracking
}

// NewCommandHandler creates a new CommandHandler instance.
func NewCommandHandler(db *domain.Database, logger utils.Logger) *CommandHandler {
	return &CommandHandler{
		db:     db,
		logger: logger,
	}
}

// Command defines the interface for all commands.
type Command interface {
	Execute(ctx context.Context, handler *CommandHandler) error
}

// CreateTableCommand represents a command to create a table.
type CreateTableCommand struct {
	TableName string
}

// Execute executes the CreateTableCommand.
func (c *CreateTableCommand) Execute(ctx context.Context, handler *CommandHandler) error {
	handler.logger.Info(fmt.Sprintf("Executing CreateTableCommand for table %s", c.TableName))
	err := handler.db.CreateTable(c.TableName)
	if err != nil {
		handler.logger.Error(fmt.Sprintf("Failed to create table %s: %v", c.TableName, err))
		return err
	}
	return nil
}

// DropTableCommand represents a command to drop a table.
type DropTableCommand struct {
	TableName string
}

// Execute executes the DropTableCommand.
func (c *DropTableCommand) Execute(ctx context.Context, handler *CommandHandler) error {
	handler.logger.Info(fmt.Sprintf("Executing DropTableCommand for table %s", c.TableName))
	err := handler.db.DropTable(c.TableName)
	if err != nil {
		handler.logger.Error(fmt.Sprintf("Failed to drop table %s: %v", c.TableName, err))
		return err
	}
	return nil
}

// InsertCommand represents a command to insert a key-value pair into a table.
type InsertCommand struct {
	TableName string
	Key       string
	Value     string
}

// Execute executes the InsertCommand.
func (c *InsertCommand) Execute(ctx context.Context, handler *CommandHandler) error {
	handler.logger.Info(fmt.Sprintf("Executing InsertCommand for key %s in table %s", c.Key, c.TableName))
	err := handler.db.Insert(c.TableName, c.Key, c.Value)
	if err != nil {
		handler.logger.Error(fmt.Sprintf("Failed to insert key %s into table %s: %v", c.Key, c.TableName, err))
		return err
	}
	return nil
}

// DeleteCommand represents a command to delete a key-value pair from a table.
type DeleteCommand struct {
	TableName string
	Key       string
}

// Execute executes the DeleteCommand.
func (c *DeleteCommand) Execute(ctx context.Context, handler *CommandHandler) error {
	handler.logger.Info(fmt.Sprintf("Executing DeleteCommand for key %s in table %s", c.Key, c.TableName))
	err := handler.db.Delete(c.TableName, c.Key)
	if err != nil {
		handler.logger.Error(fmt.Sprintf("Failed to delete key %s from table %s: %v", c.Key, c.TableName, err))
		return err
	}
	return nil
}

// ExecuteCommand executes a command synchronously.
func (h *CommandHandler) ExecuteCommand(ctx context.Context, cmd Command) error {
	return cmd.Execute(ctx, h)
}

// ExecuteCommandAsync executes a command asynchronously.
func (h *CommandHandler) ExecuteCommandAsync(ctx context.Context, cmd Command) {
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		if err := cmd.Execute(ctx, h); err != nil {
			h.logger.Error(fmt.Sprintf("Async command execution failed: %v", err))
		}
	}()
}

func (h *CommandHandler) DB() *domain.Database {
	return h.db
}

// Wait waits for all asynchronous commands to complete.
func (h *CommandHandler) Wait() {
	h.wg.Wait()
}
