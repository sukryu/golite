package application

import (
	"context"
	"fmt"
	"sync"

	"github.com/sukryu/GoLite/pkg/domain"
	"github.com/sukryu/GoLite/pkg/utils"
)

// QueryHandler handles execution of queries against the database.
type QueryHandler struct {
	db     *domain.Database
	logger utils.Logger
	wg     sync.WaitGroup // For async query execution tracking
}

// NewQueryHandler creates a new QueryHandler instance.
func NewQueryHandler(db *domain.Database, logger utils.Logger) *QueryHandler {
	return &QueryHandler{
		db:     db,
		logger: logger,
	}
}

// Query defines the interface for all queries.
type Query interface {
	Execute(ctx context.Context, handler *QueryHandler) (interface{}, error)
}

// GetValueQuery represents a query to retrieve a value by key from a table.
type GetValueQuery struct {
	TableName string
	Key       string
}

// Execute executes the GetValueQuery.
func (q *GetValueQuery) Execute(ctx context.Context, handler *QueryHandler) (interface{}, error) {
	handler.logger.Info(fmt.Sprintf("Executing GetValueQuery for key %s in table %s", q.Key, q.TableName))
	value, err := handler.db.Get(q.TableName, q.Key)
	if err != nil {
		handler.logger.Warn(fmt.Sprintf("Failed to get key %s from table %s: %v", q.Key, q.TableName, err))
		return nil, err
	}
	return value, nil
}

// GetStatusQuery represents a query to retrieve the database status.
type GetStatusQuery struct{}

// Execute executes the GetStatusQuery.
func (q *GetStatusQuery) Execute(ctx context.Context, handler *QueryHandler) (interface{}, error) {
	handler.logger.Info("Executing GetStatusQuery")
	status := handler.db.GetStatus()
	return status, nil
}

// GetSpecQuery represents a query to retrieve the database spec.
type GetSpecQuery struct{}

// Execute executes the GetSpecQuery.
func (q *GetSpecQuery) Execute(ctx context.Context, handler *QueryHandler) (interface{}, error) {
	handler.logger.Info("Executing GetSpecQuery")
	spec := handler.db.GetSpec()
	return spec, nil
}

// ExecuteQuery executes a query synchronously and returns the result.
func (h *QueryHandler) ExecuteQuery(ctx context.Context, query Query) (interface{}, error) {
	return query.Execute(ctx, h)
}

// ExecuteQueryAsync executes a query asynchronously and returns a channel for the result.
func (h *QueryHandler) ExecuteQueryAsync(ctx context.Context, query Query) <-chan QueryResult {
	resultChan := make(chan QueryResult, 1)
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		result, err := query.Execute(ctx, h)
		resultChan <- QueryResult{Result: result, Err: err}
		close(resultChan)
	}()
	return resultChan
}

// Wait waits for all asynchronous queries to complete.
func (h *QueryHandler) Wait() {
	h.wg.Wait()
}

func (h *QueryHandler) DB() *domain.Database {
	return h.db
}

// QueryResult wraps the result and error of an asynchronous query.
type QueryResult struct {
	Result interface{}
	Err    error
}
