package bench

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/sukryu/GoLite/pkg/adapters/btree"
	newfile "github.com/sukryu/GoLite/pkg/adapters/file"
	"github.com/sukryu/GoLite/pkg/application"
	"github.com/sukryu/GoLite/pkg/domain"
	"github.com/sukryu/GoLite/pkg/utils"
)

func setupBench(storageType string, b *testing.B) (*application.CommandHandler, *application.QueryHandler, func()) {
	logger := &utils.SilentLogger{} // Silent logger to reduce output noise
	file, err := os.CreateTemp("", "bench_test_*.db")
	if err != nil {
		b.Fatalf("failed to create temp file: %v", err)
	}
	config := domain.DatabaseConfig{
		Name:       "benchdb",
		FilePath:   file.Name(),
		MaxTables:  100,
		ThreadSafe: true,
	}
	var db *domain.Database
	if storageType == "file" {
		config.UsePages = false
		f, err := newfile.NewFile(newfile.FileConfig{FilePath: config.FilePath, ThreadSafe: true})
		if err != nil {
			b.Fatalf("failed to initialize file storage: %v", err)
		}
		fileHandle, _ := os.OpenFile(config.FilePath, os.O_RDWR|os.O_CREATE, 0666)
		db, err = domain.NewDatabaseWithStorage(config, f, fileHandle, logger)
		if err != nil {
			b.Fatalf("failed to initialize database with file storage: %v", err)
		}
	} else {
		config.UsePages = true
		config.BtConfig = btree.BtConfig{
			Degree:     32,
			PageSize:   4096,
			ThreadSafe: true,
			CacheSize:  10,
		}
		db, err = domain.NewDatabase(config, logger)
		if err != nil {
			b.Fatalf("failed to initialize database: %v", err)
		}
	}
	cmdHandler := application.NewCommandHandler(db, logger)
	queryHandler := application.NewQueryHandler(db, logger)
	cleanup := func() {
		db.Close()
		os.Remove(file.Name())
	}
	cmdHandler.ExecuteCommand(context.Background(), &application.CreateTableCommand{TableName: "users"})
	return cmdHandler, queryHandler, cleanup
}

// BenchmarkInsertSequential benchmarks sequential insert operations.
func BenchmarkInsertSequential(b *testing.B) {
	for _, storage := range []string{"btree", "file"} {
		b.Run(storage, func(b *testing.B) {
			cmdHandler, _, cleanup := setupBench(storage, b)
			defer cleanup()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				cmd := &application.InsertCommand{
					TableName: "users",
					Key:       fmt.Sprintf("key%d", i),
					Value:     fmt.Sprintf("value%d", i),
				}
				err := cmdHandler.ExecuteCommand(context.Background(), cmd)
				if err != nil {
					b.Fatalf("Insert failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkGetSequential benchmarks sequential get operations.
func BenchmarkGetSequential(b *testing.B) {
	for _, storage := range []string{"btree", "file"} {
		b.Run(storage, func(b *testing.B) {
			cmdHandler, queryHandler, cleanup := setupBench(storage, b)
			defer cleanup()
			// Pre-populate data
			for i := 0; i < 1000; i++ {
				cmdHandler.ExecuteCommand(context.Background(), &application.InsertCommand{
					TableName: "users",
					Key:       fmt.Sprintf("key%d", i),
					Value:     fmt.Sprintf("value%d", i),
				})
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				query := &application.GetValueQuery{
					TableName: "users",
					Key:       fmt.Sprintf("key%d", i%1000),
				}
				_, err := queryHandler.ExecuteQuery(context.Background(), query)
				if err != nil {
					b.Fatalf("Get failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkInsertConcurrent benchmarks concurrent insert operations.
func BenchmarkInsertConcurrent(b *testing.B) {
	for _, storage := range []string{"btree", "file"} {
		b.Run(storage, func(b *testing.B) {
			cmdHandler, _, cleanup := setupBench(storage, b)
			defer cleanup()
			b.ResetTimer()
			var wg sync.WaitGroup
			for i := 0; i < b.N; i++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					cmd := &application.InsertCommand{
						TableName: "users",
						Key:       fmt.Sprintf("key%d", id),
						Value:     fmt.Sprintf("value%d", id),
					}
					err := cmdHandler.ExecuteCommand(context.Background(), cmd)
					if err != nil {
						b.Errorf("Insert failed: %v", err)
					}
				}(i)
			}
			wg.Wait()
		})
	}
}

// BenchmarkGetConcurrent benchmarks concurrent get operations.
func BenchmarkGetConcurrent(b *testing.B) {
	for _, storage := range []string{"btree", "file"} {
		b.Run(storage, func(b *testing.B) {
			cmdHandler, queryHandler, cleanup := setupBench(storage, b)
			defer cleanup()
			// Pre-populate data
			for i := 0; i < 1000; i++ {
				cmdHandler.ExecuteCommand(context.Background(), &application.InsertCommand{
					TableName: "users",
					Key:       fmt.Sprintf("key%d", i),
					Value:     fmt.Sprintf("value%d", i),
				})
			}
			b.ResetTimer()
			var wg sync.WaitGroup
			for i := 0; i < b.N; i++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					query := &application.GetValueQuery{
						TableName: "users",
						Key:       fmt.Sprintf("key%d", id%1000),
					}
					_, err := queryHandler.ExecuteQuery(context.Background(), query)
					if err != nil {
						b.Errorf("Get failed: %v", err)
					}
				}(i)
			}
			wg.Wait()
		})
	}
}
