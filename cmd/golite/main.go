package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/sukryu/GoLite/pkg/adapters/btree"
	"github.com/sukryu/GoLite/pkg/adapters/file"
	"github.com/sukryu/GoLite/pkg/application"
	"github.com/sukryu/GoLite/pkg/domain"
	"github.com/sukryu/GoLite/pkg/utils"
)

type Config struct {
	StorageType string
	FilePath    string
	ThreadSafe  bool
}

func main() {
	config := Config{}
	flag.StringVar(&config.StorageType, "storage", "btree", "Storage type (btree or file)")
	flag.StringVar(&config.FilePath, "file", "golite.db", "Database file path")
	flag.BoolVar(&config.ThreadSafe, "threadsafe", true, "Enable thread safety")
	flag.Parse()

	logger := utils.NewSimpleLogger()

	dbConfig := domain.DatabaseConfig{
		Name:       "golite",
		FilePath:   config.FilePath,
		MaxTables:  100,
		ThreadSafe: config.ThreadSafe,
	}
	var db *domain.Database
	var err error
	if config.StorageType == "file" {
		dbConfig.UsePages = false // File adapter doesn't use pages
		f, err := file.NewFile(file.FileConfig{FilePath: config.FilePath, ThreadSafe: config.ThreadSafe})
		if err != nil {
			logger.Error(fmt.Sprintf("Failed to initialize file storage: %v", err))
			os.Exit(1)
		}
		fileHandle, _ := os.OpenFile(config.FilePath, os.O_RDWR|os.O_CREATE, 0666)
		db, err = domain.NewDatabaseWithStorage(dbConfig, f, fileHandle, logger)
		if err != nil {
			logger.Error(fmt.Sprintf("Failed to initialize database with file storage: %v", err))
			os.Exit(1)
		}
	} else {
		dbConfig.UsePages = true
		dbConfig.BtConfig = btree.BtConfig{
			Degree:     32,
			PageSize:   4096,
			ThreadSafe: config.ThreadSafe,
			CacheSize:  10,
		}
		db, err = domain.NewDatabase(dbConfig, logger)
		if err != nil {
			logger.Error(fmt.Sprintf("Failed to initialize database: %v", err))
			os.Exit(1)
		}
	}
	defer db.Close()

	cmdHandler := application.NewCommandHandler(db, logger)
	queryHandler := application.NewQueryHandler(db, logger)

	ctx := context.Background()

	err = cmdHandler.ExecuteCommand(ctx, &application.CreateTableCommand{TableName: "users"})
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to create table: %v", err))
		os.Exit(1)
	}

	cmdHandler.ExecuteCommandAsync(ctx, &application.InsertCommand{TableName: "users", Key: "user1", Value: "Alice"})
	cmdHandler.ExecuteCommandAsync(ctx, &application.InsertCommand{TableName: "users", Key: "user2", Value: "Bob"})
	cmdHandler.Wait()

	resultChan := queryHandler.ExecuteQueryAsync(ctx, &application.GetValueQuery{TableName: "users", Key: "user1"})
	res := <-resultChan
	if res.Err != nil {
		logger.Error(fmt.Sprintf("Failed to query user1: %v", res.Err))
	} else {
		fmt.Printf("User1: %s\n", res.Result)
	}

	statusResult, err := queryHandler.ExecuteQuery(ctx, &application.GetStatusQuery{})
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to query status: %v", err))
	} else {
		status := statusResult.(domain.DatabaseStatus)
		fmt.Printf("Database Status: Ready=%v, TableCount=%d\n", status.Ready, status.TableCount)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	logger.Info("Shutting down GoLite...")
	cmdHandler.Wait()
	queryHandler.Wait()
}
