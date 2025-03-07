package domain

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sync"

	"github.com/sukryu/GoLite/pkg/adapters/btree"
	"github.com/sukryu/GoLite/pkg/ports"
	"github.com/sukryu/GoLite/pkg/utils"
)

// DatabaseConfig defines the configuration for a Database, inspired by K8s resource spec.
type DatabaseConfig struct {
	Name       string         // Database name (like K8s resource name)
	FilePath   string         // File path for persistence
	BtConfig   btree.BtConfig // B-tree configuration
	MaxTables  int            // Maximum number of tables (resource limit)
	ThreadSafe bool           // Enable thread safety
}

// DatabaseSpec defines the desired state of a Database, K8s-style.
type DatabaseSpec struct {
	Tables map[string]*TableSpec // Desired tables
}

// DatabaseStatus defines the observed state of a Database, K8s-style.
type DatabaseStatus struct {
	TableCount int    // Number of tables
	Ready      bool   // Database readiness
	Error      string // Last error, if any
}

// Database is the aggregate root for managing tables, inspired by SQLite's struct sqlite.
type Database struct {
	config  DatabaseConfig
	spec    DatabaseSpec
	status  DatabaseStatus
	file    *os.File
	storage ports.StoragePort // B-tree adapter
	mu      sync.RWMutex      // Thread safety
	logger  utils.Logger      // Logging for production readiness
}

// TableSpec defines the desired state of a Table, K8s-style.
type TableSpec struct {
	Name string // Table name
}

// NewDatabase creates a new Database instance with production-ready features.
func NewDatabase(config DatabaseConfig, logger utils.Logger) (*Database, error) {
	if config.Name == "" || config.FilePath == "" {
		return nil, fmt.Errorf("database name and file path are required")
	}
	if config.MaxTables <= 0 {
		config.MaxTables = 100
	}

	file, err := os.OpenFile(config.FilePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open database file: %v", err)
	}

	// Ensure file is at least 2 pages long (B-tree header + Database metadata)
	minSize := int64(config.BtConfig.PageSize * 2)
	if stat, err := file.Stat(); err == nil && stat.Size() < minSize {
		if err := file.Truncate(minSize); err != nil {
			return nil, fmt.Errorf("failed to extend file to %d bytes: %v", minSize, err)
		}
	}

	storage := btree.NewBtree(file, config.BtConfig)
	db := &Database{
		config:  config,
		spec:    DatabaseSpec{Tables: make(map[string]*TableSpec)},
		status:  DatabaseStatus{Ready: true},
		file:    file,
		storage: storage,
		logger:  logger,
	}

	if err := db.loadHeader(); err != nil {
		db.logger.Warn(fmt.Sprintf("failed to load header, initializing new: %v", err))
		if err := db.saveHeader(); err != nil {
			return nil, err
		}
	}
	logger.Info(fmt.Sprintf("Database %s initialized with file %s", config.Name, config.FilePath))
	return db, nil
}

// loadHeader reads table metadata from page 1 (B-tree uses page 0).
func (db *Database) loadHeader() error {
	data := make([]byte, db.config.BtConfig.PageSize)
	n, err := db.file.ReadAt(data, int64(db.config.BtConfig.PageSize)) // Page 1
	if err != nil && err.Error() != "EOF" {
		return fmt.Errorf("failed to read header at offset %d: %v", db.config.BtConfig.PageSize, err)
	}
	if n == 0 || (err != nil && err.Error() == "EOF") {
		db.logger.Info("No header data found, assuming new database")
		return nil // New file, no tables yet
	}

	buf := bytes.NewReader(data)
	var tableCount uint32
	if err := binary.Read(buf, binary.LittleEndian, &tableCount); err != nil {
		db.logger.Warn(fmt.Sprintf("Failed to read table count: %v, assuming empty", err))
		return nil
	}

	for i := uint32(0); i < tableCount; i++ {
		var nameLen uint16
		if err := binary.Read(buf, binary.LittleEndian, &nameLen); err != nil {
			db.logger.Warn(fmt.Sprintf("Failed to read table name length at index %d: %v", i, err))
			break
		}
		nameBytes := make([]byte, nameLen)
		if _, err := buf.Read(nameBytes); err != nil {
			db.logger.Warn(fmt.Sprintf("Failed to read table name at index %d: %v", i, err))
			break
		}
		name := string(nameBytes)
		db.spec.Tables[name] = &TableSpec{Name: name}
	}

	db.status.TableCount = len(db.spec.Tables)
	db.logger.Info(fmt.Sprintf("Loaded %d tables from header", db.status.TableCount))
	return nil
}

// saveHeader writes table metadata to page 1.
func (db *Database) saveHeader() error {
	buf := bytes.NewBuffer(make([]byte, 0, db.config.BtConfig.PageSize))

	if err := binary.Write(buf, binary.LittleEndian, uint32(len(db.spec.Tables))); err != nil {
		return fmt.Errorf("failed to write table count: %v", err)
	}
	for name := range db.spec.Tables {
		nameLen := uint16(len(name))
		if err := binary.Write(buf, binary.LittleEndian, nameLen); err != nil {
			return fmt.Errorf("failed to write table name length: %v", err)
		}
		if _, err := buf.WriteString(name); err != nil {
			return fmt.Errorf("failed to write table name: %v", err)
		}
	}

	data := buf.Bytes()
	if len(data) > db.config.BtConfig.PageSize {
		return fmt.Errorf("header exceeds page size: %d > %d", len(data), db.config.BtConfig.PageSize)
	}
	padded := make([]byte, db.config.BtConfig.PageSize)
	copy(padded, data)
	_, err := db.file.WriteAt(padded, int64(db.config.BtConfig.PageSize)) // Page 1
	if err != nil {
		return fmt.Errorf("failed to write header: %v", err)
	}
	if err := db.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync header: %v", err)
	}
	db.logger.Info("Saved header with table metadata")
	return nil
}

func (db *Database) CreateTable(name string) error {
	if db.config.ThreadSafe {
		db.mu.Lock()
		defer db.mu.Unlock()
	}
	if db.status.TableCount >= db.config.MaxTables {
		err := fmt.Errorf("max tables limit reached: %d", db.config.MaxTables)
		db.status.Error = err.Error()
		db.logger.Error(err.Error())
		return err
	}
	if _, exists := db.spec.Tables[name]; exists {
		return fmt.Errorf("table %s already exists", name)
	}
	db.spec.Tables[name] = &TableSpec{Name: name}
	db.status.TableCount++
	if err := db.saveHeader(); err != nil {
		return err
	}
	db.logger.Info(fmt.Sprintf("Table %s created in database %s", name, db.config.Name))
	return nil
}

func (db *Database) DropTable(name string) error {
	if db.config.ThreadSafe {
		db.mu.Lock()
		defer db.mu.Unlock()
	}
	if _, exists := db.spec.Tables[name]; !exists {
		err := fmt.Errorf("table %s not found", name)
		db.status.Error = err.Error()
		db.logger.Error(err.Error())
		return err
	}
	delete(db.spec.Tables, name)
	db.status.TableCount--
	if err := db.saveHeader(); err != nil {
		return err
	}
	db.logger.Info(fmt.Sprintf("Table %s dropped from database %s", name, db.config.Name))
	return nil
}

// Insert inserts a key-value pair into a table.
func (db *Database) Insert(tableName, key, value string) error {
	if db.config.ThreadSafe {
		db.mu.Lock()
		defer db.mu.Unlock()
	}

	if _, exists := db.spec.Tables[tableName]; !exists {
		return fmt.Errorf("table %s not found", tableName)
	}

	// Prefix key with table name for B-tree storage
	prefixedKey := fmt.Sprintf("%s:%s", tableName, key)
	err := db.storage.Insert(prefixedKey, value)
	if err != nil {
		db.status.Error = err.Error()
		db.logger.Error(fmt.Sprintf("Failed to insert into %s: %v", tableName, err))
		return err
	}

	// TODO: Emit InsertEvent (for event-driven architecture)
	db.logger.Info(fmt.Sprintf("Inserted key %s into table %s", key, tableName))
	return nil
}

// Get retrieves a value from a table by key.
func (db *Database) Get(tableName, key string) (string, error) {
	if db.config.ThreadSafe {
		db.mu.RLock()
		defer db.mu.RUnlock()
	}

	if _, exists := db.spec.Tables[tableName]; !exists {
		return "", fmt.Errorf("table %s not found", tableName)
	}

	prefixedKey := fmt.Sprintf("%s:%s", tableName, key)
	value, err := db.storage.Get(prefixedKey)
	if err != nil {
		db.logger.Warn(fmt.Sprintf("Key %s not found in table %s: %v", key, tableName, err))
		return "", err
	}

	return value.(string), nil
}

// Delete removes a key-value pair from a table.
func (db *Database) Delete(tableName, key string) error {
	if db.config.ThreadSafe {
		db.mu.Lock()
		defer db.mu.Unlock()
	}

	if _, exists := db.spec.Tables[tableName]; !exists {
		return fmt.Errorf("table %s not found", tableName)
	}

	prefixedKey := fmt.Sprintf("%s:%s", tableName, key)
	err := db.storage.Delete(prefixedKey)
	if err != nil {
		db.status.Error = err.Error()
		db.logger.Error(fmt.Sprintf("Failed to delete key %s from %s: %v", key, tableName, err))
		return err
	}

	// TODO: Emit DeleteEvent (for event-driven architecture)
	db.logger.Info(fmt.Sprintf("Deleted key %s from table %s", key, tableName))
	return nil
}

// Close gracefully shuts down the database.
func (db *Database) Close() error {
	if db.config.ThreadSafe {
		db.mu.Lock()
		defer db.mu.Unlock()
	}

	err := db.file.Close()
	if err != nil {
		db.logger.Error(fmt.Sprintf("Failed to close database %s: %v", db.config.Name, err))
		return err
	}
	db.status.Ready = false
	db.logger.Info(fmt.Sprintf("Database %s closed", db.config.Name))
	return nil
}

// GetStatus returns the current status of the database.
func (db *Database) GetStatus() DatabaseStatus {
	if db.config.ThreadSafe {
		db.mu.RLock()
		defer db.mu.RUnlock()
	}
	return db.status
}

// GetSpec returns the current spec of the database.
func (db *Database) GetSpec() DatabaseSpec {
	if db.config.ThreadSafe {
		db.mu.RLock()
		defer db.mu.RUnlock()
	}
	return db.spec
}
