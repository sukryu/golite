package file

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/sukryu/GoLite/pkg/ports"
)

var _ ports.StoragePort = (*File)(nil)

type FileConfig struct {
	FilePath   string
	ThreadSafe bool
}

type File struct {
	config FileConfig
	file   *os.File
	data   map[string]string
	mu     sync.RWMutex
}

func NewFile(config FileConfig) (*File, error) {
	if config.FilePath == "" {
		return nil, fmt.Errorf("file path is required")
	}

	file, err := os.OpenFile(config.FilePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %v", err)
	}

	f := &File{
		config: config,
		file:   file,
		data:   make(map[string]string),
	}

	if err := f.loadFromFile(); err != nil {
		if err := f.saveToFile(); err != nil {
			return nil, fmt.Errorf("failed to initialize file: %v", err)
		}
	}

	return f, nil
}

func (f *File) loadFromFile() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	stat, err := f.file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %v", err)
	}
	if stat.Size() == 0 {
		return nil
	}

	data, err := os.ReadFile(f.config.FilePath)
	if err != nil {
		return fmt.Errorf("failed to read file: %v", err)
	}

	if err := json.Unmarshal(data, &f.data); err != nil {
		return fmt.Errorf("failed to unmarshal data: %v", err)
	}
	return nil
}

func (f *File) saveToFile() error {
	data, err := json.Marshal(f.data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %v", err)
	}

	if err := os.WriteFile(f.config.FilePath, data, 0666); err != nil {
		return fmt.Errorf("failed to write file: %v", err)
	}

	if err := f.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %v", err)
	}
	return nil
}

func (f *File) Insert(key string, value interface{}) error {
	if f.config.ThreadSafe {
		f.mu.Lock()
		defer f.mu.Unlock()
	}

	valStr, ok := value.(string)
	if !ok {
		return fmt.Errorf("value must be string")
	}

	f.data[key] = valStr
	return f.saveToFile() // No additional lock here
}

func (f *File) Get(key string) (interface{}, error) {
	if f.config.ThreadSafe {
		f.mu.RLock()
		defer f.mu.RUnlock()
	}

	value, exists := f.data[key]
	if !exists {
		return nil, ports.ErrKeyNotFound
	}
	return value, nil
}

func (f *File) Delete(key string) error {
	if f.config.ThreadSafe {
		f.mu.Lock()
		defer f.mu.Unlock()
	}

	if _, exists := f.data[key]; !exists {
		return ports.ErrKeyNotFound
	}

	delete(f.data, key)
	return f.saveToFile() // No additional lock here
}

func (f *File) Close() error {
	if f.config.ThreadSafe {
		f.mu.Lock()
		defer f.mu.Unlock()
	}

	if err := f.saveToFile(); err != nil {
		return err
	}
	return f.file.Close()
}
