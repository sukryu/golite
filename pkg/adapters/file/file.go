package file

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/sukryu/GoLite/pkg/ports"
)

var _ ports.StoragePort = (*File)(nil)

type FileConfig struct {
	FilePath   string
	ThreadSafe bool
}

type File struct {
	config      FileConfig
	file        *os.File
	walFile     *os.File
	data        map[string]string
	mu          sync.RWMutex
	walMu       sync.Mutex
	compactChan chan struct{}
	stopChan    chan struct{}
	wg          sync.WaitGroup
	walChan     chan walEntry // Channel for async WAL writes
	buffer      []byte        // WAL buffer
	bufferMu    sync.Mutex    // Buffer-specific mutex
	flushSize   int           // Buffer flush size (e.g., 4MB)
}

type walEntry struct {
	op, key, value string
}

func NewFile(config FileConfig) (*File, error) {
	if config.FilePath == "" {
		return nil, fmt.Errorf("file path is required")
	}

	file, err := os.OpenFile(config.FilePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open main file: %v", err)
	}

	walFile, err := os.OpenFile(config.FilePath+".wal", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open wal file: %v", err)
	}

	f := &File{
		config:      config,
		file:        file,
		walFile:     walFile,
		data:        make(map[string]string),
		compactChan: make(chan struct{}, 1),
		stopChan:    make(chan struct{}),
		walChan:     make(chan walEntry, 1000), // Buffered channel
		flushSize:   4 * 1024 * 1024,           // 4MB buffer
	}

	if err := f.loadFromFile(); err != nil {
		return nil, fmt.Errorf("failed to load main file: %v", err)
	}
	if err := f.loadFromWAL(); err != nil {
		return nil, fmt.Errorf("failed to load wal file: %v", err)
	}

	f.wg.Add(1)
	go f.compactWorker()
	f.wg.Add(1)
	go f.walWorker()

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

	return json.Unmarshal(data, &f.data)
}

func (f *File) loadFromWAL() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	stat, err := f.walFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat wal file: %v", err)
	}
	if stat.Size() == 0 {
		return nil
	}

	scanner := bufio.NewScanner(f.walFile)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 3)
		if len(parts) < 3 {
			continue
		}
		op, key, value := parts[0], parts[1], parts[2]
		switch op {
		case "INSERT":
			f.data[key] = value
		case "DELETE":
			delete(f.data, key)
		}
	}
	return scanner.Err()
}

func (f *File) Insert(key string, value interface{}) error {
	valStr, ok := value.(string)
	if !ok {
		return fmt.Errorf("value must be string")
	}

	if f.config.ThreadSafe {
		f.mu.Lock()
		f.data[key] = valStr
		f.mu.Unlock()
	} else {
		f.data[key] = valStr
	}

	f.walChan <- walEntry{op: "INSERT", key: key, value: valStr}
	return nil
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
	f.walChan <- walEntry{op: "DELETE", key: key, value: ""}
	return nil
}

func (f *File) appendWAL(entry walEntry) {
	f.bufferMu.Lock()
	defer f.bufferMu.Unlock()

	logEntry := fmt.Sprintf("%s:%s:%s\n", entry.op, entry.key, entry.value)
	f.buffer = append(f.buffer, []byte(logEntry)...)

	if len(f.buffer) >= f.flushSize {
		f.flushBuffer()
	}
}

func (f *File) flushBuffer() error {
	f.walMu.Lock()
	defer f.walMu.Unlock()

	if len(f.buffer) == 0 {
		return nil
	}

	_, err := f.walFile.Write(f.buffer)
	if err != nil {
		return fmt.Errorf("failed to write to wal: %v", err)
	}
	if err := f.walFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync wal: %v", err)
	}

	f.buffer = f.buffer[:0]
	return nil
}

func (f *File) walWorker() {
	defer f.wg.Done()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-f.stopChan:
			f.bufferMu.Lock()
			f.flushBuffer()
			f.bufferMu.Unlock()
			return
		case entry := <-f.walChan:
			f.appendWAL(entry)
		case <-ticker.C:
			f.bufferMu.Lock()
			f.flushBuffer()
			f.bufferMu.Unlock()
		}
	}
}

func (f *File) compactWorker() {
	defer f.wg.Done()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-f.stopChan:
			return
		case <-f.compactChan:
			f.compact()
		case <-ticker.C:
			f.compact()
		}
	}
}

func (f *File) compact() error {
	f.mu.Lock()
	defer f.mu.Unlock()

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

	f.walMu.Lock()
	defer f.walMu.Unlock()
	if err := f.walFile.Truncate(0); err != nil {
		return fmt.Errorf("failed to truncate wal: %v", err)
	}
	if _, err := f.walFile.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to reset wal: %v", err)
	}
	if err := f.walFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync wal: %v", err)
	}

	return nil
}

func (f *File) Close() error {
	close(f.stopChan)
	close(f.walChan) // Stop WAL worker
	f.wg.Wait()

	f.bufferMu.Lock()
	f.flushBuffer() // Flush any remaining buffer
	f.bufferMu.Unlock()

	if err := f.compact(); err != nil {
		return err
	}

	if err := f.file.Close(); err != nil {
		return err
	}
	return f.walFile.Close()
}
