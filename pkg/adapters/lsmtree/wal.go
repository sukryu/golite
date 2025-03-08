package lsmtree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var ErrWALFull = errors.New("WAL channel is full")

var entryPool = sync.Pool{
	New: func() interface{} { return new(bytes.Buffer) },
}

// WAL represents the Write-Ahead Log with asynchronous writes.
type WAL struct {
	file       *os.File
	mu         sync.Mutex
	syncWrites bool
	walCh      chan WalEntry
	wg         sync.WaitGroup
	// Atomic counter for appended entries.
	entryCount int64
}

// NewWAL opens or creates a WAL file.
func NewWAL(path string, syncWrites bool) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	w := &WAL{
		file:       file,
		syncWrites: syncWrites,
		walCh:      make(chan WalEntry, 30000),
	}
	w.wg.Add(1)
	go w.worker()
	return w, nil
}

// WalEntry represents a record in the WAL.
type WalEntry struct {
	Op    byte // 0x00 for insert, 0x01 for delete
	Key   string
	Value string
}

// Append writes a WAL entry asynchronously.
func (w *WAL) Append(entry WalEntry) error {
	// 원자적 카운터 증가
	atomic.AddInt64(&w.entryCount, 1)
	w.walCh <- entry
	return nil
}

// worker processes WAL entries from the channel.
func (w *WAL) worker() {
	defer w.wg.Done()
	for entry := range w.walCh {
		buf := entryPool.Get().(*bytes.Buffer)
		buf.Reset()
		buf.WriteByte(entry.Op)
		binary.Write(buf, binary.BigEndian, uint16(len(entry.Key)))
		buf.Write([]byte(entry.Key))
		binary.Write(buf, binary.BigEndian, uint16(len(entry.Value)))
		buf.Write([]byte(entry.Value))

		w.mu.Lock()
		w.file.Write(buf.Bytes())
		if w.syncWrites {
			w.file.Sync()
		}
		w.mu.Unlock()

		entryPool.Put(buf)
	}
}

// Reset truncates and resets the WAL file.
func (w *WAL) Reset() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.file.Close(); err != nil {
		return err
	}
	file, err := os.OpenFile(w.file.Name(), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	w.file = file
	// 리셋 후 카운터도 초기화.
	atomic.StoreInt64(&w.entryCount, 0)
	return nil
}

// Close shuts down the WAL gracefully.
func (w *WAL) Close() error {
	close(w.walCh)
	w.wg.Wait()
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

// Flush waits until the WAL channel is empty.
func (w *WAL) Flush() {
	for {
		if len(w.walCh) == 0 {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}
}
