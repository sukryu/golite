package lockfree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"sync/atomic"
	"time"
)

// WalEntry represents a log entry.
type WalEntry struct {
	Op    byte   // 0x00 for insert, 0x01 for delete
	Key   string // key string
	Value string // value string
}

// LFWAL implements WALInterface using a lock-free ring buffer.
type LFWAL struct {
	capacity int64        // 최대 버퍼 용량
	buffer   []WalEntry   // 고정 크기 버퍼
	head     atomic.Int64 // 읽기 인덱스
	tail     atomic.Int64 // 쓰기 인덱스
	file     *os.File     // 디스크에 플러시할 파일
	closed   atomic.Bool  // 종료 플래그
}

// NewLFWAL creates a new lock-free WAL with the given capacity and file path.
// 파일은 append‑mode로 연다.
func NewLFWAL(filePath string, capacity int64) (*LFWAL, error) {
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	return &LFWAL{
		capacity: capacity,
		buffer:   make([]WalEntry, capacity),
		file:     f,
	}, nil
}

// Append appends a WalEntry to the WAL.
// 만약 버퍼가 가득 찼으면 에러를 반환합니다.
func (w *LFWAL) Append(entry WalEntry) error {
	// if closed, return error
	if w.closed.Load() {
		return errors.New("WAL is closed")
	}

	for {
		tail := w.tail.Load()
		head := w.head.Load()
		if tail-head >= w.capacity {
			// 버퍼 가득 참.
			return errors.New("WAL buffer full")
		}
		// 새로운 tail 인덱스를 예약.
		if w.tail.CompareAndSwap(tail, tail+1) {
			index := tail % w.capacity
			w.buffer[index] = entry
			return nil
		}
		// CAS 실패 시 재시도.
	}
}

// Flush writes all pending entries in the buffer to disk.
// 버퍼에 저장된 엔트리를 순서대로 디스크에 기록하고 head 인덱스를 tail로 업데이트합니다.
func (w *LFWAL) Flush() error {
	// Read current head and tail.
	head := w.head.Load()
	tail := w.tail.Load()
	count := tail - head
	if count <= 0 {
		return nil
	}

	// Create a temporary buffer to hold binary data.
	var buf bytes.Buffer
	// For each entry, write: Op, key length, key bytes, value length, value bytes.
	for i := head; i < tail; i++ {
		entry := w.buffer[i%w.capacity]
		// Write Op.
		if err := buf.WriteByte(entry.Op); err != nil {
			return err
		}
		// Write key length and key.
		keyBytes := []byte(entry.Key)
		if err := binary.Write(&buf, binary.BigEndian, uint16(len(keyBytes))); err != nil {
			return err
		}
		if _, err := buf.Write(keyBytes); err != nil {
			return err
		}
		// Write value length and value.
		valBytes := []byte(entry.Value)
		if err := binary.Write(&buf, binary.BigEndian, uint16(len(valBytes))); err != nil {
			return err
		}
		if _, err := buf.Write(valBytes); err != nil {
			return err
		}
	}
	// Write buffer to file.
	_, err := w.file.Write(buf.Bytes())
	if err != nil {
		return err
	}
	// Force sync to disk.
	if err := w.file.Sync(); err != nil {
		return err
	}
	// Update head index.
	w.head.Store(tail)
	return nil
}

// Reset flushes pending entries and resets the buffer indices.
func (w *LFWAL) Reset() error {
	if err := w.Flush(); err != nil {
		return err
	}
	w.head.Store(0)
	w.tail.Store(0)
	return nil
}

// Close flushes pending entries, closes the file, and marks WAL as closed.
func (w *LFWAL) Close() error {
	// Mark as closed.
	w.closed.Store(true)
	// Flush pending entries.
	if err := w.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}

// EntryCount returns the current number of pending entries.
func (w *LFWAL) EntryCount() int64 {
	return w.tail.Load() - w.head.Load()
}

// Simulate asynchronous flush: periodically flush every flushInterval.
func (w *LFWAL) StartFlushWorker(flushInterval time.Duration, stopCh <-chan struct{}) {
	go func() {
		ticker := time.NewTicker(flushInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := w.Flush(); err != nil {
					// log error in production.
				}
			case <-stopCh:
				return
			}
		}
	}()
}
