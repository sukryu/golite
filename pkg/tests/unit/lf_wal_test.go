package unit

import (
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sukryu/GoLite/pkg/adapters/lockfree"
)

func TestLFWALAppendAndFlush(t *testing.T) {
	// 임시 파일 생성
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "wal.log")

	// 용량 1000개의 엔트리를 수용하도록 설정.
	wal, err := lockfree.NewLFWAL(filePath, 1000)
	if err != nil {
		t.Fatalf("Failed to create LFWAL: %v", err)
	}
	defer func() {
		if err := wal.Close(); err != nil {
			t.Errorf("Close failed: %v", err)
		}
	}()

	// 500개의 엔트리를 추가.
	entryCount := 500
	for i := 0; i < entryCount; i++ {
		entry := lockfree.WalEntry{
			Op:    0x00,
			Key:   "key" + strconv.Itoa(i),
			Value: "value" + strconv.Itoa(i),
		}
		if err := wal.Append(entry); err != nil {
			t.Fatalf("Append failed at i=%d: %v", i, err)
		}
	}
	if count := wal.EntryCount(); count != int64(entryCount) {
		t.Errorf("Expected entry count %d, got %d", entryCount, count)
	}

	// Flush 엔트리들을 디스크에 기록.
	if err := wal.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	if count := wal.EntryCount(); count != 0 {
		t.Errorf("Expected entry count 0 after flush, got %d", count)
	}
}

func TestLFWALReset(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "wal_reset.log")

	wal, err := lockfree.NewLFWAL(filePath, 1000)
	if err != nil {
		t.Fatalf("Failed to create LFWAL: %v", err)
	}
	defer wal.Close()

	// Append 일부 엔트리
	for i := 0; i < 300; i++ {
		entry := lockfree.WalEntry{
			Op:    0x00,
			Key:   "reset_key" + strconv.Itoa(i),
			Value: "reset_value" + strconv.Itoa(i),
		}
		if err := wal.Append(entry); err != nil {
			t.Fatalf("Append failed at i=%d: %v", i, err)
		}
	}
	// Reset
	if err := wal.Reset(); err != nil {
		t.Fatalf("Reset failed: %v", err)
	}
	if count := wal.EntryCount(); count != 0 {
		t.Errorf("Expected entry count 0 after reset, got %d", count)
	}
}

func TestLFWALConcurrentAppend(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "wal_concurrent.log")
	// 충분한 용량 할당
	wal, err := lockfree.NewLFWAL(filePath, 10000)
	if err != nil {
		t.Fatalf("Failed to create LFWAL: %v", err)
	}
	defer wal.Close()

	totalEntries := 5000
	concurrency := 10
	var totalAppended int64

	doneCh := make(chan struct{})
	for i := 0; i < concurrency; i++ {
		go func(offset int) {
			for j := 0; j < totalEntries/concurrency; j++ {
				entry := lockfree.WalEntry{
					Op:    0x00,
					Key:   "key" + strconv.Itoa(offset*1000+j),
					Value: "value" + strconv.Itoa(offset*1000+j),
				}
				// Append until success
				for {
					if err := wal.Append(entry); err == nil {
						atomic.AddInt64(&totalAppended, 1)
						break
					} else {
						// 버퍼가 가득 찼다면 Flush하고 재시도.
						if err := wal.Flush(); err != nil {
							t.Errorf("Flush during concurrent append failed: %v", err)
						}
						time.Sleep(1 * time.Millisecond)
					}
				}
			}
			doneCh <- struct{}{}
		}(i)
	}

	// 모든 고루틴 완료 대기
	for i := 0; i < concurrency; i++ {
		<-doneCh
	}
	if totalAppended != int64(totalEntries) {
		t.Errorf("Expected total appended %d, got %d", totalEntries, totalAppended)
	}

	// Flush 후 엔트리 카운터 확인
	if err := wal.Flush(); err != nil {
		t.Errorf("Final flush failed: %v", err)
	}
	if count := wal.EntryCount(); count != 0 {
		t.Errorf("Expected entry count 0 after final flush, got %d", count)
	}
}
