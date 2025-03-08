package unit

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/sukryu/GoLite/pkg/adapters/lsmtree"
)

// createTempDir는 테스트용 임시 디렉토리를 생성합니다.
func createTempDir(t *testing.T) string {
	dir, err := os.MkdirTemp("", "lsmtree_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	return dir
}

// removeTempDir는 테스트용 임시 디렉토리를 삭제합니다.
func removeTempDir(t *testing.T, dir string) {
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("failed to remove temp dir: %v", err)
	}
}

// TestBasicOperations는 Insert, Get, Delete 기본 연산을 검증합니다.
func TestBasicOperations(t *testing.T) {
	tempDir := createTempDir(t)
	defer removeTempDir(t, tempDir)

	config := lsmtree.DefaultConfig()
	config.FilePath = tempDir
	// 테스트를 위해 MemTable 크기와 컴팩션 간격을 단축합니다.
	config.MemTableSize = 1024 * 1024 // 1MB
	config.CompactionInterval = 2 * time.Second

	lsm, err := lsmtree.NewLSMTree(config)
	if err != nil {
		t.Fatalf("failed to create LSMTree: %v", err)
	}
	defer lsm.Close()

	// Insert key-value pairs.
	keys := []string{"alpha", "beta", "gamma"}
	values := []string{"1", "2", "3"}
	for i, key := range keys {
		if err := lsm.Insert(key, values[i]); err != nil {
			t.Fatalf("failed to insert key %s: %v", key, err)
		}
	}

	// Retrieve and verify inserted values.
	for i, key := range keys {
		val, err := lsm.Get(key)
		if err != nil {
			t.Fatalf("failed to get key %s: %v", key, err)
		}
		if val != values[i] {
			t.Errorf("expected value %s for key %s, got %s", values[i], key, val)
		}
	}

	// Delete "beta" key.
	if err := lsm.Delete("beta"); err != nil {
		t.Fatalf("failed to delete key beta: %v", err)
	}

	// Verify that "beta" is no longer available.
	if _, err := lsm.Get("beta"); err == nil {
		t.Errorf("expected error for deleted key beta, but got value")
	}
}

// TestRecovery는 WAL 기반 복구 기능을 검증합니다.
func TestRecovery(t *testing.T) {
	tempDir := createTempDir(t)
	defer removeTempDir(t, tempDir)

	config := lsmtree.DefaultConfig()
	config.FilePath = tempDir
	config.MemTableSize = 1024 * 1024 // 1MB
	config.CompactionInterval = 2 * time.Second

	// 처음 LSM Tree 생성 후 데이터 삽입.
	lsm, err := lsmtree.NewLSMTree(config)
	if err != nil {
		t.Fatalf("failed to create LSMTree: %v", err)
	}

	keys := []string{"delta", "epsilon", "zeta"}
	values := []string{"4", "5", "6"}
	for i, key := range keys {
		if err := lsm.Insert(key, values[i]); err != nil {
			t.Fatalf("failed to insert key %s: %v", key, err)
		}
	}

	// LSM Tree를 종료하여 WAL 및 memTable flush.
	if err := lsm.Close(); err != nil {
		t.Fatalf("failed to close LSMTree: %v", err)
	}

	// 동일한 디렉토리에서 새로운 LSM Tree를 재생성하여 복구 검증.
	lsm2, err := lsmtree.NewLSMTree(config)
	if err != nil {
		t.Fatalf("failed to reopen LSMTree: %v", err)
	}
	defer lsm2.Close()

	// 복구된 데이터 검증.
	for i, key := range keys {
		val, err := lsm2.Get(key)
		if err != nil {
			t.Fatalf("failed to get key %s after recovery: %v", key, err)
		}
		if val != values[i] {
			t.Errorf("expected value %s for key %s after recovery, got %s", values[i], key, val)
		}
	}
}

// TestConcurrentAccess는 동시성 환경에서의 Insert 및 Get 동작을 검증합니다.
func TestConcurrentAccess(t *testing.T) {
	tempDir := createTempDir(t)
	defer removeTempDir(t, tempDir)

	config := lsmtree.DefaultConfig()
	config.FilePath = tempDir
	config.MemTableSize = 2 * 1024 * 1024 // 2MB
	config.CompactionInterval = 2 * time.Second

	lsm, err := lsmtree.NewLSMTree(config)
	if err != nil {
		t.Fatalf("failed to create LSMTree: %v", err)
	}
	defer lsm.Close()

	numGoroutines := 10
	numInsertsPerGoroutine := 100
	var wg sync.WaitGroup

	// 동시 삽입 테스트.
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numInsertsPerGoroutine; j++ {
				key := fmt.Sprintf("key_%d_%d", id, j)
				value := fmt.Sprintf("value_%d_%d", id, j)
				if err := lsm.Insert(key, value); err != nil {
					t.Errorf("failed to insert key %s: %v", key, err)
				}
			}
		}(i)
	}
	wg.Wait()

	// 동시 조회 테스트.
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numInsertsPerGoroutine; j++ {
				key := fmt.Sprintf("key_%d_%d", id, j)
				value, err := lsm.Get(key)
				if err != nil {
					t.Errorf("failed to get key %s: %v", key, err)
					continue
				}
				expected := fmt.Sprintf("value_%d_%d", id, j)
				if value != expected {
					t.Errorf("expected %s, got %s for key %s", expected, value, key)
				}
			}
		}(i)
	}
	wg.Wait()
}

// TestForceCompaction는 ForceCompaction 명령을 통한 컴팩션 동작 및 데이터 무결성을 검증합니다.
func TestForceCompaction(t *testing.T) {
	tempDir := createTempDir(t)
	defer removeTempDir(t, tempDir)

	config := lsmtree.DefaultConfig()
	config.FilePath = tempDir
	config.MemTableSize = 1024 * 1024 // 1MB
	// 자동 컴팩션을 방지하기 위해 컴팩션 간격을 길게 설정합니다.
	config.CompactionInterval = 10 * time.Second
	config.UseBloomFilter = true

	lsm, err := lsmtree.NewLSMTree(config)
	if err != nil {
		t.Fatalf("failed to create LSMTree: %v", err)
	}
	defer lsm.Close()

	// 여러 키 삽입.
	keys := []string{"a", "b", "c", "d", "e"}
	for i, key := range keys {
		if err := lsm.Insert(key, fmt.Sprintf("%d", i)); err != nil {
			t.Fatalf("failed to insert key %s: %v", key, err)
		}
	}

	// 강제 컴팩션 실행.
	if err := lsm.ForceCompaction(); err != nil {
		t.Fatalf("force compaction failed: %v", err)
	}

	// 컴팩션 후 SSTable이 하나로 합쳐졌는지 확인.
	stats := lsm.Stats()
	sstableCount, ok := stats["sstable_count"].(int)
	if !ok {
		t.Fatalf("sstable_count is not an int")
	}
	if sstableCount != 1 {
		t.Errorf("expected 1 SSTable after compaction, got %d", sstableCount)
	}

	// 모든 키가 정상 조회되는지 검증.
	for i, key := range keys {
		val, err := lsm.Get(key)
		if err != nil {
			t.Errorf("failed to get key %s after compaction: %v", key, err)
		}
		if val != fmt.Sprintf("%d", i) {
			t.Errorf("expected value %d for key %s, got %s", i, key, val)
		}
	}
}
