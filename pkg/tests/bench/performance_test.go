package lsmtree_test

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/sukryu/GoLite/pkg/adapters/lockfree"
	"github.com/sukryu/GoLite/pkg/adapters/lsmtree"
)

// createTempDir는 벤치마크용 임시 디렉토리를 생성합니다.
func createTempDir(b *testing.B) string {
	dir, err := os.MkdirTemp("", "lsmtree_bench")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	return dir
}

// removeTempDir는 벤치마크용 임시 디렉토리를 삭제합니다.
func removeTempDir(b *testing.B, dir string) {
	if err := os.RemoveAll(dir); err != nil {
		b.Fatalf("failed to remove temp dir: %v", err)
	}
}

// newTestLSMTree는 벤치마크용 LSMTree를 생성합니다.
func newTestLSMTree(b *testing.B, memTableSize int, compactionInterval time.Duration) (*lsmtree.LSMTree, string) {
	tempDir := createTempDir(b)
	config := lsmtree.DefaultConfig()
	config.FilePath = tempDir
	config.MemTableSize = memTableSize
	config.CompactionInterval = compactionInterval
	lsm, err := lsmtree.NewLSMTree(config)
	if err != nil {
		b.Fatalf("failed to create LSMTree: %v", err)
	}
	return lsm, tempDir
}

// BenchmarkInsertSequential는 순차 삽입 성능을 측정합니다.
func BenchmarkInsertSequential(b *testing.B) {
	lsm, tempDir := newTestLSMTree(b, 64*1024*1024, 1*time.Hour)
	defer func() {
		lsm.Close()
		removeTempDir(b, tempDir)
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i)
		value := fmt.Sprintf("value_%d", i)
		if err := lsm.Insert(key, value); err != nil {
			b.Fatalf("failed to insert key %s: %v", key, err)
		}
	}
	b.StopTimer()
}

// BenchmarkInsertConcurrent는 동시 삽입 성능을 측정합니다.
func BenchmarkInsertConcurrent(b *testing.B) {
	lsm, tempDir := newTestLSMTree(b, 64*1024*1024, 1*time.Hour)
	defer func() {
		lsm.Close()
		removeTempDir(b, tempDir)
	}()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key_%d", i)
			value := fmt.Sprintf("value_%d", i)
			if err := lsm.Insert(key, value); err != nil {
				b.Fatalf("failed to insert key %s: %v", key, err)
			}
			i++
		}
	})
	b.StopTimer()
}

// BenchmarkGetSequential는 순차 조회 성능을 측정합니다.
func BenchmarkGetSequential(b *testing.B) {
	lsm, tempDir := newTestLSMTree(b, 64*1024*1024, 1*time.Hour)
	defer func() {
		lsm.Close()
		removeTempDir(b, tempDir)
	}()

	// 미리 b.N개의 키-값 삽입.
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i)
		value := fmt.Sprintf("value_%d", i)
		if err := lsm.Insert(key, value); err != nil {
			b.Fatalf("failed to insert key %s: %v", key, err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i)
		if _, err := lsm.Get(key); err != nil {
			b.Fatalf("failed to get key %s: %v", key, err)
		}
	}
	b.StopTimer()
}

// BenchmarkGetConcurrent는 동시 조회 성능을 측정합니다.
func BenchmarkGetConcurrent(b *testing.B) {
	numKeys := 100000
	lsm, tempDir := newTestLSMTree(b, 64*1024*1024, 1*time.Hour)
	defer func() {
		lsm.Close()
		removeTempDir(b, tempDir)
	}()

	// 미리 일정 수의 키 삽입.
	for i := 0; i < numKeys; i++ {
		key := "key_" + strconv.Itoa(i)
		value := "value_" + strconv.Itoa(i)
		if err := lsm.Insert(key, value); err != nil {
			b.Fatalf("failed to insert key %s: %v", key, err)
		}
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var i int
		for pb.Next() {
			key := "key_" + strconv.Itoa(i%numKeys)
			if _, err := lsm.Get(key); err != nil {
				b.Fatalf("failed to get key %s: %v", key, err)
			}
			i++
		}
	})
	b.StopTimer()
}

// BenchmarkForceCompaction는 각 반복마다 새로운 LSMTree에서 100,000개의 키를 삽입하고 ForceCompaction을 수행합니다.
// compaction 후, 몇 개의 키가 정상적으로 조회되는지 검증하여 데이터 누락이 없는지 확인합니다.
func BenchmarkForceCompaction(b *testing.B) {
	for i := 0; i < b.N; i++ {
		lsm, tempDir := newTestLSMTree(b, 1024*1024, 10*time.Second)
		// 100,000개의 키 삽입.
		numKeys := 100000
		for j := 0; j < numKeys; j++ {
			key := fmt.Sprintf("key_%d", j)
			value := fmt.Sprintf("value_%d", j)
			if err := lsm.Insert(key, value); err != nil {
				b.Fatalf("failed to insert key %s: %v", key, err)
			}
		}
		b.StartTimer()
		if err := lsm.ForceCompaction(); err != nil {
			b.Fatalf("force compaction failed: %v", err)
		}
		b.StopTimer()

		// compaction 후 몇몇 키를 조회하여 확인.
		for j := 0; j < 10; j++ {
			key := fmt.Sprintf("key_%d", j)
			expected := fmt.Sprintf("value_%d", j)
			val, err := lsm.Get(key)
			if err != nil {
				b.Fatalf("post-compaction retrieval failed for key %s: %v", key, err)
			}
			if val != expected {
				b.Fatalf("expected %s for key %s, got %s", expected, key, val)
			}
		}
		lsm.Close()
		removeTempDir(b, tempDir)
	}
}

// BenchmarkEnqueueDequeue benchmarks the concurrent enqueue and dequeue operations.
func BenchmarkEnqueueDequeue(b *testing.B) {
	q := lockfree.NewLFQueue[int]()
	b.ResetTimer()

	b.Run("Sequential", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			q.Enqueue(i)
			q.Dequeue()
		}
	})

	b.Run("ParallelEnqueue", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				q.Enqueue(i)
				i++
			}
		})
	})

	// Pre-fill the queue for dequeue benchmark
	for i := 0; i < b.N*runtime.GOMAXPROCS(0); i++ {
		q.Enqueue(i)
	}

	b.Run("ParallelDequeue", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				q.Dequeue()
			}
		})
	})

	b.Run("ParallelMixed", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				if i%2 == 0 {
					q.Enqueue(i)
				} else {
					q.Dequeue()
				}
				i++
			}
		})
	})
}

// BenchmarkBatchOperations benchmarks the batch operations.
func BenchmarkBatchOperations(b *testing.B) {
	q := lockfree.NewLFQueue[int]()
	batchSize := 100
	batch := make([]int, batchSize)
	for i := 0; i < batchSize; i++ {
		batch[i] = i
	}

	b.Run("EnqueueBatch", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			q.EnqueueBatch(batch)
		}
	})

	// Pre-fill the queue for dequeue benchmark
	for i := 0; i < b.N*batchSize; i++ {
		q.Enqueue(i)
	}

	b.Run("DequeueBatch", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			q.DequeueBatch(batchSize)
		}
	})
}
