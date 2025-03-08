/*
Copyright 2025 Lock-Free Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package unit

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sukryu/GoLite/pkg/adapters/lockfree"
)

// TestEnqueueDequeue verifies basic enqueue and dequeue operations.
func TestEnqueueDequeue(t *testing.T) {
	q := lockfree.NewLFQueue[int]()

	// Test empty queue behavior
	val, ok := q.Dequeue()
	if ok || val != 0 {
		t.Errorf("Expected (0, false) for empty queue, got (%d, %t)", val, ok)
	}

	// Enqueue items
	items := []int{1, 2, 3, 4, 5}
	for _, item := range items {
		if !q.Enqueue(item) {
			t.Errorf("Failed to enqueue item %d", item)
		}
	}

	// Check length
	if q.Length() != len(items) {
		t.Errorf("Expected length %d, got %d", len(items), q.Length())
	}

	// Dequeue and verify items
	for _, expected := range items {
		val, ok := q.Dequeue()
		if !ok {
			t.Errorf("Failed to dequeue item %d", expected)
		}
		if val != expected {
			t.Errorf("Expected %d, got %d", expected, val)
		}
	}

	// Verify queue is empty
	if !q.IsEmpty() {
		t.Error("Queue should be empty")
	}
}

// TestPeek verifies the peek operation.
func TestPeek(t *testing.T) {
	q := lockfree.NewLFQueue[int]()

	// Test peek on empty queue
	val, ok := q.Peek()
	if ok || val != 0 {
		t.Errorf("Expected (0, false) for empty queue peek, got (%d, %t)", val, ok)
	}

	// Enqueue an item
	q.Enqueue(42)

	// Peek should show the item without removing it
	val, ok = q.Peek()
	if !ok || val != 42 {
		t.Errorf("Expected (42, true), got (%d, %t)", val, ok)
	}

	// The item should still be in the queue
	val, ok = q.Dequeue()
	if !ok || val != 42 {
		t.Errorf("Expected (42, true), got (%d, %t)", val, ok)
	}

	// Queue should now be empty
	if !q.IsEmpty() {
		t.Error("Queue should be empty after dequeue")
	}
}

// TestConcurrentEnqueueDequeue tests concurrent enqueues and dequeues.
func TestConcurrentEnqueueDequeue(t *testing.T) {
	q := lockfree.NewLFQueue[int]()
	itemCount := 10000
	goroutineCount := 8

	var enqueueCount, dequeueCount int32

	var mu sync.Mutex
	dequeuedItems := make(map[int]bool)

	var wg sync.WaitGroup

	// Channel to signal production is done.
	doneProducing := make(chan struct{})

	// Start goroutines for concurrent enqueues.
	wg.Add(goroutineCount)
	for g := 0; g < goroutineCount; g++ {
		go func(offset int) {
			defer wg.Done()
			for i := 0; i < itemCount/goroutineCount; i++ {
				item := offset*itemCount/goroutineCount + i
				if q.Enqueue(item) {
					atomic.AddInt32(&enqueueCount, 1)
				}
			}
		}(g)
	}

	// Wait for all producers to finish, then close doneProducing.
	go func() {
		wg.Wait()
		close(doneProducing)
	}()

	// Start consumer goroutines.
	var consumerWg sync.WaitGroup
	consumerWg.Add(goroutineCount)
	for g := 0; g < goroutineCount; g++ {
		go func() {
			defer consumerWg.Done()
			for {
				val, ok := q.Dequeue()
				if !ok {
					// If production is done and queue is empty, exit.
					select {
					case <-doneProducing:
						// Double-check: if queue is still empty, break.
						if q.IsEmpty() {
							return
						}
					default:
						// Production not done yet, yield.
					}
					runtime.Gosched()
					continue
				}

				atomic.AddInt32(&dequeueCount, 1)
				mu.Lock()
				if dequeuedItems[val] {
					t.Errorf("Item %d was dequeued multiple times", val)
				}
				dequeuedItems[val] = true
				mu.Unlock()

				// Optionally, check if we've dequeued all items.
				if atomic.LoadInt32(&dequeueCount) >= int32(itemCount) {
					return
				}
			}
		}()
	}

	// Wait for consumers to finish.
	consumerWg.Wait()

	// Verify that all items were processed.
	if int(enqueueCount) != itemCount {
		t.Errorf("Expected %d enqueues, got %d", itemCount, enqueueCount)
	}
	if int(dequeueCount) != itemCount {
		t.Errorf("Expected %d dequeues, got %d", itemCount, dequeueCount)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(dequeuedItems) != itemCount {
		t.Errorf("Expected %d unique dequeued items, got %d", itemCount, len(dequeuedItems))
	}
}

// TestTryDequeue tests the timeout-based dequeue operation.
func TestTryDequeue(t *testing.T) {
	q := lockfree.NewLFQueue[int]()

	// Try dequeue with timeout on empty queue
	start := time.Now()
	timeout := 50 * time.Millisecond
	_, ok := q.TryDequeue(timeout)
	elapsed := time.Since(start)

	if ok {
		t.Error("Expected TryDequeue to fail on empty queue")
	}
	if elapsed < timeout {
		t.Errorf("TryDequeue returned too early: %v < %v", elapsed, timeout)
	}

	// Test with available items
	q.Enqueue(123)
	val, ok := q.TryDequeue(timeout)
	if !ok || val != 123 {
		t.Errorf("Expected (123, true), got (%d, %t)", val, ok)
	}
}

// TestBatchOperations tests batch enqueue and dequeue operations.
func TestBatchOperations(t *testing.T) {
	q := lockfree.NewLFQueue[int]()
	batch := []int{10, 20, 30, 40, 50}

	// Test batch enqueue
	enqueued := q.EnqueueBatch(batch)
	if enqueued != len(batch) {
		t.Errorf("Expected to enqueue %d items, got %d", len(batch), enqueued)
	}

	// Test batch dequeue
	dequeued, count := q.DequeueBatch(3) // Dequeue only first 3 items
	if count != 3 {
		t.Errorf("Expected to dequeue 3 items, got %d", count)
	}
	for i, val := range dequeued {
		if val != batch[i] {
			t.Errorf("Expected %d at position %d, got %d", batch[i], i, val)
		}
	}

	// Dequeue the remaining items
	dequeued, count = q.DequeueBatch(len(batch))
	if count != 2 {
		t.Errorf("Expected to dequeue 2 items, got %d", count)
	}
	for i, val := range dequeued {
		if val != batch[i+3] {
			t.Errorf("Expected %d at position %d, got %d", batch[i+3], i, val)
		}
	}

	// Queue should now be empty
	if !q.IsEmpty() {
		t.Error("Queue should be empty after batch dequeue")
	}
}

// TestStressWithContention creates high contention to stress test the queue.
func TestStressWithContention(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	q := lockfree.NewLFQueue[int]()
	iterations := 100000
	goroutines := 16

	// 생산 완료 신호용 채널.
	doneProducing := make(chan struct{})

	// 생산자 고루틴 시작.
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations/goroutines; i++ {
				q.Enqueue(id*iterations/goroutines + i)
			}
		}(g)
	}

	// 모든 생산자 완료 후 doneProducing 채널 닫기.
	go func() {
		wg.Wait()
		close(doneProducing)
	}()

	// 소비자 고루틴 시작.
	var consumerWg sync.WaitGroup
	var consumed int64
	consumerWg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer consumerWg.Done()
			for {
				_, success := q.Dequeue()
				if success {
					atomic.AddInt64(&consumed, 1)
				} else {
					// 생산 완료 신호가 있으면, 큐가 비어있을 때 종료.
					select {
					case <-doneProducing:
						if q.IsEmpty() {
							return
						}
					default:
						// 생산 중이면 계속 시도.
					}
					runtime.Gosched()
				}
			}
		}()
	}

	consumerWg.Wait()

	if atomic.LoadInt64(&consumed) != int64(iterations) {
		t.Errorf("Expected %d consumed items, got %d", iterations, consumed)
	}

	time.Sleep(100 * time.Millisecond)
	if !q.IsEmpty() {
		t.Error("Queue should be empty after stress test")
	}
}
