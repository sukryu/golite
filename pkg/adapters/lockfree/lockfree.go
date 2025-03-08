/*
Copyright 2025 Lock-Free Jinhyeok

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

// Package lockfree provides lock-free data structures for high-performance concurrent applications.
package lockfree

import (
	"sync/atomic"
	"time"
	"unsafe"
)

// LFQueue is a lock-free queue implementation using the Michael-Scott algorithm.
// It is safe for concurrent use by multiple goroutines.
type LFQueue[T any] struct {
	head   unsafe.Pointer // *node[T]
	tail   unsafe.Pointer // *node[T]
	length int64          // tracks approximate length for metrics
}

// node represents a single element in the queue.
// The next pointer uses tagged pointers to prevent the ABA problem.
type node[T any] struct {
	value    T
	next     *nodePointer[T]
	dequeued uint32 // 0: not dequeued, 1: dequeued
}

// nodePointer is a pointer wrapper with a tag to prevent the ABA problem.
type nodePointer[T any] struct {
	ptr *node[T]
	tag uint64 // Tag to prevent ABA problem
}

// NewLFQueue creates a new lock-free queue.
func NewLFQueue[T any]() *LFQueue[T] {
	// Create a sentinel node (dummy node) to simplify operations
	sentinel := &node[T]{
		next: &nodePointer[T]{
			ptr: nil,
			tag: 0,
		},
		dequeued: 0,
	}

	q := &LFQueue[T]{
		length: 0,
	}
	// Initialize both head and tail to point to the sentinel node
	q.head = unsafe.Pointer(sentinel)
	q.tail = unsafe.Pointer(sentinel)
	return q
}

// Enqueue adds an item to the end of the queue.
// It returns true if the operation was successful.
func (q *LFQueue[T]) Enqueue(value T) bool {
	newNode := &node[T]{
		value: value,
		next: &nodePointer[T]{
			ptr: nil,
			tag: 0,
		},
		dequeued: 0,
	}

	for {
		tail := (*node[T])(atomic.LoadPointer(&q.tail))
		next := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&tail.next)))
		tailNext := (*nodePointer[T])(next)

		// Check if tail is still valid (hasn't been moved by another thread)
		if atomic.LoadPointer(&q.tail) != unsafe.Pointer(tail) {
			continue // Retry from the beginning
		}

		// If the tail is pointing to a node with a non-nil next pointer,
		// help advance the tail pointer
		if tailNext.ptr != nil {
			// Help advance the tail pointer (tail is lagging)
			atomic.CompareAndSwapPointer(
				&q.tail,
				unsafe.Pointer(tail),
				unsafe.Pointer(tailNext.ptr),
			)
			continue // Retry from the beginning
		}

		// Try to link the new node at the end of the list
		newNodePointer := &nodePointer[T]{
			ptr: newNode,
			tag: tailNext.tag + 1, // Increment tag for ABA prevention
		}
		if atomic.CompareAndSwapPointer(
			(*unsafe.Pointer)(unsafe.Pointer(&tail.next)),
			next,
			unsafe.Pointer(newNodePointer),
		) {
			// Successfully linked the new node, now try to advance the tail
			atomic.CompareAndSwapPointer(
				&q.tail,
				unsafe.Pointer(tail),
				unsafe.Pointer(newNode),
			)
			atomic.AddInt64(&q.length, 1)
			return true // Enqueue successful
		}
		// CAS failed - retry
	}
}

// Dequeue removes and returns the item at the front of the queue.
// If the queue is empty, it returns the zero value for type T and false.
func (q *LFQueue[T]) Dequeue() (T, bool) {
	var value T

	for {
		// Load current head and tail pointers.
		headPtr := atomic.LoadPointer(&q.head)
		tailPtr := atomic.LoadPointer(&q.tail)
		head := (*node[T])(headPtr)
		tail := (*node[T])(tailPtr)

		// Load the next pointer from the current head.
		nextPtr := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&head.next)))
		headNext := (*nodePointer[T])(nextPtr)

		// If the next node is nil (i.e. head.next.ptr is nil), the queue is empty.
		if headNext == nil || headNext.ptr == nil {
			return value, false
		}

		// If head equals tail, help advance the tail pointer.
		if head == tail {
			atomic.CompareAndSwapPointer(&q.tail, tailPtr, unsafe.Pointer(headNext.ptr))
			continue
		}

		// Retrieve the value from the next node.
		value = headNext.ptr.value

		// Try to advance the head pointer atomically.
		if atomic.CompareAndSwapPointer(&q.head, headPtr, unsafe.Pointer(headNext.ptr)) {
			atomic.AddInt64(&q.length, -1)
			return value, true
		}
		// CAS failed, retry.
	}
}

// Length returns the approximate number of elements in the queue.
// This is not guaranteed to be exact due to concurrent operations.
func (q *LFQueue[T]) Length() int {
	return int(atomic.LoadInt64(&q.length))
}

// IsEmpty returns true if the queue is likely empty.
// Due to concurrency, this is only an approximation.
func (q *LFQueue[T]) IsEmpty() bool {
	head := (*node[T])(atomic.LoadPointer(&q.head))
	next := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&head.next)))
	headNext := (*nodePointer[T])(next)
	return headNext.ptr == nil
}

// Peek returns the value at the front of the queue without removing it.
// If the queue is empty, it returns the zero value for type T and false.
func (q *LFQueue[T]) Peek() (T, bool) {
	var value T

	for {
		head := (*node[T])(atomic.LoadPointer(&q.head))
		next := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&head.next)))
		headNext := (*nodePointer[T])(next)

		// Check if head is still valid
		if atomic.LoadPointer(&q.head) != unsafe.Pointer(head) {
			continue // Retry from the beginning
		}

		// If queue is empty
		if headNext.ptr == nil {
			return value, false
		}

		// If node is already dequeued, help advance the head
		if headNext.ptr.dequeued != 0 {
			atomic.CompareAndSwapPointer(
				&q.head,
				unsafe.Pointer(head),
				unsafe.Pointer(headNext.ptr),
			)
			continue // Retry
		}

		// Return the value without modifying the queue
		return headNext.ptr.value, true
	}
}

// TryDequeue attempts to dequeue an item from the queue.
// If the queue is empty or if the operation exceeds the timeout, it returns the zero value for type T and false.
func (q *LFQueue[T]) TryDequeue(timeout time.Duration) (T, bool) {
	var value T
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		value, success := q.Dequeue()
		if success {
			return value, true
		}
		// Small pause to reduce CPU usage
		time.Sleep(time.Microsecond)
	}

	return value, false
}

// EnqueueBatch attempts to enqueue multiple items at once.
// It returns the number of items successfully enqueued.
func (q *LFQueue[T]) EnqueueBatch(values []T) int {
	count := 0
	for _, v := range values {
		if q.Enqueue(v) {
			count++
		}
	}
	return count
}

// DequeueBatch attempts to dequeue up to maxItems from the queue.
// It returns the dequeued items and the number of items dequeued.
func (q *LFQueue[T]) DequeueBatch(maxItems int) ([]T, int) {
	if maxItems <= 0 {
		return nil, 0
	}

	result := make([]T, 0, maxItems)
	for i := 0; i < maxItems; i++ {
		value, success := q.Dequeue()
		if !success {
			break
		}
		result = append(result, value)
	}

	return result, len(result)
}
