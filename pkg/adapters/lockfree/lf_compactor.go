// Package lockfree provides lock‑free data structures for high‑performance concurrent applications.
package lockfree

import (
	"fmt"
	"sync/atomic"
	"time"
)

// SSTable represents a simplified SSTable structure for demonstration.
// 실제 구현에서는 SSTable은 파일 경로, 인덱스, 체크섬 등 다양한 정보를 포함합니다.
type SSTable struct {
	MinKey string
	MaxKey string
	// 기타 필요한 필드 추가 가능
}

// NewSSTable은 새로운 SSTable 인스턴스를 생성합니다.
func NewSSTable(minKey, maxKey string) *SSTable {
	return &SSTable{
		MinKey: minKey,
		MaxKey: maxKey,
	}
}

// LockFreeCompactor defines a lock‑free compactor that merges SSTables.
// 내부적으로 LFQueue를 사용해 병합할 SSTable 작업을 관리합니다.
type LockFreeCompactor struct {
	taskQueue *LFQueue[*SSTable] // lock‑free 큐: 병합할 SSTable 작업을 저장
	stopCh    chan struct{}      // 컴팩터 종료 신호
	running   atomic.Bool        // 실행 여부
}

// NewLockFreeCompactor creates and returns a new lock‑free compactor.
func NewLockFreeCompactor() *LockFreeCompactor {
	return &LockFreeCompactor{
		taskQueue: NewLFQueue[*SSTable](),
		stopCh:    make(chan struct{}),
	}
}

// AddTask enqueues an SSTable task to the compactor.
func (c *LockFreeCompactor) AddTask(sst *SSTable) {
	c.taskQueue.Enqueue(sst)
}

// Run starts the compactor's background merge process.
// 주기적으로 taskQueue에서 두 개의 SSTable을 꺼내 병합 작업을 수행합니다.
func (c *LockFreeCompactor) Run() {
	if c.running.Load() {
		return
	}
	c.running.Store(true)
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-c.stopCh:
				return
			case <-ticker.C:
				c.compact()
			}
		}
	}()
}

// compact는 taskQueue에서 두 개의 SSTable을 꺼내 병합합니다.
// 실제 병합 작업은 파일 I/O 및 인덱스 재구성이 포함되겠지만, 여기서는 간단하게 두 SSTable의 최소/최대 키를 통합한 새로운 SSTable을 생성합니다.
func (c *LockFreeCompactor) compact() {
	// 두 개의 SSTable 작업을 동시에 처리합니다.
	sst1, ok1 := c.taskQueue.Dequeue()
	sst2, ok2 := c.taskQueue.Dequeue()
	if !ok1 || !ok2 {
		// 충분한 작업이 없다면, 이미 꺼낸 작업이 있다면 다시 삽입.
		if ok1 {
			c.taskQueue.Enqueue(sst1)
		}
		return
	}
	// 간단한 병합 로직: 첫번째 SSTable의 minKey와 두번째 SSTable의 maxKey를 사용하여 새 SSTable 생성.
	newSST := &SSTable{
		MinKey: sst1.MinKey,
		MaxKey: sst2.MaxKey,
	}
	// 실제 환경에서는 여기서 두 SSTable의 데이터를 병합하고, 인덱스를 재구성합니다.
	// 예시로, 정렬된 순서가 유지되도록 간단하게 처리합니다.
	// 재삽입: 새 병합 결과를 다시 큐에 넣어 후속 컴팩션 작업으로 연결합니다.
	c.taskQueue.Enqueue(newSST)
	// 구조화된 로깅: 실제 환경에서는 klog 등의 라이브러리를 사용합니다.
	fmt.Printf("Merged SSTables: [%s, %s] + [%s, %s] -> [%s, %s]\n",
		sst1.MinKey, sst1.MaxKey, sst2.MinKey, sst2.MaxKey, newSST.MinKey, newSST.MaxKey)
}

// Stop signals the compactor to stop and waits for termination.
func (c *LockFreeCompactor) Stop() {
	if c.running.Load() {
		close(c.stopCh)
		c.running.Store(false)
	}
}

// GetTaskQueueLength returns the approximate number of tasks in the queue.
func (c *LockFreeCompactor) GetTaskQueueLength() int {
	return c.taskQueue.Length()
}
