package unit

import (
	"testing"
	"time"

	"github.com/sukryu/GoLite/pkg/adapters/lockfree"
)

func TestLockFreeCompactor(t *testing.T) {
	compactor := lockfree.NewLockFreeCompactor()
	compactor.Run()

	// SSTable 작업 생성 (예시)
	tasks := []*lockfree.SSTable{
		lockfree.NewSSTable("a", "c"),
		lockfree.NewSSTable("d", "f"),
		lockfree.NewSSTable("g", "i"),
		lockfree.NewSSTable("j", "l"),
		lockfree.NewSSTable("m", "o"),
	}
	// 각 작업을 compactor에 추가.
	for _, task := range tasks {
		compactor.AddTask(task)
	}

	// 일정 시간 대기하여 compactor가 작업을 처리할 시간을 줍니다.
	time.Sleep(2 * time.Second)

	// compactor의 taskQueue에는 병합 결과로 생성된 SSTable이 있어야 합니다.
	queueLength := compactor.GetTaskQueueLength()
	if queueLength >= len(tasks) {
		t.Errorf("Expected fewer tasks after compaction, got %d", queueLength)
	}

	compactor.Stop()
}
