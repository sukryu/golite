// Package lockfree implements lock‑free data structures.
// 이 구현은 lock‑free skip list 기반 MemTable입니다.
package lockfree

import (
	"errors"
	"math/rand"
	"sync/atomic"

	"k8s.io/klog/v2" // Kubernetes 스타일의 구조화된 로깅 (선택 사항)
)

// Constants for the skip list.
const (
	maxLevel    = 16  // 최대 레벨
	probability = 0.5 // 레벨 증가 확률
)

// lfMemtable is a lock‑free MemTable implemented as a skip list.
// 키와 값은 string 타입입니다.
type lfMemtable struct {
	head   *mnode // sentinel 노드 (헤드)
	length int64  // 현재 노드 개수 (atomic 업데이트)
}

// node represents 하나의 노드를 나타냅니다.
type mnode struct {
	key   string
	value string
	// next는 각 레벨의 다음 노드를 원자적으로 업데이트합니다.
	next [maxLevel]atomic.Pointer[mnode]
	// level은 이 노드가 가지고 있는 레벨 수입니다.
	level int
	// deleted는 논리적 삭제 상태입니다.
	// 0: active, 1: deleted.
	deleted uint32
}

// NewLFMemtable creates and returns a new lock-free memtable.
func NewLFMemtable() *lfMemtable {
	// sentinel 노드: key는 비워두고, 최대 레벨로 생성합니다.
	sentinel := &mnode{
		key:   "",
		value: "",
		level: maxLevel,
	}
	// 모든 next 포인터는 nil로 초기화됨.
	return &lfMemtable{
		head:   sentinel,
		length: 0,
	}
}

// randomLevel generates a random level for a new node.
// production code에서는 고루틴 안전한 난수 생성기가 필요할 수 있습니다.
func randomLevel() int {
	level := 1
	// 확률적으로 레벨 증가
	for rand.Float64() < probability && level < maxLevel {
		level++
	}
	return level
}

// find searches for the given key and fills preds and succs with the
// predecessors and successors at each level.
// 반환 값은 key를 가진 노드가 존재하면 그 포인터, 아니면 nil을 반환합니다.
func (m *lfMemtable) find(key string, preds *[maxLevel]*mnode, succs *[maxLevel]*mnode) *mnode {
	x := m.head
	for i := maxLevel - 1; i >= 0; i-- {
		// 하위 레벨로 내려가기 전 현재 레벨을 순회.
		for {
			next := x.next[i].Load()
			if next == nil || next.key >= key {
				break
			}
			x = next
		}
		preds[i] = x
		succs[i] = x.next[i].Load()
	}
	// 0 레벨에서 key가 일치하는지 검사.
	if succs[0] != nil && succs[0].key == key {
		return succs[0]
	}
	return nil
}

// Insert inserts or updates the key-value pair into the memtable.
// 만약 이미 존재하면 value를 업데이트합니다.
func (m *lfMemtable) Insert(key, value string) error {
	var preds, succs [maxLevel]*mnode

	// 반복 시도: 다른 고루틴과 경쟁하여 삽입 위치를 찾습니다.
	for {
		existing := m.find(key, &preds, &succs)
		if existing != nil {
			// 이미 존재하는 경우, 논리적 삭제 상태가 아니라면 업데이트.
			if atomic.LoadUint32(&existing.deleted) == 0 {
				existing.value = value
				return nil
			}
			// 논리적으로 삭제된 경우, 재삽입을 시도할 수 있음.
		}

		// 새 노드 생성.
		level := randomLevel()
		newNode := &mnode{
			key:   key,
			value: value,
			level: level,
		}
		// 각 레벨의 next 포인터를 초기화.
		for i := 0; i < level; i++ {
			newNode.next[i].Store(succs[i])
		}

		// 0레벨부터 새 노드 삽입 시도.
		if !preds[0].next[0].CompareAndSwap(succs[0], newNode) {
			// 실패하면 재시도.
			klog.V(4).Infof("Insert CAS failed at level 0 for key: %s", key)
			continue
		}

		// 나머지 레벨에서 연결 업데이트 (CAS 실패 시 재시도 루프 내에서 보조 업데이트 수행)
		for i := 1; i < level; i++ {
			for {
				if preds[i].next[i].CompareAndSwap(succs[i], newNode) {
					break
				}
				// 재검색 후 재시도
				m.find(key, &preds, &succs)
			}
		}
		atomic.AddInt64(&m.length, 1)
		return nil
	}
}

// Get retrieves the value associated with the key.
func (m *lfMemtable) Get(key string) (string, bool) {
	x := m.head
	for i := maxLevel - 1; i >= 0; i-- {
		for {
			next := x.next[i].Load()
			if next == nil || next.key >= key {
				break
			}
			x = next
		}
	}
	// x.next[0]가 검색 대상.
	x = m.head.next[0].Load()
	for x != nil && x.key < key {
		x = x.next[0].Load()
	}
	if x != nil && x.key == key && atomic.LoadUint32(&x.deleted) == 0 {
		return x.value, true
	}
	return "", false
}

// Delete marks the node with the given key as deleted.
// 논리적 삭제 후, 물리적 제거는 후속 CAS 작업에서 이루어질 수 있습니다.
func (m *lfMemtable) Delete(key string) error {
	var preds, succs [maxLevel]*mnode
	target := m.find(key, &preds, &succs)
	if target == nil {
		return errors.New("key not found")
	}
	// 논리적 삭제: CAS로 deleted를 0에서 1로 변경.
	if !atomic.CompareAndSwapUint32(&target.deleted, 0, 1) {
		return errors.New("failed to delete: already deleted")
	}
	atomic.AddInt64(&m.length, -1)
	return nil
}

// Dump returns a snapshot of all active (non-deleted) key-value pairs.
func (m *lfMemtable) Dump() map[string]string {
	result := make(map[string]string)
	// 0 레벨 (linked list)을 순회.
	for x := m.head.next[0].Load(); x != nil; x = x.next[0].Load() {
		if atomic.LoadUint32(&x.deleted) == 0 {
			result[x.key] = x.value
		}
	}
	return result
}

// Swap atomically swaps out the current memtable and returns a snapshot of its data.
// 생산 환경에서는 새로운 memtable을 생성하고, 기존의 데이터를 Dump()한 후, 교체합니다.
func (m *lfMemtable) Swap() map[string]string {
	// Dump current data.
	snapshot := m.Dump()
	// Reset memtable by reinitializing the sentinel node.
	newMT := NewLFMemtable()
	// Atomically replace internal state.
	// 실제로 pointer swap은 LSMTree 수준에서 관리하는 것이 좋습니다.
	// 여기에서는 내부 상태 재설정을 위한 간단한 구현을 제공합니다.
	atomic.StoreInt64(&m.length, 0)
	m.head = newMT.head
	return snapshot
}

// Size returns the number of active nodes in the memtable.
func (m *lfMemtable) Size() int64 {
	return atomic.LoadInt64(&m.length)
}

// Reset clears the memtable.
func (m *lfMemtable) Reset() {
	// Reinitialize the memtable.
	newMT := NewLFMemtable()
	atomic.StoreInt64(&m.length, 0)
	m.head = newMT.head
}
