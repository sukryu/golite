package unit

import (
	"testing"

	"github.com/sukryu/GoLite/pkg/adapters/lockfree"
)

func TestLFMemtableInsertGet(t *testing.T) {
	mt := lockfree.NewLFMemtable()

	// 초기 상태: 크기는 0이어야 함.
	if size := mt.Size(); size != 0 {
		t.Errorf("Expected size 0, got %d", size)
	}

	// 키-값 쌍 삽입
	if err := mt.Insert("key1", "value1"); err != nil {
		t.Fatalf("Insert failed for key1: %v", err)
	}
	if err := mt.Insert("key2", "value2"); err != nil {
		t.Fatalf("Insert failed for key2: %v", err)
	}

	// 크기는 2이어야 함.
	if size := mt.Size(); size != 2 {
		t.Errorf("Expected size 2, got %d", size)
	}

	// Get 테스트
	if val, ok := mt.Get("key1"); !ok || val != "value1" {
		t.Errorf("Expected key1 -> value1, got (%s, %t)", val, ok)
	}
	if val, ok := mt.Get("key2"); !ok || val != "value2" {
		t.Errorf("Expected key2 -> value2, got (%s, %t)", val, ok)
	}
	// 존재하지 않는 키
	if _, ok := mt.Get("key3"); ok {
		t.Errorf("Expected key3 not found")
	}
}

func TestLFMemtableDelete(t *testing.T) {
	mt := lockfree.NewLFMemtable()
	mt.Insert("key1", "value1")
	mt.Insert("key2", "value2")

	// key1 삭제
	if err := mt.Delete("key1"); err != nil {
		t.Fatalf("Delete failed for key1: %v", err)
	}

	// key1 조회 실패
	if _, ok := mt.Get("key1"); ok {
		t.Errorf("Expected key1 to be deleted")
	}
	// 크기는 1이어야 함.
	if size := mt.Size(); size != 1 {
		t.Errorf("Expected size 1 after deletion, got %d", size)
	}
}

func TestLFMemtableDump(t *testing.T) {
	mt := lockfree.NewLFMemtable()
	mt.Insert("a", "1")
	mt.Insert("b", "2")
	mt.Insert("c", "3")

	dump := mt.Dump()
	if len(dump) != 3 {
		t.Errorf("Expected dump length 3, got %d", len(dump))
	}
	if dump["a"] != "1" || dump["b"] != "2" || dump["c"] != "3" {
		t.Errorf("Dump returned incorrect values: %v", dump)
	}
}

func TestLFMemtableSwap(t *testing.T) {
	mt := lockfree.NewLFMemtable()
	mt.Insert("x", "100")
	mt.Insert("y", "200")

	// Swap 전, 데이터 스냅샷을 받아옵니다.
	oldDump := mt.Swap()
	if len(oldDump) != 2 {
		t.Errorf("Expected old dump length 2, got %d", len(oldDump))
	}
	if oldDump["x"] != "100" || oldDump["y"] != "200" {
		t.Errorf("Old dump has incorrect values: %v", oldDump)
	}
	// Swap 후, MemTable은 비어 있어야 함.
	if size := mt.Size(); size != 0 {
		t.Errorf("Expected size 0 after swap, got %d", size)
	}
	if _, ok := mt.Get("x"); ok {
		t.Errorf("Expected key x not found after swap")
	}
}

func TestLFMemtableReset(t *testing.T) {
	mt := lockfree.NewLFMemtable()
	mt.Insert("k", "v")
	mt.Reset()

	if size := mt.Size(); size != 0 {
		t.Errorf("Expected size 0 after reset, got %d", size)
	}
	if _, ok := mt.Get("k"); ok {
		t.Errorf("Expected key k not found after reset")
	}
}
