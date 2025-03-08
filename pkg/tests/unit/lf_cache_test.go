package unit

import (
	"testing"

	"github.com/sukryu/GoLite/pkg/adapters/lockfree"
)

func TestLockFreeCachePutGet(t *testing.T) {
	cache := lockfree.NewLockFreeCache()

	// 초기 캐시 길이는 0이어야 함.
	if cache.Length() != 0 {
		t.Errorf("Expected empty cache, got length %d", cache.Length())
	}

	// 항목 삽입
	cache.Put("foo", "bar")
	cache.Put("hello", "world")

	// 길이 검증
	if cache.Length() != 2 {
		t.Errorf("Expected cache length 2, got %d", cache.Length())
	}

	// Get 테스트
	if val, ok := cache.Get("foo"); !ok || val != "bar" {
		t.Errorf("Expected (bar, true) for key 'foo', got (%s, %t)", val, ok)
	}
	if val, ok := cache.Get("hello"); !ok || val != "world" {
		t.Errorf("Expected (world, true) for key 'hello', got (%s, %t)", val, ok)
	}
	if _, ok := cache.Get("nonexistent"); ok {
		t.Errorf("Expected key 'nonexistent' to be missing")
	}
}

func TestLockFreeCacheUpdate(t *testing.T) {
	cache := lockfree.NewLockFreeCache()
	cache.Put("key", "value1")

	// 업데이트
	cache.Put("key", "value2")

	if val, ok := cache.Get("key"); !ok || val != "value2" {
		t.Errorf("Expected updated value 'value2', got (%s, %t)", val, ok)
	}

	// 길이는 여전히 1이어야 함.
	if cache.Length() != 1 {
		t.Errorf("Expected cache length 1, got %d", cache.Length())
	}
}

func TestLockFreeCacheClear(t *testing.T) {
	cache := lockfree.NewLockFreeCache()
	cache.Put("a", "1")
	cache.Put("b", "2")
	cache.Put("c", "3")

	if cache.Length() != 3 {
		t.Errorf("Expected length 3 before clear, got %d", cache.Length())
	}

	cache.Clear()

	if cache.Length() != 0 {
		t.Errorf("Expected length 0 after clear, got %d", cache.Length())
	}

	if _, ok := cache.Get("a"); ok {
		t.Errorf("Expected key 'a' to be cleared")
	}
	if _, ok := cache.Get("b"); ok {
		t.Errorf("Expected key 'b' to be cleared")
	}
	if _, ok := cache.Get("c"); ok {
		t.Errorf("Expected key 'c' to be cleared")
	}
}
