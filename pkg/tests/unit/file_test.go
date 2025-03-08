package unit

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/sukryu/GoLite/pkg/adapters/file"
	"github.com/sukryu/GoLite/pkg/ports"
)

func TestFileBasicOperations(t *testing.T) {
	config := file.FileConfig{
		FilePath:   "test_basic.db",
		ThreadSafe: true,
	}
	f, err := file.NewFile(config)
	if err != nil {
		t.Fatalf("NewFile should succeed\n\tError Trace:\t%s\n\tError: %v", t.Name(), err)
	}
	defer os.Remove(config.FilePath)
	defer os.Remove(config.FilePath + ".wal")
	defer f.Close()

	// Insert and Get
	err = f.Insert("key1", "value1")
	if err != nil {
		t.Fatalf("Insert should succeed\n\tError Trace:\t%s\n\tError: %v", t.Name(), err)
	}
	time.Sleep(10 * time.Millisecond) // 비동기 인덱스 반영 대기
	val, err := f.Get("key1")
	if err != nil {
		t.Errorf("Get should succeed\n\tError Trace:\t%s\n\tError: %v", t.Name(), err)
	}
	if val != "value1" {
		t.Errorf("Get should return correct value\n\tError Trace:\t%s\n\tError: Not equal: \n\t\texpected: string(\"value1\")\n\t\tactual  : %#v(%v)", t.Name(), val, val)
	}

	// Insert and Delete
	err = f.Insert("key2", "value2")
	if err != nil {
		t.Fatalf("Insert should succeed\n\tError Trace:\t%s\n\tError: %v", t.Name(), err)
	}
	time.Sleep(10 * time.Millisecond)
	err = f.Delete("key2")
	if err != nil {
		t.Fatalf("Delete should succeed\n\tError Trace:\t%s\n\tError: %v", t.Name(), err)
	}
	time.Sleep(10 * time.Millisecond)
	val, err = f.Get("key2")
	if err == nil {
		t.Errorf("Get should fail after delete\n\tError Trace:\t%s\n\tError: An error is expected but got nil", t.Name())
	}
	if val != nil {
		t.Errorf("Get should return nil after delete\n\tError Trace:\t%s\n\tError: Expected nil, but got: %#v(%v)", t.Name(), val, val)
	}
}

func TestFileConcurrency(t *testing.T) {
	config := file.FileConfig{
		FilePath:   "test_concurrency.db",
		ThreadSafe: true,
	}
	f, err := file.NewFile(config)
	if err != nil {
		t.Fatalf("NewFile should succeed\n\tError Trace:\t%s\n\tError: %v", t.Name(), err)
	}
	defer os.Remove(config.FilePath)
	defer os.Remove(config.FilePath + ".wal")
	defer f.Close()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := "key" + string(rune('0'+i))
			err := f.Insert(key, "value"+string(rune('0'+i)))
			if err != nil {
				t.Errorf("Insert should succeed\n\tError Trace:\t%s\n\tError: %v", t.Name(), err)
			}
		}(i)
	}
	wg.Wait()
	time.Sleep(50 * time.Millisecond) // 비동기 인덱스 반영 대기

	for i := 0; i < 10; i++ {
		key := "key" + string(rune('0'+i))
		val, err := f.Get(key)
		if err != nil {
			t.Errorf("Get should succeed for %s\n\tError Trace:\t%s\n\tError: %v", key, t.Name(), err)
		}
		expected := "value" + string(rune('0'+i))
		if val != expected {
			t.Errorf("Get should return correct value for %s\n\tError Trace:\t%s\n\tError: Not equal: \n\t\texpected: string(\"%s\")\n\t\tactual  : %#v(%v)", key, t.Name(), expected, val, val)
		}
	}
}

func TestFilePersistence(t *testing.T) {
	config := file.FileConfig{
		FilePath:   "test_persistence.db",
		ThreadSafe: false,
	}
	f, err := file.NewFile(config)
	if err != nil {
		t.Fatalf("NewFile should succeed\n\tError Trace:\t%s\n\tError: %v", t.Name(), err)
	}
	defer os.Remove(config.FilePath)
	defer os.Remove(config.FilePath + ".wal")

	err = f.Insert("key1", "value1")
	if err != nil {
		t.Fatalf("Insert should succeed\n\tError Trace:\t%s\n\tError: %v", t.Name(), err)
	}
	err = f.Insert("key2", "value2")
	if err != nil {
		t.Fatalf("Insert should succeed\n\tError Trace:\t%s\n\tError: %v", t.Name(), err)
	}
	time.Sleep(10 * time.Millisecond) // 비동기 인덱스 반영 대기
	f.Close()

	f2, err := file.NewFile(config)
	if err != nil {
		t.Fatalf("NewFile should succeed\n\tError Trace:\t%s\n\tError: %v", t.Name(), err)
	}
	defer f2.Close()

	val, err := f2.Get("key1")
	if err != nil {
		t.Errorf("Get should succeed\n\tError Trace:\t%s\n\tError: %v", t.Name(), err)
	}
	if val != "value1" {
		t.Errorf("Get should return persisted value\n\tError Trace:\t%s\n\tError: Not equal: \n\t\texpected: string(\"value1\")\n\t\tactual  : %#v(%v)", t.Name(), val, val)
	}

	val, err = f2.Get("key2")
	if err != nil {
		t.Errorf("Get should succeed\n\tError Trace:\t%s\n\tError: %v", t.Name(), err)
	}
	if val != "value2" {
		t.Errorf("Get should return persisted value\n\tError Trace:\t%s\n\tError: Not equal: \n\t\texpected: string(\"value2\")\n\t\tactual  : %#v(%v)", t.Name(), val, val)
	}
}

func TestFileErrorHandling(t *testing.T) {
	config := file.FileConfig{
		FilePath:   "test_error.db",
		ThreadSafe: true,
	}
	f, err := file.NewFile(config)
	if err != nil {
		t.Fatalf("NewFile should succeed\n\tError Trace:\t%s\n\tError: %v", t.Name(), err)
	}
	defer os.Remove(config.FilePath)
	defer os.Remove(config.FilePath + ".wal")
	defer f.Close()

	// Invalid value type
	err = f.Insert("key1", 123)
	if err == nil {
		t.Errorf("Insert should fail with invalid value type\n\tError Trace:\t%s\n\tError: An error is expected but got nil", t.Name())
	}

	// Delete non-existent key
	err = f.Delete("nonexistent")
	if err != ports.ErrKeyNotFound {
		t.Errorf("Delete should return ErrKeyNotFound for nonexistent key\n\tError Trace:\t%s\n\tError: Expected %v, but got: %v", t.Name(), ports.ErrKeyNotFound, err)
	}

	// Get non-existent key
	val, err := f.Get("nonexistent")
	if err != ports.ErrKeyNotFound {
		t.Errorf("Get should return ErrKeyNotFound for nonexistent key\n\tError Trace:\t%s\n\tError: Expected %v, but got: %v", t.Name(), ports.ErrKeyNotFound, err)
	}
	if val != nil {
		t.Errorf("Get should return nil for nonexistent key\n\tError Trace:\t%s\n\tError: Expected nil, but got: %#v(%v)", t.Name(), val, val)
	}
}
