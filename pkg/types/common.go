package types

// Entry는 키-값 쌍과 삭제 여부를 나타냅니다.
type Entry struct {
	Key       string
	Value     string
	Tombstone bool
}

// Storage defines the interface for a complete key-value storage system.
type Storage interface {
	// Insert inserts or updates a key-value pair.
	Insert(key string, value string) error

	// Get retrieves the value for the given key.
	Get(key string) (string, error)

	// Delete removes or marks the key as deleted.
	Delete(key string) error

	// ForceCompaction triggers a manual compaction.
	ForceCompaction() error

	// Stats returns runtime statistics of the storage system.
	Stats() map[string]interface{}

	// Close gracefully shuts down the storage system.
	Close() error
}

// ConcurrentQueue는 동시성 환경에서 안전하게 사용할 수 있는 큐 인터페이스입니다.
type ConcurrentQueue[T any] interface {
	Enqueue(item T) bool
	Dequeue() (T, bool)
	Peek() (T, bool)
	Length() int
	IsEmpty() bool
}

// WALInterface는 Write-Ahead Log의 기본 동작을 정의합니다.
type WALInterface interface {
	Append(entry Entry) error
	Flush() error
	Reset() error
	Close() error
}

// MemTableStorage defines the operations of an in-memory table.
type MemTableStorage interface {
	// Insert inserts or updates a key-value pair.
	Insert(key, value string) error

	// Get retrieves the value associated with the key.
	// It returns the value and true if the key exists, or the zero value and false otherwise.
	Get(key string) (string, bool)

	// Delete marks a key as deleted (or inserts a tombstone).
	Delete(key string) error

	// Dump returns a snapshot of all non-deleted key-value pairs.
	Dump() map[string]string

	// Swap atomically swaps out the current table with a new empty one and returns a snapshot of the old data.
	Swap() map[string]string

	// Size returns the current size (e.g. number of bytes or count) of the table.
	Size() int64

	// Reset clears the table.
	Reset()
}

// SSTableInterface defines operations on a disk-based Sorted String Table.
type SSTableInterface interface {
	// Get retrieves the value associated with the given key from the SSTable.
	Get(key string) (string, bool)

	// Length returns the number of entries in the SSTable.
	Length() int

	// FilePath returns the path to the SSTable file.
	FilePath() string

	// VerifyIntegrity checks the integrity of the SSTable (e.g. via checksum).
	VerifyIntegrity() bool

	// Close releases any resources held by the SSTable.
	Close() error
}

// CacheInterface defines basic operations for a key-value cache.
type CacheInterface interface {
	// Get retrieves a cached value for the given key if present.
	Get(key string) (string, bool)

	// Put stores a key-value pair in the cache.
	Put(key, value string)

	// Length returns the current number of items in the cache.
	Length() int

	// Clear clears all cached entries.
	Clear()
}

// CompactorInterface defines operations for background compaction.
type CompactorInterface interface {
	// Run starts the compaction loop until the provided stop channel is closed.
	Run(stopCh <-chan struct{})

	// Compact performs a single compaction cycle, merging tables as needed.
	Compact() error
}
