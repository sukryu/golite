package lsmtree

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// LSMTree represents the Log-Structured Merge Tree.
type LSMTree struct {
	config Config
	// memTable을 atomic.Pointer로 관리하여 flush 시 원자적 교체를 가능하게 함.
	memTable  atomic.Pointer[MemTable]
	wal       *WAL
	levels    [][]*SSTable // levels[0] is level0, higher levels follow
	mu        sync.RWMutex // protects levels(LSMTree 전체 동기화를 위한 락)
	flushMu   sync.RWMutex // flush 작업 전용 락
	cache     *Cache
	metrics   *Metrics
	compactor *Compactor
	stopCh    chan struct{}
	wg        sync.WaitGroup
}

// NewLSMTree creates a new LSMTree instance with the given configuration.
func NewLSMTree(config Config) (*LSMTree, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(config.FilePath, 0755); err != nil {
		return nil, err
	}
	walPath := filepath.Join(config.FilePath, "db.wal")
	wal, err := NewWAL(walPath, config.SyncWrites)
	if err != nil {
		return nil, err
	}
	// 새로운 MemTable 생성 및 atomic.Pointer에 저장.
	mt := NewMemTable(config.MemTableSize)
	lsm := &LSMTree{
		config:  config,
		wal:     wal,
		levels:  make([][]*SSTable, 1),
		cache:   NewCache(config.CacheSize),
		metrics: NewMetrics(),
		stopCh:  make(chan struct{}),
	}
	lsm.memTable.Store(mt)

	// 기존 SSTable 로딩 및 WAL 복구는 그대로...
	if err := lsm.loadSSTables(); err != nil {
		return nil, err
	}
	if err := RecoverFromWAL(walPath, mt); err != nil {
		return nil, err
	}

	compactor, err := NewCompactor(lsm)
	if err != nil {
		return nil, err
	}
	lsm.compactor = compactor
	lsm.wg.Add(1)
	go func() {
		defer lsm.wg.Done()
		compactor.Run(lsm.stopCh)
	}()
	return lsm, nil
}

// loadSSTables loads existing SSTable files from the data directory into level0.
func (l *LSMTree) loadSSTables() error {
	files, err := os.ReadDir(l.config.FilePath)
	if err != nil {
		return err
	}
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if filepath.Ext(file.Name()) == ".sst" {
			sstPath := filepath.Join(l.config.FilePath, file.Name())
			sst, err := OpenSSTable(sstPath, l.config.UseBloomFilter)
			if err != nil {
				return err
			}
			l.levels[0] = append(l.levels[0], sst)
		}
	}
	// Sort level0 by minKey.
	sort.Slice(l.levels[0], func(i, j int) bool {
		return l.levels[0][i].minKey < l.levels[0][j].minKey
	})
	return nil
}

// Insert adds or updates a key-value pair in the LSM Tree.
func (l *LSMTree) Insert(key string, value string) error {
	entry := WalEntry{Op: 0x00, Key: key, Value: value}
	if err := l.wal.Append(entry); err != nil {
		return err
	}

	// 읽어온 memTable에 대해 삽입 시도.
	mt := l.memTable.Load()
	l.mu.RLock()
	err := mt.Insert(key, value)
	l.mu.RUnlock()
	if err == nil {
		l.metrics.IncWrites()
		return nil
	}
	if !errors.Is(err, ErrMemTableFull) {
		return err
	}
	// memTable이 가득 찼다면 flush 전에, 먼저 현재 memTable을 atomic하게 교체.
	if err := l.flushMemTable(); err != nil {
		return err
	}
	// flush 후 새 memTable에 다시 삽입.
	mt = l.memTable.Load()
	l.mu.RLock()
	err = mt.Insert(key, value)
	l.mu.RUnlock()
	if err != nil {
		return err
	}
	l.metrics.IncWrites()
	return nil
}

// Get retrieves the value associated with the given key.
func (l *LSMTree) Get(key string) (string, error) {
	// Check memTable.
	mt := l.memTable.Load()
	if value, ok := mt.Get(key); ok {
		l.metrics.IncCacheHit()
		return value, nil
	}

	// Check cache.
	if value, ok := l.cache.Get(key); ok {
		l.metrics.IncCacheHit()
		return value, nil
	}

	// Search SSTables across levels.
	l.mu.RLock()
	defer l.mu.RUnlock()
	for _, level := range l.levels {
		// Assume each level is sorted by minKey.
		idx := sort.Search(len(level), func(i int) bool {
			return level[i].maxKey >= key
		})
		if idx < len(level) && level[idx].minKey <= key {
			if val, found := level[idx].Get(key); found {
				l.cache.Put(key, val)
				l.metrics.IncReads()
				return val, nil
			}
		}
	}
	return "", ErrKeyNotFound
}

// Delete marks a key as deleted using a tombstone.
func (l *LSMTree) Delete(key string) error {
	entry := WalEntry{Op: 0x01, Key: key, Value: ""}
	if err := l.wal.Append(entry); err != nil {
		return err
	}
	mt := l.memTable.Load()
	if err := mt.Delete(key); err != nil {
		return err
	}
	l.metrics.IncWrites()
	return nil
}

// flushMemTable atomically flushes the current memTable.
func (l *LSMTree) flushMemTable() error {
	// flush 전용 락으로 중복 flush 방지.
	l.flushMu.Lock()
	defer l.flushMu.Unlock()

	// l.mu로 levels 등 내부 상태 업데이트 보호.
	l.mu.Lock()
	oldMT := l.memTable.Load()
	if oldMT.Size() == 0 {
		l.mu.Unlock()
		return nil
	}
	data := oldMT.Swap()
	// 새로운 memTable 생성.
	newMT := NewMemTable(l.config.MemTableSize)
	l.memTable.Store(newMT)
	// SSTable 생성.
	sstPath := filepath.Join(l.config.FilePath, fmt.Sprintf("db.sst.%d.sst", time.Now().UnixNano()))
	sst, err := CreateSSTable(sstPath, data, l.config.CompressionType, l.config.UseBloomFilter)
	if err != nil {
		l.mu.Unlock()
		return err
	}
	l.levels[0] = append(l.levels[0], sst)
	sort.Slice(l.levels[0], func(i, j int) bool {
		return l.levels[0][i].minKey < l.levels[0][j].minKey
	})
	l.mu.Unlock()

	// WAL 처리는 락 해제 후 진행.
	l.wal.Flush()
	if err := l.wal.Reset(); err != nil {
		return err
	}
	return nil
}

// ForceCompaction triggers manual compaction.
func (l *LSMTree) ForceCompaction() error {
	// Flush memTable if not empty.
	mt := l.memTable.Load()
	if mt.Size() > 0 {
		if err := l.flushMemTable(); err != nil {
			return err
		}
	}
	return l.compactor.Compact()
}

// Stats returns current statistics of the LSM Tree.
func (l *LSMTree) Stats() map[string]interface{} {
	l.mu.RLock()
	defer l.mu.RUnlock()
	mt := l.memTable.Load()
	stats := make(map[string]interface{})
	stats["memtable_size"] = mt.Size()
	totalSSTables := 0
	for _, level := range l.levels {
		totalSSTables += len(level)
	}
	stats["sstable_count"] = totalSSTables
	stats["writes"] = l.metrics.Writes
	stats["reads"] = l.metrics.Reads
	return stats
}

// Close gracefully shuts down the LSM Tree.
func (l *LSMTree) Close() error {
	close(l.stopCh)
	l.wg.Wait()
	if err := l.flushMemTable(); err != nil {
		return err
	}
	return l.wal.Close()
}
