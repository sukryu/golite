package lsmtree

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
	"sync"
	"time"
)

// Compactor handles background compaction using leveling.
type Compactor struct {
	lsm *LSMTree
	mu  sync.Mutex
}

// NewCompactor creates a new Compactor for the given LSMTree.
func NewCompactor(lsm *LSMTree) (*Compactor, error) {
	return &Compactor{
		lsm: lsm,
	}, nil
}

// Run starts the compaction loop.
func (c *Compactor) Run(stopCh <-chan struct{}) {
	ticker := time.NewTicker(c.lsm.config.CompactionInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			if err := c.Compact(); err != nil {
				fmt.Printf("Compaction error: %v\n", err)
			}
		}
	}
}

// Compact performs leveling compaction on level0 if threshold is reached.
func (c *Compactor) Compact() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	lsm := c.lsm
	lsm.mu.Lock()
	defer lsm.mu.Unlock()

	// Trigger compaction if level0 has 4 or more SSTables.
	if len(lsm.levels[0]) < 4 {
		return nil
	}

	// Merge level0 SSTables using streaming merge.
	merged, err := mergeSSTables(lsm.levels[0], lsm.config)
	if err != nil {
		return err
	}
	// Remove level0 files.
	lsm.levels[0] = nil
	// Append merged SSTable to level1.
	if len(lsm.levels) < 2 {
		lsm.levels = append(lsm.levels, []*SSTable{merged})
	} else {
		lsm.levels[1] = append(lsm.levels[1], merged)
	}
	// Sort level1 by minKey.
	sort.Slice(lsm.levels[1], func(i, j int) bool {
		return lsm.levels[1][i].minKey < lsm.levels[1][j].minKey
	})
	return nil
}

// mergeSSTables performs a streaming merge of the provided SSTables into one.
func mergeSSTables(ssts []*SSTable, config Config) (*SSTable, error) {
	newPath := fmt.Sprintf("%s/db.sst.%d.sst", config.FilePath, time.Now().UnixNano())
	outFile, err := os.Create(newPath)
	if err != nil {
		return nil, err
	}
	defer outFile.Close()

	// For each SSTable, copy its data excluding the last 4 bytes (checksum).
	for _, sst := range ssts {
		f, err := os.Open(sst.filePath)
		if err != nil {
			return nil, err
		}
		fi, err := f.Stat()
		if err != nil {
			f.Close()
			return nil, err
		}
		dataSize := fi.Size() - 4
		if _, err := io.CopyN(outFile, f, dataSize); err != nil {
			f.Close()
			return nil, err
		}
		f.Close()
	}
	// Compute checksum for the merged file.
	outFile.Sync()
	outFile.Seek(0, io.SeekStart)
	data, err := io.ReadAll(outFile)
	if err != nil {
		return nil, err
	}
	checksum := crc32.ChecksumIEEE(data)
	if err := binary.Write(outFile, binary.BigEndian, checksum); err != nil {
		return nil, err
	}
	// Open new SSTable.
	newSST, err := OpenSSTable(newPath, config.UseBloomFilter)
	if err != nil {
		return nil, err
	}
	return newSST, nil
}
