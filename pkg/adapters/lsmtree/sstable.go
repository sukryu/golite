package lsmtree

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"sort"
)

// SSTable represents a Sorted String Table stored on disk.
type SSTable struct {
	filePath string
	minKey   string
	maxKey   string
	size     int64
	index    map[string]int64 // Simplified index: key -> file offset.
	Bloom    *BloomFilter
	checksum uint32
}

// CreateSSTable creates a new SSTable file from the given data.
func CreateSSTable(path string, data map[string]string, compressionType string, useBloom bool) (*SSTable, error) {
	// Open file for writing.
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Prepare sorted keys.
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	index := make(map[string]int64)
	var minKey, maxKey string
	var offset int64 = 0
	hasher := crc32.NewIEEE()

	// Write entries: [KeyLen][Key][ValLen][Value]
	for i, key := range keys {
		value := data[key]
		if i == 0 {
			minKey = key
		}
		maxKey = key
		keyLen := uint16(len(key))
		valLen := uint16(len(value))
		buf := new(bytes.Buffer)
		if err := binary.Write(buf, binary.BigEndian, keyLen); err != nil {
			return nil, err
		}
		if _, err := buf.Write([]byte(key)); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.BigEndian, valLen); err != nil {
			return nil, err
		}
		if _, err := buf.Write([]byte(value)); err != nil {
			return nil, err
		}
		entryBytes := buf.Bytes()
		// Update checksum.
		hasher.Write(entryBytes)
		// Write to file.
		n, err := file.Write(entryBytes)
		if err != nil {
			return nil, err
		}
		index[key] = offset
		offset += int64(n)
	}

	// Write checksum at the end.
	checksum := hasher.Sum32()
	if err := binary.Write(file, binary.BigEndian, checksum); err != nil {
		return nil, err
	}

	sst := &SSTable{
		filePath: path,
		minKey:   minKey,
		maxKey:   maxKey,
		size:     offset,
		index:    index,
		checksum: checksum,
	}
	if useBloom {
		bf := NewBloomFilter(1000) // Arbitrary capacity.
		for k := range data {
			bf.Add(k)
		}
		sst.Bloom = bf
	}
	return sst, nil
}

// OpenSSTable opens an existing SSTable file and loads its index.
func OpenSSTable(path string, useBloom bool) (*SSTable, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := fi.Size()

	// 마지막 4바이트는 체크섬임.
	dataEnd := fileSize - 4

	index := make(map[string]int64)
	var minKey, maxKey string
	var offset int64 = 0
	hasher := crc32.NewIEEE()

	for {
		currentOffset, _ := file.Seek(0, io.SeekCurrent)
		if currentOffset >= dataEnd {
			break
		}

		var keyLen uint16
		if err := binary.Read(file, binary.BigEndian, &keyLen); err != nil {
			return nil, err
		}
		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(file, keyBytes); err != nil {
			return nil, err
		}
		key := string(keyBytes)
		var valLen uint16
		if err := binary.Read(file, binary.BigEndian, &valLen); err != nil {
			return nil, err
		}
		valBytes := make([]byte, valLen)
		if _, err := io.ReadFull(file, valBytes); err != nil {
			return nil, err
		}

		// 체크섬 계산을 위한 버퍼 업데이트.
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.BigEndian, keyLen)
		buf.Write(keyBytes)
		binary.Write(buf, binary.BigEndian, valLen)
		buf.Write(valBytes)
		entryBytes := buf.Bytes()
		hasher.Write(entryBytes)

		if offset == 0 {
			minKey = key
		}
		maxKey = key
		index[key] = offset
		offset += int64(len(entryBytes))
	}

	// 체크섬 읽기.
	var fileChecksum uint32
	if err := binary.Read(file, binary.BigEndian, &fileChecksum); err != nil {
		return nil, err
	}
	computedChecksum := hasher.Sum32()
	if computedChecksum != fileChecksum {
		return nil, ErrSSTableCorrupted
	}

	sst := &SSTable{
		filePath: path,
		minKey:   minKey,
		maxKey:   maxKey,
		size:     offset,
		index:    index,
		checksum: fileChecksum,
	}
	if useBloom {
		bf := NewBloomFilter(1000)
		for k := range index {
			bf.Add(k)
		}
		sst.Bloom = bf
	}
	return sst, nil
}

// Get retrieves the value associated with the given key from the SSTable.
func (s *SSTable) Get(key string) (string, bool) {
	pos, exists := s.index[key]
	if !exists {
		return "", false
	}
	file, err := os.Open(s.filePath)
	if err != nil {
		return "", false
	}
	defer file.Close()
	// Seek to the key's position.
	if _, err := file.Seek(pos, io.SeekStart); err != nil {
		return "", false
	}
	var keyLen uint16
	if err := binary.Read(file, binary.BigEndian, &keyLen); err != nil {
		return "", false
	}
	keyBytes := make([]byte, keyLen)
	if _, err := io.ReadFull(file, keyBytes); err != nil {
		return "", false
	}
	var valLen uint16
	if err := binary.Read(file, binary.BigEndian, &valLen); err != nil {
		return "", false
	}
	valBytes := make([]byte, valLen)
	if _, err := io.ReadFull(file, valBytes); err != nil {
		return "", false
	}
	return string(valBytes), true
}
