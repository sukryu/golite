package file

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/sukryu/GoLite/pkg/ports"
)

// FileConfig defines configuration for the File storage adapter.
type FileConfig struct {
	FilePath   string
	ThreadSafe bool
}

// File implements the StoragePort interface using a file-based backend.
type File struct {
	config    FileConfig
	file      *os.File
	walFile   *os.File
	data      []entry         // 모든 엔트리를 보관 (compaction 대상)
	index     *sync.Map       // 빠른 조회를 위한 인메모리 해시 인덱스
	isSorted  bool            // compaction 후 정렬 여부
	mu        sync.RWMutex    // data와 isSorted 보호
	walMu     sync.Mutex      // WAL 버퍼 관련 동기화
	compactCh chan struct{}   // compaction 요청 채널
	stopCh    chan struct{}   // 워커 종료 채널
	walCh     chan []WalEntry // 배치 WAL 엔트리 전송 채널
	wg        sync.WaitGroup
	walBuffer []byte // WAL 바이너리 버퍼
	walBufIdx int
	flushSize int
	seqBuffer []byte // ThreadSafe=false일 때의 WAL 버퍼
	seqBufIdx int
}

// WalEntry represents a write-ahead log entry.
type WalEntry struct {
	Op    string
	Key   string
	Value string
}

type entry struct {
	key     string
	value   string
	deleted bool
}

// Operation codes for binary WAL format.
const (
	OpInsert byte = 0x00
	OpDelete byte = 0x01
)

// Magic number for binary WAL format (version 1).
var magicNumber = []byte("GLB1")

func NewFile(config FileConfig) (*File, error) {
	if config.FilePath == "" {
		return nil, fmt.Errorf("file path is required")
	}

	file, err := os.OpenFile(config.FilePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open main file: %v", err)
	}

	walFile, err := os.OpenFile(config.FilePath+".wal", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to open wal file: %v", err)
	}

	f := &File{
		config:    config,
		file:      file,
		walFile:   walFile,
		data:      make([]entry, 0, 1000),
		index:     &sync.Map{},
		isSorted:  true,
		compactCh: make(chan struct{}, 1),
		stopCh:    make(chan struct{}),
		walCh:     make(chan []WalEntry, 1000),
		walBuffer: make([]byte, 4*1024*1024),
		flushSize: 4 * 1024 * 1024,
		seqBuffer: make([]byte, 4*1024*1024),
	}

	if err := f.loadFromFile(); err != nil {
		file.Close()
		walFile.Close()
		return nil, fmt.Errorf("failed to load main file: %v", err)
	}
	if err := f.loadFromWAL(); err != nil {
		file.Close()
		walFile.Close()
		return nil, fmt.Errorf("failed to load wal file: %v", err)
	}

	// 초기 데이터로 인덱스 구축
	for _, e := range f.data {
		if !e.deleted {
			f.index.Store(e.key, e.value)
		}
	}

	f.wg.Add(1)
	go f.walWorker()
	f.wg.Add(1)
	go f.compactWorker()

	return f, nil
}

func (f *File) loadFromFile() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	stat, err := f.file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %v", err)
	}
	if stat.Size() == 0 {
		return nil
	}

	data, err := os.ReadFile(f.config.FilePath)
	if err != nil {
		return fmt.Errorf("failed to read file: %v", err)
	}

	if len(data) < 8 || string(data[:4]) != string(magicNumber) {
		log.Printf("loadFromFile: invalid format, len=%d, magic=%s", len(data), data[:4])
		return fmt.Errorf("invalid main file format")
	}
	numEntries := binary.LittleEndian.Uint32(data[4:8])
	f.data = make([]entry, 0, numEntries)
	pos := 8
	for i := uint32(0); i < numEntries; i++ {
		if pos+4 > len(data) {
			log.Printf("loadFromFile: insufficient data at pos=%d, len=%d", pos, len(data))
			return fmt.Errorf("corrupted main file: insufficient data")
		}
		// 기록 순서: keyLen (2), valLen (2), key, value
		keyLen := binary.LittleEndian.Uint16(data[pos : pos+2])
		valLen := binary.LittleEndian.Uint16(data[pos+2 : pos+4])
		pos += 4
		if pos+int(keyLen)+int(valLen) > len(data) {
			log.Printf("loadFromFile: data overflow at pos=%d, keyLen=%d, valLen=%d, len=%d", pos, keyLen, valLen, len(data))
			return fmt.Errorf("corrupted main file: data overflow")
		}
		key := string(data[pos : pos+int(keyLen)])
		pos += int(keyLen)
		value := string(data[pos : pos+int(valLen)])
		pos += int(valLen)
		f.data = append(f.data, entry{key: key, value: value})
	}
	sort.Slice(f.data, func(i, j int) bool { return f.data[i].key < f.data[j].key })
	log.Printf("loadFromFile: loaded entries=%d, final pos=%d, data len=%d", len(f.data), pos, len(data))
	return nil
}

func (f *File) loadFromWAL() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	stat, err := f.walFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat wal file: %v", err)
	}
	if stat.Size() == 0 {
		if _, err := f.walFile.Write(magicNumber); err != nil {
			return fmt.Errorf("failed to write magic number: %v", err)
		}
		return f.walFile.Sync()
	}

	scanner := bufio.NewReader(f.walFile)
	magic := make([]byte, len(magicNumber))
	if _, err := scanner.Read(magic); err != nil {
		return fmt.Errorf("failed to read magic number: %v", err)
	}
	if string(magic) != string(magicNumber) {
		return fmt.Errorf("invalid WAL format: expected %s, got %s", magicNumber, magic)
	}

	for {
		op, err := scanner.ReadByte()
		if err != nil {
			break // EOF 정상 종료
		}

		keyLenBuf := make([]byte, 2)
		if _, err := scanner.Read(keyLenBuf); err != nil {
			return fmt.Errorf("failed to read key length: %v", err)
		}
		keyLen := binary.LittleEndian.Uint16(keyLenBuf)
		if keyLen > uint16(f.flushSize) {
			return fmt.Errorf("key length %d exceeds max buffer size %d", keyLen, f.flushSize)
		}

		key := make([]byte, keyLen)
		if _, err := scanner.Read(key); err != nil {
			return fmt.Errorf("failed to read key: %v", err)
		}

		switch op {
		case OpInsert:
			valLenBuf := make([]byte, 2)
			if _, err := scanner.Read(valLenBuf); err != nil {
				return fmt.Errorf("failed to read value length: %v", err)
			}
			valLen := binary.LittleEndian.Uint16(valLenBuf)
			if valLen > uint16(f.flushSize) {
				return fmt.Errorf("value length %d exceeds max buffer size %d", valLen, f.flushSize)
			}

			value := make([]byte, valLen)
			if _, err := scanner.Read(value); err != nil {
				return fmt.Errorf("failed to read value: %v", err)
			}
			f.data = append(f.data, entry{key: string(key), value: string(value)})
			f.index.Store(string(key), string(value))
		case OpDelete:
			f.data = append(f.data, entry{key: string(key), deleted: true})
			f.index.Delete(string(key))
		default:
			return fmt.Errorf("unknown operation code: %d", op)
		}
	}
	f.isSorted = false
	return nil
}

func (f *File) Insert(key string, value interface{}) error {
	valStr, ok := value.(string)
	if !ok {
		return fmt.Errorf("value must be string")
	}
	if f.config.ThreadSafe {
		f.mu.Lock()
		f.data = append(f.data, entry{key: key, value: valStr})
		f.isSorted = false
		f.mu.Unlock()
		go f.index.Store(key, valStr)
		f.walCh <- []WalEntry{{Op: "INSERT", Key: key, Value: valStr}}
	} else {
		f.data = append(f.data, entry{key: key, value: valStr})
		f.isSorted = false
		go f.index.Store(key, valStr)
		keyLen := uint16(len(key))
		valLen := uint16(len(valStr))
		entryLen := 1 + 2 + int(keyLen) + 2 + int(valLen)
		if f.seqBufIdx+entryLen > f.flushSize {
			f.flushSeqBuffer()
		}
		buf := f.seqBuffer[f.seqBufIdx : f.seqBufIdx+entryLen]
		buf[0] = OpInsert
		buf[1] = byte(keyLen & 0xFF)
		buf[2] = byte(keyLen >> 8)
		copy(buf[3:3+keyLen], key)
		buf[3+keyLen] = byte(valLen & 0xFF)
		buf[4+keyLen] = byte(valLen >> 8)
		copy(buf[5+keyLen:], valStr)
		f.seqBufIdx += entryLen
	}
	return nil
}

func (f *File) InsertBatch(entries []WalEntry) error {
	if f.config.ThreadSafe {
		f.mu.Lock()
		for _, e := range entries {
			if e.Op == "INSERT" {
				f.data = append(f.data, entry{key: e.Key, value: e.Value})
				f.index.Store(e.Key, e.Value)
			} else if e.Op == "DELETE" {
				f.data = append(f.data, entry{key: e.Key, deleted: true})
				f.index.Delete(e.Key)
			}
		}
		f.isSorted = false
		f.mu.Unlock()
		f.walCh <- entries
	} else {
		totalLen := 0
		for _, e := range entries {
			if e.Op == "INSERT" {
				totalLen += 1 + 2 + len(e.Key) + 2 + len(e.Value)
			} else if e.Op == "DELETE" {
				totalLen += 1 + 2 + len(e.Key)
			}
		}
		if f.seqBufIdx+totalLen > f.flushSize {
			f.flushSeqBuffer()
		}
		buf := f.seqBuffer[f.seqBufIdx : f.seqBufIdx+totalLen]
		pos := 0
		for _, e := range entries {
			if e.Op == "INSERT" {
				keyLen := uint16(len(e.Key))
				valLen := uint16(len(e.Value))
				buf[pos] = OpInsert
				pos++
				buf[pos] = byte(keyLen & 0xFF)
				buf[pos+1] = byte(keyLen >> 8)
				pos += 2
				copy(buf[pos:pos+int(keyLen)], e.Key)
				pos += int(keyLen)
				buf[pos] = byte(valLen & 0xFF)
				buf[pos+1] = byte(valLen >> 8)
				pos += 2
				copy(buf[pos:pos+int(valLen)], e.Value)
				pos += int(valLen)
				f.data = append(f.data, entry{key: e.Key, value: e.Value})
				f.index.Store(e.Key, e.Value)
			} else if e.Op == "DELETE" {
				keyLen := uint16(len(e.Key))
				buf[pos] = OpDelete
				pos++
				buf[pos] = byte(keyLen & 0xFF)
				buf[pos+1] = byte(keyLen >> 8)
				pos += 2
				copy(buf[pos:pos+int(keyLen)], e.Key)
				pos += int(keyLen)
				f.data = append(f.data, entry{key: e.Key, deleted: true})
				f.index.Delete(e.Key)
			}
		}
		f.isSorted = false
		f.seqBufIdx += totalLen
	}
	return nil
}

func (f *File) Get(key string) (interface{}, error) {
	if f == nil {
		return nil, fmt.Errorf("file adapter is nil")
	}
	if val, ok := f.index.Load(key); ok {
		return val, nil
	}
	return nil, ports.ErrKeyNotFound
}

func (f *File) Delete(key string) error {
	if f.config.ThreadSafe {
		f.mu.Lock()
		found := false
		newData := f.data[:0]
		for _, e := range f.data {
			if e.key == key {
				if !e.deleted {
					found = true
				}
			} else {
				newData = append(newData, e)
			}
		}
		if !found {
			f.mu.Unlock()
			return ports.ErrKeyNotFound
		}
		f.data = append(newData, entry{key: key, deleted: true})
		f.index.Delete(key)
		f.isSorted = false
		f.mu.Unlock()
		f.walCh <- []WalEntry{{Op: "DELETE", Key: key, Value: ""}}
	} else {
		found := false
		newData := f.data[:0]
		for _, e := range f.data {
			if e.key == key {
				if !e.deleted {
					found = true
				}
			} else {
				newData = append(newData, e)
			}
		}
		if !found {
			return ports.ErrKeyNotFound
		}
		f.data = append(newData, entry{key: key, deleted: true})
		f.index.Delete(key)
		f.isSorted = false
		keyLen := uint16(len(key))
		entryLen := 1 + 2 + int(keyLen)
		if f.seqBufIdx+entryLen > f.flushSize {
			f.flushSeqBuffer()
		}
		buf := f.seqBuffer[f.seqBufIdx : f.seqBufIdx+entryLen]
		buf[0] = OpDelete
		buf[1] = byte(keyLen & 0xFF)
		buf[2] = byte(keyLen >> 8)
		copy(buf[3:], key)
		f.seqBufIdx += entryLen
		f.walCh <- []WalEntry{{Op: "DELETE", Key: key, Value: ""}}
	}
	return nil
}

func (f *File) flushBuffer() error {
	f.walMu.Lock()
	defer f.walMu.Unlock()

	if f.walBufIdx == 0 {
		return nil
	}

	if _, err := f.walFile.Write(f.walBuffer[:f.walBufIdx]); err != nil {
		return fmt.Errorf("failed to write to wal: %v", err)
	}
	if err := f.walFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync wal: %v", err)
	}

	f.walBufIdx = 0
	return nil
}

func (f *File) flushSeqBuffer() error {
	if f.seqBufIdx == 0 {
		return nil
	}

	f.walMu.Lock()
	defer f.walMu.Unlock()
	if _, err := f.walFile.Write(f.seqBuffer[:f.seqBufIdx]); err != nil {
		return fmt.Errorf("failed to write to wal: %v", err)
	}
	if err := f.walFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync wal: %v", err)
	}

	f.seqBufIdx = 0
	return nil
}

func (f *File) appendWAL(entries []WalEntry) {
	for _, entry := range entries {
		if entry.Op == "INSERT" {
			keyLen := uint16(len(entry.Key))
			valLen := uint16(len(entry.Value))
			entryLen := 1 + 2 + int(keyLen) + 2 + int(valLen)
			if f.walBufIdx+entryLen > f.flushSize {
				f.flushBuffer()
			}
			buf := f.walBuffer[f.walBufIdx : f.walBufIdx+entryLen]
			buf[0] = OpInsert
			buf[1] = byte(keyLen & 0xFF)
			buf[2] = byte(keyLen >> 8)
			copy(buf[3:3+keyLen], entry.Key)
			buf[3+keyLen] = byte(valLen & 0xFF)
			buf[4+keyLen] = byte(valLen >> 8)
			copy(buf[5+keyLen:], entry.Value)
			f.walBufIdx += entryLen
		} else if entry.Op == "DELETE" {
			keyLen := uint16(len(entry.Key))
			entryLen := 1 + 2 + int(keyLen)
			if f.walBufIdx+entryLen > f.flushSize {
				f.flushBuffer()
			}
			buf := f.walBuffer[f.walBufIdx : f.walBufIdx+entryLen]
			buf[0] = OpDelete
			buf[1] = byte(keyLen & 0xFF)
			buf[2] = byte(keyLen >> 8)
			copy(buf[3:], entry.Key)
			f.walBufIdx += entryLen
		}
	}
}

func (f *File) walWorker() {
	defer f.wg.Done()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case entries, ok := <-f.walCh:
			if !ok {
				f.flushBuffer()
				return
			}
			f.appendWAL(entries)
		case <-ticker.C:
			f.flushBuffer()
		}
	}
}

func (f *File) compactWorker() {
	defer f.wg.Done()
	ticker := time.NewTicker(968 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-f.stopCh:
			return
		case <-f.compactCh:
			f.compact()
		case <-ticker.C:
			f.compact()
		}
	}
}

func (f *File) compact() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Build compacted slice: 마지막 유효 엔트리만 유지
	compacted := make([]entry, 0, len(f.data))
	seen := make(map[string]int)
	for i, e := range f.data {
		if !e.deleted {
			seen[e.key] = i
		} else {
			delete(seen, e.key)
		}
	}
	for _, idx := range seen {
		compacted = append(compacted, f.data[idx])
	}
	sort.Slice(compacted, func(i, j int) bool { return compacted[i].key < compacted[j].key })

	totalSize := 4 + 4 // magicNumber (4) + numEntries (4)
	for _, e := range compacted {
		totalSize += 2 + 2 + len(e.key) + len(e.value)
	}

	buf := make([]byte, totalSize)
	copy(buf[0:4], magicNumber)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(compacted)))
	pos := 8
	for _, e := range compacted {
		keyLen := uint16(len(e.key))
		valLen := uint16(len(e.value))
		// Write keyLen and valLen
		buf[pos] = byte(keyLen & 0xFF)
		buf[pos+1] = byte(keyLen >> 8)
		buf[pos+2] = byte(valLen & 0xFF)
		buf[pos+3] = byte(valLen >> 8)
		pos += 4
		copy(buf[pos:pos+int(keyLen)], e.key)
		pos += int(keyLen)
		copy(buf[pos:pos+int(valLen)], e.value)
		pos += int(valLen)
	}

	log.Printf("Compaction: buffer size=%d, entries=%d", len(buf), len(compacted))
	if err := os.WriteFile(f.config.FilePath, buf, 0666); err != nil {
		log.Printf("Compaction failed: failed to write file: %v", err)
		return fmt.Errorf("failed to write file: %v", err)
	}
	if err := f.file.Sync(); err != nil {
		log.Printf("Compaction failed: failed to sync file: %v", err)
		return fmt.Errorf("failed to sync file: %v", err)
	}

	f.walMu.Lock()
	defer f.walMu.Unlock()
	if err := f.walFile.Truncate(0); err != nil {
		log.Printf("Compaction failed: failed to truncate wal: %v", err)
		return fmt.Errorf("failed to truncate wal: %v", err)
	}
	if _, err := f.walFile.Seek(0, 0); err != nil {
		log.Printf("Compaction failed: failed to reset wal: %v", err)
		return fmt.Errorf("failed to reset wal: %v", err)
	}
	if _, err := f.walFile.Write(magicNumber); err != nil {
		log.Printf("Compaction failed: failed to write magic number: %v", err)
		return fmt.Errorf("failed to write magic number: %v", err)
	}
	if err := f.walFile.Sync(); err != nil {
		log.Printf("Compaction failed: failed to sync wal: %v", err)
		return fmt.Errorf("failed to sync wal: %v", err)
	}

	f.data = compacted
	newIndex := &sync.Map{}
	for _, e := range compacted {
		newIndex.Store(e.key, e.value)
	}
	f.index = newIndex
	f.isSorted = true
	return nil
}

func (f *File) Close() error {
	if f == nil {
		return fmt.Errorf("file adapter is nil")
	}
	close(f.walCh)
	close(f.stopCh)
	f.wg.Wait()

	if f.config.ThreadSafe {
		f.flushBuffer()
	} else {
		f.flushSeqBuffer()
	}
	if err := f.compact(); err != nil {
		return err
	}

	if err := f.file.Close(); err != nil {
		return err
	}
	return f.walFile.Close()
}
