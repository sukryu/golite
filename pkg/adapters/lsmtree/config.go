package lsmtree

import (
	"time"
)

// Config는 LSM Tree의 설정을 저장하는 구조체입니다.
type Config struct {
	// FilePath는 데이터베이스 파일이 저장될 기본 경로입니다.
	FilePath string

	// ThreadSafe는 스레드 안전 모드 활성화 여부를 결정합니다.
	ThreadSafe bool

	// MemTableSize는 메모리 테이블의 최대 크기(바이트)입니다.
	// 기본값은 16MB입니다.
	MemTableSize int

	// SSTableSize는 SSTable 파일의 목표 크기(바이트)입니다.
	// 기본값은 2MB입니다.
	SSTableSize int

	// CompactionInterval은 자동 컴팩션 간의 시간 간격입니다.
	// 기본값은 10초입니다.
	CompactionInterval time.Duration

	// CacheSize는 SSTable 블록 캐시의 최대 크기(바이트)입니다.
	// 기본값은 100MB입니다.
	CacheSize int

	// UseBloomFilter는 SSTable에 블룸 필터 사용 여부를 결정합니다.
	UseBloomFilter bool

	// CompactionStrategy는 사용할 컴팩션 전략을 지정합니다.
	// "leveling" 또는 "sizing"이 가능합니다.
	CompactionStrategy string

	// CompressionType은 SSTable 압축에 사용할 알고리즘을 지정합니다.
	// "none", "snappy", "zstd" 중 하나가 가능합니다.
	CompressionType string

	// SyncWrites는 WAL에 쓰기 후 디스크 동기화를 강제할지 여부입니다.
	// 활성화하면 안전성이 증가하지만 성능이 저하됩니다.
	SyncWrites bool

	// MaxOpenFiles는 동시에 열 수 있는 최대 SSTable 파일 수입니다.
	MaxOpenFiles int

	// RecoveryMode는 시작 시 복구 모드를 지정합니다.
	// "strict" 또는 "best_effort"가 가능합니다.
	RecoveryMode string

	// LogLevel은 로깅 세부 정보 수준을 지정합니다.
	// "debug", "info", "warn", "error" 중 하나가 가능합니다.
	LogLevel string
}

// DefaultConfig는 기본 설정으로 Config 인스턴스를 반환합니다.
func DefaultConfig() Config {
	return Config{
		FilePath:           "./lsmtree_data",
		ThreadSafe:         true,
		MemTableSize:       16 * 1024 * 1024, // 16MB
		SSTableSize:        2 * 1024 * 1024,  // 2MB
		CompactionInterval: 10 * time.Second,
		CacheSize:          100 * 1024 * 1024, // 100MB
		UseBloomFilter:     true,
		CompactionStrategy: "leveling",
		CompressionType:    "snappy",
		SyncWrites:         false,
		MaxOpenFiles:       1000,
		RecoveryMode:       "strict",
		LogLevel:           "info",
	}
}

// Validate는 설정의 유효성을 검사하고 잘못된 설정이 있으면 오류를 반환합니다.
func (c *Config) Validate() error {
	if c.MemTableSize <= 0 {
		return ErrInvalidConfig{"MemTableSize must be positive"}
	}
	if c.SSTableSize <= 0 {
		return ErrInvalidConfig{"SSTableSize must be positive"}
	}
	if c.CompactionInterval <= 0 {
		return ErrInvalidConfig{"CompactionInterval must be positive"}
	}
	if c.CacheSize < 0 {
		return ErrInvalidConfig{"CacheSize cannot be negative"}
	}
	if c.MaxOpenFiles <= 0 {
		return ErrInvalidConfig{"MaxOpenFiles must be positive"}
	}

	// 컴팩션 전략 검증
	switch c.CompactionStrategy {
	case "leveling", "sizing":
		// 유효함
	default:
		return ErrInvalidConfig{"CompactionStrategy must be 'leveling' or 'sizing'"}
	}

	// 압축 유형 검증
	switch c.CompressionType {
	case "none", "snappy", "zstd":
		// 유효함
	default:
		return ErrInvalidConfig{"CompressionType must be 'none', 'snappy', or 'zstd'"}
	}

	// 복구 모드 검증
	switch c.RecoveryMode {
	case "strict", "best_effort":
		// 유효함
	default:
		return ErrInvalidConfig{"RecoveryMode must be 'strict' or 'best_effort'"}
	}

	// 로그 레벨 검증
	switch c.LogLevel {
	case "debug", "info", "warn", "error":
		// 유효함
	default:
		return ErrInvalidConfig{"LogLevel must be 'debug', 'info', 'warn', or 'error'"}
	}

	return nil
}
