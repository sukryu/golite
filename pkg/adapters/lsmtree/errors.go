package lsmtree

import (
	"errors"
	"fmt"
)

// 기본 오류 정의
var (
	// ErrKeyNotFound는 요청된 키를 찾을 수 없을 때 반환됩니다.
	ErrKeyNotFound = errors.New("key not found")

	// ErrDBClosed는 닫힌 데이터베이스에 액세스하려고 할 때 반환됩니다.
	ErrDBClosed = errors.New("database is closed")

	// ErrInvalidKey는 유효하지 않은 키를 사용했을 때 반환됩니다.
	ErrInvalidKey = errors.New("invalid key")

	// ErrInvalidValue는 유효하지 않은 값을 사용했을 때 반환됩니다.
	ErrInvalidValue = errors.New("invalid value")

	// ErrMemTableFull은 메모리 테이블이 가득 찼을 때 반환됩니다.
	ErrMemTableFull = errors.New("memtable is full")

	// ErrWALCorrupted는 WAL 파일이 손상되었을 때 반환됩니다.
	ErrWALCorrupted = errors.New("WAL file is corrupted")

	// ErrSSTableCorrupted는 SSTable 파일이 손상되었을 때 반환됩니다.
	ErrSSTableCorrupted = errors.New("SSTable is corrupted")

	// ErrCompactionFailed는 컴팩션이 실패했을 때 반환됩니다.
	ErrCompactionFailed = errors.New("compaction failed")

	// ErrIOError는 I/O 작업 중 오류가 발생했을 때 반환됩니다.
	ErrIOError = errors.New("I/O error occurred")

	// ErrTooManyOpenFiles는 최대 허용 파일 수를 초과했을 때 반환됩니다.
	ErrTooManyOpenFiles = errors.New("too many open files")

	// ErrInvalidOperation은 현재 상태에서 유효하지 않은 작업을 수행하려고 할 때 반환됩니다.
	ErrInvalidOperation = errors.New("invalid operation for current state")

	// ErrRecoveryFailed는 복구 과정이 실패했을 때 반환됩니다.
	ErrRecoveryFailed = errors.New("recovery failed")

	// ErrConcurrentAccess는 동시성 문제가 발생했을 때 반환됩니다.
	ErrConcurrentAccess = errors.New("concurrent access conflict")
)

// ErrInvalidConfig는 설정 유효성 검사 오류를 표현합니다.
type ErrInvalidConfig struct {
	Message string
}

// Error는 error 인터페이스를 구현합니다.
func (e ErrInvalidConfig) Error() string {
	return fmt.Sprintf("invalid configuration: %s", e.Message)
}

// ErrSSTableError는 SSTable 관련 오류를 표현합니다.
type ErrSSTableError struct {
	TableID string
	Message string
}

// Error는 error 인터페이스를 구현합니다.
func (e ErrSSTableError) Error() string {
	return fmt.Sprintf("SSTable error (%s): %s", e.TableID, e.Message)
}

// ErrWALError는 WAL 관련 오류를 표현합니다.
type ErrWALError struct {
	Operation string
	Message   string
	Err       error
}

// Error는 error 인터페이스를 구현합니다.
func (e ErrWALError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("WAL error during %s: %s (%v)", e.Operation, e.Message, e.Err)
	}
	return fmt.Sprintf("WAL error during %s: %s", e.Operation, e.Message)
}

// Unwrap은 errors.Unwrap과 함께 사용하기 위한 메서드입니다.
func (e ErrWALError) Unwrap() error {
	return e.Err
}

// ErrCompactionError는 컴팩션 관련 오류를 표현합니다.
type ErrCompactionError struct {
	Level   int
	Message string
	Err     error
}

// Error는 error 인터페이스를 구현합니다.
func (e ErrCompactionError) Error() string {
	if e.Level >= 0 {
		if e.Err != nil {
			return fmt.Sprintf("compaction error at level %d: %s (%v)", e.Level, e.Message, e.Err)
		}
		return fmt.Sprintf("compaction error at level %d: %s", e.Level, e.Message)
	}

	if e.Err != nil {
		return fmt.Sprintf("compaction error: %s (%v)", e.Message, e.Err)
	}
	return fmt.Sprintf("compaction error: %s", e.Message)
}

// Unwrap은 errors.Unwrap과 함께 사용하기 위한 메서드입니다.
func (e ErrCompactionError) Unwrap() error {
	return e.Err
}

// IsNotFound는 주어진 오류가 키를 찾을 수 없는 오류인지 확인합니다.
func IsNotFound(err error) bool {
	return errors.Is(err, ErrKeyNotFound)
}

// IsCorrupted는 주어진 오류가 데이터 손상과 관련된 오류인지 확인합니다.
func IsCorrupted(err error) bool {
	return errors.Is(err, ErrWALCorrupted) || errors.Is(err, ErrSSTableCorrupted)
}

// IsIOError는 주어진 오류가 I/O 관련 오류인지 확인합니다.
func IsIOError(err error) bool {
	return errors.Is(err, ErrIOError)
}
