// Package ports는 GoLite의 헥사고날 아키텍처에서 저장소 관련 인터페이스를 정의합니다.
// 이 패키지는 도메인 로직과 어댑터(B-트리, LSM 등)를 연결하는 포트 역할을 합니다.
package ports

import "errors"

// StoragePort는 GoLite의 저장소 동작을 정의하는 인터페이스입니다.
// SQLite 1.0의 키-값 저장 방식을 기반으로 하며, 삽입, 조회, 삭제를 지원합니다.
type StoragePort interface {
	// Insert는 키-값 쌍을 저장소에 삽입합니다.
	// 키가 이미 존재하면 값을 덮어씌우고, 오류가 없으면 nil을 반환합니다.
	Insert(key string, value interface{}) error

	// Get은 주어진 키에 해당하는 값을 조회합니다.
	// 키가 존재하지 않으면 ErrKeyNotFound 오류를 반환합니다.
	Get(key string) (interface{}, error)

	// Delete는 주어진 키에 해당하는 키-값 쌍을 삭제합니다.
	// 키가 존재하지 않으면 ErrKeyNotFound 오류를 반환합니다.
	Delete(key string) error
}

// Item은 저장소에 저장되는 아이템의 비교를 위한 인터페이스입니다.
// B-트리와 같은 정렬 기반 자료구조에서 사용됩니다.
type Item interface {
	// Less는 현재 아이템이 주어진 아이템보다 작은지 여부를 반환합니다.
	// 키 기반으로 정렬을 보장합니다.
	Less(than Item) bool
}

// ErrKeyNotFound는 키가 저장소에 존재하지 않을 때 반환되는 오류입니다.
var ErrKeyNotFound = errors.New("key not found")

// StorageEventPort는 이벤트 기반 아키텍처를 위한 저장소 이벤트 인터페이스입니다.
// 삽입/삭제 작업 후 이벤트를 발생시키기 위해 사용됩니다.
type StorageEventPort interface {
	// OnInsert는 삽입 작업 후 호출되어 이벤트를 발생시킵니다.
	OnInsert(key string, value interface{})

	// OnDelete는 삭제 작업 후 호출되어 이벤트를 발생시킵니다.
	OnDelete(key string)
}
