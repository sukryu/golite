# GoLite Performance Evolution

이 문서는 GoLite의 `File` 어댑터에 대한 성능 최적화 과정을 기록합니다. 각 단계에서의 벤치마크 결과를 비교하고, 어떻게 개선했는지 설명합니다. 모든 테스트는 MacBook Air M2 (8GB/256GB, Apple M2 CPU, Darwin/arm64) 환경에서 수행되었습니다.

## 초기 상태 (No Optimization)
최적화 전 `File` 어댑터는 매 `Insert`마다 전체 데이터를 JSON으로 직렬화하여 디스크에 기록했습니다. 이는 심각한 I/O 부하를 초래했습니다.

### 벤치마크 결과
- **InsertSequential/file**: 390번, 3,572,813 ns/op (~280 ops/s), 20,485 B/op, 408 allocs/op.
- **InsertConcurrent/file**: 414번, 3,224,270 ns/op (~310 ops/s), 21,693 B/op, 434 allocs/op.
- **GetSequential/file**: 3,270,780번, 318.2 ns/op (~3,142,995 ops/s), 189 B/op, 9 allocs/op.
- **GetConcurrent/file**: 1,947,231번, 597.2 ns/op (~1,674,528 ops/s), 245 B/op, 11 allocs/op.

### 분석
- **Insert**: 초당 280-310 작업으로, 전체 파일 쓰기(`os.WriteFile`)가 병목. SSD에서도 매번 동기화로 인해 극도로 느림.
- **Get**: 메모리 맵(`f.data`) 기반이라 읽기 성능은 우수 (~1.6M-3.1M ops/s).
- **메모리**: JSON 직렬화로 작업당 20KB 이상, 400+ 할당 발생 → 비효율적.

---

## 1단계: Append-Only Log + WAL
### 적용 내용
- **WAL 도입**: `Insert`와 `Delete`를 `wal.log` 파일에 순차 추가(append-only).
- **메모리 맵**: `f.data`로 즉시 읽기 지원 유지.
- **컴팩션**: 주기적(10초) 또는 WAL 크기(1MB 초과) 시 `main.db`에 압축.

### 코드 변경
- `appendWAL`: 매 작업마다 로그에 기록, `Sync()`로 디스크 반영.
- `compactWorker`: 백그라운드에서 주기적 컴팩션 수행.

### 벤치마크 결과
- **InsertSequential/file**: 754번, 2,396,372 ns/op (~417 ops/s), 666 B/op, 18 allocs/op.
- **InsertConcurrent/file**: 630번, 2,475,479 ns/op (~404 ops/s), 1,007 B/op, 23 allocs/op.
- **GetSequential/file**: 3,532,046번, 316.0 ns/op (~3,164,557 ops/s), 189 B/op, 9 allocs/op.
- **GetConcurrent/file**: 2,221,436번, 798.0 ns/op (~1,253,132 ops/s), 245 B/op, 11 allocs/op.

### 변화
- **InsertSequential**: 280 → 417 ops/s (~49% 증가), latency 33% 감소.
- **InsertConcurrent**: 310 → 404 ops/s (~30% 증가), latency 23% 감소.
- **메모리**: 20KB+ → 666-1,007 B/op, 할당 400+ → 18-23으로 대폭 감소.

### 분석
- **개선**: 전체 파일 쓰기 대신 로그 추가로 I/O 부하 감소.
- **한계**: 매 `Insert`마다 `Sync()` 호출로 동기 I/O 병목 잔존 → 목표(50K-100K ops/s) 미달성.
- **읽기**: 변화 미미 (메모리 기반 유지).

---

## 2단계: Buffered Writing
### 적용 내용
- **버퍼링**: `Insert` 시 WAL 엔트리를 메모리 버퍼(`[]byte`)에 모음.
- **플러시**: 버퍼 크기(4MB) 초과 또는 1초 주기 도달 시 디스크에 일괄 쓰기.
- **백그라운드 워커**: `bufferFlushWorker`로 주기적 플러시 수행.

### 코드 변경
- `appendWAL`: 디스크 대신 `f.buffer`에 추가, 크기 조건 시 `flushBuffer`.
- `bufferFlushWorker`: 1초마다 버퍼 플러시, `Close`에서 잔여 버퍼 처리.
- `bufferMu`: 버퍼 접근 동기화.

### 벤치마크 결과
- **InsertSequential/file**: 1,335,028번, 885.7 ns/op (~1,128,847 ops/s), 521 B/op, 18 allocs/op.
- **InsertConcurrent/file**: 727,887번, 5,585 ns/op (~179,047 ops/s), 1,344 B/op, 25 allocs/op.
- **GetSequential/file**: 3,652,507번, 339.4 ns/op (~2,946,402 ops/s), 189 B/op, 9 allocs/op.
- **GetConcurrent/file**: 2,302,314번, 517.0 ns/op (~1,934,306 ops/s), 245 B/op, 11 allocs/op.

### 변화
- **InsertSequential**: 417 → 1,128,847 ops/s (~2,700배 증가), latency 63% 감소.
- **InsertConcurrent**: 404 → 179,047 ops/s (~443배 증가), latency 99.8% 감소.
- **메모리**: 521-1,344 B/op 유지, 할당량 약간 증가 (18 → 25).

### 분석
- **성공 요인**: 매번 I/O 대신 버퍼링으로 호출 횟수 대폭 감소, M2 SSD의 빠른 쓰기 활용.
- **목표 달성**: 1단계 목표(50K-100K ops/s) 초과, 2단계 목표(100K-200K ops/s) 달성.
- **한계**: 동시성에서 `f.mu`와 `f.bufferMu` 경쟁으로 순차(1.13M ops/s)보다 낮음(179K ops/s).

---

## 요약
| 단계             | InsertSequential (ops/s) | InsertConcurrent (ops/s) | 주요 개선 요소          |
|------------------|--------------------------|--------------------------|-------------------------|
| 초기 상태        | 280                     | 310                     | -                       |
| 1단계 (WAL)      | 417 (~1.5x)             | 404 (~1.3x)             | 로그 추가, 동기 I/O     |
| 2단계 (Buffering)| 1,128,847 (~2,700x)     | 179,047 (~443x)         | 버퍼링, I/O 감소        |

- **초기 → 1단계**: 로그 기반으로 약간 개선, 동기 I/O 병목 잔존.
- **1단계 → 2단계**: 버퍼링으로 I/O 부하 제거, 처리량 폭발적 증가.

---

## 어떻게 만들었는가?
### 1단계: Append-Only Log + WAL
- **아이디어**: PostgreSQL의 WAL에서 영감, 전체 파일 쓰기를 로그 추가로 대체.
- **구현**: `appendWAL`로 `wal.log`에 순차 기록, `compactWorker`로 주기적 압축.
- **한계 해결 부족**: `Sync()` 호출로 여전히 동기 I/O 부담.

### 2단계: Buffered Writing
- **아이디어**: PostgreSQL의 버퍼링 전략, 매 작업 I/O를 최소화.
- **구현**: `f.buffer`에 엔트리 모으고, 4MB 또는 1초 조건으로 `flushBuffer`, `bufferFlushWorker`로 비동기 처리.
- **성공 요인**: I/O 호출 횟수 감소, M2 SSD의 빠른 쓰기 활용, 뮤텍스 범위 최소화.

---

## 3단계: Background Writing
### 적용 내용
- **비동기 쓰기**: `walChan`으로 WAL 요청을 백그라운드 워커에 전달.
- **워커 통합**: `walWorker`가 버퍼링과 주기적(1초) 플러시 처리.
- **종료 보장**: `Close`에서 `walChan` 닫고 최종 플러시.

### 코드 변경
- `Insert`: `f.walChan`에 요청 전송, 즉시 반환.
- `walWorker`: 채널 처리 및 버퍼 관리.
- `Close`: 워커 종료 후 버퍼 플러시.

### 벤치마크 결과 (평균)
- **InsertSequential/file**: 1,000,000번, 1,104 ns/op (~905,797 ops/s), 566 B/op, 18 allocs/op.
- **InsertConcurrent/file**: 839,052번, 2,542.5 ns/op (~393,387 ops/s), 775.5 B/op, 20 allocs/op.
- **GetSequential/file**: 3,677,744번, 324.5 ns/op (~3,082,819 ops/s), 189 B/op, 9 allocs/op.
- **GetConcurrent/file**: 2,393,516번, 508.5 ns/op (~1,966,568 ops/s), 245 B/op, 11 allocs/op.

### 변화
- **InsertSequential**: 1,128,847 → 905,797 ops/s (~20% 감소), 채널 오버헤드 영향.
- **InsertConcurrent**: 179,047 → 393,387 ops/s (~120% 증가), 동기 부하 제거로 폭발적 향상.
- **메모리**: 안정적 유지, 약간의 오버헤드 추가.

### 분석
- **성공**: 동시성에서 목표(300K-500K ops/s) 달성, PostgreSQL 수준에 근접.
- **한계**: 순차 작업은 이미 최적화돼 비동기 효과 제한적.

## 요약 (업데이트)
| 단계             | InsertSequential (ops/s) | InsertConcurrent (ops/s) | 주요 개선 요소            |
|------------------|--------------------------|--------------------------|---------------------------|
| 초기 상태        | 280                     | 310                     | -                         |
| 1단계 (WAL)      | 417 (~1.5x)             | 404 (~1.3x)             | 로그 추가, 동기 I/O       |
| 2단계 (Buffering)| 1,128,847 (~2,700x)     | 179,047 (~443x)         | 버퍼링, I/O 감소          |
| 3단계 (Async)    | 905,797 (~2,165x)       | 393,387 (~974x)         | 비동기 쓰기, 동기 부하 제거 |

- **2단계 → 3단계**: 동시성에서 2배 이상 증가, 순차는 약간 하락.

---