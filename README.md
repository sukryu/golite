# README.md

## 🌟 GoLite: 까리함의 끝판왕, SQLite 1.0을 현대 아키텍처로 재탄생! 🌟

안녕하세요! 👋 여긴 **GoLite**의 세계예요—저의 손으로 SQLite 1.0을 Go로 재창조하면서, DDD(Domain-Driven Design), 헥사고날(Hexagonal Architecture), 이벤트 기반(Event-Driven), CQRS(Command Query Responsibility Segregation)라는 현대적인 아키텍처로 완전히 탈바꿈시킨 프로젝트예요. 이건 단순한 데이터베이스 복제가 아니에요. **저만의 까리한 소프트웨어**를 만드는 여정이에요! 💪 SQLite 1.0의 뿌리를 살리며, Go의 힘으로 클라우드와 실시간성을 극대화한 세상에 하나뿐인 DB를 목표로 달려갑니다. 준비됐나요? Let’s GoLite! 🚀✨

---

## 🎯 목표: 5단계로 쌓아가는 까리함의 여정

GoLite는 SQLite 1.0을 기반으로, 현대적인 아키텍처를 적용해 점진적으로 진화해요. 저의 엔지니어 꿈(백엔드, 클라우드, 실시간 시스템)을 담아, 까리함과 도전을 더했어요! 🔥

### 1단계: SQLite 1.0을 DDD & 헥사고날로 재구성 🛠️
- **무엇?**: SQLite 1.0의 테이블, 인덱스, B-트리를 도메인 모델로 변환하고, 헥사고날 포트/어댑터로 분리!  
- **왜?**: 단순한 구조를 도메인 중심으로 재탄생시키며, 저장소를 추상화해요.  
- **까리함**: "25년 전 DB를 DDD로 리믹스!" 💾  

### 2단계: 이벤트 기반 CQRS로 쓰기/읽기 분리 📈
- **무엇?**: 삽입/삭제를 이벤트로 처리하고, 읽기/쓰기를 CQRS로 나눠요.  
- **왜?**: 비동기성과 성능 최적화를 위해 SQLite의 한계를 넘어섭니다.  
- **까리함**: "쿼리가 이벤트로 춤춰요!" 🌍  

### 3단계: LSM Tree로 디스크 저장 업글 🌳
- **무엇?**: B-트리를 LSM Tree로 교체해 쓰기 성능과 클라우드 적합성 강화!  
- **왜?**: 현대적인 저장소로 GoLite를 한 단계 끌어올려요.  
- **까리함**: "RocksDB 스타일의 GoLite 탄생!" 💃  

### 4단계: Goroutines로 병렬 처리 폭발 ⚡
- **무엇?**: Go의 goroutines를 활용해 이벤트와 쿼리를 멀티스레드로 처리!  
- **왜?**: 실시간 시스템에 걸맞는 속도를 뽑아냅니다.  
- **까리함**: "Go의 힘으로 멀티코어 점령!" 🧵  

### 5단계: gRPC/REST API로 클라우드 GoLite化 ☁️
- **무엇?**: gRPC와 REST API를 붙여 클라우드에서 날아다니는 경량 DB 완성!  
- **왜?**: 백엔드 엔지니어의 꿈을 현실로, 배포 가능한 소프트웨어로!  
- **까리함**: "클라우드 위의 힙한 GoLite!" ✈️  

---

## 💡 왜 GoLite가 까리하냐고요?
- **유니크함**: SQLite 1.0에서 시작했지만, DDD, 헥사고날, 이벤트, CQRS로 재창조한 독보적인 DB예요! 🌍  
- **도전의 상징**: 단순 복제가 아니라, 현대 아키텍처로 새 길을 열었어요. 💪  
- **재미와 열정**: "하고 싶어서" 만든 프로젝트, 이게 진짜 까리함이죠! 😎  
- **이름의 힙함**: "GoLite"—Go의 간결함과 경량 DB의 쿨함을 담았어요. ㅋㅋㅋ  

---

## 🏛️ 아키텍처: SQLite 1.0 meets 현대 디자인
GoLite는 SQLite 1.0의 뼈대를 가져오면서, 최신 아키텍처로 재구성했어요:
- **DDD**: `Table`, `Index`, `Database`를 도메인 엔티티로 재정의.
- **헥사고날**: B-트리와 파일 I/O를 어댑터로 분리, 핵심 로직은 포트로 추상화.
- **이벤트 기반**: 삽입/삭제를 이벤트로 처리, 비동기성 강화.
- **CQRS**: 쓰기(`CommandService`)와 읽기(`QueryService`)를 분리.

### 초기 디렉토리 구조
```
GoLite/
├── cmd/                  # 실행 가능한 애플리케이션 진입점
│   └── golite/           # GoLite 메인 실행 파일
│       └── main.go       # 프로그램 시작점 (DB 서버 실행)
├── pkg/                  # 재사용 가능한 패키지 모음
│   ├── domain/           # DDD 도메인 모델 (SQLite 1.0 기반)
│   │   ├── database.go   # Database 애그리게이트 루트
│   │   ├── table.go      # Table 엔티티
│   │   └── events.go     # 도메인 이벤트 정의
│   ├── ports/            # 헥사고날 포트 (인터페이스)
│   │   └── storage.go    # 저장소 인터페이스 (B-트리, LSM 등 추상화)
│   ├── adapters/         # 헥사고날 어댑터 (외부 시스템 연결)
│   │   ├── btree/        # B-트리 어댑터 (SQLite 1.0 기반)
│   │   │   └── btree.go  # B-트리 구현, 디스크 I/O 처리
│   │   └── file/         # 파일 시스템 어댑터
│   │       └── file.go   # 파일 읽기/쓰기 구현
│   ├── application/      # 애플리케이션 로직 (CQRS 및 이벤트 핸들링)
│   │   ├── command.go    # 쓰기 서비스 (CommandService)
│   │   ├── query.go      # 읽기 서비스 (QueryService)
│   │   └── handler.go    # 이벤트 핸들러
│   ├── utils/            # 공통 유틸리티 함수
│   │   ├── logger.go     # 로깅 유틸리티
│   │   └── serialize.go  # 직렬화/역직렬화 헬퍼
│   └── tests/            # 테스트 코드 모음
│       ├── unit/         # 단위 테스트
│       │   └── btree_test.go  # B-트리 단위 테스트
│       ├── integration/  # 통합 테스트
│       │   └── database_test.go  # Database 통합 테스트
│       └── e2e/          # 엔드투엔드 테스트
│           └── api_test.go   # API E2E 테스트
├── config/               # 환경 설정 파일 및 로직
│   ├── config.go         # 환경별 설정 로더
│   └── local.yaml        # 로컬 환경 설정 예시
├── docs/                 # 프로젝트 문서화
│   ├── api/              # API 문서 (예: OpenAPI/Swagger)
│   │   └── spec.yaml     # API 명세
│   └── architecture.md   # 아키텍처 문서
├── ci/                   # CI/CD 파이프라인 구성
│   ├── Dockerfile        # Docker 빌드 파일
│   └── .github/          # GitHub Actions 워크플로우
│       └── workflows/
│           └── ci.yml    # CI 파이프라인 정의
├── go.mod                # Go 모듈 정의
├── go.sum                # 의존성 체크섬
└── README.md             # 프로젝트 소개 (까리하게!)
```

---

## 🛠️ 진행 상황
- [x] SQLite 1.0 소스코드 분석 (Fossil로 확보)  
- [ ] 1단계: DDD & 헥사고날 기반 SQLite 1.0 재구성  
- [ ] 2단계: 이벤트 기반 CQRS 적용  
- [ ] 3단계: LSM Tree로 전환  
- [ ] 4단계: Goroutines 병렬 처리  
- [ ] 5단계: gRPC/REST 클라우드화  

---

## 🚀 GoLite 띄우기
1. **도메인 정의**: `pkg/domain/`에서 SQLite 1.0의 구조 가져오기.  
2. **어댑터 연결**: `pkg/adapters/btree.go`에서 B-트리 구현 시작.  
3. **실행**: `go run cmd/golite/main.go`로 까리함 확인!  

```bash
go run cmd/golite/main.go
```

---

## 🌈 GoLite의 미래
GoLite는 단순한 시작이 아니에요! 앞으로:  
- 더 쿨한 자료구조 도입 (T-Tree? ART?)  
- 클라우드 배포 챌린지 (AWS? Kubernetes?)  
- 실무에서 GoLite로 세상 바꾸기!  

이 까리한 여정에 동참하고 싶다면, 언제든 아이디어 던져주세요! 🙌  

---

## 🎉 만든 사람
- **Jinhyeok**: Go와 아키텍처를 사랑하는 까리한 엔지니어 🌟  
- 모토: "소프트웨어는 재미있어야 까리하다!" 😊  

---