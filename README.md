# simple-dbms

A minimal educational Database Management System implementing core DBMS components from scratch in C/C++.

---

## 프로젝트 개요

이 프로젝트는 DBMS의 핵심 계층들을 단계적으로 구현한 교육용 데이터베이스 시스템입니다.  
각 단계별로 독립적인 README를 통해 상세한 설계 과정과 기술적 의사결정을 확인할 수 있습니다.

**개발 환경**: Ubuntu 24.04.3 LTS (WSL2)  
**언어**: C/C++ (초기 C → STL 활용을 위해 C++ 전환)

---

## 아키텍처

```
[Application Layer]
        ↓
[Storage Engine API]
        ↓
[Index Layer (B+Tree)]
        ↓
[Buffer Manager]
        ↓
[File & Disk Space Manager]
        ↓
[Disk (Data Files)]
```

각 계층은 **레이어드 아키텍처** 원칙에 따라 독립적으로 작동하며,  
하위 계층으로의 직접 접근을 차단하여 계층 간 책임을 명확히 분리했습니다.

---

## 구현 단계별 README

각 단계의 상세한 설계, 의사결정, 트러블슈팅은 아래 문서에서 확인하세요:

| 단계 | 주요 구현 | 문서 링크 | 소스 코드 |
|------|----------|----------|-----------|
| **1단계** | Disk-based B+Tree | [README_bptree.md](README_bptree.md) | [bptree 브랜치](https://github.com/hosigi7171-ux/DiskBasedBPlusTree) |
| **2단계** | Buffer Manager (Multi-table) | [README_buffer.md](README_buffer.md) | [buffer 브랜치](https://github.com/hosigi7171-ux/simple-dbms/tree/feature/buffer-manager) |
| **3단계** | Lock Table (Concurrency) | [README_lock.md](README_lock.md) | [lock-table 브랜치](https://github.com/hosigi7171-ux/simple-dbms/tree/feature/lock-table) |
| **4단계** | Lock Manager (Transaction) | [README_cc.md](README_cc.md) | [concurrency-control 브랜치](https://github.com/hosigi7171-ux/simple-dbms/tree/feature/concurrency-control) |
| **5단계** | Log Manager (Recovery) | [README_recovery.md](https://github.com/hosigi7171-ux/simple-dbms/blob/feature/crash-recovery/README_recovery.md) | [crash-recovery 브랜치](https://github.com/hosigi7171-ux/simple-dbms/tree/feature/crash-recovery) |


---

## 주요 기능

### 1단계: Disk-based B+Tree
- 페이지 기반 디스크 I/O
- Delayed Merge 적용
- File & Disk Space Management Layer 구현

### 2단계: Buffer Manager
- LRU-Clock 기반 Eviction Policy
- Prefetch를 통한 Spatial Locality 향상
- Multi-table 지원

### 3단계: Lock Table
- Record-level Locking
- Mutex 기반 동시성 제어
- Deadlock-free Lock Queue 구조

### 4단계: Lock Manager
- Strict 2PL (Two-Phase Locking)
- S/X Lock 모드 지원
- Deadlock Detection (Wait-for Graph)
- Transaction Abort & Rollback

### 5단계: Log Manager
- ARIES 기반 Recovery (Analysis-Redo-Undo)
- WAL (Write-Ahead Logging)
- CLR (Compensate Log Record)
- No-force / Steal Policy

---

## 빌드 및 실행

### 라이브러리 생성
```bash
make              # libbpt.a 생성
```

### 테스트 실행 예시
```bash
# Buffer Manager 테스트
cd build && cmake .. && make && ctest

# Lock Manager 테스트
cd test_cc && make && ./test slock_test

# Recovery 테스트
cd test_recovery && make && ./basic_test
```

---

## 기술 스택 & 도구

- **언어**: C/C++ (C11, C++17)
- **빌드**: GNU Make, CMake
- **테스트**: Ceedling (C), Google Test (C++)
- **동시성**: POSIX Threads (pthread)

---

## 핵심 설계 원칙

1. **계층 분리**: 각 계층은 독립적으로 테스트 가능하며 하위 계층만 호출
2. **Latch Hierarchy**: 데드락 방지를 위한 엄격한 락 순서 정립
3. **점진적 구현**: 단계별로 기능을 추가하며 이전 계층의 안정성 보장
4. **실제 DBMS 구조 반영**: 학습용이지만 실무와 유사한 설계 적용

---

## 주요 트러블슈팅 사례

- **B+Tree**: 내부 페이지 구조 불일치로 인한 인덱스 경계값 문제
- **Buffer**: Latch Crabbing 과정에서의 AB-BA Deadlock
- **Lock Manager**: 다중 트랜잭션 간 Wait-for Graph 중복 간선 처리
- **Recovery**: 컴파일러 구조체 패딩으로 인한 로그 크기 불일치

→ 상세 내용은 각 단계별 README 참고

---

## 아쉬운 점 및 개선 여지

* 프로젝트는 **On-disk B+Tree를 C 언어로 구현하며 시작**했습니다. 디스크 페이지 구조와 메모리 배치를 직접 다루는 데에는 C가 적합하다고 판단했기 때문입니다.
  이후 STL이 필요한 시점에서 C++로 전환했지만, 초기 코드가 C 기반이었고 **C++에 충분히 익숙하지 않았던 점**도 있어 결과적으로 C 스타일 코드와 C++ STL이 혼재된 구조가 되었습니다.

* C++를 더 깊게 학습한 뒤 전체 구조를 C++ 스타일로 정리하는 방법도 고려했지만,
  이 프로젝트의 목적이 **언어 숙련도보다는 DBMS 내부 동작 이해**에 있었기 때문에
  언어 학습에 과도한 시간을 투자하지는 않았습니다.
  다만 C/C++ 시스템 프로그래밍 경험을 바탕으로, **C++ 학습이 요구된다면 비교적 빠르게 습득할 수 있다고 생각합니다.**

* 또한 프로젝트를 진행하며, 디스크·버퍼·락 등 **메모리와 밀접한 계층에서는 객체지향적 설계가 항상 이점을 주지는 않는다**는 인상을 받았습니다.
  실제로 일부 영역에서는 절차지향적 접근이 구현과 디버깅 측면에서 더 명확했으며, 프로젝트를 진행하면서 많은 상황에서 굳이 객체로 관리할 필요는 없다고 판단했습니다.
  다만 프로젝트 규모가 더 커지거나 확장성이 중요한 상황이라면, **C++ 기반 설계로 전환하는 것이 바람직할 것**이라고 판단합니다.

---

## 참고 자료

- Database System Concepts (Silberschatz et al.)
- Database Management System (Raghu Ramakrishnan & Johannes Gehrke)
- ARIES Recovery Algorithm (IBM Research)
- Amittai's B+Tree Implementation (http://www.amittai.com/)

---
