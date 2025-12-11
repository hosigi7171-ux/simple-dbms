unittest lock table

해당 테스트 코드는 락 테이블의 기능을 송금이라는 시나리오로 점검함
송금을 담당하는 trasnfer thread와
검증을 담당하는 scan thread가 존재

- 계좌
```cpp
/* shared data pretected by lock table. */
int accounts[TABLE_NUMBER][RECORD_NUMBER];
```

- transfer thread가 수행할 함수
랜덤 숫자를 배정하여 수행
데드락을 방지하기 위해 source->destination의 일관된 방향성(좌->우)을 가지게 함
```cpp
/*
 * This thread repeatedly transfers some money between accounts randomly.
 */
void* transfer_thread_func(void* arg) {}
```

- scan thread가 수행할 함수
계좌 총합이 일치하는지 확인
```cpp
/*
 * This thread repeatedly check the summation of all accounts.
 * Because the locking strategy is 2PL (2 Phase Locking), the summation must
 * always be consistent.
 */
void* scan_thread_func(void* arg) {}
```
