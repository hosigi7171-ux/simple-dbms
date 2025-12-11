/*
g++ -g -I../include -o stresstest_lock_table stresstest_lock_table.cpp
../src/lock_table.cpp -lpthread
*/

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include <atomic>

#include "lock_table.h"

// 훨씬 더 많은 스레드와 더 적은 리소스로 경합 증가
#define TRANSFER_THREAD_NUMBER (32)
#define SCAN_THREAD_NUMBER (8)
#define RANDOM_ACCESS_THREAD_NUMBER (16)

#define TRANSFER_COUNT (500000)
#define SCAN_COUNT (50000)
#define RANDOM_ACCESS_COUNT (500000)

// 더 적은 리소스로 경합 극대화
#define TABLE_NUMBER (2)
#define RECORD_NUMBER (3)
#define INITIAL_MONEY (100000)
#define MAX_MONEY_TRANSFERRED (100)
#define SUM_MONEY (TABLE_NUMBER * RECORD_NUMBER * INITIAL_MONEY)

/* shared data protected by lock table. */
int accounts[TABLE_NUMBER][RECORD_NUMBER];

/* 통계 정보 */
std::atomic<long long> total_lock_acquires(0);
std::atomic<long long> total_lock_releases(0);
std::atomic<long long> total_conflicts(0);
std::atomic<bool> test_running(true);

/*
 * 기본 transfer 스레드 - 오름차순 락킹
 */
void* transfer_thread_func(void* arg) {
  lock_t* source_lock;
  lock_t* destination_lock;
  int source_table_id;
  int source_record_id;
  int destination_table_id;
  int destination_record_id;
  int money_transferred;

  for (int i = 0; i < TRANSFER_COUNT; i++) {
    source_table_id = rand() % TABLE_NUMBER;
    source_record_id = rand() % RECORD_NUMBER;
    destination_table_id = rand() % TABLE_NUMBER;
    destination_record_id = rand() % RECORD_NUMBER;

    if ((source_table_id > destination_table_id) ||
        (source_table_id == destination_table_id &&
         source_record_id >= destination_record_id)) {
      continue;
    }

    money_transferred = rand() % MAX_MONEY_TRANSFERRED;
    money_transferred =
        rand() % 2 == 0 ? (-1) * money_transferred : money_transferred;

    source_lock = lock_acquire(source_table_id, source_record_id);
    total_lock_acquires++;

    accounts[source_table_id][source_record_id] -= money_transferred;

    destination_lock =
        lock_acquire(destination_table_id, destination_record_id);
    total_lock_acquires++;

    accounts[destination_table_id][destination_record_id] += money_transferred;

    lock_release(destination_lock);
    lock_release(source_lock);
    total_lock_releases += 2;
  }

  printf("Transfer thread %ld is done.\n", pthread_self());
  return NULL;
}

/*
 * 전체 스캔 스레드 - 모든 락을 한번에 획득
 */
void* scan_thread_func(void* arg) {
  int sum_money;
  lock_t* lock_array[TABLE_NUMBER][RECORD_NUMBER];

  for (int i = 0; i < SCAN_COUNT; i++) {
    sum_money = 0;

    // 모든 락 획득
    for (int table_id = 0; table_id < TABLE_NUMBER; table_id++) {
      for (int record_id = 0; record_id < RECORD_NUMBER; record_id++) {
        lock_array[table_id][record_id] = lock_acquire(table_id, record_id);
        total_lock_acquires++;
        sum_money += accounts[table_id][record_id];
      }
    }

    // 일관성 체크
    if (sum_money != SUM_MONEY) {
      printf("INCONSISTENT STATE DETECTED!!\n");
      printf("sum_money : %d\n", sum_money);
      printf("SUM_MONEY : %d\n", SUM_MONEY);

      // 디버그 정보 출력
      for (int t = 0; t < TABLE_NUMBER; t++) {
        for (int r = 0; r < RECORD_NUMBER; r++) {
          printf("accounts[%d][%d] = %d\n", t, r, accounts[t][r]);
        }
      }

      // 모든 락 해제 후 종료
      for (int table_id = 0; table_id < TABLE_NUMBER; table_id++) {
        for (int record_id = 0; record_id < RECORD_NUMBER; record_id++) {
          lock_release(lock_array[table_id][record_id]);
        }
      }

      exit(1);
    }

    // 모든 락 해제
    for (int table_id = 0; table_id < TABLE_NUMBER; table_id++) {
      for (int record_id = 0; record_id < RECORD_NUMBER; record_id++) {
        lock_release(lock_array[table_id][record_id]);
        total_lock_releases++;
      }
    }
  }

  printf("Scan thread %ld is done.\n", pthread_self());
  return NULL;
}

/*
 * 랜덤 액세스 스레드 - 같은 리소스에 대한 극심한 경합 유발
 * 모든 연산이 총합을 보존하도록 수정
 */
void* random_access_thread_func(void* arg) {
  lock_t* lock1;
  lock_t* lock2;
  lock_t* lock3;

  for (int i = 0; i < RANDOM_ACCESS_COUNT; i++) {
    int pattern = rand() % 5;

    switch (pattern) {
      case 0: {
        // 패턴 1: 단일 락
        int tid = rand() % TABLE_NUMBER;
        int rid = rand() % RECORD_NUMBER;
        lock1 = lock_acquire(tid, rid);
        total_lock_acquires++;

        accounts[tid][rid] += 1;
        accounts[tid][rid] -= 1;

        lock_release(lock1);
        total_lock_releases++;
        break;
      }

      case 1: {
        // 패턴 2: 두 계좌 간 이체
        int tid = rand() % TABLE_NUMBER;
        int rid1 = rand() % RECORD_NUMBER;
        int rid2 = (rid1 + 1) % RECORD_NUMBER;

        if (rid1 > rid2) {
          int tmp = rid1;
          rid1 = rid2;
          rid2 = tmp;
        }

        lock1 = lock_acquire(tid, rid1);
        lock2 = lock_acquire(tid, rid2);
        total_lock_acquires += 2;

        int temp = accounts[tid][rid1];
        accounts[tid][rid1] = accounts[tid][rid2];
        accounts[tid][rid2] = temp;

        lock_release(lock2);
        lock_release(lock1);
        total_lock_releases += 2;
        break;
      }

      case 2: {
        // 패턴 3: 세 계좌 간 순환 이동
        lock1 = lock_acquire(0, 0);
        lock2 = lock_acquire(0, 1);
        lock3 = lock_acquire(0, 2);
        total_lock_acquires += 3;

        int temp = accounts[0][0];
        accounts[0][0] = accounts[0][2];
        accounts[0][2] = accounts[0][1];
        accounts[0][1] = temp;

        lock_release(lock3);
        lock_release(lock2);
        lock_release(lock1);
        total_lock_releases += 3;
        break;
      }

      case 3: {
        // 패턴 4: 핫스팟
        lock1 = lock_acquire(0, 0);
        total_lock_acquires++;

        int amount = rand() % 10;
        accounts[0][0] += amount;
        accounts[0][0] -= amount;

        lock_release(lock1);
        total_lock_releases++;
        total_conflicts++;
        break;
      }

      case 4: {
        // 패턴 5: 두 테이블 간 이체
        int tid1 = 0, tid2 = 1;
        int rid1 = rand() % RECORD_NUMBER;
        int rid2 = rand() % RECORD_NUMBER;

        // 데드락 방지를 위한 정렬
        if (tid1 > tid2 || (tid1 == tid2 && rid1 > rid2)) {
          int t = tid1;
          tid1 = tid2;
          tid2 = t;
          int r = rid1;
          rid1 = rid2;
          rid2 = r;
        }

        lock1 = lock_acquire(tid1, rid1);
        lock2 = lock_acquire(tid2, rid2);
        total_lock_acquires += 2;

        int transfer = rand() % 50;
        accounts[tid1][rid1] -= transfer;
        accounts[tid2][rid2] += transfer;

        lock_release(lock2);
        lock_release(lock1);
        total_lock_releases += 2;
        break;
      }
    }

    // 가끔씩 sleep으로 타이밍 변화
    if (i % 10000 == 0) {
      usleep(1);
    }
  }

  printf("Random access thread %ld is done.\n", pthread_self());
  return NULL;
}

/*
 * 통계 출력 스레드
 */
void* stats_thread_func(void* arg) {
  int seconds = 0;
  while (test_running.load()) {
    sleep(1);
    seconds++;
    printf("\n=== STATS [%ds] ===\n", seconds);
    printf("Lock Acquires: %lld\n", total_lock_acquires.load());
    printf("Lock Releases: %lld\n", total_lock_releases.load());
    printf("Conflicts: %lld\n", total_conflicts.load());
    printf("==================\n\n");
  }
  return NULL;
}

int main() {
  pthread_t transfer_threads[TRANSFER_THREAD_NUMBER];
  pthread_t scan_threads[SCAN_THREAD_NUMBER];
  pthread_t random_threads[RANDOM_ACCESS_THREAD_NUMBER];
  pthread_t stats_thread;

  srand(time(NULL));

  printf("STRESS TEST CONFIGURATION\n");
  printf("Transfer Threads: %d\n", TRANSFER_THREAD_NUMBER);
  printf("Scan Threads: %d\n", SCAN_THREAD_NUMBER);
  printf("Random Access Threads: %d\n", RANDOM_ACCESS_THREAD_NUMBER);
  printf("Total Threads: %d\n", TRANSFER_THREAD_NUMBER + SCAN_THREAD_NUMBER +
                                    RANDOM_ACCESS_THREAD_NUMBER);
  printf("Tables: %d, Records per Table: %d\n", TABLE_NUMBER, RECORD_NUMBER);
  printf("Total Resources: %d (HIGH CONTENTION!)\n\n",
         TABLE_NUMBER * RECORD_NUMBER);

  // Initialize accounts
  for (int table_id = 0; table_id < TABLE_NUMBER; table_id++) {
    for (int record_id = 0; record_id < RECORD_NUMBER; record_id++) {
      accounts[table_id][record_id] = INITIAL_MONEY;
    }
  }

  // Initialize lock table
  init_lock_table();

  printf("Starting stress test...\n\n");

  // 통계 스레드 시작
  pthread_create(&stats_thread, 0, stats_thread_func, NULL);

  // 모든 스레드 생성
  for (int i = 0; i < TRANSFER_THREAD_NUMBER; i++) {
    pthread_create(&transfer_threads[i], 0, transfer_thread_func, NULL);
  }
  for (int i = 0; i < SCAN_THREAD_NUMBER; i++) {
    pthread_create(&scan_threads[i], 0, scan_thread_func, NULL);
  }
  for (int i = 0; i < RANDOM_ACCESS_THREAD_NUMBER; i++) {
    pthread_create(&random_threads[i], 0, random_access_thread_func, NULL);
  }

  // 모든 워커 스레드 대기
  for (int i = 0; i < TRANSFER_THREAD_NUMBER; i++) {
    pthread_join(transfer_threads[i], NULL);
  }
  for (int i = 0; i < SCAN_THREAD_NUMBER; i++) {
    pthread_join(scan_threads[i], NULL);
  }
  for (int i = 0; i < RANDOM_ACCESS_THREAD_NUMBER; i++) {
    pthread_join(random_threads[i], NULL);
  }

  // 통계 스레드 종료
  test_running.store(false);
  pthread_join(stats_thread, NULL);

  // 최종 일관성 체크
  int final_sum = 0;
  for (int table_id = 0; table_id < TABLE_NUMBER; table_id++) {
    for (int record_id = 0; record_id < RECORD_NUMBER; record_id++) {
      final_sum += accounts[table_id][record_id];
    }
  }

  printf("\n\n=== FINAL RESULT ===\n");
  printf("Expected Sum: %d\n", SUM_MONEY);
  printf("Actual Sum: %d\n", final_sum);
  printf("Total Lock Acquires: %lld\n", total_lock_acquires.load());
  printf("Total Lock Releases: %lld\n", total_lock_releases.load());

  if (final_sum == SUM_MONEY &&
      total_lock_acquires.load() == total_lock_releases.load()) {
    printf("\n ALL TESTS PASSED! Lock table is working correctly!\n");
    return 0;
  } else {
    printf("\n TEST FAILED!\n");
    if (final_sum != SUM_MONEY) {
      printf("Money inconsistency: expected %d, got %d, diff %d\n", SUM_MONEY,
             final_sum, final_sum - SUM_MONEY);
    }
    if (total_lock_acquires.load() != total_lock_releases.load()) {
      printf("Lock leak detected: acquires %lld, releases %lld\n",
             total_lock_acquires.load(), total_lock_releases.load());
    }
    return 1;
  }
}
