#include "lock_table.h"

#include <stdlib.h>

#include <cstdio>

std::unordered_map<hashkey_t, sentinel_t*, Hash> lock_table;

pthread_mutex_t lock_table_latch;

/**
 * helper function
 */

inline void print_status(char* msg, hashkey_t& hashkey) {
  printf("func: %s, thread : %ld, tableid: %d, recordid: %ld \n", msg,
         pthread_self(), hashkey.tableid, hashkey.recordid);
}

inline int check_success(int result) {
  return result == SUCCESS ? SUCCESS : FAILURE;
}

/**
 * init lock table
 * init lock table latch
 */
int init_lock_table() {
  check_success(pthread_mutex_init(&lock_table_latch, 0));
  lock_table.clear();
  return SUCCESS;
}

/**
 * lock acquire helper function
 */
lock_t* handle_no_sentinel(lock_t* lock, hashkey_t& hashkey) {
  // Case: there is no sentinel
  if (lock_table.count(hashkey) == 0) {
    sentinel_t* sentinel = (sentinel_t*)malloc(sizeof(sentinel_t));
    lock->sentinel = sentinel;

    sentinel->hashkey = hashkey;
    sentinel->head = lock;
    sentinel->tail = lock;

    lock_table.insert(std::make_pair(hashkey, sentinel));

    return lock;
  }
  return NULL;
}

/**
 * lock acquire helper function
 */
lock_t* handle_pred_exists(lock_t* lock, sentinel_t* sentinel) {
  lock->is_sleep = true;
  lock->sentinel = sentinel;
  lock->prev = sentinel->tail;
  lock->next = NULL;

  sentinel->tail->next = lock;
  sentinel->tail = lock;

  // 각 락 객체의 고유한 조건 변수를 사용
  while (lock->is_sleep) {
    pthread_cond_wait(&lock->cond, &lock_table_latch);
  }

  return lock;
}

/**
 * lock acquire
 */
lock_t* lock_acquire(tableid_t table_id, recordid_t key) {
  pthread_mutex_lock(&lock_table_latch);
  hashkey_t hashkey = {table_id, key};

  lock_t* lock_obj = (lock_t*)malloc(sizeof(lock_t));

  pthread_cond_init(&lock_obj->cond, NULL);

  // Case: handle sentinel
  if (handle_no_sentinel(lock_obj, hashkey) != NULL) {
    // print_status("case1", hashkey);
    pthread_mutex_unlock(&lock_table_latch);
    return lock_obj;
  }

  sentinel_t* sentinel = lock_table.find(hashkey)->second;

  if (sentinel->head == NULL) {
    lock_obj->sentinel = sentinel;
    sentinel->head = lock_obj;
    sentinel->tail = lock_obj;

    pthread_mutex_unlock(&lock_table_latch);
    return lock_obj;
  }

  // Case: there is predecessor
  // print_status("case3", hashkey);
  handle_pred_exists(lock_obj, sentinel);
  pthread_mutex_unlock(&lock_table_latch);
  return lock_obj;
}

/**
 * lock release
 */
int lock_release(lock_t* lock_obj) {
  pthread_mutex_lock(&lock_table_latch);
  sentinel_t* sentinel = lock_obj->sentinel;
  lock_t* next_lock = NULL;

  // 1. 락 대기열에서 lock_obj 제거
  if (lock_obj == sentinel->head) {
    next_lock = lock_obj->next;
    if (lock_obj == sentinel->tail) {
      sentinel->head = NULL;
      sentinel->tail = NULL;
    } else {
      sentinel->head = next_lock;
      if (next_lock != NULL) {
        next_lock->prev = NULL;
      }
    }
  } else if (lock_obj == sentinel->tail) {
    sentinel->tail = lock_obj->prev;
    if (sentinel->tail != NULL) {
      sentinel->tail->next = NULL;
    }
  } else {
    lock_obj->prev->next = lock_obj->next;
    lock_obj->next->prev = lock_obj->prev;
  }

  // 2. 다음 스레드 깨우기
  if (next_lock != NULL) {
    next_lock->is_sleep = false;
    pthread_cond_signal(&next_lock->cond);
  }

  // 3. sentinel 제거
  if (sentinel->head == NULL && sentinel->tail == NULL) {
    lock_table.erase(sentinel->hashkey);
    free(sentinel);
  }

  // 4. lock_obj 메모리 해제
  pthread_cond_destroy(&lock_obj->cond);
  free(lock_obj);
  pthread_mutex_unlock(&lock_table_latch);
  return 0;
}
