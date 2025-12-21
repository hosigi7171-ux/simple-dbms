#include "lock_table.h"

#include <deadlock.h>
#include <stdlib.h>
#include <wait_for_graph.h>

#include <cstdio>

#include "time.h"
#include "txn_mgr.h"

std::unordered_map<hashkey_t, sentinel_t*, Hash> lock_table;

pthread_mutex_t lock_table_latch;

/**
 * helper function
 */
inline void print_status(char* msg, hashkey_t& hashkey) {
  printf("func: %s, thread : %ld, tableid: %d, recordid: %ld \n", msg,
         pthread_self(), hashkey.tableid, hashkey.recordid);
}

/**
 * helper function
 * check can grant
 */
bool can_grant(lock_t* head, int mode) {
  for (lock_t* p = head; p != nullptr; p = p->next) {
    if (p->mode == X_LOCK) return false;
    if (p->mode == S_LOCK && p->granted && mode == S_LOCK) {
      continue;
    } else {
      return false;
    }
  }
  return true;
}

/**
 * init lock table and latch
 * @return if success 0 else -1(FAILURE)
 */
int init_lock_table() {
  if (pthread_mutex_init(&lock_table_latch, 0) != SUCCESS) {
    return FAILURE;
  }
  lock_table.clear();
  return SUCCESS;
}

/**
 * HELPER FUNCTION for lock acquire
 * create new sentinel and add lock obj as first entry
 * @return if success 0 else -1(FAILURE)
 */
int create_new_sentinel(lock_t* lock_obj, hashkey_t& hashkey) {
  sentinel_t* sentinel = (sentinel_t*)malloc(sizeof(sentinel_t));
  if (sentinel == NULL) {
    return FAILURE;
  }
  lock_obj->sentinel = sentinel;
  lock_obj->prev = nullptr;
  lock_obj->next = nullptr;
  lock_obj->granted = true;

  sentinel->hashkey = hashkey;
  sentinel->head = lock_obj;
  sentinel->tail = lock_obj;

  lock_table.insert(std::make_pair(hashkey, sentinel));
  return SUCCESS;
}

/**
 * HELPER FUNCTION for lock acquire
 * add lock obj as first entry to empty sentinel
 */
void add_to_empty_sentinel(lock_t* lock_obj, sentinel_t* sentinel) {
  lock_obj->sentinel = sentinel;
  sentinel->head = lock_obj;
  sentinel->tail = lock_obj;
}

/**
 * HELPER FUNCTION for lock acquire
 * add lock obj to sentinel's wait queue (WITHOUT sleeping)
 */
void add_to_wait_queue(lock_t* lock_obj, sentinel_t* sentinel) {
  lock_obj->granted = false;
  lock_obj->sentinel = sentinel;
  lock_obj->prev = sentinel->tail;
  lock_obj->next = nullptr;

  if (sentinel->tail) {
    sentinel->tail->next = lock_obj;
  } else {
    sentinel->head = lock_obj;
  }
  sentinel->tail = lock_obj;

  // pthread_cond_wait 제거! 단순히 큐에만 추가
  // while (!lock_obj->granted) {
  //   pthread_cond_wait(&lock_obj->cond, &lock_table_latch);
  // }
}

/**
 * helper function
 * create lock object
 */
lock_t* create_lock_object(txnid_t txn_id, tcb_t* owner_tcb,
                           LockMode lock_mode) {
  lock_t* lock_obj = (lock_t*)calloc(1, sizeof(lock_t));
  pthread_cond_init(&lock_obj->cond, nullptr);
  lock_obj->mode = lock_mode;
  // lock_obj->owner_txn_id = txn_id;
  lock_obj->owner_tcb = owner_tcb;
  lock_obj->granted = false;
  return lock_obj;
}

/**
 * helper function
 * destroy lock object
 */
void destroy_lock_object(lock_t* lock_obj) {
  pthread_cond_destroy(&lock_obj->cond);
  free(lock_obj);
}

/**
 * helper function
 * grant lock immediately
 * returns ACQUIRED if grant, -1 if not grant
 * caller must hold lock_table_latch
 */
LockState try_immediate_grant(lock_t* lock_obj, sentinel_t* sentinel,
                              int lock_mode, lock_t** ret_lock) {
  // Case: sentinel is empty
  if (sentinel->head == NULL) {
    add_to_empty_sentinel(lock_obj, sentinel);
    lock_obj->granted = true;
    *ret_lock = lock_obj;
    return ACQUIRED;
  }

  // Case: check compatibility
  if (can_grant(sentinel->head, lock_mode)) {
    lock_obj->granted = true;
    lock_obj->sentinel = sentinel;
    lock_obj->prev = sentinel->tail;
    lock_obj->next = nullptr;
    sentinel->tail->next = lock_obj;
    sentinel->tail = lock_obj;

    *ret_lock = lock_obj;
    return ACQUIRED;
  }

  return (LockState)-1;  // cannot grant immediately
}

/**
 * Lock acquire
 */
LockState lock_acquire(tableid_t table_id, recordid_t key, txnid_t txn_id,
                       tcb_t* owner_tcb, LockMode lock_mode,
                       lock_t** ret_lock) {
  hashkey_t hashkey = {table_id, key};

  pthread_mutex_lock(&lock_table_latch);
  pthread_mutex_lock(&owner_tcb->latch);

  if (owner_tcb->state != TXN_ACTIVE) {
    pthread_mutex_unlock(&owner_tcb->latch);
    pthread_mutex_unlock(&lock_table_latch);
    return DEADLOCK;
  }

  // 중복 락 확인 check if duplicate lock
  for (lock_t* curr = owner_tcb->lock_head; curr != nullptr;
       curr = curr->txn_next_lock) {
    if (curr->sentinel && curr->sentinel->hashkey.tableid == table_id &&
        curr->sentinel->hashkey.recordid == key) {
      if (curr->granted && curr->mode == lock_mode) {
        *ret_lock = curr;
        pthread_mutex_unlock(&owner_tcb->latch);
        pthread_mutex_unlock(&lock_table_latch);
        return ACQUIRED;
      }
      if (!curr->granted && curr->mode == lock_mode) {
        *ret_lock = curr;
        pthread_mutex_unlock(&owner_tcb->latch);
        pthread_mutex_unlock(&lock_table_latch);
        return NEED_TO_WAIT;
      }
      if (curr->mode == S_LOCK && lock_mode == X_LOCK) {
        pthread_mutex_unlock(&owner_tcb->latch);
        pthread_mutex_unlock(&lock_table_latch);
        return DEADLOCK;
      }
    }
  }

  lock_t* lock_obj = create_lock_object(txn_id, owner_tcb, lock_mode);
  link_lock_to_txn(owner_tcb, lock_obj);

  sentinel_t* sentinel = nullptr;
  if (lock_table.count(hashkey) == 0) {
    sentinel = (sentinel_t*)malloc(sizeof(sentinel_t));
    sentinel->hashkey = hashkey;
    sentinel->head = lock_obj;
    sentinel->tail = lock_obj;
    lock_obj->sentinel = sentinel;
    lock_obj->prev = nullptr;
    lock_obj->next = nullptr;
    lock_obj->granted = true;
    lock_table.insert(std::make_pair(hashkey, sentinel));
    *ret_lock = lock_obj;
    pthread_mutex_unlock(&owner_tcb->latch);
    pthread_mutex_unlock(&lock_table_latch);
    return ACQUIRED;
  }

  sentinel = lock_table[hashkey];
  lock_obj->sentinel = sentinel;

  // 큐에 추가 add to lock obj queue
  lock_obj->prev = sentinel->tail;
  lock_obj->next = nullptr;
  if (sentinel->tail) {
    sentinel->tail->next = lock_obj;
  } else {
    sentinel->head = lock_obj;
  }
  sentinel->tail = lock_obj;

  // 즉시 획득 가능한지 확인 check if available immediately
  if (can_grant_specific(sentinel->head, lock_obj)) {
    lock_obj->granted = true;
    *ret_lock = lock_obj;
    pthread_mutex_unlock(&owner_tcb->latch);
    pthread_mutex_unlock(&lock_table_latch);
    // printf(" Txn %d: ACQUIRED %s-lock on key=%ld immediately\n", txn_id,
    //        lock_mode == S_LOCK ? "S" : "X", key);
    return ACQUIRED;
  }

  // need to wait
  lock_obj->granted = false;
  *ret_lock = lock_obj;

  // find blocking transaction
  std::unordered_set<txnid_t> blocking_txns;
  // printf(" Txn %d: WAITING for %s-lock on key=%ld\n", txn_id,
  //        lock_mode == S_LOCK ? "S" : "X", key);
  // printf("   Lock queue for key=%ld: ", key);

  for (lock_t* p = sentinel->head; p != nullptr; p = p->next) {
    // printf("[Txn%d:%s:%s] ", p->owner_tcb->id, p->mode == S_LOCK ? "S" : "X",
    //        p->granted ? "G" : "W");

    if (p == lock_obj) break;

    txnid_t blocker = p->owner_tcb->id;
    if (blocker == txn_id) continue;

    // 내 앞에 있는 락을 확인
    if (p->granted) {
      // Granted된 락과의 충돌 검사
      bool conflicts = (p->mode == X_LOCK || lock_mode == X_LOCK);
      if (conflicts) {
        blocking_txns.insert(blocker);
        // printf("\n   -> Blocked by Txn %d (granted)\n", blocker);
      }
    } else {
      // 대기 중인 락도 blocking 가능
      // 내가 S-lock이고 앞의 대기 락도 S-lock이면 함께 진행 가능
      if (!(lock_mode == S_LOCK && p->mode == S_LOCK)) {
        blocking_txns.insert(blocker);
        // printf("\n   -> Blocked by Txn %d (waiting, FIFO)\n", blocker);
      }
    }
  }
  // printf("\n");

  pthread_mutex_unlock(&owner_tcb->latch);

  pthread_mutex_lock(&wait_for_graph_latch);

  // 이 레코드에서의 edge만 추가
  for (txnid_t blocker : blocking_txns) {
    wait_for_graph[txn_id].insert(blocker);
  }

  // Wait-for graph 출력
  // printf("   Wait-for graph:\n");
  // for (auto& entry : wait_for_graph) {
  //   printf("     Txn %d waits for: ", entry.first);
  //   std::unordered_set<txnid_t> unique_blockers;
  //   for (txnid_t b : entry.second) {
  //     unique_blockers.insert(b);
  //   }
  //   for (txnid_t b : unique_blockers) {
  //     printf("%d ", b);
  //   }
  //   printf("\n");
  // }

  // Deadlock 검사
  std::vector<txnid_t> cycle = find_cycle_from_unlocked(txn_id);
  bool has_deadlock = !cycle.empty();
  std::vector<txnid_t> saved_cycle = cycle;

  if (has_deadlock) {
    // printf(" DEADLOCK DETECTED by Txn %d!\n", txn_id);

    // 이 레코드에서 추가한 edge 제거
    for (txnid_t blocker : blocking_txns) {
      auto it = wait_for_graph[txn_id].find(blocker);
      if (it != wait_for_graph[txn_id].end()) {
        wait_for_graph[txn_id].erase(it);
      }
    }
    if (wait_for_graph[txn_id].empty()) {
      wait_for_graph.erase(txn_id);
    }

    pthread_mutex_unlock(&wait_for_graph_latch);
    // print_deadlock_info(txn_id, saved_cycle, txn_id);

    pthread_mutex_lock(&owner_tcb->latch);
    unlink_lock_from_txn(owner_tcb, lock_obj);
    pthread_mutex_unlock(&owner_tcb->latch);

    remove_lock_from_queue(lock_obj, sentinel);

    if (sentinel->head == nullptr && sentinel->tail == nullptr) {
      lock_table.erase(hashkey);
      free(sentinel);
    }

    destroy_lock_object(lock_obj);
    pthread_mutex_unlock(&lock_table_latch);

    return DEADLOCK;
  }

  pthread_mutex_unlock(&wait_for_graph_latch);

  pthread_mutex_lock(&owner_tcb->latch);
  if (owner_tcb->state != TXN_ACTIVE) {
    pthread_mutex_unlock(&owner_tcb->latch);
    pthread_mutex_unlock(&lock_table_latch);
    return DEADLOCK;
  }

  pthread_mutex_unlock(&lock_table_latch);
  return NEED_TO_WAIT;
}

/**
 * lock wait
 * caller must hold transaction latch
 * 락 대기 중 주기적 deadlock 재검사 포함
 * 반환값: true = granted, false = deadlock or aborted
 */
bool lock_wait(lock_t* lock_obj) {
  tcb_t* owner_tcb = lock_obj->owner_tcb;
  txnid_t txn_id = owner_tcb->id;

  while (!lock_obj->granted) {
    // TCB의 state를 체크하여 abort 여부 확인
    if (owner_tcb->state != TXN_ACTIVE) {
      pthread_mutex_unlock(&owner_tcb->latch);
      return false;
    }

    // 대기 전 deadlock 재검사
    pthread_mutex_unlock(&owner_tcb->latch);

    pthread_mutex_lock(&wait_for_graph_latch);
    std::vector<txnid_t> cycle = find_cycle_from_unlocked(txn_id);

    bool has_deadlock = !cycle.empty();
    std::vector<txnid_t> saved_cycle = cycle;

    if (has_deadlock) {
      // Wait-for graph 정리
      wait_for_graph.erase(txn_id);
      pthread_mutex_unlock(&wait_for_graph_latch);

      // printf(" DEADLOCK DETECTED during wait by Txn %d!\n", txn_id);
      // print_deadlock_info(txn_id, saved_cycle, txn_id);

      return false;
    }
    pthread_mutex_unlock(&wait_for_graph_latch);

    // TCB latch 재획득 후 대기
    pthread_mutex_lock(&owner_tcb->latch);

    // Double-check: granted 되었거나 abort 되었으면 바로 리턴
    if (lock_obj->granted || owner_tcb->state != TXN_ACTIVE) {
      break;
    }

    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += 1;  // 1초 타임아웃

    int wait_result =
        pthread_cond_timedwait(&lock_obj->cond, &owner_tcb->latch, &ts);

    // 타임아웃이나 시그널로 깨어났을 때 상태 재확인
    if (owner_tcb->state != TXN_ACTIVE) {
      pthread_mutex_unlock(&owner_tcb->latch);
      return false;
    }
    if (lock_obj->granted) {
      break;
    }
    // 타임아웃으로 깨어났으면 루프로 deadlock 재검사
  }

  pthread_mutex_unlock(&owner_tcb->latch);
  return true;
}

/**
 * HELPER FUNCTION
 * remove target lock object from queue
 * caller must hold lock_table latch
 */
void remove_lock_from_queue(lock_t* lock_obj, sentinel_t* sentinel) {
  if (!lock_obj || !sentinel) return;

  // double remove 방지
  if (lock_obj->sentinel != sentinel) {
    // printf(" WARNING: lock_obj->sentinel mismatch!\n");
    return;
  }

  if (lock_obj->prev != nullptr) {
    lock_obj->prev->next = lock_obj->next;
  } else {
    sentinel->head = lock_obj->next;
  }

  if (lock_obj->next != nullptr) {
    lock_obj->next->prev = lock_obj->prev;
  } else {
    sentinel->tail = lock_obj->prev;
  }

  lock_obj->prev = nullptr;
  lock_obj->next = nullptr;
  lock_obj->sentinel = nullptr;
}

/**
 * Remove the lock_obj from the lock list.
 * If there is a successor’s lock waiting for the thread releasing the lock,
 * wake up the successor.
 * @return if success 0 else -1(FAILURE)
 */
int lock_release(lock_t* lock_obj) {
  pthread_mutex_lock(&lock_table_latch);

  sentinel_t* sentinel = lock_obj->sentinel;
  if (sentinel == nullptr) {
    pthread_mutex_unlock(&lock_table_latch);
    return FAILURE;
  }

  hashkey_t hashkey = sentinel->hashkey;
  txnid_t releasing_txn = lock_obj->owner_tcb->id;

  remove_lock_from_queue(lock_obj, sentinel);

  bool sentinel_empty =
      (sentinel->head == nullptr && sentinel->tail == nullptr);

  if (sentinel_empty) {
    lock_table.erase(sentinel->hashkey);
    free(sentinel);

    pthread_mutex_lock(&wait_for_graph_latch);
    for (auto it = wait_for_graph.begin(); it != wait_for_graph.end();) {
      it->second.erase(releasing_txn);
      if (it->second.empty()) {
        it = wait_for_graph.erase(it);
      } else {
        ++it;
      }
    }
    pthread_mutex_unlock(&wait_for_graph_latch);
  }

  destroy_lock_object(lock_obj);

  if (!sentinel_empty) {
    try_grant_waiters_on_record(hashkey);
  }

  pthread_mutex_unlock(&lock_table_latch);
  return 0;
}

/**
 * helper function
 * Check if a specific lock granted available now
 * 특정 락이 지금 grant될 수 있는지 확인
 */
bool can_grant_specific(lock_t* head, lock_t* target) {
  for (lock_t* p = head; p != target; p = p->next) {
    if (p->owner_tcb->id == target->owner_tcb->id) {
      continue;
    }

    // granted된 락만 검사
    if (!p->granted) {
      if (target->mode == S_LOCK && p->mode == S_LOCK) {
        continue;
      }
      return false;
    }

    // granted된 락과의 충돌 검사
    bool conflicts = false;

    if (p->mode == X_LOCK) {
      conflicts = true;
    } else if (p->mode == S_LOCK && target->mode == X_LOCK) {
      conflicts = true;
    }

    if (conflicts) {
      return false;
    }
  }
  return true;
}

/**
 * grant waiters lock authority in lock queue
 */
void try_grant_waiters_on_record(hashkey_t hashkey) {
  if (lock_table.count(hashkey) == 0) return;
  sentinel_t* sentinel = lock_table[hashkey];

  std::vector<std::pair<lock_t*, tcb_t*>> ready_locks;

  // grant 가능한 락 찾기
  lock_t* p = sentinel->head;
  while (p != nullptr) {
    lock_t* next = p->next;

    if (p->granted) {
      p = next;
      continue;
    }

    pthread_mutex_lock(&txn_table.latch);
    auto it = txn_table.transactions.find(p->owner_tcb->id);
    bool txn_exists = (it != txn_table.transactions.end());
    tcb_t* tcb = txn_exists ? it->second : nullptr;

    bool is_active = false;
    if (txn_exists && tcb) {
      pthread_mutex_lock(&tcb->latch);
      is_active = (tcb->state == TXN_ACTIVE);
      pthread_mutex_unlock(&tcb->latch);
    }
    pthread_mutex_unlock(&txn_table.latch);

    if (!txn_exists || !is_active) {
      remove_lock_from_queue(p, sentinel);
      pthread_cond_destroy(&p->cond);
      free(p);
      p = next;
      continue;
    }

    if (can_grant_specific(sentinel->head, p)) {
      ready_locks.push_back({p, tcb});
      if (p->mode == X_LOCK) {
        break;
      }
    } else {
      break;
    }

    p = next;
  }

  if (ready_locks.empty()) return;

  // granted 설정
  for (auto& pair : ready_locks) {
    pair.first->granted = true;
  }

  // wait-for graph 재구성
  rebuild_wait_for_graph_for_record(sentinel);

  // 스레드 깨우기
  for (auto& pair : ready_locks) {
    lock_t* lock_obj = pair.first;
    tcb_t* tcb = pair.second;

    pthread_mutex_lock(&tcb->latch);
    if (tcb->state == TXN_ACTIVE) {
      pthread_cond_signal(&lock_obj->cond);
    }
    pthread_mutex_unlock(&tcb->latch);
  }
}
