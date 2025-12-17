#include "lock_table.h"

#include <deadlock.h>
#include <stdlib.h>
#include <wait_for_graph.h>

#include <cstdio>

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
  for (lock_t* p = head; p; p = p->next) {
    if (!p->granted) continue;
    if (p->mode == X_LOCK) return false;
    if (mode == X_LOCK) return false;
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
lock_t* create_lock_object(txnid_t txn_id, LockMode lock_mode) {
  lock_t* lock_obj = (lock_t*)calloc(1, sizeof(lock_t));
  pthread_cond_init(&lock_obj->cond, nullptr);
  lock_obj->mode = lock_mode;
  lock_obj->owner_txn_id = txn_id;
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
 * helper function : handle deadlock
 * return ACQUIRED if victim aborted and lock granted
 * returns DEADLOCK if current transaction is victim
 * returns -1 if need to continue waiting
 */
LockState handle_deadlock(lock_t* lock_obj, sentinel_t* sentinel,
                          txnid_t txn_id, int lock_mode,
                          const std::vector<txnid_t>& cycle) {
  txnid_t victim = select_victim_from_cycle(cycle);

  if (victim == txn_id) {
    // current transaction is victim
    pthread_mutex_lock(&lock_table_latch);
    remove_lock_from_queue(lock_obj, sentinel);
    pthread_mutex_unlock(&lock_table_latch);

    remove_wait_for_edges_for_txn(txn_id);
    destroy_lock_object(lock_obj);

    return DEADLOCK;
  } else {
    // another transaction is victim
    txn_abort(victim);

    // check if can grant
    pthread_mutex_lock(&lock_table_latch);
    bool can_grant_now = can_grant(sentinel->head, lock_mode);
    if (can_grant_now) {
      lock_obj->granted = true;
    }
    pthread_mutex_unlock(&lock_table_latch);

    if (can_grant_now) {
      return ACQUIRED;
    }
    return (LockState)-1;  // continue to wait
  }
}

/**
 * helper function
 * prepare for waiting (acquire transaction latch)
 * returns NEED_TO_WAIT if ready to wait
 * returns DEADLOCK if transaction not exist
 */
LockState prepare_for_wait(lock_t* lock_obj, sentinel_t* sentinel,
                           txnid_t txn_id, lock_t** ret_lock) {
  pthread_mutex_lock(&txn_table.latch);

  if (txn_table.transactions.count(txn_id) == 0) {
    // transaction not exist -> clean up
    pthread_mutex_unlock(&txn_table.latch);

    pthread_mutex_lock(&lock_table_latch);
    remove_lock_from_queue(lock_obj, sentinel);
    pthread_mutex_unlock(&lock_table_latch);

    remove_wait_for_edges_for_txn(txn_id);
    destroy_lock_object(lock_obj);

    return DEADLOCK;
  }

  tcb_t* tcb = txn_table.transactions[txn_id];
  pthread_mutex_lock(&tcb->latch);
  pthread_mutex_unlock(&txn_table.latch);

  *ret_lock = lock_obj;
  return NEED_TO_WAIT;
}

/**
 * Allocate and append a new lock object to the lock list of the record having
 * the key  Even if there is a predecessor’s conflicting lock object in the lock
 * list, do not sleep in this function, instead, acquire the latch of the
 * transaction calling this function. Set ret_lock with the address of the new
 * lock object if no deadlock have occured
 * not release the transaction latch in this function
 * @return Return value: 0 (ACQUIRED), 1 (NEED_TO_WAIT), 2 (DEADLOCK)
 */
LockState lock_acquire(tableid_t table_id, recordid_t key, txnid_t txn_id,
                       LockMode lock_mode, lock_t** ret_lock) {
  pthread_mutex_lock(&lock_table_latch);
  hashkey_t hashkey = {table_id, key};

  // sentinel 없으면 생성 후 즉시 grant
  if (lock_table.count(hashkey) == 0) {
    lock_t* lock_obj = create_lock_object(txn_id, lock_mode);
    create_new_sentinel(lock_obj, hashkey);
    lock_obj->granted = true;
    *ret_lock = lock_obj;
    pthread_mutex_unlock(&lock_table_latch);
    return ACQUIRED;
  }

  sentinel_t* sentinel = lock_table[hashkey];

  // 새 lock 생성
  lock_t* lock_obj = create_lock_object(txn_id, lock_mode);
  lock_obj->sentinel = sentinel;

  // 즉시 grant 시도
  if (try_immediate_grant(lock_obj, sentinel, lock_mode, ret_lock) ==
      ACQUIRED) {
    pthread_mutex_unlock(&lock_table_latch);
    return ACQUIRED;
  }

  // wait queue 삽입
  add_to_wait_queue(lock_obj, sentinel);
  *ret_lock = lock_obj;

  pthread_mutex_unlock(&lock_table_latch);

  // deadlock detection
  add_wait_for_edges(lock_obj, sentinel);
  std::vector<txnid_t> cycle = find_cycle_from(txn_id);

  if (!cycle.empty()) {
    txnid_t victim = select_victim_from_cycle(cycle);
    if (victim == txn_id) {
      pthread_mutex_lock(&lock_table_latch);
      remove_lock_from_queue(lock_obj, sentinel);
      pthread_mutex_unlock(&lock_table_latch);

      remove_wait_for_edges_for_txn(txn_id);
      // destroy_lock_object(lock_obj);
      return DEADLOCK;
    } else {
      txn_abort(victim);
    }
  }

  return NEED_TO_WAIT;
}

/**
 * sleep on the condition variable of the lock_obj,
 * atomically releasing the transaction latch
 * caller must hold transaction latch
 * after lock_wait, caller must release transaction latch
 */
void lock_wait(lock_t* lock_obj) {
  pthread_mutex_lock(&lock_table_latch);
  while (!lock_obj->granted) {
    pthread_cond_wait(&lock_obj->cond, &lock_table_latch);
  }
  pthread_mutex_unlock(&lock_table_latch);
}

/**
 * HELPER FUNCTION
 * remove target lock object from queue
 * caller must hold lock_table latch
 */
void remove_lock_from_queue(lock_t* lock_obj, sentinel_t* sentinel) {
  if (lock_obj->prev) {
    lock_obj->prev->next = lock_obj->next;
  } else {
    sentinel->head = lock_obj->next;
  }

  if (lock_obj->next) {
    lock_obj->next->prev = lock_obj->prev;
  } else {
    sentinel->tail = lock_obj->prev;
  }
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

  remove_lock_from_queue(lock_obj, sentinel);

  // // if there is no lock obj, remove sentinel
  if (sentinel->head == nullptr && sentinel->tail == nullptr) {
    lock_table.erase(sentinel->hashkey);
    free(sentinel);
  }

  // free lock obj
  destroy_lock_object(lock_obj);

  // try to grant waiting locks
  hashkey_t hashkey = sentinel->hashkey;
  // printf("debug:  Lock released for key=%ld. Trying to grant waiters.\n",
  //        hashkey.recordid);
  try_grant_waiters_on_record(hashkey);

  pthread_mutex_unlock(&lock_table_latch);
  return 0;
}

/**
 * helper function for try_grant_waiters_on_record
 * Check if a specific lock can be granted
 */
bool can_grant_specific(lock_t* head, lock_t* target) {
  for (lock_t* p = head; p != target; p = p->next) {
    if (!p->granted) {
      continue;
    }
    if (p->owner_txn_id == target->owner_txn_id) {
      continue;
    }

    // check compatibility
    if (target->mode == X_LOCK || p->mode == X_LOCK) {
      return false;
    }
  }
  return true;
}

/**
 * try to grant waiters in lock queue
 * lock_table_latch must be held by caller
 */
void try_grant_waiters_on_record(hashkey_t hashkey) {
  if (lock_table.count(hashkey) == 0) {
    return;
  }

  sentinel_t* sentinel = lock_table[hashkey];
  if (!sentinel || !sentinel->head) {
    return;
  }

  // printf("debug:  Trying to grant locks for key=%ld\n", hashkey.recordid);

  // 이미 granted된 lock 출력
  int granted_count = 0;
  for (lock_t* p = sentinel->head; p != nullptr; p = p->next) {
    // printf("  Lock: txn=%d, mode=%d, granted=%d\n", p->owner_txn_id, p->mode,
    //  p->granted);
    if (p->granted) granted_count++;
  }

  if (granted_count > 0) {
    // printf(" warning: %d locks already granted but blocking others\n",
    //  granted_count);
  }

  // Print current lock queue
  for (lock_t* p = sentinel->head; p != nullptr; p = p->next) {
    // printf("  Lock: txn=%d, mode=%d, granted=%d\n", p->owner_txn_id,
    // p->mode,p->granted);
  }

  // try to grant each waiting lock
  for (lock_t* p = sentinel->head; p != nullptr; p = p->next) {
    if (p->granted) {
      continue;
    }

    if (can_grant_specific(sentinel->head, p)) {
      p->granted = true;
      remove_wait_for_edges_for_txn(p->owner_txn_id);

      // printf("debug:  Granting lock to txn=%d for key=%ld\n",
      // p->owner_txn_id,
      //  hashkey.recordid);

      txnid_t waiter_txn_id = p->owner_txn_id;

      pthread_mutex_lock(&txn_table.latch);
      if (txn_table.transactions.count(waiter_txn_id)) {
        tcb_t* waiter_tcb = txn_table.transactions[waiter_txn_id];
        pthread_cond_signal(&p->cond);
        // printf("debug:  Signaled txn=%d\n", waiter_txn_id);
      }
      pthread_mutex_unlock(&txn_table.latch);
    } else {
      if (p->mode == X_LOCK) {
        break;
      }
    }
  }
}
