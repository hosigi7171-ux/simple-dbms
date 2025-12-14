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
 * add lock obj to sentinel's wait queue
 */
void add_to_wait_queue(lock_t* lock_obj, sentinel_t* sentinel) {
  lock_obj->granted = false;
  lock_obj->sentinel = sentinel;
  lock_obj->prev = sentinel->tail;
  lock_obj->next = nullptr;

  sentinel->tail->next = lock_obj;
  sentinel->tail = lock_obj;

  while (!lock_obj->granted) {
    pthread_cond_wait(&lock_obj->cond, &lock_table_latch);
  }
}

/**
 * Allocate and append a new lock object to the lock list of the record having
 * the key. If there is a predecessor’s lock object in the lock list, sleep
 * until the predecessor to release its lock. If there is no predecessor’s lock
 * object, return the address of the new lock object.
 * @return if success lock object else NULL
 */
lock_t* lock_acquire(tableid_t table_id, recordid_t key, int txn_id,
                     LockMode lock_mode) {
  pthread_mutex_lock(&lock_table_latch);
  hashkey_t hashkey = {table_id, key};

  lock_t* lock_obj = (lock_t*)calloc(1, sizeof(lock_t));
  pthread_cond_init(&lock_obj->cond, nullptr);
  lock_obj->mode = lock_mode;
  lock_obj->owner_txn_id = txn_id;

  // Case: no sentinel
  if (lock_table.count(hashkey) == 0) {
    if (create_new_sentinel(lock_obj, hashkey) == FAILURE) {
      pthread_cond_destroy(&lock_obj->cond);
      free(lock_obj);
      pthread_mutex_unlock(&lock_table_latch);
      return nullptr;
    }
    pthread_mutex_unlock(&lock_table_latch);
    return lock_obj;
  }

  // Case: sentinel exists
  sentinel_t* sentinel = lock_table[hashkey];

  // Case: sentinel has no lock object
  if (sentinel->head == NULL) {
    add_to_empty_sentinel(lock_obj, sentinel);
    pthread_mutex_unlock(&lock_table_latch);
    return lock_obj;
  }

  // Case: check compatibility: if okay, grant
  if (can_grant(sentinel->head, lock_mode)) {
    lock_obj->granted = true;
    lock_obj->sentinel = sentinel;
    lock_obj->prev = sentinel->tail;
    lock_obj->next = nullptr;

    sentinel->tail->next = lock_obj;
    sentinel->tail = lock_obj;

    pthread_mutex_unlock(&lock_table_latch);
    return lock_obj;
  }

  // Case: cannot grant, wait

  add_wait_for_edges(lock_obj, sentinel);

  pthread_mutex_unlock(&lock_table_latch);

  // deadlock detection
  std::vector<txnid_t> cycle = find_cycle_from(txn_id);
  if (!cycle.empty()) {
    txnid_t victim = select_victim_from_cycle(cycle);
    txn_abort(victim);
  }

  // sleep again
  pthread_mutex_lock(&lock_table_latch);
  add_to_wait_queue(lock_obj, sentinel);
  pthread_mutex_unlock(&lock_table_latch);
  return lock_obj;
}

/**
 * HELPER FUNCTION for lock release
 * remove target lock object from queue
 */
lock_t* remove_lock_from_queue(lock_t* lock_obj, sentinel_t* sentinel) {
  if (lock_obj == sentinel->head) {
    sentinel->head = lock_obj->next;
    if (sentinel->head) {
      sentinel->head->prev = nullptr;
    }
  } else {
    lock_obj->prev->next = lock_obj->next;
  }

  if (lock_obj == sentinel->tail) {
    sentinel->tail = lock_obj->prev;
  } else if (lock_obj->next) {
    lock_obj->next->prev = lock_obj->prev;
  }
}

/**
 * wake up waiters according to S/X rule
 */
void wakeup_waiters(sentinel_t* sentinel) {
  lock_t* p = sentinel->head;

  // skip granted
  while (p && p->granted) {
    p = p->next;
  }
  if (!p) {
    return;
  }

  if (p->mode == S_LOCK) {
    // grant all successive S locks
    while (p && p->mode == S_LOCK) {
      p->granted = true;
      pthread_cond_signal(&p->cond);
      p = p->next;
    }
  } else {  // X lock
    p->granted = true;
    pthread_cond_signal(&p->cond);
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

  // wake up compatible waiters
  wakeup_waiters(sentinel);

  // if there is no lock obj, remove sentinel
  if (sentinel->head == nullptr && sentinel->tail == nullptr) {
    lock_table.erase(sentinel->hashkey);
    free(sentinel);
  }

  // free lock obj
  pthread_cond_destroy(&lock_obj->cond);
  free(lock_obj);

  pthread_mutex_unlock(&lock_table_latch);
  return 0;
}

/**
 * try to grant waiters in lock queue
 */
void try_grant_waiters_on_record(hashkey_t hashkey) {
  lock_t* head = lock_table[hashkey]->head;
  if (!head) {
    return;
  }

  lock_t* waiter = head;
  while (waiter && waiter->granted) {
    waiter = waiter->next;
  }
  if (!waiter) {
    return;
  }

  if (has_granted_x(head)) {
    return;
  }

  for (lock_t* p = waiter; p && !p->granted; p = p->next) {
    if (p->mode == S_LOCK) {
      p->granted = true;
      clear_wait_for_edges(p->owner_txn_id);

      if (!p->next || p->next->mode != S_LOCK) {
        break;
      }
    } else {  // X_LOCK
      // ensure no other granted locks
      bool any_granted = false;
      for (lock_t* q = head; q; q = q->next) {
        if (q->granted) {
          any_granted = true;
          break;
        }
      }
      if (!any_granted) {
        p->granted = true;
        clear_wait_for_edges(p->owner_txn_id);
      }
      break;
    }
  }
}
