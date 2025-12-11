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
  lock_obj->is_sleep = true;
  lock_obj->sentinel = sentinel;
  lock_obj->prev = sentinel->tail;
  lock_obj->next = NULL;

  sentinel->tail->next = lock_obj;
  sentinel->tail = lock_obj;

  while (lock_obj->is_sleep) {
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
lock_t* lock_acquire(tableid_t table_id, recordid_t key) {
  pthread_mutex_lock(&lock_table_latch);
  hashkey_t hashkey = {table_id, key};

  lock_t* lock_obj = (lock_t*)calloc(1, sizeof(lock_t));
  pthread_cond_init(&lock_obj->cond, NULL);

  // Case: no sentinel
  if (lock_table.count(hashkey) == 0) {
    if (create_new_sentinel(lock_obj, hashkey) == FAILURE) {
      pthread_cond_destroy(&lock_obj->cond);
      free(lock_obj);
      pthread_mutex_unlock(&lock_table_latch);
      return NULL;
    }
    pthread_mutex_unlock(&lock_table_latch);
    return lock_obj;
  }

  // Case: sentinel exists
  sentinel_t* sentinel = lock_table.find(hashkey)->second;

  // Case: sentinel has no lock object
  if (sentinel->head == NULL) {
    add_to_empty_sentinel(lock_obj, sentinel);
    pthread_mutex_unlock(&lock_table_latch);
    return lock_obj;
  }

  // Case: there is predecessor
  add_to_wait_queue(lock_obj, sentinel);
  pthread_mutex_unlock(&lock_table_latch);
  return lock_obj;
}

/**
 * HELPER FUNCTION for lock release
 * remove target lock object from queue
 * @return next lock object
 */
lock_t* remove_lock_from_queue(lock_t* lock_obj, sentinel_t* sentinel) {
  lock_t* next_lock = NULL;

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

  return next_lock;
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
  if (sentinel == NULL) {
    pthread_mutex_unlock(&lock_table_latch);
    return FAILURE;
  }

  // 1. remove lock object from queue
  lock_t* next_lock = remove_lock_from_queue(lock_obj, sentinel);

  // 2. wake up next lock object
  if (next_lock != NULL) {
    next_lock->is_sleep = false;
    pthread_cond_signal(&next_lock->cond);
  }

  // 3. if there is no lock obj, remove sentinel
  if (sentinel->head == NULL && sentinel->tail == NULL) {
    lock_table.erase(sentinel->hashkey);
    free(sentinel);
  }

  // 4. free lock obj
  pthread_cond_destroy(&lock_obj->cond);
  free(lock_obj);
  pthread_mutex_unlock(&lock_table_latch);
  return 0;
}
