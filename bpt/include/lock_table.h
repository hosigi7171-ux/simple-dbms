#ifndef SIMPLE_DBMS_INCLUDE_LOCK_TABLE_H_
#define SIMPLE_DBMS_INCLUDE_LOCK_TABLE_H_

#include <pthread.h>

#include <cstdint>
#include <functional>
#include <unordered_map>

#include "common_config.h"

struct sentinel_t;
struct hashkey_t;
struct tcb_t;

typedef struct hashkey_t {
  tableid_t tableid;
  recordid_t recordid;

  bool operator==(const hashkey_t& other) const {
    return (tableid == other.tableid && recordid == other.recordid);
  }
} hashkey_t;

typedef struct Hash {
  size_t operator()(const hashkey_t& key) const {
    std::hash<size_t> hasher;
    size_t tableid = hasher((size_t)key.tableid);
    size_t recordid = hasher((size_t)key.recordid);
    return tableid ^ (recordid << 1);
  }
} Hash;

typedef struct lock_t {
  lock_t* prev;
  lock_t* next;
  pthread_cond_t cond;
  sentinel_t* sentinel;
  bool granted;
  LockMode mode;
  // txnid_t owner_txn_id;
  tcb_t* owner_tcb;
  lock_t* txn_next_lock;
  lock_t* txn_prev_lock;
} lock_t;

typedef struct sentinel_t {
  lock_t* head;
  lock_t* tail;
  hashkey_t hashkey;
} sentinel_t;

extern std::unordered_map<hashkey_t, sentinel_t*, Hash> lock_table;
extern pthread_mutex_t lock_table_latch;

/**
 * API for lock table
 */

int init_lock_table();
LockState lock_acquire(tableid_t table_id, recordid_t key, txnid_t txn_id,
                       tcb_t* owner_tcb, LockMode lock_mode, lock_t** ret_lock);
int lock_release(lock_t* lock_obj);
bool lock_wait(lock_t* lock_obj);
void try_grant_waiters_on_record(hashkey_t hashkey);
void remove_lock_from_queue(lock_t* lock_obj, sentinel_t* sentinel);
bool can_grant_specific(lock_t* head, lock_t* target);
#endif