#include "txn_mgr.h"

#include <bpt.h>

#include <functional>
#include <stack>
#include <unordered_set>

#include "lock_table.h"
#include "wait_for_graph.h"

txnid_t next_id = 1;
txn_table_t txn_table;

/**
 * init table
 * if success return 0
 */
int init_txn_table() {
  pthread_mutex_init(&txn_table.latch, nullptr);
  return 0;
}

/**
 * destroy transaction table
 * if success return 0
 */
int destroy_txn_table() {
  pthread_mutex_lock(&txn_table.latch);

  for (auto& pair : txn_table.transactions) {
    tcb_t* tcb = pair.second;
    pthread_mutex_destroy(&tcb->latch);
    pthread_cond_destroy(&tcb->cond);
    free(tcb);
  }

  txn_table.transactions.clear();

  pthread_mutex_unlock(&txn_table.latch);
  pthread_mutex_destroy(&txn_table.latch);

  return 0;
}

/**
 * transaction begin
 * if success return txn_id otherwise 0
 */
int txn_begin(void) {
  tcb_t* tcb = (tcb_t*)malloc(sizeof(tcb_t));
  if (tcb == nullptr) {
    return 0;
  }

  pthread_mutex_lock(&txn_table.latch);

  tcb->id = next_id++;
  pthread_mutex_init(&tcb->latch, nullptr);
  pthread_cond_init(&tcb->cond, nullptr);
  tcb->lock_head = nullptr;
  tcb->lock_tail = nullptr;

  txn_table.transactions[tcb->id] = tcb;

  pthread_mutex_unlock(&txn_table.latch);

  return tcb->id;
}

/**
 * 원하는 transaction latch를 얻음
 * if success return 0 otherwise -1
 */
int acquire_txn_latch(txnid_t txn_id, tcb_t** out_tcb) {
  pthread_mutex_lock(&txn_table.latch);

  if (txn_table.transactions.count(txn_id) == 0) {
    pthread_mutex_unlock(&txn_table.latch);
    return -1;
  }
  tcb_t* tcb = txn_table.transactions[txn_id];

  pthread_mutex_lock(&tcb->latch);
  pthread_mutex_unlock(&txn_table.latch);

  *out_tcb = tcb;
  return 0;
}

/**
 * 원하는 transaction latch를 얻고 table에서 transaction 매핑 삭제
 * transaction 자체를 없앤 것은 아님
 * if success return 0 otherwise -1
 */
int acquire_txn_latch_and_pop_txn(txnid_t txn_id, tcb_t** out_tcb) {
  pthread_mutex_lock(&txn_table.latch);

  if (txn_table.transactions.count(txn_id) == 0) {
    pthread_mutex_unlock(&txn_table.latch);
    return -1;
  }
  tcb_t* tcb = txn_table.transactions[txn_id];

  txn_table.transactions.erase(txn_id);

  pthread_mutex_lock(&tcb->latch);
  pthread_mutex_unlock(&txn_table.latch);

  *out_tcb = tcb;
  return 0;
}

void release_txn_latch(tcb_t* tcb) { pthread_mutex_unlock(&tcb->latch); }

/**
 * clean up transaction
 * if success return txn_id otherwise 0
 */
int txn_commit(txnid_t txn_id) {
  tcb_t* tcb = nullptr;

  // clean up transaction
  if (acquire_txn_latch_and_pop_txn(txn_id, &tcb) != 0) {
    return 0;
  }
  lock_t* cur_lock = tcb->lock_head;
  while (cur_lock != nullptr) {
    lock_t* next_lock = cur_lock->txn_next_lock;
    lock_release(cur_lock);
    cur_lock = next_lock;
  }
  release_txn_latch(tcb);

  // clean up TCB
  pthread_mutex_destroy(&tcb->latch);
  pthread_cond_destroy(&tcb->cond);
  free(tcb);

  return txn_id;
}

/**
 * helper function for txn abort
 * remove lock node in lock queue
 * caller must have lock_table_latch
 */
void unlink_lock_from_queue(lock_t* lock) {
  if (!lock) {
    return;
  }
  hashkey_t hashkey = lock->sentinel->hashkey;
  sentinel_t* sentinel = lock_table[hashkey];
  lock_t* head = sentinel->head;

  if (lock->prev) {
    lock->prev->next = lock->next;
  }
  if (lock->next) {
    lock->next->prev = lock->prev;
  }
  if (head == lock) {
    sentinel->head = lock->next;
  }
  lock->next = lock->prev = nullptr;
}

/**
 * helper function
 * check granted x in lock queue
 */
bool has_granted_x(lock_t* head) {
  for (lock_t* p = head; p; p = p->next) {
    if (p->granted && p->mode == X_LOCK) {
      return true;
    }
  }
  return false;
}

void undo_transaction(tcb_t* tcb) {
  undo_log_t* log = tcb->undo_head;

  while (log) {
    // 이전 값으로 복구
    bpt_update(log->fd, log->table_id, log->key, log->old_value);

    undo_log_t* next = log->prev;
    free(log);
    log = next;
  }

  tcb->undo_head = nullptr;
  release_txn_latch(tcb);
}

/**
 * helper function for txn_abort
 * release all locks
 * caller must have txn table latch and lock table latch
 */
void release_all_locks(tcb_t* txn_entry,
                       std::vector<hashkey_t>& touched_records) {
  lock_t* cur = txn_entry->lock_head;
  while (cur) {
    lock_t* next = cur->txn_next_lock;
    touched_records.push_back(cur->sentinel->hashkey);

    unlink_lock_from_queue(cur);
    free(cur);

    cur = next;
  }
  txn_entry->lock_head = nullptr;
}

/**
 * helper function for txn_abort
 * wake up waiters in lock queue
 *
 */
void wakeup_waiters_in_records(const std::vector<hashkey_t>& records) {
  for (hashkey_t hashkey : records) {
    pthread_mutex_lock(&lock_table_latch);
    try_grant_waiters_on_record(hashkey);
    pthread_mutex_unlock(&lock_table_latch);
  }
}

void txn_abort(txnid_t victim) {
  tcb_t* txn_entry = nullptr;

  // get tcb
  pthread_mutex_lock(&txn_table.latch);
  auto it = txn_table.transactions.find(victim);
  if (it == txn_table.transactions.end()) {
    pthread_mutex_unlock(&txn_table.latch);
    return;
  }

  txn_entry = it->second;
  pthread_mutex_lock(&txn_entry->latch);
  pthread_mutex_unlock(&txn_table.latch);

  // undo
  undo_transaction(txn_entry);

  // release locks
  std::vector<hashkey_t> touched_records;

  pthread_mutex_lock(&lock_table_latch);
  release_all_locks(txn_entry, touched_records);
  pthread_mutex_unlock(&lock_table_latch);

  // remove wait-for edges
  remove_wait_for_edges_for_txn(victim);

  pthread_mutex_unlock(&txn_entry->latch);

  // wake up waiters
  wakeup_waiters_in_records(touched_records);

  // remove txn entry
  pthread_mutex_lock(&txn_table.latch);
  txn_table.transactions.erase(victim);
  free(txn_entry);
  pthread_mutex_unlock(&txn_table.latch);

  printf("txn_abort: transaction %d aborted\n", victim);  // for test
}

/**
 * helper function for txn_lock_acquire
 * link lock and transaction
 */
void link_lock_to_txn(tcb_t* txn, lock_t* lock) {
  lock->txn_prev_lock = txn->lock_tail;
  lock->txn_next_lock = nullptr;

  if (txn->lock_tail) {
    txn->lock_tail->txn_next_lock = lock;
  } else {
    txn->lock_head = lock;
  }
  txn->lock_tail = lock;
}

/**
 * transaction acquire lock
 * used in other layers
 */
lock_t* txn_lock_acquire(tableid_t table_id, recordid_t rid, LockMode mode,
                         txnid_t tid) {
  tcb_t* tcb;
  if (acquire_txn_latch(tid, &tcb) != 0) {
    return nullptr;
  }

  lock_t* lock = lock_acquire(table_id, rid, mode, tid);
  if (lock && lock->granted) {
    link_lock_to_txn(tcb, lock);
  }

  release_txn_latch(tcb);
  return lock;
}
