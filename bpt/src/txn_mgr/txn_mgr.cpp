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
 * 트랜잭션 매니저 초기화
 */
int init_txn_table() {
  if (pthread_mutex_init(&txn_table.latch, NULL) != 0) {
    return FAILURE;
  }

  pthread_mutex_lock(&txn_table.latch);
  txn_table.transactions.clear();
  pthread_mutex_unlock(&txn_table.latch);

  return SUCCESS;
}

/**
 * destroy transaction table
 * if success return 0
 */
int destroy_txn_table() {
  pthread_mutex_lock(&txn_table.latch);

  for (auto it = txn_table.transactions.begin();
       it != txn_table.transactions.end(); ++it) {
    tcb_t* tcb = it->second;
    if (tcb != nullptr) {
      // TCB 내부의 뮤텍스와 조건 변수 파괴
      pthread_mutex_destroy(&tcb->latch);
      pthread_cond_destroy(&tcb->cond);

      // 언두 로그 메모리 해제
      undo_log_t* curr_log = tcb->undo_head;
      while (curr_log != nullptr) {
        undo_log_t* next_log = curr_log->prev;
        free(curr_log);
        curr_log = next_log;
      }
      free(tcb);
    }
  }
  txn_table.transactions.clear();
  pthread_mutex_unlock(&txn_table.latch);

  pthread_mutex_destroy(&txn_table.latch);

  return SUCCESS;
}

/**
 * transaction begin
 * if success return txn_id otherwise 0
 */
int txn_begin(void) {
  tcb_t* tcb = (tcb_t*)calloc(1, sizeof(tcb_t));
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

  if (acquire_txn_latch_and_pop_txn(txn_id, &tcb) != 0) {
    return 0;
  }

  if (tcb->lock_head != nullptr) {
    lock_t* cur_lock = tcb->lock_head;
    tcb->lock_head = nullptr;
    tcb->lock_tail = nullptr;
    while (cur_lock) {
      lock_t* next = cur_lock->txn_next_lock;
      cur_lock->txn_next_lock = nullptr;
      cur_lock->txn_prev_lock = nullptr;
      lock_release(cur_lock);
      cur_lock = next;
    }
  }

  undo_log_t* log = tcb->undo_head;
  tcb->undo_head = nullptr;  //
  while (log) {
    undo_log_t* next = log->prev;
    free(log);
    log = next;
  }
  release_txn_latch(tcb);
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
  sentinel_t* sentinel = lock->sentinel;

  if (lock->prev) {
    lock->prev->next = lock->next;
  } else {
    sentinel->head = lock->next;
  }
  if (lock->next) {
    lock->next->prev = lock->prev;
  } else {
    sentinel->tail = lock->prev;
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

/**
 * helper function for txn_abort
 * undo all modifications
 */
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
    pthread_cond_destroy(&cur->cond);
    free(cur);

    cur = next;
  }
  txn_entry->lock_head = nullptr;
  txn_entry->lock_tail = nullptr;
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

/**
 * abort transaction
 * This function is called both by db_api and deadlock
 */
void txn_abort(txnid_t victim) {
  tcb_t* txn_entry = nullptr;

  // get tcb and remove from table immediately
  pthread_mutex_lock(&txn_table.latch);
  auto it = txn_table.transactions.find(victim);
  if (it == txn_table.transactions.end()) {
    pthread_mutex_unlock(&txn_table.latch);
    return;
  }

  txn_entry = it->second;
  txn_table.transactions.erase(victim);
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

  // wake up waiters
  wakeup_waiters_in_records(touched_records);

  // clean up TCB
  release_txn_latch(txn_entry);
  pthread_mutex_destroy(&txn_entry->latch);
  pthread_cond_destroy(&txn_entry->cond);
  free(txn_entry);

  printf("txn_abort: transaction %d aborted\n", victim);
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
