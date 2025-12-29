#include "txn_mgr.h"

#include <bpt.h>

#include <functional>
#include <stack>
#include <unordered_set>

#include "buf_mgr.h"
#include "lock_table.h"
#include "log.h"
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
  tcb->state = TXN_ACTIVE;  // 초기 상태
  txn_table.transactions[tcb->id] = tcb;

  uint64_t begin_lsn = log_append_begin(tcb->id);
  tcb->last_lsn = begin_lsn;
  if (begin_lsn == 0) {
    txn_table.transactions.erase(tcb->id);
    pthread_mutex_unlock(&txn_table.latch);
    pthread_mutex_destroy(&tcb->latch);
    pthread_cond_destroy(&tcb->cond);
    free(tcb);
    return 0;
  }

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
 * 원하는 transaction table latch를 얻고 table에서 transaction 매핑 삭제
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

  // printf(" txn_commit: Txn %d committing\n", txn_id);

  // TCB 획득 및 상태 변경
  pthread_mutex_lock(&txn_table.latch);
  auto it = txn_table.transactions.find(txn_id);
  if (it == txn_table.transactions.end()) {
    pthread_mutex_unlock(&txn_table.latch);
    return 0;
  }
  tcb = it->second;

  pthread_mutex_lock(&tcb->latch);
  if (tcb->state != TXN_ACTIVE) {
    // 이미 abort된 트랜잭션
    pthread_mutex_unlock(&tcb->latch);
    pthread_mutex_unlock(&txn_table.latch);
    return 0;
  }
  tcb->state = TXN_COMMITTING;
  uint64_t commit_lsn = log_append_commit(txn_id, tcb->last_lsn);
  tcb->last_lsn = commit_lsn;
  pthread_mutex_unlock(&tcb->latch);

  // txn_table에서 제거
  txn_table.transactions.erase(txn_id);
  pthread_mutex_unlock(&txn_table.latch);

  // wait-for graph 정리
  pthread_mutex_lock(&wait_for_graph_latch);
  wait_for_graph.erase(txn_id);
  for (auto& entry : wait_for_graph) {
    entry.second.erase(txn_id);
  }
  pthread_mutex_unlock(&wait_for_graph_latch);

  // 락 해제
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

  // Undo log 해제
  undo_log_t* log = tcb->undo_head;
  tcb->undo_head = nullptr;
  while (log) {
    undo_log_t* next = log->prev;
    free(log);
    log = next;
  }

  pthread_mutex_destroy(&tcb->latch);
  pthread_cond_destroy(&tcb->cond);
  free(tcb);

  // printf(" txn_commit: Txn %d completed\n", txn_id);
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
  undo_log_t* undo = tcb->undo_head;

  while (undo != nullptr) {
    buf_ctl_block_t* bcb =
        read_buffer_with_txn(undo->fd, undo->table_id, undo->page_num);

    if (bcb != nullptr) {
      leaf_page_t* page = (leaf_page_t*)bcb->frame;

      // find record frame
      int frame_idx = -1;
      for (int i = 0; i < page->num_of_keys; i++) {
        if (page->records[i].key == undo->key) {
          frame_idx = i;
          break;
        }
      }

      if (frame_idx != -1) {
        // save current (new) value for CLR
        char new_value[VALUE_SIZE];
        memcpy(new_value, page->records[frame_idx].value, VALUE_SIZE);

        // write CLR log before applying undo
        uint64_t clr_lsn = log_append_compensate(
            tcb->id, undo->table_id, undo->page_num, undo->offset, VALUE_SIZE,
            new_value,        // old_image: current value
            undo->old_value,  // new_image: value will be restored
            undo->prev_update_lsn, tcb->last_lsn);

        if (clr_lsn > 0) {
          tcb->last_lsn = clr_lsn;

          // apply undo - restore old value
          memcpy(page->records[frame_idx].value, undo->old_value, VALUE_SIZE);

          page->page_lsn = clr_lsn;
          bcb->is_dirty = true;
        }
      }
      unlock_and_unpin_bcb(bcb);
    }
    undo = undo->prev;
  }
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
 * This function is called both by db_api
 * aborted transaction id if success, otherwise return 0
 */
int txn_abort(txnid_t victim) {
  pthread_mutex_lock(&lock_table_latch);
  pthread_mutex_lock(&txn_table.latch);

  auto it = txn_table.transactions.find(victim);
  if (it == txn_table.transactions.end()) {
    pthread_mutex_unlock(&txn_table.latch);
    pthread_mutex_unlock(&lock_table_latch);
    return 0;
  }

  tcb_t* tcb = it->second;

  pthread_mutex_lock(&tcb->latch);

  if (tcb->state != TXN_ACTIVE) {
    pthread_mutex_unlock(&tcb->latch);
    pthread_mutex_unlock(&txn_table.latch);
    pthread_mutex_unlock(&lock_table_latch);
    return 0;
  }

  // 상태를 ABORTING으로 변경
  tcb->state = TXN_ABORTING;

  // 이 트랜잭션의 모든 락에 대해 broadcast, 대기 중인 스레드 깨우기
  lock_t* cur_for_wakeup = tcb->lock_head;
  while (cur_for_wakeup) {
    pthread_cond_broadcast(&cur_for_wakeup->cond);
    cur_for_wakeup = cur_for_wakeup->txn_next_lock;
  }

  pthread_mutex_unlock(&tcb->latch);
  pthread_mutex_unlock(&txn_table.latch);

  pthread_mutex_unlock(&lock_table_latch);

  pthread_mutex_lock(&tcb->latch);
  undo_transaction(tcb);

  // Rollback 로그 남기기
  uint64_t rollback_lsn = log_append_rollback(victim, tcb->last_lsn);
  tcb->last_lsn = rollback_lsn;
  pthread_mutex_unlock(&tcb->latch);

  pthread_mutex_lock(&lock_table_latch);
  // 락 수집 및 제거
  std::vector<hashkey_t> touched_records;
  std::vector<lock_t*> locks_to_free;

  lock_t* cur = tcb->lock_head;
  while (cur) {
    lock_t* next = cur->txn_next_lock;

    sentinel_t* sentinel = cur->sentinel;
    if (sentinel) {
      hashkey_t hashkey = sentinel->hashkey;
      touched_records.push_back(hashkey);

      remove_lock_from_queue(cur, sentinel);

      if (sentinel->head == nullptr && sentinel->tail == nullptr) {
        lock_table.erase(hashkey);
        free(sentinel);
      }
    }

    locks_to_free.push_back(cur);
    cur = next;
  }

  tcb->lock_head = nullptr;
  tcb->lock_tail = nullptr;

  // 대기자 깨우기
  for (const hashkey_t& hashkey : touched_records) {
    if (lock_table.count(hashkey) > 0) {
      try_grant_waiters_on_record(hashkey);
    }
  }

  // 락 객체 해제
  for (lock_t* lock : locks_to_free) {
    pthread_cond_destroy(&lock->cond);
    free(lock);
  }

  pthread_mutex_unlock(&lock_table_latch);

  // Wait-for graph 정리
  pthread_mutex_lock(&wait_for_graph_latch);
  wait_for_graph.erase(victim);
  for (auto& entry : wait_for_graph) {
    entry.second.erase(victim);
  }
  pthread_mutex_unlock(&wait_for_graph_latch);

  // txn_table에서 완전히 제거
  pthread_mutex_lock(&txn_table.latch);
  txn_table.transactions.erase(victim);
  pthread_mutex_unlock(&txn_table.latch);

  pthread_mutex_lock(&tcb->latch);
  tcb->state = TXN_ABORTED;
  pthread_mutex_unlock(&tcb->latch);

  pthread_mutex_destroy(&tcb->latch);
  pthread_cond_destroy(&tcb->cond);
  free(tcb);

  // printf("txn_abort: transaction %d aborted\n", victim);
  return victim;
}

void unlink_lock_from_txn(tcb_t* txn, lock_t* lock) {
  if (lock->txn_prev_lock) {
    lock->txn_prev_lock->txn_next_lock = lock->txn_next_lock;
  } else {
    txn->lock_head = lock->txn_next_lock;
  }

  if (lock->txn_next_lock) {
    lock->txn_next_lock->txn_prev_lock = lock->txn_prev_lock;
  } else {
    txn->lock_tail = lock->txn_prev_lock;
  }

  lock->txn_prev_lock = nullptr;
  lock->txn_next_lock = nullptr;
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
