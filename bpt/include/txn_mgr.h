#ifndef SIMPLE_DBMS_INCLUDE_TXN_MGR_H_
#define SIMPLE_DBMS_INCLUDE_TXN_MGR_H_

#include <pthread.h>

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common_config.h"

struct tcb_t;
struct txn_table_t;
struct sentinel_t;
struct lock_t;

typedef struct victim_cand_t {
  txnid_t tid;
  int s_count;
  bool s_only;
} victim_cand_t;  // to find deadlock victim

typedef struct undo_log_t {
  int fd;
  tableid_t table_id;
  recordid_t key;
  char old_value[VALUE_SIZE];
  struct undo_log_t* prev;
} undo_log_t;  // for only undo update

typedef struct tcb_t {
  txnid_t id;
  pthread_mutex_t latch;
  pthread_cond_t cond;
  lock_t* lock_head;
  lock_t* lock_tail;
  undo_log_t* undo_head;
} tcb_t;  // Transaction Control Block

typedef struct txn_table_t {
  std::unordered_map<txnid_t, tcb_t*> transactions;
  pthread_mutex_t latch;
} txn_table_t;

extern txn_table_t txn_table;
extern pthread_mutex_t wait_for_graph_latch;
extern std::unordered_map<txnid_t, std::unordered_set<txnid_t>> wait_for_graph;

int init_txn_table();
int destroy_txn_table();

int txn_begin();
int txn_commit(txnid_t tid);
void txn_abort(txnid_t tid);

int acquire_txn_latch(txnid_t tid, tcb_t** out);
int acquire_txn_latch_and_pop_txn(txnid_t tid, tcb_t** out);
void release_txn_latch(tcb_t* tcb);

lock_t* txn_lock_acquire(tableid_t table_id, recordid_t rid, LockMode mode,
                         txnid_t tid);

void link_lock_to_txn(tcb_t* txn, lock_t* lock);
bool has_granted_x(lock_t* head);

#endif