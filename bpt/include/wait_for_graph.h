#ifndef SIMPLE_DBMS_INCLUDE_WAIT_FOR_GRAPH_H_
#define SIMPLE_DBMS_INCLUDE_WAIT_FOR_GRAPH_H_

#include <cstdio>

#include "lock_table.h"
#include "txn_mgr.h"

void recalculate_wait_for_edges(txnid_t txn_id, tcb_t* tcb);
void print_wait_for_graph_unlocked();

void update_wait_for_graph_after_release(sentinel_t* sentinel);
void add_wait_for_edges(lock_t* waiter, sentinel_t* sentinel);
void remove_wait_for_edges_for_txn(txnid_t tid);
void clear_wait_for_edges(txnid_t tid);
void remove_wait_for_edges_on_grant(txnid_t txn_id, sentinel_t* sentinel);
void rebuild_wait_for_graph_for_record(sentinel_t* sentinel);
void update_wait_for_graph_on_grant(lock_t* granted_lock, sentinel_t* sentinel);
void print_wait_for_graph();
void print_deadlock_info(txnid_t detector, const std::vector<txnid_t>& cycle,
                         txnid_t victim);
#endif