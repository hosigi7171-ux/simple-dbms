#ifndef SIMPLE_DBMS_INCLUDE_WAIT_FOR_GRAPH_H_
#define SIMPLE_DBMS_INCLUDE_WAIT_FOR_GRAPH_H_

#include "lock_table.h"
#include "txn_mgr.h"

void add_wait_for_edges(lock_t* waiter, sentinel_t* sentinel);
void remove_wait_for_edges_for_txn(txnid_t tid);
void clear_wait_for_edges(txnid_t tid);

#endif