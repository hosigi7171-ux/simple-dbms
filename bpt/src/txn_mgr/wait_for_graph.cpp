#include "wait_for_graph.h"

std::unordered_map<txnid_t, std::unordered_set<txnid_t>> wait_for_graph;
pthread_mutex_t wait_for_graph_latch = PTHREAD_MUTEX_INITIALIZER;

/**
 * helper function
 * add in wait-for-graph
 */
void add_wait_for_edges(lock_t* waiter, sentinel_t* sentinel) {
  if (!waiter || !sentinel) return;

  txnid_t me = waiter->owner_txn_id;

  pthread_mutex_lock(&lock_table_latch);
  pthread_mutex_lock(&wait_for_graph_latch);

  for (lock_t* p = sentinel->head; p; p = p->next) {
    if (!p->granted) continue;
    if (p->owner_txn_id == me) continue;

    wait_for_graph[me].insert(p->owner_txn_id);
  }

  pthread_mutex_unlock(&wait_for_graph_latch);
  pthread_mutex_unlock(&lock_table_latch);
}

/**
 * helper function for txn_abort
 * remove edges related to the transaction
 */
void remove_wait_for_edges_for_txn(txnid_t tid) {
  pthread_mutex_lock(&wait_for_graph_latch);

  wait_for_graph.erase(tid);
  for (auto it = wait_for_graph.begin(); it != wait_for_graph.end(); it++) {
    it->second.erase(tid);
  }

  pthread_mutex_unlock(&wait_for_graph_latch);
}

void clear_wait_for_edges(txnid_t tid) {
  wait_for_graph.erase(tid);
  for (auto& e : wait_for_graph) {
    e.second.erase(tid);
  }
}
