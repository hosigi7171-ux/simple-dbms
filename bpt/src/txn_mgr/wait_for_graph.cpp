#include "wait_for_graph.h"

std::unordered_map<txnid_t, std::unordered_set<txnid_t>> wait_for_graph;
pthread_mutex_t wait_for_graph_latch = PTHREAD_MUTEX_INITIALIZER;

/**
 * helper function
 * add in wait-for-graph
 */
void add_wait_for_edges(lock_t* waiter, sentinel_t* sentinel) {
  if (!waiter || !sentinel) return;
  txnid_t me = waiter->owner_tcb->id;

  pthread_mutex_lock(&wait_for_graph_latch);

  for (lock_t* p = sentinel->head; p != waiter; p = p->next) {
    if (p == nullptr) break;
    if (p->owner_tcb->id == me) continue;

    // 나와 충돌하는 락이라면 에지 추가
    if (p->granted || (waiter->mode == X_LOCK || p->mode == X_LOCK)) {
      wait_for_graph[me].insert(p->owner_tcb->id);
    }
  }

  pthread_mutex_unlock(&wait_for_graph_latch);
}

/**
 * 내가 누군가를 기다리던 에지(Outgoing edges)만 삭제
 */
void clear_outgoing_edges(txnid_t tid) {
  pthread_mutex_lock(&wait_for_graph_latch);
  wait_for_graph.erase(tid);
  pthread_mutex_unlock(&wait_for_graph_latch);
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
