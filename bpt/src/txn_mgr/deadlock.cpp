#include "deadlock.h"

#include <stack>

enum DfsState { FORWARD = 0, BACKTRACK = 1 };

/**
 * helper function for find cycle members
 * collect all unique transaction id in wait-for graph
 */
std::vector<txnid_t> collect_all_txn_nodes() {
  std::vector<txnid_t> txn_nodes;
  for (const auto& entry : wait_for_graph) {
    txn_nodes.push_back(entry.first);
    for (txnid_t neighbor : entry.second) {
      txn_nodes.push_back(neighbor);
    }
  }

  std::sort(txn_nodes.begin(), txn_nodes.end());
  txn_nodes.erase(std::unique(txn_nodes.begin(), txn_nodes.end()),
                  txn_nodes.end());

  return txn_nodes;
}

/**
 * helper function for priority of victim candidates
 */
bool is_better(const victim_cand_t& a, const victim_cand_t& b) {
  if (a.s_only != b.s_only) {
    return b.s_only;
  }
  if (a.s_count != b.s_count) {
    return b.s_count < a.s_count;
  }
  return b.tid > a.tid;
}

/**
 * helper function for find cycle members
 * dfs from single node to find cycle
 * @return if cycle exists return cycle path, otherwise empty vector
 */
std::vector<txnid_t> dfs_find_cycle(txnid_t start_node,
                                    std::unordered_set<txnid_t>& visited) {
  std::stack<std::pair<txnid_t, DfsState>> dfs_stack;  // <node, dfs state>
  std::vector<txnid_t> dfs_path;        // current path, used for get cycle
  std::unordered_set<txnid_t> on_path;  // current path to check cycle

  dfs_stack.push({start_node, FORWARD});
  dfs_path.push_back(start_node);
  on_path.insert(start_node);

  while (!dfs_stack.empty()) {
    txnid_t u = dfs_stack.top().first;
    DfsState state = dfs_stack.top().second;
    dfs_stack.pop();

    if (state == BACKTRACK) {
      // backtrack: mark u as visited and remove from path
      visited.insert(u);
      on_path.erase(u);
      dfs_path.pop_back();
      continue;
    }

    // forward: explore neighbors of u
    dfs_stack.push({u, BACKTRACK});

    if (wait_for_graph.count(u)) {
      for (txnid_t v : wait_for_graph[u]) {
        // cycle exists
        if (on_path.count(v)) {
          auto it = std::find(dfs_path.begin(), dfs_path.end(), v);
          std::vector<txnid_t> cycle_members(it, dfs_path.end());
          return cycle_members;
        }

        // neighbor not visited, dfs
        if (visited.find(v) == visited.end()) {
          dfs_stack.push({v, FORWARD});
          dfs_path.push_back(v);
          on_path.insert(v);
        }
      }
    }
  }

  return {};
}

/**
 * find cycle path in wait-for graph
 * @return if cycle exists return cycle path, otherwise empty vector
 */
std::vector<txnid_t> find_cycle_members() {
  std::unordered_set<txnid_t> visited;  // nodes explored
  pthread_mutex_lock(&wait_for_graph_latch);

  std::vector<txnid_t> txn_nodes = collect_all_txn_nodes();
  for (txnid_t node : txn_nodes) {
    if (visited.count(node)) continue;

    std::vector<txnid_t> cycle = dfs_find_cycle(node, visited);
    if (!cycle.empty()) {
      pthread_mutex_unlock(&wait_for_graph_latch);
      return cycle;
    }
  }

  pthread_mutex_unlock(&wait_for_graph_latch);
  return {};
}

/**
 * find cycle path from current transaction_id in wait-for graph
 * @return if cycle exists return cycle path, otherwise empty vector
 */
std::vector<txnid_t> find_cycle_from(txnid_t txn_id) {
  pthread_mutex_lock(&wait_for_graph_latch);

  if (!wait_for_graph.count(txn_id)) {
    pthread_mutex_unlock(&wait_for_graph_latch);
    return {};
  }

  std::unordered_set<txnid_t> visited;
  std::vector<txnid_t> cycle = dfs_find_cycle(txn_id, visited);

  pthread_mutex_unlock(&wait_for_graph_latch);
  return cycle;
}

/**
 * select victim in cycle
 * 1. s-only, younger
 * 2. s many, younger
 * 3. younger
 */
txnid_t select_victim_from_cycle(const std::vector<txnid_t>& cycle) {
  std::vector<victim_cand_t> cand;

  pthread_mutex_lock(&txn_table.latch);

  for (txnid_t tid : cycle) {
    auto it = txn_table.transactions.find(tid);
    if (it == txn_table.transactions.end()) continue;

    tcb_t* tcb = it->second;

    pthread_mutex_lock(&tcb->latch);
    pthread_mutex_unlock(&txn_table.latch);

    int s_count = 0;
    bool has_x = false;

    for (lock_t* l = tcb->lock_head; l; l = l->txn_next_lock) {
      if (!l->granted) continue;
      if (l->mode == S_LOCK)
        s_count++;
      else
        has_x = true;
    }

    cand.push_back({tid, s_count, !has_x});

    pthread_mutex_unlock(&tcb->latch);
    pthread_mutex_lock(&txn_table.latch);
  }

  pthread_mutex_unlock(&txn_table.latch);

  victim_cand_t best = cand[0];
  for (size_t i = 1; i < cand.size(); i++) {
    if (is_better(best, cand[i])) {
      best = cand[i];
    }
  }

  return best.tid;
}
