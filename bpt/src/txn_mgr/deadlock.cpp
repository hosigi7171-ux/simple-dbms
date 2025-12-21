#include "deadlock.h"

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
 * DFS 상태
 * WHITE:미방문, GRAY:탐색중, BLACK:방문완료
 */
enum NodeColor { WHITE = 0, GRAY = 1, BLACK = 2 };

/**
 * DFS 기반 사이클 탐지
 */
std::vector<txnid_t> dfs_find_cycle(
    txnid_t u, std::unordered_map<txnid_t, NodeColor>& color,
    std::vector<txnid_t>& path) {
  color[u] = GRAY;  // 현재 경로(Stack)에 추가됨을 표시
  path.push_back(u);

  if (wait_for_graph.count(u)) {
    for (txnid_t v : wait_for_graph[u]) {
      // 현재 탐색 중인 경로(GRAY)에 있는 노드를 만나면 사이클
      if (color[v] == GRAY) {
        auto it = std::find(path.begin(), path.end(), v);
        return std::vector<txnid_t>(it, path.end());
      }

      // 미방문 노드(WHITE)라면 재귀적으로 탐색
      if (color[v] == WHITE) {
        std::vector<txnid_t> cycle = dfs_find_cycle(v, color, path);
        if (!cycle.empty()) return cycle;
      }

      // 이미 방문 완료(BLACK)된 노드는 무시, 이미 cycle 아님이 검증됨
    }
  }

  path.pop_back();
  color[u] = BLACK;  // 이 노드로부터 시작되는 모든 경로는 사이클이 없음
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

  std::unordered_map<txnid_t, NodeColor> color;
  // 모든 노드를 WHITE로 초기화
  std::vector<txnid_t> nodes = collect_all_txn_nodes();
  for (txnid_t n : nodes) color[n] = WHITE;

  std::vector<txnid_t> path;
  std::vector<txnid_t> cycle = dfs_find_cycle(txn_id, color, path);

  pthread_mutex_unlock(&wait_for_graph_latch);
  return cycle;
}

/**
 * find cycle from unlocked version
 * caller must hold wait_for_graph_latch
 */
std::vector<txnid_t> find_cycle_from_unlocked(txnid_t txn_id) {
  if (!wait_for_graph.count(txn_id)) {
    pthread_mutex_unlock(&wait_for_graph_latch);
    return {};
  }

  std::unordered_map<txnid_t, NodeColor> color;
  // 모든 노드를 WHITE로 초기화
  std::vector<txnid_t> nodes = collect_all_txn_nodes();
  for (txnid_t n : nodes) color[n] = WHITE;

  std::vector<txnid_t> path;
  std::vector<txnid_t> cycle = dfs_find_cycle(txn_id, color, path);

  return cycle;
}
