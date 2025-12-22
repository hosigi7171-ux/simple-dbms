#include "wait_for_graph.h"

#include <set>

std::unordered_map<txnid_t, std::multiset<txnid_t>> wait_for_graph;
pthread_mutex_t wait_for_graph_latch = PTHREAD_MUTEX_INITIALIZER;

void print_wait_for_graph_unlocked() {
  printf("=== Wait-For Graph ===\n");
  for (auto& entry : wait_for_graph) {
    printf("Txn %d waits for: ", entry.first);

    std::unordered_set<txnid_t> unique_blockers;
    for (txnid_t blocker : entry.second) {
      unique_blockers.insert(blocker);
    }

    for (txnid_t b : unique_blockers) {
      printf("%d ", b);
    }
    printf("\n");
  }
}

void print_wait_for_graph() {
  pthread_mutex_lock(&wait_for_graph_latch);
  print_wait_for_graph_unlocked();
  pthread_mutex_unlock(&wait_for_graph_latch);
}

/**
 * wait-for edge 추가
 */
void add_wait_for_edges(lock_t* waiter, sentinel_t* sentinel) {
  if (!waiter || !sentinel) return;
  txnid_t me = waiter->owner_tcb->id;

  pthread_mutex_lock(&wait_for_graph_latch);

  std::unordered_set<txnid_t> blocking_txns;

  // 내 앞에 있는 granted 락만 검사
  for (lock_t* p = sentinel->head; p != waiter; p = p->next) {
    if (p == nullptr) break;

    // granted된 락만 blocking으로 간주
    if (!p->granted) continue;

    txnid_t blocker_txn = p->owner_tcb->id;
    if (blocker_txn == me) continue;

    // 충돌 검사
    bool conflicts = false;
    if (p->mode == X_LOCK || waiter->mode == X_LOCK) {
      conflicts = true;
    } else if (p->mode == S_LOCK && waiter->mode == S_LOCK) {
      conflicts = false;
    }

    if (conflicts) {
      blocking_txns.insert(blocker_txn);
    }
  }

  // 이 레코드에서의 blocking 관계 추가
  for (txnid_t blocker : blocking_txns) {
    wait_for_graph[me].insert(blocker);
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
 * 트랜잭션 관련 모든 edge 제거
 */
void remove_wait_for_edges_for_txn(txnid_t tid) {
  pthread_mutex_lock(&wait_for_graph_latch);

  // Outgoing edges 제거
  wait_for_graph.erase(tid);

  // Incoming edges 제거
  for (auto it = wait_for_graph.begin(); it != wait_for_graph.end();) {
    it->second.erase(tid);

    if (it->second.empty()) {
      it = wait_for_graph.erase(it);
    } else {
      ++it;
    }
  }

  pthread_mutex_unlock(&wait_for_graph_latch);
}

void clear_wait_for_edges(txnid_t tid) {
  pthread_mutex_lock(&wait_for_graph_latch);
  wait_for_graph.erase(tid);
  for (auto& e : wait_for_graph) {
    e.second.erase(tid);
  }
  pthread_mutex_unlock(&wait_for_graph_latch);
}

/**
 * 락 획득 시 해당 레코드에서의 대기 edge 제거
 */
void remove_wait_for_edges_on_grant(txnid_t txn_id, sentinel_t* sentinel) {
  pthread_mutex_lock(&wait_for_graph_latch);

  // 이미 제거된 트랜잭션이면 skip
  if (wait_for_graph.count(txn_id) == 0) {
    pthread_mutex_unlock(&wait_for_graph_latch);
    return;
  }

  // 내가 방금 획득한 락 찾기
  lock_t* my_lock = nullptr;
  for (lock_t* p = sentinel->head; p != nullptr; p = p->next) {
    if (p->owner_tcb->id == txn_id && p->granted) {
      my_lock = p;
      break;
    }
  }

  if (!my_lock) {
    pthread_mutex_unlock(&wait_for_graph_latch);
    return;
  }

  // 이 레코드에서 나를 blocking하던 트랜잭션들 찾기
  std::unordered_set<txnid_t> blockers_on_this_record;

  for (lock_t* p = sentinel->head; p != my_lock; p = p->next) {
    if (!p->granted) continue;

    txnid_t blocker = p->owner_tcb->id;
    if (blocker == txn_id) continue;

    bool conflicts = (p->mode == X_LOCK || my_lock->mode == X_LOCK);

    if (conflicts) {
      blockers_on_this_record.insert(blocker);
    }
  }

  // 제거
  auto& my_edges = wait_for_graph[txn_id];
  for (txnid_t blocker : blockers_on_this_record) {
    my_edges.erase(blocker);
  }

  if (my_edges.empty()) {
    wait_for_graph.erase(txn_id);
  }

  pthread_mutex_unlock(&wait_for_graph_latch);
}

/**
 * not used now(12.22)
 * helper function used in try_grant_waiters_on_record
 * rebuild wait for graph to check accurate status
 */
void rebuild_wait_for_graph_for_record(sentinel_t* sentinel) {
  pthread_mutex_lock(&wait_for_graph_latch);
  // 레코드의 모든 대기자 수집
  std::unordered_set<txnid_t> waiters_in_this_record;
  for (lock_t* waiter = sentinel->head; waiter != nullptr;
       waiter = waiter->next) {
    if (!waiter->granted) {
      waiters_in_this_record.insert(waiter->owner_tcb->id);
    }
  }

  // 각 대기자에 대해 이 레코드 관련 edge만 재구성
  for (txnid_t me : waiters_in_this_record) {
    pthread_mutex_lock(&txn_table.latch);
    auto it = txn_table.transactions.find(me);
    if (it == txn_table.transactions.end()) {
      pthread_mutex_unlock(&txn_table.latch);
      continue;
    }
    tcb_t* my_tcb = it->second;
    pthread_mutex_unlock(&txn_table.latch);

    // 이 레코드에서의 이전 blocking 관계 제거
    std::unordered_set<txnid_t> old_blockers_from_this_record;
    for (lock_t* holder = sentinel->head; holder != nullptr;
         holder = holder->next) {
      if (holder->owner_tcb->id != me && holder->granted) {
        old_blockers_from_this_record.insert(holder->owner_tcb->id);
      }
    }

    // multiset에서 제거
    for (txnid_t old_blocker : old_blockers_from_this_record) {
      auto edge_it = wait_for_graph[me].find(old_blocker);
      if (edge_it != wait_for_graph[me].end()) {
        wait_for_graph[me].erase(edge_it);
      }
    }

    // 이 레코드 기준의 새로운 blocking 관계 추가
    for (lock_t* my_lock = sentinel->head; my_lock != nullptr;
         my_lock = my_lock->next) {
      if (my_lock->owner_tcb->id != me) continue;
      if (my_lock->granted) continue;  // 이미 획득한 락은 스킵

      // 내 앞의 granted 락만 검사
      for (lock_t* blocker_lock = sentinel->head; blocker_lock != my_lock;
           blocker_lock = blocker_lock->next) {
        if (!blocker_lock->granted) continue;

        txnid_t blocker = blocker_lock->owner_tcb->id;
        if (blocker == me) continue;

        bool conflicts =
            (blocker_lock->mode == X_LOCK || my_lock->mode == X_LOCK);
        if (conflicts) {
          wait_for_graph[me].insert(blocker);
        }
      }
    }

    if (wait_for_graph[me].empty()) {
      wait_for_graph.erase(me);
    }
  }
  pthread_mutex_unlock(&wait_for_graph_latch);
}

/**
 * update wait for graph
 * update only changed part
 */
void update_wait_for_graph_on_grant(lock_t* granted_lock,
                                    sentinel_t* sentinel) {
  pthread_mutex_lock(&wait_for_graph_latch);

  txnid_t granted_txn = granted_lock->owner_tcb->id;

  // 트랜잭션의 outgoing edges 제거
  wait_for_graph.erase(granted_txn);

  // 트랜잭션의 뒤에서 대기 중인 트랜잭션들만 업데이트
  bool found_granted = false;
  for (lock_t* p = sentinel->head; p != nullptr; p = p->next) {
    if (p == granted_lock) {
      found_granted = true;
      continue;
    }

    if (found_granted && !p->granted) {
      txnid_t waiter = p->owner_tcb->id;

      // granted_lock과의 충돌만 확인
      bool conflicts = (granted_lock->mode == X_LOCK || p->mode == X_LOCK);

      if (conflicts) {
        // 새로운 blocking 관계 추가
        wait_for_graph[waiter].insert(granted_txn);
      }
    }
  }

  pthread_mutex_unlock(&wait_for_graph_latch);
}

/**
 * 특정 레코드의 락 큐 상태 출력
 */
void print_lock_queue(hashkey_t hashkey) {
  pthread_mutex_lock(&lock_table_latch);

  if (lock_table.count(hashkey) == 0) {
    printf("Record (table=%d, key=%ld): No locks\n", hashkey.tableid,
           hashkey.recordid);
    pthread_mutex_unlock(&lock_table_latch);
    return;
  }

  sentinel_t* sentinel = lock_table[hashkey];
  printf("=== Lock Queue for Record (table=%d, key=%ld) ===\n", hashkey.tableid,
         hashkey.recordid);

  int idx = 0;
  for (lock_t* p = sentinel->head; p != nullptr; p = p->next) {
    printf("[%d] Txn %d: %s, %s\n", idx++, p->owner_tcb->id,
           p->mode == S_LOCK ? "S-LOCK" : "X-LOCK",
           p->granted ? "GRANTED" : "WAITING");
  }
  printf("\n");

  pthread_mutex_unlock(&lock_table_latch);
}

/**
 * 모든 레코드의 락 큐 상태 출력
 */
void print_all_lock_queues() {
  pthread_mutex_lock(&lock_table_latch);

  printf("========== ALL LOCK QUEUES ==========\n");

  if (lock_table.empty()) {
    printf("No locks in system\n");
    pthread_mutex_unlock(&lock_table_latch);
    return;
  }

  for (auto& entry : lock_table) {
    hashkey_t hashkey = entry.first;
    sentinel_t* sentinel = entry.second;

    printf("Record (table=%d, key=%ld):\n", hashkey.tableid, hashkey.recordid);

    int idx = 0;
    for (lock_t* p = sentinel->head; p != nullptr; p = p->next) {
      printf("  [%d] Txn %d: %s, %s\n", idx++, p->owner_tcb->id,
             p->mode == S_LOCK ? "S-LOCK" : "X-LOCK",
             p->granted ? "GRANTED" : "WAITING");
    }
  }
  printf("=====================================\n\n");

  pthread_mutex_unlock(&lock_table_latch);
}

/**
 * 특정 트랜잭션이 보유/대기 중인 모든 락 출력
 */
void print_transaction_locks(txnid_t txn_id) {
  pthread_mutex_lock(&txn_table.latch);

  if (txn_table.transactions.count(txn_id) == 0) {
    printf("Transaction %d not found\n", txn_id);
    pthread_mutex_unlock(&txn_table.latch);
    return;
  }

  tcb_t* tcb = txn_table.transactions[txn_id];

  printf("=== Locks for Transaction %d ===\n", txn_id);
  // printf("Aborted: %s\n", tcb->aborted ? "YES" : "NO");

  pthread_mutex_lock(&lock_table_latch);

  int idx = 0;
  for (lock_t* lock = tcb->lock_head; lock != nullptr;
       lock = lock->txn_next_lock) {
    if (lock->sentinel) {
      printf("[%d] Record (table=%d, key=%ld): %s, %s\n", idx++,
             lock->sentinel->hashkey.tableid, lock->sentinel->hashkey.recordid,
             lock->mode == S_LOCK ? "S-LOCK" : "X-LOCK",
             lock->granted ? "GRANTED" : "WAITING");
    }
  }

  pthread_mutex_unlock(&lock_table_latch);
  pthread_mutex_unlock(&txn_table.latch);

  printf("\n");
}

/**
 * 전체 시스템 상태 출력
 */
void print_system_state() {
  printf("\n");
  printf("##################################################\n");
  printf("############### SYSTEM STATE #####################\n");
  printf("##################################################\n");

  // 1. Wait-for graph
  print_wait_for_graph();
  printf("\n");

  // 2. All lock queues
  print_all_lock_queues();

  // 3. Active transactions
  pthread_mutex_lock(&txn_table.latch);
  printf("=== Active Transactions ===\n");
  for (auto& entry : txn_table.transactions) {
    // printf("Txn %d (aborted: %s)\n", entry.first,
    //  entry.second->aborted ? "YES" : "NO");
  }
  printf("\n");
  pthread_mutex_unlock(&txn_table.latch);

  printf("##################################################\n\n");
}

/**
 * Deadlock 감지 시 상세 정보 출력
 */
void print_deadlock_info(txnid_t detector, const std::vector<txnid_t>& cycle,
                         txnid_t victim) {
  printf("\n");
  printf("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n");
  printf("!!!!!!!!!!!!!! DEADLOCK DETECTED !!!!!!!!!!!!!!!!!\n");
  printf("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n");

  printf("Detector: Txn %d\n", detector);
  printf("Cycle: ");
  for (txnid_t tid : cycle) {
    printf("%d ", tid);
  }
  printf("\n");
  printf("Victim: Txn %d\n", victim);
  printf("\n");

  print_wait_for_graph();

  printf("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n\n");
}
