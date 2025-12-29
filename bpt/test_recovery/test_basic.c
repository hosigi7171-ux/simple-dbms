#include <assert.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "bpt.h"

#define NUM_TABLES 3
#define NUM_THREADS 8
#define OPS_PER_THREAD 30
#define VALUE_SIZE 120
#define INIT_KEYS 50

typedef struct {
  int passed;
  int failed;
  pthread_mutex_t mutex;
} TestSummary;
TestSummary summary = {0, 0, PTHREAD_MUTEX_INITIALIZER};

void assert_true(int condition, const char* msg) {
  pthread_mutex_lock(&summary.mutex);
  if (condition) {
    summary.passed++;
    printf("[PASS] %s\n", msg);
  } else {
    summary.failed++;
    printf("[FAIL] %s\n", msg);
  }
  pthread_mutex_unlock(&summary.mutex);
}

void cleanup() {
  system("rm -f DATA*.db logfile.data logmsg.txt");
  sync();
  usleep(100000);
}

typedef struct {
  int thread_id;
} ThreadData;

void* basic_recovery_worker(void* arg) {
  ThreadData* data = (ThreadData*)arg;
  int tid = data->thread_id;
  for (int i = 0; i < OPS_PER_THREAD; i++) {
    uint32_t txn_id = txn_begin();
    int table_id = (tid % NUM_TABLES) + 1;
    int key = (rand() % INIT_KEYS) + 1;
    char val[VALUE_SIZE];
    snprintf(val, VALUE_SIZE, "T%d_OP%d", tid, i);
    if (db_update(table_id, key, val, txn_id) != 0) {
      txn_abort(txn_id);
      continue;
    }
    if (rand() % 5 == 0)
      txn_abort(txn_id);
    else
      txn_commit(txn_id);
  }
  return NULL;
}

int main() {
  srand(time(NULL));
  cleanup();
  printf("Test 1: Basic Recovery (Winners/Losers)\n");

  // 1. Setup Initial Data (single-threaded with transaction)
  printf("\n=== Phase 1: Initial Setup ===\n");
  init_db(100, 0, 0, "logfile.data", "logmsg.txt");

  for (int table_num = 1; table_num <= NUM_TABLES; table_num++) {
    char table_name[20];
    snprintf(table_name, 20, "DATA%d.db", table_num);
    int table_id = open_table(table_name);

    // Begin transaction for initial inserts
    uint32_t setup_tid = txn_begin();

    // Insert initial keys
    for (int key = 1; key <= INIT_KEYS; key++) {
      char initial_value[VALUE_SIZE];
      snprintf(initial_value, VALUE_SIZE, "INITIAL_%d", key);
      db_insert(table_id, key, initial_value);
    }

    // Commit the setup transaction
    txn_commit(setup_tid);
    printf("Table %d: Inserted %d initial records\n", table_id, INIT_KEYS);
  }

  shutdown_db();
  printf("Initial setup completed.\n\n");

  // 2. Run workload in separate process (to simulate crash)
  printf("=== Phase 2: Workload Execution ===\n");
  pid_t workload_pid = fork();
  if (workload_pid == 0) {
    // Child process: run workload
    init_db(100, 0, 0, "logfile.data", "logmsg.txt");

    for (int i = 1; i <= NUM_TABLES; i++) {
      char name[20];
      snprintf(name, 20, "DATA%d.db", i);
      open_table(name);
    }

    pthread_t threads[NUM_THREADS];
    ThreadData t_data[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; i++) {
      t_data[i].thread_id = i;
      pthread_create(&threads[i], NULL, basic_recovery_worker, &t_data[i]);
    }
    for (int i = 0; i < NUM_THREADS; i++) {
      pthread_join(threads[i], NULL);
    }

    shutdown_db();
    exit(0);
  }
  waitpid(workload_pid, NULL, 0);
  printf("Workload completed. Simulating crash...\n\n");

  // 3. Recovery
  printf("=== Phase 3: Recovery ===\n");
  init_db(100, 0, 0, "logfile.data", "logmsg.txt");

  FILE* fp = fopen("logmsg.txt", "r");
  if (fp == NULL) {
    printf("Failed to open logmsg.txt\n");
    return 1;
  }

  char line[512];
  int analysis_ok = 0, redo_ok = 0, undo_ok = 0;
  int winner_count = 0, loser_count = 0;

  while (fgets(line, sizeof(line), fp)) {
    if (strstr(line, "[ANALYSIS] Analysis success")) {
      analysis_ok = 1;
    }
    if (strstr(line, "[REDO] Redo pass end")) redo_ok = 1;
    if (strstr(line, "[UNDO] Undo pass end")) undo_ok = 1;
  }
  fclose(fp);

  assert_true(analysis_ok, "Analysis pass completed");
  assert_true(redo_ok, "Redo pass completed");
  assert_true(undo_ok, "Undo pass completed");
  printf("\n");
  return (summary.failed > 0);
}
