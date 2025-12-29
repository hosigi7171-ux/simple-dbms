#include <assert.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#include "bpt.h"

#define VALUE_SIZE 120
typedef struct {
  int passed;
  int failed;
  pthread_mutex_t mutex;
} TestSummary;
TestSummary summary = {0, 0, PTHREAD_MUTEX_INITIALIZER};

void assert_true(int condition, const char* msg) {
  if (condition) {
    summary.passed++;
    printf("[PASS] %s\n", msg);
  } else {
    summary.failed++;
    printf("[FAIL] %s\n", msg);
  }
}

int main() {
  system("rm -f DATA*.db logfile.data logmsg.txt");
  printf("Test 2: REDO CRASH Recovery\n");
  // 1. Setup Initial Data
  printf("\n=== Phase 1: Initial Setup ===\n");
  init_db(100, 0, 0, "logfile.data", "logmsg.txt");
  open_table("DATA1.db");

  uint32_t setup_tid = txn_begin();
  for (int i = 1; i <= 10; i++) {
    db_insert(1, i, "ORIGINAL");
  }
  txn_commit(setup_tid);

  shutdown_db();
  printf("Initial data inserted.\n");

  pid_t setup_pid = fork();
  if (setup_pid == 0) {
    init_db(100, 0, 0, "logfile.data", "logmsg.txt");
    open_table("DATA1.db");
    for (int i = 1; i <= 5; i++) {
      uint32_t tid = txn_begin();
      db_update(1, i, "WINNER", tid);
      txn_commit(tid);
    }
    shutdown_db();
    exit(0);
  }
  waitpid(setup_pid, NULL, 0);

  // system("ls -lh logfile.data");
  // system("hexdump -C logfile.data | head -20");

  // 2. Simulate REDO crash (flag=1, log_num=3)
  printf("Simulating REDO crash (stops after 3rd REDO)...\n");
  pid_t redo_pid = fork();
  if (redo_pid == 0) {
    int ret = init_db(100, 1, 3, "logfile.data", "logmsg.txt");
    exit(ret == 0 ? 0 : 1);
  }
  int status;
  waitpid(redo_pid, &status, 0);
  assert_true(WIFEXITED(status) && WEXITSTATUS(status) == 0,
              "REDO crash simulated");

  // 3. Final Recovery
  printf("Performing full recovery...\n");
  init_db(100, 0, 0, "logfile.data", "logmsg.txt");
  open_table("DATA1.db");

  FILE* fp = fopen("logmsg.txt", "r");
  char line[512];
  int consider_redo = 0;
  while (fgets(line, sizeof(line), fp))
    if (strstr(line, "[CONSIDER-REDO]")) consider_redo = 1;
  fclose(fp);

  assert_true(consider_redo, "CONSIDER-REDO found (Idempotency checked)");
  shutdown_db();
  return (summary.failed > 0);
}
