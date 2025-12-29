#include <assert.h>
#include <pthread.h>
#include <signal.h>
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
  // system("rm -f DATA*.db logfile.data logmsg.txt");
  printf("Test 3: UNDO CRASH Recovery (CLR check)\n");

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

  // 2. Create Winner and Loser, then CRASH
  printf("\n=== Phase 2: Create Winner and Loser (CRASH) ===\n");
  pid_t crash_pid = fork();
  if (crash_pid == 0) {
    init_db(100, 0, 0, "logfile.data", "logmsg.txt");
    open_table("DATA1.db");

    // Winner transaction (will commit)
    uint32_t winner_tid = txn_begin();
    printf("Winner txn %u started\n", winner_tid);
    db_update(1, 5, "WINNER", winner_tid);
    txn_commit(winner_tid);
    printf("Winner txn %u committed\n", winner_tid);

    // Loser transaction (will NOT commit)
    uint32_t loser_tid = txn_begin();
    printf("Loser txn %u started\n", loser_tid);
    db_update(1, 1, "LOSER_1", loser_tid);
    db_update(1, 2, "LOSER_2", loser_tid);
    printf("Loser txn %u made updates (no commit)\n", loser_tid);

    // Force flush log buffer to ensure logs are written
    log_force_flush();

    printf("Simulating CRASH without shutdown\n");
    _exit(0);  // Exit WITHOUT cleanup
  }

  int status;
  waitpid(crash_pid, &status, 0);
  printf("Child process exited (crash simulation complete)\n");

  // 3. Check log file
  // printf("\n=== Phase 3: Checking Log File ===\n");
  // system("ls -lh logfile.data");
  // printf("\nFirst 50 lines of hexdump:\n");
  // system("hexdump -C logfile.data | head -50");

  // 4. Simulate UNDO crash (stops after 1st UNDO)
  printf("\n=== Phase 4: Simulating UNDO CRASH (flag=2, log_num=1) ===\n");
  pid_t undo_pid = fork();
  if (undo_pid == 0) {
    int ret = init_db(100, 2, 1, "logfile.data", "logmsg.txt");
    printf("UNDO crash simulation returned: %d\n", ret);
    _exit(0);
  }
  waitpid(undo_pid, &status, 0);
  printf("UNDO crash simulation complete\n");

  // 5. Check logmsg after UNDO crash
  printf("\n=== Phase 5: Log Messages After UNDO Crash ===\n");
  // system("cat logmsg.txt");

  // 6. Final Recovery
  printf("\n=== Phase 6: Final Recovery ===\n");
  init_db(100, 0, 0, "logfile.data", "logmsg.txt");
  int tid = open_table("DATA1.db");
  printf("open_table returned: %d\n", tid);

  if (tid < 0) {
    printf("Failed to open table!\n");
    return 1;
  }

  // 7. Verify CLR in log messages
  printf("\n=== Phase 7: Checking for CLR ===\n");
  FILE* fp = fopen("logmsg.txt", "r");
  if (!fp) {
    printf("Cannot open logmsg.txt\n");
    return 1;
  }

  char line[512];
  int analysis_found = 0;
  int winner_found = 0;
  int loser_found = 0;

  while (fgets(line, sizeof(line), fp)) {
    printf("LOG: %s", line);

    if (strstr(line, "[ANALYSIS]")) {
      analysis_found = 1;
    }
    if (strstr(line, "Winner:") && !strstr(line, "Winner:,")) {
      winner_found = 1;
    }
    if (strstr(line, "Loser:") && !strstr(line, "Loser:\n")) {
      loser_found = 1;
    }
  }
  fclose(fp);

  // 8. Verify data - txn_id는 0으로 (read-only)
  char val[VALUE_SIZE];
  uint32_t read_tid = txn_begin();

  if (read_tid == 0) {
    printf("Failed to begin transaction!\n");
    return 1;
  }
  printf("\nAttempting to read data...\n");

  int find_result = db_find(1, 1, val, read_tid);
  if (find_result == 0) {
    printf("Key 1: %s (expected: ORIGINAL)\n", val);
    int key1_ok = (strcmp(val, "ORIGINAL") == 0);

    db_find(1, 2, val, read_tid);
    printf("Key 2: %s (expected: ORIGINAL)\n", val);
    int key2_ok = (strcmp(val, "ORIGINAL") == 0);

    txn_commit(read_tid);
    // 9. Assertions
    printf("\n=== Test Results ===\n");
    assert_true(analysis_found, "Analysis pass executed");
    assert_true(winner_found, "Winner transaction found");
    assert_true(loser_found, "Loser transaction found");

    assert_true(key1_ok, "Key 1 has correct value (ORIGINAL)");
    assert_true(key2_ok, "Key 2 has correct value (ORIGINAL)");
  } else {
    printf("db_find failed - data may be corrupted\n");
    txn_abort(read_tid);
    summary.failed += 3;
  }

  shutdown_db();

  printf("\n=== Summary ===\n");
  printf("Passed: %d\n", summary.passed);
  printf("Failed: %d\n", summary.failed);

  return (summary.failed > 0);
}
