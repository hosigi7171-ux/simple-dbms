#ifndef SIMPLE_DBMS_INCLUDE_LOG_H
#define SIMPLE_DBMS_INCLUDE_LOG_H

#include <pthread.h>

#include <cstdint>
#include <cstdio>
#include <set>
#include <unordered_map>

#include "common_config.h"

// 로그 버퍼 크기 설정: 1MB
#define LOG_BUFFER_SIZE (1024 * 1024)
#define INITIAL_LOG_OFFSET (0)
#define BASE_SIZE (28)
#define UPDATE_SIZE (288)
#define COMPENSATE_SIZE (296)
#define PAGE_LSN_OFFSET (24)

// log type
#define LOG_BEGIN (0)
#define LOG_COMMIT (1)
#define LOG_ROLLBACK (2)
#define LOG_UPDATE (3)
#define LOG_COMPENSATE (4)

#define CRASH (99)

#pragma pack(push, 1)
/**
 * BEGIN/COMMIT/ROLLBACK log record
 * also used for common part of record
 * 28 bytes
 */
typedef struct base_log_t {
  uint64_t lsn;
  uint64_t prev_lsn;
  uint32_t txn_id;
  uint32_t type;
  uint32_t log_size;
} base_log_t;

/**
 * UPDATE log record
 * 288 bytes
 */
typedef struct update_log_t {
  // base
  uint64_t lsn;
  uint64_t prev_lsn;
  uint32_t txn_id;
  uint32_t type;
  // update
  uint32_t table_id;
  uint64_t page_num;
  uint32_t offset;
  uint32_t data_length;
  char old_image[VALUE_SIZE];
  char new_image[VALUE_SIZE];
  uint32_t log_size;
} update_log_t;

/**
 * COMPENSATE log record
 * 296 bytes
 */
typedef struct compensate_log_t {
  // base
  uint64_t lsn;
  uint64_t prev_lsn;
  uint32_t txn_id;
  uint32_t type;
  // update
  uint32_t table_id;
  uint64_t page_num;
  uint32_t offset;
  uint32_t data_length;
  char old_image[VALUE_SIZE];
  char new_image[VALUE_SIZE];
  // compensate
  int64_t next_undo_lsn;
  uint32_t log_size;
} compensate_log_t;

#pragma pack(pop)

typedef struct LogHash {
  size_t operator()(const std::pair<tableid_t, pagenum_t>& key) const {
    std::hash<size_t> hasher;
    size_t h1 = hasher((size_t)key.first);
    size_t h2 = hasher((size_t)key.second);
    return h1 ^ (h2 << 1);
  }
} LogHash;

/**
 * recovery state
 * metadata for recovery
 */
typedef struct recovery_state_t {
  std::set<txnid_t> winners;
  std::set<txnid_t> losers;
  std::unordered_map<txnid_t, uint64_t> active_txn_table;  // txn_id -> last LSN
  std::unordered_map<std::pair<tableid_t, pagenum_t>, uint64_t, LogHash>
      dirty_page_table;  //(table_id, page_num) -> LSN
  uint64_t max_lsn;
  int log_count;  // for crash during recovery
} recovery_state_t;

typedef struct log_manager_t {
  char* log_buffer;
  uint64_t buffer_size;
  uint64_t buffer_offset;  // 버퍼 내 현재 위치 (0 ~ buffer_size)

  // LSN 관리 (end offset, 절대 위치, 계속 증가)
  uint64_t last_lsn;     // 버퍼에 현재까지 기록된 LSN (절대 위치)
  uint64_t flushed_lsn;  // 디스크에 flush된 마지막 LSN (절대 위치)

  int log_fd;       // 로그 파일 디스크립터
  FILE* logmsg_fp;  // 로그 메시지 파일 포인터
  pthread_mutex_t log_latch;
  pthread_cond_t flush_cond;  // 버퍼 공간 대기용

  recovery_state_t recovery;
  int recovery_flag;  // 0: normal, 1: REDO crash, 2: UNDO crash
} log_manager_t;

extern log_manager_t log_mgr;

int log_init(char* log_path, char* logmsg_path);
void log_close();

uint64_t log_append_begin(uint32_t txn_id);
uint64_t log_append_commit(uint32_t txn_id, uint64_t prev_lsn);
uint64_t log_append_rollback(uint32_t txn_id, uint64_t prev_lsn);
uint64_t log_append_update(uint32_t txn_id, int table_id, uint64_t page_num,
                           uint32_t offset, uint32_t length, char* old_img,
                           char* new_img, int64_t prev_lsn);
uint64_t log_append_compensate(uint32_t txn_id, int table_id, uint64_t page_num,
                               uint32_t offset, uint32_t length, char* old_img,
                               char* new_img, uint64_t next_undo_lsn,
                               uint64_t prev_lsn);
ssize_t read_log_record_sequential(uint64_t start_offset, void* buffer,
                                   size_t max_size, uint64_t* out_end_offset);
void log_flush(uint64_t target_lsn);
void log_force_flush();
uint64_t append_to_buffer(void* log_record, uint32_t log_size);

/* recovery */
int log_recovery(int flag, int log_count);

#endif