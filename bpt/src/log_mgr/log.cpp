#include "log.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>

#include "txn_mgr.h"

#define MODE (0644)

log_manager_t log_mgr;

/**
 * init log manager
 */
int log_init(char* log_path, char* logmsg_path) {
  // open or create log file
  log_mgr.log_fd = open(log_path, O_RDWR | O_CREAT, MODE);

  if (log_mgr.log_fd == FD_FAIL) {
    return FAILURE;
  }

  // open log message file
  log_mgr.logmsg_fp = fopen(logmsg_path, "w");
  if (log_mgr.logmsg_fp == nullptr) {
    close(log_mgr.log_fd);
    return FAILURE;
  }

  // init last_lsn and flushed_lsn
  struct stat st;
  fstat(log_mgr.log_fd, &st);
  log_mgr.last_lsn = st.st_size;
  log_mgr.flushed_lsn = st.st_size;

  // init buffer
  log_mgr.log_buffer = (char*)malloc(LOG_BUFFER_SIZE);
  if (log_mgr.log_buffer == nullptr) {
    close(log_mgr.log_fd);
    fclose(log_mgr.logmsg_fp);
    return FAILURE;
  }
  log_mgr.buffer_size = LOG_BUFFER_SIZE;
  memset(log_mgr.log_buffer, 0, LOG_BUFFER_SIZE);
  log_mgr.buffer_size = LOG_BUFFER_SIZE;

  // circular buffer 위치: last_lsn의 버퍼 내 위치
  log_mgr.buffer_offset = st.st_size % LOG_BUFFER_SIZE;

  pthread_mutex_init(&log_mgr.log_latch, nullptr);
  pthread_cond_init(&log_mgr.flush_cond, nullptr);

  return SUCCESS;
}

/**
 * Close log manager
 */
void log_close() {
  log_force_flush();

  if (log_mgr.log_buffer) {
    free(log_mgr.log_buffer);
    log_mgr.log_buffer = nullptr;
  }

  if (log_mgr.log_fd != FD_FAIL) {
    close(log_mgr.log_fd);
    log_mgr.log_fd = FD_FAIL;
  }
  if (log_mgr.logmsg_fp) {
    fclose(log_mgr.logmsg_fp);
    log_mgr.logmsg_fp = nullptr;
  }

  pthread_mutex_destroy(&log_mgr.log_latch);
  pthread_cond_destroy(&log_mgr.flush_cond);
}

/**
 * calculate available space in circular buffer
 * @return bytes available for writing
 */
static uint64_t get_available_space() {
  uint64_t used = log_mgr.last_lsn - log_mgr.flushed_lsn;

  if (used >= log_mgr.buffer_size) {
    return 0;  // buffer full
  }

  return log_mgr.buffer_size - used;
}

/**
 * wait for buffer space (blocks until flush complete)
 */
static void wait_for_buffer_space(uint64_t needed) {
  while (get_available_space() < needed) {
    log_flush(log_mgr.last_lsn);

    if (get_available_space() >= needed) {
      break;
    }

    // wait for signal (shouldn't normally reach here)
    pthread_cond_wait(&log_mgr.flush_cond, &log_mgr.log_latch);
  }
}

/**
 * Append log to circular buffer
 * @return new LSN of the appended log (end offset)
 */
uint64_t append_to_buffer(void* log_record, uint32_t log_size) {
  wait_for_buffer_space(log_size);

  uint64_t write_offset = log_mgr.buffer_offset;

  // handle wrap-around
  if (write_offset + log_size > log_mgr.buffer_size) {
    uint64_t first_part = log_mgr.buffer_size - write_offset;
    uint64_t second_part = log_size - first_part;

    memcpy(log_mgr.log_buffer + write_offset, log_record, first_part);
    memcpy(log_mgr.log_buffer, (char*)log_record + first_part, second_part);
  } else {
    memcpy(log_mgr.log_buffer + write_offset, log_record, log_size);
  }
  log_mgr.buffer_offset = (write_offset + log_size) % log_mgr.buffer_size;

  // update last_lsn (absolute position in file)
  uint64_t new_lsn = log_mgr.last_lsn + log_size;
  log_mgr.last_lsn = new_lsn;

  return new_lsn;
}

/**
 * Flush logs from flushed_lsn to target_lsn
 * Circular buffer: flush only the unflushed range
 */
void log_flush(uint64_t target_lsn) {
  if (target_lsn <= log_mgr.flushed_lsn) {
    return;
  }
  if (target_lsn > log_mgr.last_lsn) {
    target_lsn = log_mgr.last_lsn;
  }

  uint64_t flush_size = target_lsn - log_mgr.flushed_lsn;
  if (flush_size == 0) {
    return;
  }

  // flushed_lsn부터 flush_size만큼의 데이터가 버퍼 어디에 있는지 찾기
  uint64_t buffer_start = (log_mgr.flushed_lsn % log_mgr.buffer_size);
  uint64_t file_offset = log_mgr.flushed_lsn;

  // handle circular wrap-around
  if (buffer_start + flush_size > log_mgr.buffer_size) {
    // 두 번에 나눠서 써야 함
    uint64_t first_part = log_mgr.buffer_size - buffer_start;
    uint64_t second_part = flush_size - first_part;

    pwrite(log_mgr.log_fd, log_mgr.log_buffer + buffer_start, first_part,
           file_offset);
    pwrite(log_mgr.log_fd, log_mgr.log_buffer, second_part,
           file_offset + first_part);
  } else {
    // 한 번에 쓰기
    pwrite(log_mgr.log_fd, log_mgr.log_buffer + buffer_start, flush_size,
           file_offset);
  }

  fsync(log_mgr.log_fd);
  log_mgr.flushed_lsn = target_lsn;
  pthread_cond_broadcast(&log_mgr.flush_cond);
}

/**
 * flush log buffer until last lsn
 * holding log manager latch
 */
void log_force_flush() {
  pthread_mutex_lock(&log_mgr.log_latch);
  log_flush(log_mgr.last_lsn);
  pthread_mutex_unlock(&log_mgr.log_latch);
}

/**
 * append BEGIN log record
 */
uint64_t log_append_begin(uint32_t txn_id) {
  pthread_mutex_lock(&log_mgr.log_latch);

  base_log_t log;
  memset(&log, 0, sizeof(log));
  log.lsn = log_mgr.last_lsn + sizeof(base_log_t);
  log.prev_lsn = 0;
  log.txn_id = txn_id;
  log.type = LOG_BEGIN;
  log.log_size = sizeof(base_log_t);

  uint64_t lsn = append_to_buffer(&log, sizeof(base_log_t));

  pthread_mutex_unlock(&log_mgr.log_latch);
  return lsn;
}
/**
 * append COMMIT log and flush log atomically
 */
uint64_t log_append_commit(uint32_t txn_id, uint64_t prev_lsn) {
  pthread_mutex_lock(&log_mgr.log_latch);

  base_log_t log;
  memset(&log, 0, sizeof(log));
  log.lsn = log_mgr.last_lsn + sizeof(base_log_t);
  log.prev_lsn = prev_lsn;
  log.txn_id = txn_id;
  log.type = LOG_COMMIT;
  log.log_size = sizeof(base_log_t);

  uint64_t lsn = append_to_buffer(&log, sizeof(base_log_t));

  // COMMIT requires durability - flush immediately
  log_flush(lsn);

  pthread_mutex_unlock(&log_mgr.log_latch);
  return lsn;
}

/**
 * append ROLLBACK log
 */
uint64_t log_append_rollback(uint32_t txn_id, uint64_t prev_lsn) {
  pthread_mutex_lock(&log_mgr.log_latch);

  base_log_t log;
  memset(&log, 0, sizeof(log));
  log.lsn = log_mgr.last_lsn + sizeof(base_log_t);
  log.prev_lsn = prev_lsn;
  log.txn_id = txn_id;
  log.type = LOG_ROLLBACK;
  log.log_size = sizeof(base_log_t);

  uint64_t lsn = append_to_buffer(&log, sizeof(base_log_t));

  pthread_mutex_unlock(&log_mgr.log_latch);
  return lsn;
}

/**
 * append UPDATE log
 */
uint64_t log_append_update(uint32_t txn_id, int table_id, uint64_t page_num,
                           uint32_t offset, uint32_t length, char* old_img,
                           char* new_img, int64_t prev_lsn) {
  pthread_mutex_lock(&log_mgr.log_latch);

  update_log_t log;
  memset(&log, 0, sizeof(log));
  log.lsn = log_mgr.last_lsn + sizeof(update_log_t);
  log.prev_lsn = prev_lsn;
  log.txn_id = txn_id;
  log.type = LOG_UPDATE;
  log.table_id = table_id;
  log.page_num = page_num;
  log.offset = offset;
  log.data_length = length;
  memcpy(log.old_image, old_img, VALUE_SIZE);
  memcpy(log.new_image, new_img, VALUE_SIZE);
  log.log_size = sizeof(update_log_t);

  uint64_t lsn = append_to_buffer(&log, sizeof(update_log_t));

  pthread_mutex_unlock(&log_mgr.log_latch);
  return lsn;
}

/**
 * append COMPENSATE log
 */
uint64_t log_append_compensate(uint32_t txn_id, int table_id, uint64_t page_num,
                               uint32_t offset, uint32_t length, char* old_img,
                               char* new_img, uint64_t next_undo_lsn,
                               uint64_t prev_lsn) {
  pthread_mutex_lock(&log_mgr.log_latch);

  compensate_log_t log;
  memset(&log, 0, sizeof(log));
  log.lsn = log_mgr.last_lsn + sizeof(compensate_log_t);
  log.prev_lsn = prev_lsn;
  log.txn_id = txn_id;
  log.type = LOG_COMPENSATE;
  log.table_id = table_id;
  log.page_num = page_num;
  log.offset = offset;
  log.data_length = length;
  memcpy(log.old_image, old_img, VALUE_SIZE);
  memcpy(log.new_image, new_img, VALUE_SIZE);
  log.next_undo_lsn = next_undo_lsn;
  log.log_size = sizeof(compensate_log_t);

  uint64_t lsn = append_to_buffer(&log, sizeof(compensate_log_t));

  pthread_mutex_unlock(&log_mgr.log_latch);
  return lsn;
}

/**
 * Sequential scan을 위한 함수
 * start_offset부터 로그를 읽고, 읽은 로그의 end offset을 반환
 */
ssize_t read_log_record_sequential(uint64_t start_offset, void* buffer,
                                   size_t max_size, uint64_t* out_end_offset) {
  uint32_t possible_sizes[] = {BASE_SIZE, UPDATE_SIZE, COMPENSATE_SIZE};

  for (uint32_t log_size : possible_sizes) {
    // log_size 필드 확인 (구조체 끝에 위치)
    uint32_t size_in_file;
    ssize_t ret = pread(log_mgr.log_fd, &size_in_file, sizeof(uint32_t),
                        start_offset + log_size - sizeof(uint32_t));

    if (ret == sizeof(uint32_t) && size_in_file == log_size) {
      // 전체 로그 읽기
      ret = pread(log_mgr.log_fd, buffer, log_size, start_offset);
      if (ret == (ssize_t)log_size) {
        *out_end_offset = start_offset + log_size;
        return log_size;
      }
    }
  }

  return -1;
}
