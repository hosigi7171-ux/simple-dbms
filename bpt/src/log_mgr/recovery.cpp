#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <queue>
#include <set>
#include <unordered_map>
#include <vector>

#include "buf_mgr.h"
#include "db_api.h"
#include "log.h"

/**
 * ANALYSIS PASS
 * build ATT, DPT
 * identify Winner/Loser
 */
void log_pass_analysis() {
  fprintf(log_mgr.logmsg_fp, "[ANALYSIS] Analysis pass start\n");
  fflush(log_mgr.logmsg_fp);

  // Clear recovery state
  log_mgr.recovery.winners.clear();
  log_mgr.recovery.losers.clear();
  log_mgr.recovery.active_txn_table.clear();
  log_mgr.recovery.dirty_page_table.clear();
  log_mgr.recovery.max_lsn = 0;

  std::set<uint32_t> active_txns;

  // get log file size(max lsn)
  struct stat st;
  fstat(log_mgr.log_fd, &st);
  uint64_t file_size = st.st_size;

  // scan min to max
  char log_buf[LOG_BUFFER_SIZE];
  uint64_t current_offset = 0;

  int loop_count = 0;
  while (current_offset <= file_size) {
    if (loop_count > 100) {
      break;
    }

    uint64_t end_offset;
    ssize_t log_size = read_log_record_sequential(current_offset, log_buf,
                                                  sizeof(log_buf), &end_offset);

    if (log_size <= 0) {
      break;
    }

    base_log_t* base = (base_log_t*)log_buf;
    uint32_t txn_id = base->txn_id;
    uint64_t lsn = base->lsn;
    uint32_t type = base->type;

    // update max LSN
    if (lsn > log_mgr.recovery.max_lsn) {
      log_mgr.recovery.max_lsn = lsn;
    }

    // update ATT (Active Transaction Table): store last lsn of txn
    log_mgr.recovery.active_txn_table[txn_id] = lsn;

    switch (base->type) {
      case LOG_BEGIN:
        active_txns.insert(txn_id);
        break;

      case LOG_COMMIT:
      case LOG_ROLLBACK:
        active_txns.erase(txn_id);
        log_mgr.recovery.winners.insert(txn_id);
        break;

      case LOG_UPDATE: {
        update_log_t* upd = (update_log_t*)log_buf;
        if (log_mgr.recovery.winners.find(txn_id) ==
            log_mgr.recovery.winners.end()) {
          active_txns.insert(txn_id);
        }

        uint32_t tid = upd->table_id;
        uint64_t pnum = upd->page_num;
        auto page_key = std::make_pair(tid, pnum);

        if (log_mgr.recovery.dirty_page_table.find(page_key) ==
            log_mgr.recovery.dirty_page_table.end()) {
          log_mgr.recovery.dirty_page_table[page_key] = lsn;
        }
        break;
      }

      case LOG_COMPENSATE: {
        compensate_log_t* clr = (compensate_log_t*)log_buf;

        if (log_mgr.recovery.winners.find(txn_id) ==
            log_mgr.recovery.winners.end()) {
          active_txns.insert(txn_id);
        }

        uint32_t tid = clr->table_id;
        uint64_t pnum = clr->page_num;
        auto page_key = std::make_pair(tid, pnum);

        if (log_mgr.recovery.dirty_page_table.find(page_key) ==
            log_mgr.recovery.dirty_page_table.end()) {
          log_mgr.recovery.dirty_page_table[page_key] = lsn;
        }
        break;
      }

      default:
        break;
    }

    current_offset = end_offset;
  }

  // Losers = still active transactions
  log_mgr.recovery.losers = active_txns;

  // Print results
  fprintf(log_mgr.logmsg_fp, "[ANALYSIS] Analysis success. Winner:");
  for (uint32_t winner : log_mgr.recovery.winners) {
    fprintf(log_mgr.logmsg_fp, " %u", winner);
  }
  fprintf(log_mgr.logmsg_fp, ", Loser:");
  for (uint32_t loser : log_mgr.recovery.losers) {
    fprintf(log_mgr.logmsg_fp, " %u", loser);
  }
  fprintf(log_mgr.logmsg_fp, "\n");
  fflush(log_mgr.logmsg_fp);
}

/**
 * find min recLSN of DPT(Dirty Page Table)
 * Redo Pass can start from this to get efficiency
 */
uint64_t get_min_recLSN_from_DPT() {
  if (log_mgr.recovery.dirty_page_table.empty()) {
    return 0;
  }
  uint64_t min_lsn = UINT64_MAX;
  for (auto const& [page_key, recLSN] : log_mgr.recovery.dirty_page_table) {
    if (recLSN < min_lsn) {
      min_lsn = recLSN;
    }
  }
  return min_lsn;
}

/**
 * Check the DPT to determine if Redo is required
 */
bool check_redo_requirement(uint64_t lsn, uint32_t table_id,
                            uint64_t page_num) {
  auto page_key = std::make_pair(table_id, page_num);

  // check DPT
  auto it = log_mgr.recovery.dirty_page_table.find(page_key);
  if (it == log_mgr.recovery.dirty_page_table.end()) {
    return false;
  }

  // check recLSN
  uint64_t recLSN = it->second;
  if (lsn < recLSN) {
    return false;
  }

  return true;
}

/**
 * read log record by LSN (end offset 방식)
 */
bool find_log_by_lsn(uint64_t target_lsn, uint32_t target_txn_id,
                     void* buffer) {
  if (target_lsn == 0) {
    return false;
  }

  uint32_t possible_sizes[] = {BASE_SIZE, UPDATE_SIZE, COMPENSATE_SIZE};
  for (uint32_t size : possible_sizes) {
    if (target_lsn < size) {
      continue;
    }

    uint64_t start_offset = target_lsn - size;

    uint32_t log_size_in_header;
    // log_size는 구조체의 마지막 4바이트
    // start_offset + size - 4 위치에서 읽기
    ssize_t ret = pread(log_mgr.log_fd, &log_size_in_header, sizeof(uint32_t),
                        start_offset + size - sizeof(uint32_t));

    if (ret != sizeof(uint32_t)) {
      continue;
    }

    if (log_size_in_header == size) {
      // 전체 로그 읽기
      ret = pread(log_mgr.log_fd, buffer, size, start_offset);

      if (ret == (ssize_t)size) {
        base_log_t* base = (base_log_t*)buffer;
        if (base->lsn == target_lsn && base->txn_id == target_txn_id) {
          return true;
        }
      }
    }
  }
  return false;
}

/**
 * append compensate log during recovery (without TCB)
 */
uint64_t log_append_compensate_recovery(uint32_t txn_id, int table_id,
                                        uint64_t page_num, uint32_t offset,
                                        uint32_t length, char* old_img,
                                        char* new_img, uint64_t next_undo_lsn) {
  pthread_mutex_lock(&log_mgr.log_latch);

  uint64_t prev_lsn = log_mgr.recovery.active_txn_table[txn_id];

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

  // update ATT
  log_mgr.recovery.active_txn_table[txn_id] = lsn;

  pthread_mutex_unlock(&log_mgr.log_latch);
  return lsn;
}

/**
 * append rollback log during recovery (without TCB)
 */
uint64_t log_append_rollback_recovery(uint32_t txn_id) {
  pthread_mutex_lock(&log_mgr.log_latch);

  uint64_t prev_lsn = log_mgr.recovery.active_txn_table[txn_id];

  base_log_t log;
  memset(&log, 0, sizeof(log));
  log.lsn = log_mgr.last_lsn + sizeof(base_log_t);
  log.prev_lsn = prev_lsn;
  log.txn_id = txn_id;
  log.type = LOG_ROLLBACK;
  log.log_size = sizeof(base_log_t);

  uint64_t lsn = append_to_buffer(&log, sizeof(base_log_t));

  log_mgr.recovery.active_txn_table[txn_id] = lsn;

  pthread_mutex_unlock(&log_mgr.log_latch);
  return lsn;
}

/**
 * REDO PASS
 * Page not in DPT → Skip (already flushed)
 * If in DPT → start redo from recLSN
 */
int log_pass_redo() {
  fprintf(log_mgr.logmsg_fp, "[REDO] Redo pass start\n");
  fflush(log_mgr.logmsg_fp);

  struct stat st;
  fstat(log_mgr.log_fd, &st);
  uint64_t file_size = st.st_size;

  char log_buf[LOG_BUFFER_SIZE];
  int redo_count = 0;

  // set start offset
  uint64_t redo_start = get_min_recLSN_from_DPT();
  if (redo_start == 0 || redo_start == UINT64_MAX) {
    redo_start = 0;
  }
  uint64_t current_offset = redo_start;

  // scan
  while (current_offset < file_size) {
    uint64_t end_offset;
    ssize_t log_size = read_log_record_sequential(current_offset, log_buf,
                                                  sizeof(log_buf), &end_offset);
    if (log_size <= 0) {
      break;
    }

    base_log_t* base = (base_log_t*)log_buf;
    uint64_t lsn = base->lsn;
    uint32_t txn_id = base->txn_id;

    switch (base->type) {
      case LOG_BEGIN:
        fprintf(log_mgr.logmsg_fp, "LSN %lu [BEGIN] Transaction id %u\n", lsn,
                txn_id);
        redo_count++;
        break;

      case LOG_COMMIT:
        fprintf(log_mgr.logmsg_fp, "LSN %lu [COMMIT] Transaction id %u\n", lsn,
                txn_id);
        redo_count++;
        break;

      case LOG_ROLLBACK:
        fprintf(log_mgr.logmsg_fp, "LSN %lu [ROLLBACK] Transaction id %u\n",
                lsn, txn_id);
        redo_count++;
        break;

      case LOG_UPDATE: {
        update_log_t* upd = (update_log_t*)log_buf;

        if (check_redo_requirement(lsn, upd->table_id, upd->page_num)) {
          int fd = get_fd(upd->table_id);

          if (fd == FD_FAIL || fd < 0) {
            char table_path[32];
            sprintf(table_path, "DATA%u.db", upd->table_id);
            int table_id = open_table(table_path);

            if (table_id < 0) {
              redo_count++;
              break;
            }
            fd = get_fd(upd->table_id);
            if (fd < 0) {
              redo_count++;
              break;
            }
          }

          buf_ctl_block_t* bcb =
              read_buffer_with_txn(fd, upd->table_id, upd->page_num);
          if (bcb != nullptr) {
            page_t* page_ptr = (page_t*)bcb->frame;
            uint64_t page_lsn = *(uint64_t*)((char*)page_ptr + 24);

            if (page_lsn >= lsn) {
              fprintf(log_mgr.logmsg_fp,
                      "LSN %lu [CONSIDER-REDO] Transaction id %u\n", lsn,
                      txn_id);
            } else {
              memcpy((char*)page_ptr + upd->offset, upd->new_image,
                     upd->data_length);
              *(uint64_t*)((char*)page_ptr + PAGE_LSN_OFFSET) = lsn;
              bcb->is_dirty = true;
              fprintf(log_mgr.logmsg_fp,
                      "LSN %lu [UPDATE] Transaction id %u redo apply\n", lsn,
                      txn_id);
            }
            unlock_and_unpin_bcb(bcb);
          }
        }
        redo_count++;
        break;
      }

      case LOG_COMPENSATE: {
        compensate_log_t* clr = (compensate_log_t*)log_buf;

        if (check_redo_requirement(lsn, clr->table_id, clr->page_num)) {
          int fd = get_fd(clr->table_id);

          if (fd == FD_FAIL || fd < 0) {
            char table_path[32];
            sprintf(table_path, "DATA%u.db", clr->table_id);
            int table_id = open_table(table_path);

            if (table_id < 0) {
              redo_count++;
              break;
            }

            fd = get_fd(clr->table_id);
            if (fd < 0) {
              redo_count++;
              break;
            }
          }

          buf_ctl_block_t* bcb =
              read_buffer_with_txn(fd, clr->table_id, clr->page_num);
          if (bcb != nullptr) {
            page_t* page_ptr = (page_t*)bcb->frame;
            uint64_t page_lsn = *(uint64_t*)((char*)page_ptr + 24);

            if (page_lsn < lsn) {
              memcpy((char*)page_ptr + clr->offset, clr->new_image,
                     clr->data_length);
              *(uint64_t*)((char*)page_ptr + PAGE_LSN_OFFSET) = lsn;
              bcb->is_dirty = true;

              fprintf(log_mgr.logmsg_fp, "LSN %lu [CLR] next undo lsn %lu\n",
                      lsn, clr->next_undo_lsn);
              fflush(log_mgr.logmsg_fp);
            } else {
              fprintf(log_mgr.logmsg_fp,
                      "LSN %lu [CONSIDER-REDO] Transaction id %u\n", lsn,
                      txn_id);
            }
            unlock_and_unpin_bcb(bcb);
          }
        }
        fflush(log_mgr.logmsg_fp);
        redo_count++;
        break;
      }
    }

    // REDO crash simulation
    if (log_mgr.recovery_flag == 1 &&
        redo_count >= log_mgr.recovery.log_count) {
      fprintf(log_mgr.logmsg_fp,
              "[REDO] Redo pass interrupted (crash simulation)\n");
      fflush(log_mgr.logmsg_fp);
      log_force_flush();
      flush_all_page_buffer();
      return CRASH;
    }

    current_offset = end_offset;
  }

  fprintf(log_mgr.logmsg_fp, "[REDO] Redo pass end\n");
  fflush(log_mgr.logmsg_fp);
  return SUCCESS;
}

/**
 * UNDO PASS (with detailed debug)
 */
int log_pass_undo() {
  fprintf(log_mgr.logmsg_fp, "[UNDO] Undo pass start\n");
  fflush(log_mgr.logmsg_fp);

  if (log_mgr.recovery.losers.empty()) {
    fprintf(log_mgr.logmsg_fp, "[UNDO] Undo pass end\n");
    fflush(log_mgr.logmsg_fp);
    return SUCCESS;
  }

  std::priority_queue<std::pair<uint64_t, txnid_t>> undo_pq;
  for (txnid_t loser : log_mgr.recovery.losers) {
    uint64_t last_lsn = log_mgr.recovery.active_txn_table[loser];
    if (last_lsn > 0) {
      undo_pq.push({last_lsn, loser});
    }
  }

  char log_buf[LOG_BUFFER_SIZE];
  int undo_count = 0;

  while (!undo_pq.empty()) {
    auto [lsn, txn_id] = undo_pq.top();
    undo_pq.pop();

    // 디스크에서 로그 레코드 읽기
    if (!find_log_by_lsn(lsn, txn_id, log_buf)) {
      continue;
    }

    base_log_t* base = (base_log_t*)log_buf;

    switch (base->type) {
      case LOG_UPDATE: {
        update_log_t* upd = (update_log_t*)log_buf;

        int fd = get_fd(upd->table_id);

        // 테이블이 닫혀있으면 열기
        if (fd == FD_FAIL || fd < 0) {
          char table_path[32];
          sprintf(table_path, "DATA%u.db", upd->table_id);

          int table_id = open_table(table_path);

          if (table_id < 0) {
            uint64_t next_undo_lsn = upd->prev_lsn;
            if (next_undo_lsn > 0) {
              undo_pq.push({next_undo_lsn, txn_id});
            }
            break;
          }

          // 다시 fd를 가져옴
          fd = get_fd(upd->table_id);
          if (fd < 0) {
            uint64_t next_undo_lsn = upd->prev_lsn;
            if (next_undo_lsn > 0) {
              undo_pq.push({next_undo_lsn, txn_id});
            }
            break;
          }
        }

        // 이제 fd가 유효함
        buf_ctl_block_t* bcb =
            read_buffer_with_txn(fd, upd->table_id, upd->page_num);

        if (bcb != nullptr) {
          page_t* page_ptr = (page_t*)bcb->frame;

          // apply old_image (New -> Old)
          memcpy((char*)page_ptr + upd->offset, upd->old_image,
                 upd->data_length);

          // write CLR
          uint64_t next_undo_lsn = upd->prev_lsn;
          uint64_t clr_lsn = log_append_compensate_recovery(
              txn_id, upd->table_id, upd->page_num, upd->offset,
              upd->data_length, upd->new_image, upd->old_image, next_undo_lsn);

          // update pageLSN to CLR's LSN
          *(uint64_t*)((char*)page_ptr + PAGE_LSN_OFFSET) = clr_lsn;
          bcb->is_dirty = true;

          unlock_and_unpin_bcb(bcb);
        }

        fprintf(log_mgr.logmsg_fp,
                "LSN %lu [UPDATE] Transaction id %u undo apply\n", lsn, txn_id);
        fflush(log_mgr.logmsg_fp);

        undo_count++;

        // UNDO CRASH simulation
        if (log_mgr.recovery_flag == 2 &&
            undo_count >= log_mgr.recovery.log_count) {
          fprintf(log_mgr.logmsg_fp,
                  "[UNDO] Undo pass interrupted (crash simulation)\n");
          fflush(log_mgr.logmsg_fp);
          log_force_flush();
          flush_all_page_buffer();
          return CRASH;
        }

        // 다음 undo 지점을 priority queue에 추가
        uint64_t next_undo_lsn = upd->prev_lsn;
        if (next_undo_lsn > 0) {
          undo_pq.push({next_undo_lsn, txn_id});
        }
        break;
      }

      case LOG_COMPENSATE: {
        compensate_log_t* clr = (compensate_log_t*)log_buf;

        // CLR is result of already UNDO work prev crash
        // skip UNDO process
        // just add next_undo_lsn
        uint64_t next_undo_lsn = clr->next_undo_lsn;
        if (next_undo_lsn > 0) {
          undo_pq.push({next_undo_lsn, txn_id});
        }
        break;
      }

      case LOG_BEGIN: {
        // all works(undo) done for loser txn so write rollback log
        uint64_t rollback_lsn = log_append_rollback_recovery(txn_id);
        fprintf(log_mgr.logmsg_fp, "LSN %lu [ROLLBACK] Transaction id %u\n",
                rollback_lsn, txn_id);
        fflush(log_mgr.logmsg_fp);
        break;
      }

      // skip COMMIT, ROLLBACK
      case LOG_COMMIT:
        break;
      case LOG_ROLLBACK:
        break;

      default:
        break;
    }
  }

  // recovery clear
  log_force_flush();
  struct stat st;
  fstat(log_mgr.log_fd, &st);

  flush_all_page_buffer();

  fprintf(log_mgr.logmsg_fp, "[UNDO] Undo pass end\n");
  fflush(log_mgr.logmsg_fp);
  return SUCCESS;
}

/**
 * Main recovery entry point
 * 3-Pass with ARIES
 * flush all buffers(log, page)
 */
int log_recovery(int flag, int log_count) {
  log_mgr.recovery_flag = flag;
  log_mgr.recovery.log_count = log_count;

  log_pass_analysis();
  if (log_pass_redo() == CRASH) {
    return CRASH;
  }
  if (log_pass_undo() == CRASH) {
    return CRASH;
  }

  return SUCCESS;
}
