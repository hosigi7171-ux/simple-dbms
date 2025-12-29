#include "db_api.h"

#include "bpt.h"
#include "buf_mgr.h"
#include "lock_table.h"
#include "log.h"
#include "txn_mgr.h"

table_info_t table_infos[MAX_TABLE_COUNT + 1] = {0};
std::unordered_map<std::string, tableid_t> path_table_mapper;

int get_fd(tableid_t table_id) {
  if (table_id < 1 || table_id > MAX_TABLE_COUNT) {
    return -1;
  }
  int fd = table_infos[table_id].fd;
  if (fd <= 0) {
    return -1;
  }
  return table_infos[table_id].fd;
}

/**
 * helpeer function for init_buffer_manager and shutdown_db
 * 메모리 할당 실패시 end전까지 frames를 free해줌
 */
void free_buffer_manager(int end) {
  for (int index = 0; index < end; index++) {
    free(buf_mgr.frames[index].frame);
  }
  free(buf_mgr.frames);
}

void init_table_infos() {
  for (int index = 1; index <= MAX_TABLE_COUNT; index++) {
    table_infos[index].fd = -1;
  }
}

/**
 * helper function for init_db
 */
int init_buffer_manager(int buf_num) {
  buf_mgr.frames = (buf_ctl_block_t*)malloc(buf_num * sizeof(buf_ctl_block_t));
  if (buf_mgr.frames == NULL) {
    return FAILURE;
  }
  buf_mgr.frames_size = buf_num;
  buf_mgr.clock_hand = 0;

  // init buf ctl blocks array
  for (int index = 0; index < buf_num; index++) {
    memset(&buf_mgr.frames[index], 0, sizeof(buf_ctl_block_t));

    // 페이지 래치 초기화
    if (pthread_mutex_init(&buf_mgr.frames[index].page_latch, NULL) != 0) {
      free_buffer_manager(index);
      return FAILURE;
    }

    buf_mgr.frames[index].frame = malloc(PAGE_SIZE);
    if (buf_mgr.frames[index].frame == NULL) {
      free_buffer_manager(index);
      return FAILURE;
    }
  }
  return SUCCESS;
}

/**
 * @brief Initialize buffer pool with given number and buffer manager
 * If success, return 0. Otherwise, return non-zero value
 * Allowed for compatibility with previous projects
 */
int init_db(int buf_num) {
  if (buf_num < 0) {
    return FAILURE;
  }

  init_table_infos();
  init_txn_table();
  return init_buffer_manager(buf_num);
}

/**
 * @brief Initialize DBMS and execute recovery process
 * If success, return 0. Otherwise, return non-zero value
 */
int init_db(int buf_num, int flag, int log_num, char* log_path,
            char* logmsg_path) {
  if (buf_num < 0) {
    return FAILURE;
  }
  init_table_infos();
  if (init_buffer_manager(buf_num) != SUCCESS) {
    return FAILURE;
  }
  init_txn_table();
  if (log_init(log_path, logmsg_path) != SUCCESS) {
    return FAILURE;
  }
  int recovery_result = log_recovery(flag, log_num);
  if (recovery_result == CRASH) {
    return SUCCESS;  // intended crash is also success
  }
  return SUCCESS;
}

/**
 * helper function for open_table
 * find free table id otherwise return FAILURE
 */
int find_free_table() {
  tableid_t table_id = -1;
  for (int index = 1; index <= MAX_TABLE_COUNT; index++) {
    // fd가 -1이고 path도 비어있는 슬롯만 free
    if (table_infos[index].fd == -1 && strlen(table_infos[index].path) == 0) {
      table_id = index;
      break;
    }
  }
  if (table_id == -1) {
    return FAILURE;
  }
  return table_id;
}

/**
 * @brief Extract table id from filename "DATA[NUM]"
 * Example: "DATA1" -> 1, "DATA3" -> 3
 * @return table id (1~10), or -1 on error
 */
int extract_table_id_from_path(const char* pathname) {
  // Find last '/' or '\' to get filename only
  const char* filename = strrchr(pathname, '/');
  if (filename == nullptr) {
    filename = strrchr(pathname, '\\');
  }
  if (filename != nullptr) {
    filename++;  // skip the '/' or '\'
  } else {
    filename = pathname;
  }

  // Check if filename starts with "DATA"
  if (strncmp(filename, "DATA", 4) != 0) {
    return -1;
  }

  // extract number after "DATA"
  const char* num_str = filename + 4;
  if (*num_str == '\0') return -1;
  char* endptr;
  long num = strtol(num_str, &endptr, 10);

  // validation check
  if (num_str == endptr || (*endptr != '\0' && *endptr != '.')) {
    return -1;
  }

  // range check
  if (num < 1 || num > 10) {
    return -1;
  }

  return (int)num;
}

/**
 * @brief Open existing data file using 'pathname' or create one if not existed
 * Filename must be in format "DATA[NUM]" where NUM is 1~10
 * The table id will be NUM extracted from the filename
 *
 * @param pathname Path to the data file (e.g., "DATA1", "./DATA3",
 * "/path/to/DATA5")
 * @return table id (1~10) on success, negative value on failure
 */
int open_table(char* pathname) {
  if (strlen(pathname) > PATH_NAME_MAX_LENGTH) {
    return FAILURE;
  }

  // Extract table_id from filename (must be "DATA[NUM]" format)
  int expected_table_id = extract_table_id_from_path(pathname);
  if (expected_table_id < 1 || expected_table_id > 10) {
    fprintf(stderr,
            "Error: Invalid filename format. Must be DATA[1-10]!! this is %d\n",
            expected_table_id);
    return FAILURE;
  }
  // Check if this table_id is already open
  if (table_infos[expected_table_id].fd > 0) {
    if (strcmp(table_infos[expected_table_id].path, pathname) == 0) {
      return expected_table_id;
    } else {
      fprintf(stderr, "Error: Table id %d already open with different path\n",
              expected_table_id);
      return FAILURE;
    }
  }

  // Check if this pathname was opened before
  if (path_table_mapper.count(pathname)) {
    tableid_t mapped_table_id = path_table_mapper[pathname];

    if (mapped_table_id != expected_table_id) {
      fprintf(stderr, "Error: Table id mismatch for %s\n", pathname);
      return FAILURE;
    }

    // 닫혀있으면 재오픈 (같은 table_id 재사용)
    mode_t mode = 0644;
    int fd = open(pathname, O_RDWR | O_CREAT, mode);
    if (fd == -1) {
      return FAILURE;
    }

    table_infos[expected_table_id].fd = fd;

    return expected_table_id;
  }

  // Verify this table slot is free
  if (table_infos[expected_table_id].fd > 0) {
    if (strcmp(table_infos[expected_table_id].path, pathname) != 0) {
      fprintf(stderr, "Error: Table id %d is already used by %s\n",
              expected_table_id, table_infos[expected_table_id].path);
      return FAILURE;
    }
  }
  mode_t mode = 0644;
  int fd = open(pathname, O_RDWR | O_CREAT, mode);
  if (fd == -1) {
    return FAILURE;
  }

  // register in table_infos and path_table_mapper
  table_infos[expected_table_id].fd = fd;
  strncpy(table_infos[expected_table_id].path, pathname, PATH_NAME_MAX_LENGTH);
  table_infos[expected_table_id].path[PATH_NAME_MAX_LENGTH] = '\0';
  path_table_mapper[pathname] = expected_table_id;

  // Setup metadata
  struct stat stat_buf;
  if (fstat(fd, &stat_buf) == -1) {
    close(fd);
    table_infos[expected_table_id].fd = 0;
    path_table_mapper.erase(pathname);
    return FAILURE;
  }
  if (stat_buf.st_size == 0) {
    init_header_page(fd, expected_table_id);
  }

  return expected_table_id;
}

/**
 * @brief  Insert input ‘key/value’ (record) to data file at the right place.
 * If success, return 0
 * Otherwise, return non-zero value
 */
int db_insert(int table_id, int64_t key, char* value) {
  int result = bpt_insert(get_fd(table_id), table_id, key, value);
  if (result == SUCCESS) {
    return SUCCESS;
  }
  return result;
}

/**
 * @brief Find the record containing input key
 * If found matching ‘key’, store matched ‘value’ string in ret_val and return 0
 * Otherwise, return non zero value
 * Memory allocation for ret_val should occur in caller
 */
int db_find(int table_id, int64_t key, char* ret_val) {
  if (find(get_fd(table_id), table_id, key, ret_val) == SUCCESS) {
    return SUCCESS;
  }
  return FAILURE;
}

/**
 * db_find concurrency control version
 */
int db_find(int table_id, int64_t key, char* ret_val, int txn_id) {
  int fd = get_fd(table_id);
  if (fd < 0) {
    txn_abort(txn_id);
    return FAILURE;
  }

  pthread_mutex_lock(&txn_table.latch);
  auto it = txn_table.transactions.find(txn_id);
  if (it == txn_table.transactions.end()) {
    pthread_mutex_unlock(&txn_table.latch);
    return FAILURE;
  }
  tcb_t* tcb = it->second;

  pthread_mutex_lock(&tcb->latch);
  txn_state_t current_state = tcb->state;
  pthread_mutex_unlock(&tcb->latch);
  pthread_mutex_unlock(&txn_table.latch);

  if (current_state != TXN_ACTIVE) {
    return FAILURE;
  }

  int result = find_with_txn(fd, table_id, key, ret_val, txn_id, tcb);

  if (result == FAILURE) {
    txn_abort(txn_id);
    return FAILURE;
  }

  return SUCCESS;
}

/**
 * db_update concurrency control version
 */
int db_update(int table_id, int64_t key, char* values, int txn_id) {
  int fd = get_fd(table_id);
  if (fd < 0) {
    txn_abort(txn_id);
    return FAILURE;
  }

  pthread_mutex_lock(&txn_table.latch);
  auto it = txn_table.transactions.find(txn_id);
  if (it == txn_table.transactions.end()) {
    pthread_mutex_unlock(&txn_table.latch);
    return FAILURE;
  }
  tcb_t* tcb = it->second;

  pthread_mutex_lock(&tcb->latch);
  txn_state_t current_state = tcb->state;
  pthread_mutex_unlock(&tcb->latch);
  pthread_mutex_unlock(&txn_table.latch);

  if (current_state != TXN_ACTIVE) {
    return FAILURE;
  }

  int result = update_with_txn(fd, table_id, key, values, txn_id, tcb);

  if (result == FAILURE) {
    txn_abort(txn_id);
    return FAILURE;
  }

  return SUCCESS;
}

/**
 * @brief Find the matching record and delete it if found
 * If success, return 0. Otherwise, return non-zero value
 */
int db_delete(int table_id, int64_t key) {
  int result = bpt_delete(get_fd(table_id), table_id, key);
  if (result == SUCCESS) {
    return SUCCESS;
  }
  return FAILURE;
}

int close_table(int table_id) {
  if (table_id < 1 || table_id > MAX_TABLE_COUNT) {
    printf("invalid table_id\n");
    return FAILURE;
  }

  if (table_infos[table_id].fd <= 0) {
    printf("table not open\n");
    return SUCCESS;
  }

  flush_table_buffer(get_fd(table_id), table_id);
  int result = SUCCESS;
  if (close(table_infos[table_id].fd) == -1) {
    perror("cannot close fd");
    result = FAILURE;
  }
  table_infos[table_id].fd = -1;

  printf("table closed\n");
  return result;
}

/**
 * helper function for shutdown_db
 */
void clear_path_table_mapper() {
  for (int i = 0; i <= MAX_TABLE_COUNT; ++i) {
    buf_mgr.page_table[i].clear();
  }
  path_table_mapper.clear();
  memset(table_infos, 0, sizeof(table_infos));

  buf_mgr.frames = NULL;
  buf_mgr.frames_size = 0;
  buf_mgr.clock_hand = 0;
}

/**
 * @brief Flush all data from buffer and destroy allocated buffer
 • If success, return 0. Otherwise, return non-zero value
 */
int shutdown_db(void) {
  log_force_flush();

  flush_all_page_buffer();

  if (buf_mgr.frames != NULL) {
    free_buffer_manager(buf_mgr.frames_size);
  }

  destroy_txn_table();

  log_close();

  clear_path_table_mapper();

  return SUCCESS;
}

/**
 * NOT NECESSARY-------------------
 */

// will implement later

void db_print_tree(tableid_t table_id) {
  if (table_id < 0) {
    printf("table not open\n");
    return;
  }
  print_tree(get_fd(table_id), table_id);
}

void db_print_leaves(tableid_t table_id) {
  if (table_id < 0) {
    printf("table not open\n");
    return;
  }
  print_leaves(get_fd(table_id), table_id);
}

int db_find_and_print_range(tableid_t table_id, int64_t key_start,
                            int64_t key_end) {
  if (table_id < 0) {
    printf("table not open\n");
    return FAILURE;
  }
  if (find_and_print_range(get_fd(table_id), table_id, key_start, key_end) !=
      SUCCESS) {
    return FAILURE;
  }
  return SUCCESS;
}
