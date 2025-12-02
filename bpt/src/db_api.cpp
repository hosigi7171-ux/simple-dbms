#include "db_api.h"

#include "bpt.h"
#include "buf_mgr.h"

table_info_t table_infos[MAX_TABLE_COUNT + 1] = {0};
std::unordered_map<std::string, tableid_t> path_table_mapper;

int get_fd(tableid_t table_id) { return table_infos[table_id].fd; }

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
 */
int init_db(int buf_num) {
  if (buf_num < 0) {
    return FAILURE;
  }

  return init_buffer_manager(buf_num);
}

/**
 * helper function for open_table
 * find free table id otherwise return FAILURE
 */
int find_free_table() {
  tableid_t table_id = -1;
  for (int index = 1; index <= MAX_TABLE_COUNT; index++) {
    if (table_infos[index].fd == 0) {
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
 * @brief Open existing data file using ‘pathname’ or create one if not existed
 * must give the same table id when db opens the same table
 • If success, return the table id,
 * which represents the own table in this database.
 * Otherwise, return negative value
 */
int open_table(char* pathname) {
  if (strlen(pathname) > PATH_NAME_MAX_LENGTH) {
    return FAILURE;
  }

  // Case: already opened
  if (path_table_mapper.count(pathname)) {
    return path_table_mapper[pathname];
  }

  // Case: open new one
  tableid_t table_id = find_free_table();
  if (table_id == FAILURE) {
    return FAILURE;
  }
  mode_t mode = 0644;
  int fd = open(pathname, O_RDWR | O_CREAT, mode);
  if (fd == -1) {
    return FAILURE;
  }

  // setup metadata (header_page)
  struct stat stat_buf;
  if (fstat(fd, &stat_buf) == -1) {
    return FAILURE;
  }
  if (stat_buf.st_size == 0) {
    init_header_page(get_fd(table_id), table_id);
  }

  // set table infos and mapper
  table_infos[table_id].fd = fd;
  strncpy(table_infos[table_id].path, pathname, PATH_NAME_MAX_LENGTH);
  table_infos[table_id].path[PATH_NAME_MAX_LENGTH] = '\0';

  path_table_mapper[pathname] = table_id;

  return table_id;
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
  if (table_infos[table_id].fd < 0) {
    printf("tabe not open\n");
    return SUCCESS;
  }

  int result = SUCCESS;

  if (close(table_infos[table_id].fd) == -1) {
    perror("cannot close fd");
    result = FAILURE;
  }

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
  for (int table_id = 1; table_id <= MAX_TABLE_COUNT; table_id++) {
    int fd = table_infos[table_id].fd;
    if (fd > 0) {
      flush_table_buffer(fd, table_id);
    }
  }

  if (buf_mgr.frames != NULL) {
    free_buffer_manager(buf_mgr.frames_size);
  }

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
