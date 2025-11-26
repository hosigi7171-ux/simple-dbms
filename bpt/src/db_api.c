#include "db_api.h"
#include "bpt.h"

extern int fd;
int global_table_id = -1;

/**
 * @brief Open existing data file using ‘pathname’ or create one if not existed
 • If success, return the table id,
 * which represents the own table in this database.
 * Otherwise, return negative value
 */
int open_table(char *pathname) {
  mode_t mode = 0644;
  if ((fd = open(pathname, O_RDWR | O_CREAT, mode)) == -1) {
    return FAILURE;
  }

  // setup metadata (header_page)
  struct stat stat_buf;
  if (fstat(fd, &stat_buf) == -1) {
    return FAILURE;
  }
  if (stat_buf.st_size == 0) {
    init_header_page();
  }

  global_table_id = 0;
  return global_table_id; // 추후에 table_id는 구현할 기능
}

/**
 * @brief  Insert input ‘key/value’ (record) to data file at the right place.
 * If success, return 0
 * Otherwise, return non-zero value
 */
int db_insert(int64_t key, char *value) {
  int result = insert(key, value);
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
int db_find(int64_t key, char *ret_val) {
  if (find(key, ret_val) == SUCCESS) {
    return SUCCESS;
  }
  return FAILURE;
}

/**
 * @brief Find the matching record and delete it if found
 * If success, return 0. Otherwise, return non-zero value
 */
int db_delete(int64_t key) {
  int result = delete (key);
  if (result == SUCCESS) {
    return SUCCESS;
  }
  return FAILURE;
}

/**
 * NOT NECESSARY-------------------
 */

void db_print_tree(void) {
  if (global_table_id < 0) {
    printf("table not open\n");
    return;
  }
  print_tree();
}

void db_print_leaves(void) {
  if (global_table_id < 0) {
    printf("table not open\n");
    return;
  }
  print_leaves();
}

int db_find_and_print_range(int64_t key_start, int64_t key_end) {
  if (global_table_id < 0) {
    printf("table not open\n");
    return FAILURE;
  }
  if (find_and_print_range(key_start, key_end) != SUCCESS) {
    return FAILURE;
  };
}

int close_table(void) {
  if (global_table_id < 0) {
    printf("tabe not open\n");
    return SUCCESS;
  }

  int result = SUCCESS;

  if (close(fd) == -1) {
    perror("cannot close fd");
    result = FAILURE;
  }

  global_table_id = -1;

  printf("table closed\n");
  return result;
}
