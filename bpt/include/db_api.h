#ifndef DB_API_H
#define DB_API_H

#include "bpt.h"
#include "common_config.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <unordered_map>

#define PATH_NAME_MAX_LENGTH 20

typedef struct {
  char path[PATH_NAME_MAX_LENGTH + 1];
  int fd;
} table_info_t;

extern table_info_t table_infos[MAX_TABLE_COUNT + 1];
std::unordered_map<std::string, tableid_t> path_table_mapper;

int init_db(int buf_num);
int open_table(char *pathname);
int db_insert(int table_id, int64_t key, char *value);
int db_find(int table_id, int64_t key, char *ret_val);
int db_delete(int table_id, int64_t key);
int close_table(int table_id);
int shutdown_db(void);
void db_print_tree(tableid_t table_id);
void db_print_leaves(tableid_t table_id);
int db_find_and_print_range(tableid_t table_id, int64_t key_start,
                            int64_t key_end);

#endif