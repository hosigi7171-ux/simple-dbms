#ifndef DB_API_H
#define DB_API_H

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <string>
#include <unordered_map>

#include "common_config.h"

#define PATH_NAME_MAX_LENGTH 20

typedef struct {
  char path[PATH_NAME_MAX_LENGTH + 1];
  int fd;
} table_info_t;

extern table_info_t table_infos[MAX_TABLE_COUNT + 1];
extern std::unordered_map<std::string, tableid_t> path_table_mapper;

int init_db(int buf_num);
int init_db(int buf_num, int flag, int log_num, char* log_path,
            char* logmsg_path);
int open_table(char* pathname);
int db_insert(tableid_t table_id, int64_t key, char* value);
int db_find(int table_id, int64_t key, char* ret_val);
int db_find(tableid_t table_id, int64_t key, char* ret_val, int txn_id);
int db_update(int table_id, int64_t key, char* values, int txn_id);
int db_delete(tableid_t table_id, int64_t key);
int close_table(tableid_t table_id);
int shutdown_db(void);
void db_print_tree(tableid_t table_id);
void db_print_leaves(tableid_t table_id);
int db_find_and_print_range(tableid_t table_id, int64_t key_start,
                            int64_t key_end);
int get_fd(tableid_t table_id);

#endif