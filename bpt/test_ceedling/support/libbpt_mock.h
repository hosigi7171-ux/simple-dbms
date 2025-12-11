// just for testing libbpt.a
#ifndef LIBBPT_MOCK_H
#define LIBBPT_MOCK_H

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

extern int global_table_id; // for future extension

int open_table(char *pathname);
int db_insert(int64_t key, char *value);
int db_find(int64_t key, char *ret_val);
int db_delete(int64_t key);

int close_table(void);
void db_print_tree(void);
void db_print_leaves(void);
int db_find_and_print_range(int64_t key_start, int64_t key_end);

#endif