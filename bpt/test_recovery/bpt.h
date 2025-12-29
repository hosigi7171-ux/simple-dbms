#ifndef __BPT_H_
#define __BPT_H_

#include <stdint.h>

int init_db(int buf_num, int flag, int log_num, char* log_path,
            char* logmsg_path);
int open_table(char* pathname);
int db_insert(int table_id, int64_t key, char* value);
int db_find(int table_id, int64_t key, char* ret_val, int txn_id);
int db_delete(int table_id, int64_t key);
int db_update(int table_id, int64_t key, char* value, int txn_id);
int close_table(int table_id);
int shutdown_db(void);
int txn_begin(void);
int txn_commit(uint32_t txn_id);
int txn_abort(uint32_t txn_id);
void log_force_flush();

#endif
