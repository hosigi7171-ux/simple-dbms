#ifndef SIMPLE_DBMS_INCLUDE_LOCK_TABLE_H_
#define SIMPLE_DBMS_INCLUDE_LOCK_TABLE_H_

void init_lock_table(void);
void lock_acquire(void);
void lock_release(void);

#endif