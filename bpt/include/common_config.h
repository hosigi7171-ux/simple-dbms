#ifndef COMMON_CONFIG_H
#define COMMON_CONFIG_H

#include "stdint.h"

#define MAX_TABLE_COUNT (10)
#define SUCCESS (0)
#define FAILURE (-1)

#ifndef VALUE_SIZE
#define VALUE_SIZE (120)
#endif
#define FD_FAIL (-1)

typedef int frame_idx_t;
typedef int tableid_t;
typedef uint64_t pagenum_t;
typedef int tableid_t;
typedef int64_t recordid_t;
typedef uint32_t txnid_t;

enum LockMode { S_LOCK = 0, X_LOCK = 1 };
enum LockState { ACQUIRED = 0, NEED_TO_WAIT = 1, DEADLOCK = 2 };

#endif