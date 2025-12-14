#ifndef SIMPLE_DBMS_INCLUDE_DEADLOCK_H_
#define SIMPLE_DBMS_INCLUDE_DEADLOCK_H_

#include "lock_table.h"
#include "txn_mgr.h"

std::vector<txnid_t> find_cycle_from(txnid_t txn_id);
txnid_t select_victim_from_cycle(const std::vector<txnid_t>& cycle);

#endif
