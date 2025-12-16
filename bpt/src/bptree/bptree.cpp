#include "bpt.h"
#include "bpt_internal.h"
#include "buf_mgr.h"
#include "file.h"
#include "lock_table.h"
#include "txn_mgr.h"

/* Finds and returns success(0) or fail(-1)
 */
int find(int fd, tableid_t table_id, int64_t key, char* result_buf) {
  pagenum_t leaf_num = find_leaf(fd, table_id, key);
  if (leaf_num == PAGE_NULL) {
    return FAILURE;
  }

  // leaf_page 에서 키에 해당하는 값 찾기
  leaf_page_t* leaf_page = (leaf_page_t*)read_buffer(fd, table_id, leaf_num);

  int index = 0;

  for (index = 0; index < leaf_page->num_of_keys; index++) {
    if (leaf_page->records[index].key == key) {
      break;
    }
  }
  // 해당하는 키를 찾았으면
  if (index != leaf_page->num_of_keys) {
    copy_value(result_buf, leaf_page->records[index].value, VALUE_SIZE);

    unpin(table_id, leaf_num);
    return SUCCESS;
  }

  unpin(table_id, leaf_num);
  return FAILURE;
}

/**
 * @brief init header page
 * Case: there is no header page in disk
 * write header frame in buffer
 * must write header frame to disk later
 * must be used before insert
 */
void init_header_page(int fd, tableid_t table_id) {
  frame_idx_t header_frame_idx =
      find_free_frame_index(fd, table_id, HEADER_PAGE_POS);
  buf_ctl_block_t* bcb = &buf_mgr.frames[header_frame_idx];
  page_t* frame_ptr = (page_t*)bcb->frame;

  memset(frame_ptr, 0, PAGE_SIZE);

  header_page_t* header_page = (header_page_t*)frame_ptr;
  header_page->num_of_pages = HEADER_PAGE_POS + 1;

  buf_mgr.page_table[table_id].insert(
      std::make_pair(HEADER_PAGE_POS, header_frame_idx));
  write_buffer(table_id, HEADER_PAGE_POS, (page_t*)header_page);
}

/* Master insertion function.
 * Inserts a key and an associated value into
 * the B+ tree, causing the tree to be adjusted
 * however necessary to maintain the B+ tree
 * properties.
 */
int bpt_insert(int fd, tableid_t table_id, int64_t key, char* value) {
  pagenum_t leaf;

  char result_buf[VALUE_SIZE];
  if (find(fd, table_id, key, result_buf) == SUCCESS) {
    return FAILURE;
  }

  // Case: the tree does not exist yet. Start a new tree.
  header_page_t* header_page = (header_page_t*)read_header_page(fd, table_id);

  pagenum_t root_num = header_page->root_page_num;
  unpin(table_id, HEADER_PAGE_POS);
  if (root_num == PAGE_NULL) {
    return start_new_tree(fd, table_id, key, value);
  }

  // Case: the tree already exists.(Rest of function body.)
  leaf = find_leaf(fd, table_id, key);

  // Case: leaf has room for key and pointer.
  leaf_page_t* leaf_page = (leaf_page_t*)read_buffer(fd, table_id, leaf);

  if (leaf_page->num_of_keys < RECORD_CNT) {
    return insert_into_leaf(fd, table_id, leaf, leaf_page, key, value);
  }

  unpin(table_id, leaf);
  // Case:  leaf must be split.
  return insert_into_leaf_after_splitting(fd, table_id, leaf, key, value);
}

/* Master deletion function.
 */
int bpt_delete(int fd, tableid_t table_id, int64_t key) {
  pagenum_t leaf;

  char value_buf[VALUE_SIZE];
  // if not exists fail
  if (find(fd, table_id, key, value_buf) != SUCCESS) {
    return FAILURE;
  }

  leaf = find_leaf(fd, table_id, key);

  if (leaf != PAGE_NULL) {
    return delete_entry(fd, table_id, leaf, key, value_buf);
  }
  return FAILURE;
}

/**
 * update bptree value to input value
 */
int bpt_update(int fd, tableid_t table_id, int64_t key, char* new_value) {
  pagenum_t leaf = find_leaf(fd, table_id, key);
  if (leaf == PAGE_NULL) {
    return FAILURE;
  }

  leaf_page_t* leaf_page = (leaf_page_t*)read_buffer(fd, table_id, leaf);

  for (int i = 0; i < leaf_page->num_of_keys; i++) {
    if (leaf_page->records[i].key == key) {
      copy_value(leaf_page->records[i].value, new_value, VALUE_SIZE);
      mark_dirty(table_id, leaf);
      unpin(table_id, leaf);
      return SUCCESS;
    }
  }

  unpin(table_id, leaf);
  return FAILURE;
}

/**
 * find with concurrency control
 */
int find_with_txn(int fd, tableid_t table_id, int64_t key, char* ret_val,
                  int txn_id) {
  while (true) {
    // find leaf with page latch
    buf_ctl_block_t* leaf_bcb;
    pagenum_t leaf = find_leaf(fd, table_id, key, (void**)&leaf_bcb);

    if (leaf == PAGE_NULL) {
      return FAILURE;
    }

    leaf_page_t* leaf_page = (leaf_page_t*)leaf_bcb->frame;

    // find record
    int found_idx = -1;
    for (int i = 0; i < leaf_page->num_of_keys; i++) {
      if (leaf_page->records[i].key == key) {
        found_idx = i;
        break;
      }
    }

    if (found_idx == -1) {
      unpin_bcb(leaf_bcb);
      pthread_mutex_unlock(&leaf_bcb->page_latch);
      return FAILURE;
    }

    // read value
    copy_value(ret_val, leaf_page->records[found_idx].value, VALUE_SIZE);

    // acquire lock while holding page latch
    lock_t* lock = nullptr;
    LockState lock_result = lock_acquire(table_id, key, txn_id, S_LOCK, &lock);

    if (lock_result == ACQUIRED) {
      // lock acquired immediately
      unpin_bcb(leaf_bcb);
      pthread_mutex_unlock(&leaf_bcb->page_latch);

      // link lock to transaction
      tcb_t* tcb;
      if (acquire_txn_latch(txn_id, &tcb) == SUCCESS) {
        link_lock_to_txn(tcb, lock);
        release_txn_latch(tcb);
      }

      return SUCCESS;
    } else if (lock_result == NEED_TO_WAIT) {
      // must wait, release page latch
      unpin_bcb(leaf_bcb);
      pthread_mutex_unlock(&leaf_bcb->page_latch);

      // sleep
      lock_wait(lock);

      // woke up - link lock and release transaction latch
      tcb_t* tcb = nullptr;
      pthread_mutex_lock(&txn_table.latch);
      if (txn_table.transactions.count(txn_id)) {
        tcb = txn_table.transactions[txn_id];
        link_lock_to_txn(tcb, lock);
        pthread_mutex_unlock(&tcb->latch);
      }
      pthread_mutex_unlock(&txn_table.latch);
      continue;
    } else {  // DEADLOCK
      unpin_bcb(leaf_bcb);
      pthread_mutex_unlock(&leaf_bcb->page_latch);
      return FAILURE;
    }
  }
}
/**
 * update with concurrency control
 */
int update_with_txn(int fd, tableid_t table_id, int64_t key, char* new_value,
                    int txn_id) {
  while (true) {
    buf_ctl_block_t* leaf_bcb;
    if (find_leaf(fd, table_id, key, (void**)&leaf_bcb) == PAGE_NULL) {
      return FAILURE;
    }

    leaf_page_t* leaf = (leaf_page_t*)leaf_bcb->frame;

    int idx = -1;
    for (int i = 0; i < leaf->num_of_keys; i++) {
      if (leaf->records[i].key == key) {
        idx = i;
        break;
      }
    }

    if (idx == -1) {
      unpin_bcb(leaf_bcb);
      pthread_mutex_unlock(&leaf_bcb->page_latch);
      return FAILURE;
    }

    char old_value[VALUE_SIZE];
    memcpy(old_value, leaf->records[idx].value, VALUE_SIZE);

    lock_t* lock;
    LockState st = lock_acquire(table_id, key, txn_id, X_LOCK, &lock);

    if (st == ACQUIRED) {
      memcpy(leaf->records[idx].value, new_value, VALUE_SIZE);
      leaf_bcb->is_dirty = true;

      unpin_bcb(leaf_bcb);
      pthread_mutex_unlock(&leaf_bcb->page_latch);

      tcb_t* tcb;
      if (acquire_txn_latch(txn_id, &tcb) == SUCCESS) {
        undo_log_t* log = (undo_log_t*)malloc(sizeof(undo_log_t));
        log->fd = fd;
        log->table_id = table_id;
        log->key = key;
        memcpy(log->old_value, old_value, VALUE_SIZE);
        log->prev = tcb->undo_head;
        tcb->undo_head = log;

        link_lock_to_txn(tcb, lock);
        release_txn_latch(tcb);
      }
      return SUCCESS;
    }

    unpin_bcb(leaf_bcb);
    pthread_mutex_unlock(&leaf_bcb->page_latch);

    if (st == NEED_TO_WAIT) {
      lock_wait(lock);
      continue;
    }

    return FAILURE;  // DEADLOCK
  }
}
