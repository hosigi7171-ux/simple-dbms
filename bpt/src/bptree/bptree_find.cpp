#include <db_api.h>

#include "bpt.h"
#include "bpt_internal.h"
#include "buf_mgr.h"
#include "file.h"

extern queue* q_head;

/**
 * helper function for print_leaves
 * find leftmost leaf page
 */
pagenum_t find_leftmost_leaf_page(int fd, tableid_t table_id,
                                  pagenum_t start_page_num) {
  pagenum_t current_page_num = start_page_num;

  while (true) {
    page_t* current_buf = read_buffer(fd, table_id, current_page_num);
    page_header_t* header = (page_header_t*)current_buf;

    if (header->is_leaf == LEAF) {
      return current_page_num;
    }

    internal_page_t* internal_page = (internal_page_t*)current_buf;
    pagenum_t next_page_num = internal_page->one_more_page_num;

    unpin(table_id, current_page_num);

    current_page_num = next_page_num;

    if (current_page_num == PAGE_NULL) {
      printf("error: find_leftmost_leaf_page\n");
      return PAGE_NULL;
    }
  }
}

/**
 * helper function for print_leaves
 * just print
 */
void print_leaves_in_sequence(int fd, tableid_t table_id,
                              pagenum_t start_leaf_num) {
  pagenum_t current_page_num = start_leaf_num;
  printf("Leaves: ");

  do {
    page_t* current_buf = read_buffer(fd, table_id, current_page_num);
    leaf_page_t* leaf_page = (leaf_page_t*)current_buf;
    page_header_t* header = (page_header_t*)current_buf;

    for (int i = 0; i < header->num_of_keys; i++) {
      printf("%" PRId64 " ", leaf_page->records[i].key);
    }

    pagenum_t next_page_num = leaf_page->right_sibling_page_num;

    unpin(table_id, current_page_num);

    if (next_page_num != PAGE_NULL) {
      printf("| ");
    }

    current_page_num = next_page_num;

  } while (current_page_num != PAGE_NULL);

  printf("\n");
}

/* Prints the bottom row of keys - not used
 * of the tree
 */
void print_leaves(int fd, tableid_t table_id) {
  header_page_t* header = read_header_page(fd, table_id);
  pagenum_t root_page_num = header->root_page_num;

  unpin(table_id, HEADER_PAGE_POS);

  if (root_page_num == PAGE_NULL) {
    printf("empty tree.\n");
    return;
  }

  pagenum_t leftmost_leaf_num =
      find_leftmost_leaf_page(fd, table_id, root_page_num);
  if (leftmost_leaf_num == PAGE_NULL) {
    return;
  }

  print_leaves_in_sequence(fd, table_id, leftmost_leaf_num);
}

/* Utility function to give the height - not used
 * of the tree, which length in number of edges
 * of the path from the root to any leaf.
 */
int height(int fd, tableid_t table_id, pagenum_t header_page_num) {
  int h = 0;

  header_page_t* header_page = read_header_page(fd, table_id);
  pagenum_t current_page_num = header_page->root_page_num;
  unpin(table_id, HEADER_PAGE_POS);

  if (current_page_num == PAGE_NULL) {
    return 0;
  }

  while (1) {
    page_t* cur_page = read_buffer(fd, table_id, current_page_num);
    page_header_t* page_header = (page_header_t*)cur_page;

    if (page_header->is_leaf == LEAF) {
      break;
    }

    internal_page_t* internal_page = (internal_page_t*)cur_page;
    current_page_num = internal_page->one_more_page_num;
    h++;

    if (current_page_num == PAGE_NULL) {
      return FAILURE;
    }
  }
  return h;
}

/**
 * @brief bfs로 b+tree를 level 별로 탐색 및 출력
 */
void print_tree(int fd, tableid_t table_id) {
#ifdef TEST_ENV
  printf("=== PRINT_TREE DEBUG ===\n");
  printf("table_id: %d, fd: %d\n", table_id, fd);
  printf("table_info[%d].fd: %d\n", table_id, table_infos[table_id].fd);
  printf("table_info[%d].path: %s\n", table_id, table_infos[table_id].path);

  // page_table 내용 출력
  printf("page_table[%d] size: %lu\n", table_id,
         buf_mgr.page_table[table_id].size());
  for (auto& p : buf_mgr.page_table[table_id]) {
    printf("pagenum=%lu -> frame_idx=%d (bcb.table_id=%d, bcb.page_num=%lu)\n",
           p.first, p.second, buf_mgr.frames[p.second].table_id,
           buf_mgr.frames[p.second].page_num);
  }
  printf("========================\n");
#endif
  queue* now_node_ptr = NULL;
  int i = 0;
  int current_level = 0;

  header_page_t* header = read_header_page(fd, table_id);
  pagenum_t root = header->root_page_num;
  unpin(table_id, HEADER_PAGE_POS);

  if (root == PAGE_NULL) {
    printf("empty tree.\n");
    return;
  }

  enqueue(root, 0);

  printf("--- level %d ---\n", current_level);

  while (q_head != NULL) {
    now_node_ptr = dequeue();
    if (now_node_ptr == NULL) {
      break;
    }
    pagenum_t now_page_num = now_node_ptr->page_num;

    page_t* now_buf = read_buffer(fd, table_id, now_page_num);
    page_header_t* now_header = (page_header_t*)now_buf;

    if (now_node_ptr->level != current_level) {
      current_level = now_node_ptr->level;
      printf("\n--- level %d ---\n", current_level);
    }

    printf("[");
    if (now_header->is_leaf == LEAF) {
      leaf_page_t* leaf_page = (leaf_page_t*)now_buf;

      // Leaf Node: 키 출력
      for (i = 0; i < leaf_page->num_of_keys; i++) {
        printf("%" PRId64 " ", leaf_page->records[i].key);
      }
    } else {
      internal_page_t* internal_page = (internal_page_t*)now_buf;
      int next_level = now_node_ptr->level + 1;

      // one_more_page_num 삽입
      if (internal_page->one_more_page_num != PAGE_NULL) {
        enqueue(internal_page->one_more_page_num, next_level);
      }

      // entry 삽입
      for (i = 0; i < internal_page->num_of_keys; i++) {
        printf("%" PRId64 " ", internal_page->entries[i].key);

        if (internal_page->entries[i].page_num != PAGE_NULL) {
          enqueue(internal_page->entries[i].page_num, next_level);
        }
      }
    }
    printf("] | ");

    unpin(table_id, now_page_num);
    free(now_node_ptr);
  }
  printf("\n");
}

/* Finds the record under a given key and prints an
 * appropriate message to stdout.
 */
void find_and_print(int fd, tableid_t table_id, int64_t key) {
  char result_buf[VALUE_SIZE];

  int result = find(fd, table_id, key, result_buf);

  if (result == SUCCESS) {
    printf("Record -- key %" PRId64 ", value %s.\n", key, result_buf);
  } else {
    // 실패 시
    printf("Record not found key %" PRId64 "\n", key);
  }
}

/* Finds and prints the keys, pointers, and values within a range
 * of keys between key_start and key_end, including both bounds.
 */
int find_and_print_range(int fd, tableid_t table_id, int64_t key_start,
                         int64_t key_end) {
  int i;
  int64_t returned_keys[MAX_RANGE_SIZE];
  pagenum_t returned_pages[MAX_RANGE_SIZE];
  int returned_indices[MAX_RANGE_SIZE];

  if (key_end - key_start > MAX_RANGE_SIZE) {
    return FAILURE;
  }

  int num_found = find_range(fd, table_id, key_start, key_end, returned_keys,
                             returned_pages, returned_indices);

  if (!num_found) {
    printf("not found.\n");
  } else {
    printf("found %d records in range [%" PRId64 ", %" PRId64 "]:\n", num_found,
           key_start, key_end);

    for (i = 0; i < num_found; i++) {
      leaf_page_t* temp_leaf =
          (leaf_page_t*)read_buffer(fd, table_id, returned_pages[i]);

      int64_t key = returned_keys[i];
      int index = returned_indices[i];

      char* value_ptr = temp_leaf->records[index].value;

      printf("Key: %" PRId64 "  Location: page %" PRId64
             ", index %d  Value: %s\n",
             key, returned_pages[i], index, value_ptr);
      unpin(table_id, returned_pages[i]);
    }
  }
  return SUCCESS;
}

/* Finds keys and their pointers, if present, in the range specified
 * by key_start and key_end, inclusive.  Places these in the arrays
 * returned_keys and returned_pointers, and returns the number of
 * entries found.
 */
int find_range(int fd, tableid_t table_id, int64_t key_start, int64_t key_end,
               int64_t returned_keys[], pagenum_t returned_pages[],
               int returned_indices[]) {
  int i = 0;
  int num_found = 0;

  // find start leaf
  pagenum_t current_leaf_num = find_leaf(fd, table_id, key_start);

  if (current_leaf_num == PAGE_NULL) {
    return 0;
  }

  page_t* current_buf = read_buffer(fd, table_id, current_leaf_num);
  leaf_page_t* leaf_page = (leaf_page_t*)current_buf;

  for (i = 0; i < leaf_page->num_of_keys; i++) {
    if (leaf_page->records[i].key >= key_start) {
      break;
    }
  }

  // traverse to end leaf
  while (current_leaf_num != PAGE_NULL) {
    for (; i < leaf_page->num_of_keys; i++) {
      int64_t current_key = leaf_page->records[i].key;

      if (current_key > key_end) {
        return num_found;
      }

      returned_keys[num_found] = current_key;
      returned_pages[num_found] = current_leaf_num;
      returned_indices[num_found] = i;
      num_found++;
    }

    pagenum_t next_leaf_num = leaf_page->right_sibling_page_num;
    unpin(table_id, current_leaf_num);

    current_leaf_num = next_leaf_num;
    i = 0;

    if (current_leaf_num != PAGE_NULL) {
      current_buf = read_buffer(fd, table_id, current_leaf_num);
      leaf_page = (leaf_page_t*)current_buf;
    }
  }

  return num_found;
}

/* Traces the path from the root to a leaf, searching
 * by key.  Displays information about the path
 * if the verbose flag is set.
 * Returns the leaf containing the given key.
 * This function finds the location where the key
 * should be, regardless of whether the key exists.
 */
pagenum_t find_leaf(int fd, tableid_t table_id, int64_t key) {
  header_page_t* header_page = read_header_page(fd, table_id);

  pagenum_t cur_num = header_page->root_page_num;

  // 루트 페이지가 존재하지 않으면
  if (cur_num == PAGE_NULL || header_page->num_of_pages == 1) {
    unpin(table_id, HEADER_PAGE_POS);
    return PAGE_NULL;
  }
  unpin(table_id, HEADER_PAGE_POS);

  // leaf를 찾을때까지 계속해서 읽어나감
  while (true) {
    page_t* page_buf = read_buffer(fd, table_id, cur_num);
    page_header_t* page_header = (page_header_t*)page_buf;

    uint32_t is_leaf = page_header->is_leaf;

    pagenum_t num_to_unpin = cur_num;
    if (is_leaf == LEAF) {
      unpin(table_id, num_to_unpin);
      return cur_num;
    }

    int index = 0;
    internal_page_t* internal_page = (internal_page_t*)page_buf;
    while (index < internal_page->num_of_keys &&
           key >= internal_page->entries[index].key) {
      index++;
    }

    if (index == 0) {
      cur_num = internal_page->one_more_page_num;
    } else {
      cur_num = internal_page->entries[index - 1].page_num;
    }
    if (cur_num == PAGE_NULL) {
      // 이거는 실행 안되어야 함
      // perror("find_leaf");
      return PAGE_NULL;
    }

    unpin(table_id, num_to_unpin);
  }
}
