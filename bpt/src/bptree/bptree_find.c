#include "bpt.h"
#include "bpt_internal.h"

extern queue *q_head;

/* Prints the bottom row of keys
 * of the tree
 */
void print_leaves(void) {
  page_t header_buf, current_buf;

  file_read_page(HEADER_PAGE_POS, &header_buf);
  header_page_t *header = (header_page_t *)&header_buf;
  pagenum_t current_page_num = header->root_page_num;

  if (current_page_num == PAGE_NULL) {
    printf("empty tree.\n");
    return;
  }

  // find left most leaf page
  while (1) {
    file_read_page(current_page_num, &current_buf);
    page_header_t *header = (page_header_t *)&current_buf;

    if (header->is_leaf == LEAF) {
      break;
    } else {
      internal_page_t *internal_page = (internal_page_t *)&current_buf;
      current_page_num = internal_page->one_more_page_num;

      if (current_page_num == PAGE_NULL) {
        printf("error: Internal node with no children.\n");
        return;
      }
    }
  }

  // print leaf pages
  do {
    if (current_page_num == PAGE_NULL) {
      break;
    }
    file_read_page(current_page_num, &current_buf);
    leaf_page_t *leaf_page = (leaf_page_t *)&current_buf;
    page_header_t *header = (page_header_t *)&current_buf;

    for (int i = 0; i < header->num_of_keys; i++) {
      printf("%" PRId64 " ", leaf_page->records[i].key);
    }

    current_page_num = leaf_page->right_sibling_page_num;

    if (current_page_num != PAGE_NULL) {
      printf("| ");
    }

  } while (current_page_num != PAGE_NULL);

  printf("\n");
}

/* Utility function to give the height
 * of the tree, which length in number of edges
 * of the path from the root to any leaf.
 */
int height(pagenum_t header_page_num) {
  int h = 0;
  page_t header_buf, current_buf;

  file_read_page(header_page_num, &header_buf);
  header_page_t *header = (header_page_t *)&header_buf;
  pagenum_t current_page_num = header->root_page_num;

  if (current_page_num == PAGE_NULL) {
    return 0;
  }

  while (1) {
    file_read_page(current_page_num, &current_buf);
    page_header_t *header = (page_header_t *)&current_buf;

    if (header->is_leaf == LEAF) {
      break;
    } else {
      internal_page_t *internal_page = (internal_page_t *)&current_buf;
      current_page_num = internal_page->one_more_page_num;
      h++;

      if (current_page_num == PAGE_NULL) {
        return -1;
      }
    }
  }
  return h;
}

/**
 * @brief bfs로 b+tree를 level 별로 탐색 및 출력
 */
void print_tree() {
  queue *now_node_ptr = NULL;
  int i = 0;
  int current_level = 0;

  page_t header_buf;
  file_read_page(HEADER_PAGE_POS, &header_buf);
  header_page_t *header = (header_page_t *)&header_buf;
  pagenum_t root = header->root_page_num;

  if (root == PAGE_NULL) {
    printf("empty tree.\n");
    return;
  }

  enqueue(root, 0);

  printf("--- level %d ---\n", current_level);

  while (q_head != NULL) {
    now_node_ptr = dequeue();
    pagenum_t now_page_num = now_node_ptr->page_num;

    page_t now_buf;
    file_read_page(now_page_num, &now_buf);
    page_header_t *now_header = (page_header_t *)&now_buf;

    if (now_node_ptr->level != current_level) {
      current_level = now_node_ptr->level;
      printf("\n--- level %d ---\n", current_level);
    }

    printf("[");
    if (now_header->is_leaf == LEAF) {
      leaf_page_t *leaf_page = (leaf_page_t *)&now_buf;

      // Leaf Node: 키 출력
      for (i = 0; i < leaf_page->num_of_keys; i++) {
        printf("%" PRId64 " ", leaf_page->records[i].key);
      }
    } else {
      internal_page_t *internal_page = (internal_page_t *)&now_buf;
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

    free(now_node_ptr);
  }
  printf("\n");
}

/* Finds the record under a given key and prints an
 * appropriate message to stdout.
 */
void find_and_print(int64_t key) {
  char result_buf[VALUE_SIZE];

  int result = find(key, result_buf);

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
int find_and_print_range(int64_t key_start, int64_t key_end) {
  int i;
  int64_t returned_keys[MAX_RANGE_SIZE];
  pagenum_t returned_pages[MAX_RANGE_SIZE];
  int returned_indices[MAX_RANGE_SIZE];

  if (key_end - key_start > MAX_RANGE_SIZE) {
    return FAILURE;
  }

  int num_found = find_range(key_start, key_end, returned_keys, returned_pages,
                             returned_indices);

  if (!num_found) {
    printf("not found.\n");
  } else {
    printf("found %d records in range [%" PRId64 ", %" PRId64 "]:\n", num_found,
           key_start, key_end);

    for (i = 0; i < num_found; i++) {
      page_t temp_buf;
      file_read_page(returned_pages[i], &temp_buf);
      leaf_page_t *temp_leaf = (leaf_page_t *)&temp_buf;

      int64_t key = returned_keys[i];
      int index = returned_indices[i];

      char *value_ptr = temp_leaf->records[index].value;

      printf("Key: %" PRId64 "  Location: page %" PRId64
             ", index %d  Value: %s\n",
             key, returned_pages[i], index, value_ptr);
    }
  }
  return SUCCESS;
}

/* Finds keys and their pointers, if present, in the range specified
 * by key_start and key_end, inclusive.  Places these in the arrays
 * returned_keys and returned_pointers, and returns the number of
 * entries found.
 */
int find_range(int64_t key_start, int64_t key_end, int64_t returned_keys[],
               pagenum_t returned_pages[], int returned_indices[]) {

  int i = 0;
  int num_found = 0;
  page_t leaf_buf;
  leaf_page_t *leaf_page;

  pagenum_t current_leaf_num = find_leaf(key_start);

  if (current_leaf_num == PAGE_NULL) {
    return 0;
  }

  file_read_page(current_leaf_num, &leaf_buf);
  leaf_page = (leaf_page_t *)&leaf_buf;

  for (i = 0; i < leaf_page->num_of_keys; i++) {
    if (leaf_page->records[i].key >= key_start) {
      break;
    }
  }

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

    current_leaf_num = leaf_page->right_sibling_page_num;
    i = 0;

    if (current_leaf_num != PAGE_NULL) {
      file_read_page(current_leaf_num, &leaf_buf);
      leaf_page = (leaf_page_t *)&leaf_buf;
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
pagenum_t find_leaf(int64_t key) {
  page_t header_buf;
  file_read_page(HEADER_PAGE_POS, &header_buf);
  header_page_t *header_page = (header_page_t *)&header_buf;

  pagenum_t cur_num = header_page->root_page_num;
  page_t page_buf;

  // leaf를 찾을때까지 계속해서 읽어나감
  while (true) {
    file_read_page(cur_num, &page_buf);
    page_header_t *page_header = (page_header_t *)&page_buf;
    uint32_t is_leaf = page_header->is_leaf;

    if (is_leaf == LEAF) {
      return cur_num;
    }

    int index = 0;
    internal_page_t *internal_page = (internal_page_t *)&page_buf;
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
  }
}
