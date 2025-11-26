#include "bpt.h"
#include "bpt_internal.h"

/* Finds and returns success(0) or fail(1)
 */
int find(int64_t key, char *result_buf) {
  pagenum_t leaf_num = find_leaf(key);
  if (leaf_num == PAGE_NULL) {
    return FAILURE;
  }

  // leaf_page 에서 키에 해당하는 값 찾기
  page_t tmp_page;
  file_read_page(leaf_num, &tmp_page);
  leaf_page_t *leaf_page = (leaf_page_t *)&tmp_page;

  int index = 0;

  for (index = 0; index < leaf_page->num_of_keys; index++) {
    if (leaf_page->records[index].key == key) {
      break;
    }
  }
  // 해당하는 키를 찾았으면
  if (index != leaf_page->num_of_keys) {
    copy_value(result_buf, leaf_page->records[index].value, VALUE_SIZE);
    return SUCCESS;
  }

  return FAILURE;
}

/**
 * @brief init header page
 * must be used before insert
 */
void init_header_page() {
  page_t header_buf;
  memset(&header_buf, 0, PAGE_SIZE);
  header_page_t *header_page = (header_page_t *)&header_buf;
  header_page->num_of_pages = HEADER_PAGE_POS + 1;

  file_write_page(HEADER_PAGE_POS, (page_t *)header_page);
}

/* Master insertion function.
 * Inserts a key and an associated value into
 * the B+ tree, causing the tree to be adjusted
 * however necessary to maintain the B+ tree
 * properties.
 */
int insert(int64_t key, char *value) {
  pagenum_t leaf;

  char result_buf[VALUE_SIZE];
  if (find(key, result_buf) == SUCCESS) {
    return FAILURE;
  }

  // Case: the tree does not exist yet. Start a new tree.
  page_t header_page_buf;
  file_read_page(HEADER_PAGE_POS, &header_page_buf);
  header_page_t *h_page = (header_page_t *)&header_page_buf;

  pagenum_t root_num = h_page->root_page_num;
  if (root_num == PAGE_NULL) {
    return start_new_tree(key, value);
  }

  // Case: the tree already exists.(Rest of function body.)
  leaf = find_leaf(key);

  // Case: leaf has room for key and pointer.
  page_t tmp_leaf_page;
  file_read_page(leaf, &tmp_leaf_page);
  leaf_page_t *leaf_page = (leaf_page_t *)&tmp_leaf_page;

  if (leaf_page->num_of_keys < RECORD_CNT) {
    return insert_into_leaf(leaf, &tmp_leaf_page, key, value);
  }

  // Case:  leaf must be split.
  return insert_into_leaf_after_splitting(leaf, key, value);
}

/* Master deletion function.
 */
int delete (int64_t key) {
  pagenum_t leaf;

  char value_buf[VALUE_SIZE];
  // if not exists fail
  if (find(key, value_buf) != SUCCESS) {
    return FAILURE;
  }

  leaf = find_leaf(key);

  if (leaf != PAGE_NULL) {
    return delete_entry(leaf, key, value_buf);
  }
  return FAILURE;
}
