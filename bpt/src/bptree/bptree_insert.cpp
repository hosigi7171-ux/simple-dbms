#include "bpt.h"
#include "bpt_internal.h"
#include "buf_mgr.h"
#include "file.h"

// INSERTION

void copy_value(char* dest, const char* src, size_t size) {
  strncpy(dest, src, size - 1);
  dest[size - 1] = '\0';
}

void init_leaf_page(page_t* page) {
  leaf_page_t* leaf_page = (leaf_page_t*)page;
  leaf_page->parent_page_num = PAGE_NULL;
  leaf_page->is_leaf = LEAF;
  leaf_page->num_of_keys = 0;
  leaf_page->right_sibling_page_num = PAGE_NULL;
}

void init_internal_page(page_t* page) {
  internal_page_t* internal_page = (internal_page_t*)page;
  internal_page->parent_page_num = PAGE_NULL;
  internal_page->is_leaf = INTERNAL;
  internal_page->num_of_keys = 0;
  internal_page->one_more_page_num = PAGE_NULL;
}

/* Creates a new general node, which can be adapted
 * to serve as either a leaf or an internal node.
 */
pagenum_t make_node(int fd, tableid_t table_id, uint32_t isleaf) {
  allocated_page_info_t page_info = make_and_pin_page(fd, table_id);

  page_t* page = page_info.page_ptr;
  pagenum_t new_page_num = page_info.page_num;

  if (page == NULL) {
    perror("make_node allocate and pin page.");
    exit(EXIT_FAILURE);
  }
  memset(page, 0, PAGE_SIZE);

  switch (isleaf) {
    case LEAF:
      init_leaf_page(page);
      break;
    case INTERNAL:
      init_internal_page(page);
      break;
    default:
      perror("make_node");
      unpin(table_id, new_page_num);
      exit(EXIT_FAILURE);
      break;
  }

  write_buffer(table_id, new_page_num, page);
  unpin(table_id, new_page_num);
  return new_page_num;
}

/* Finds the index within the parent's 'entries' array
 * where the new key should be inserted, based on the position
 * of the left child node (left_num).
 */
int get_index_after_left_child(page_t* parent_buffer, pagenum_t left_num) {
  internal_page_t* parent = (internal_page_t*)parent_buffer;
  // left_num이 leftmost인 경우 entries[0]
  if (parent->one_more_page_num == left_num) {
    return 0;
  }

  // left_num이 entries[index].page_num인 경우 entries[index+1]
  int index = 0;
  for (index = 0; index < parent->num_of_keys; index++) {
    if (parent->entries[index].page_num == left_num) {
      return index + 1;
    }
  }

  // 못찾으면 에러
  perror("get_left_index");
  return index;
}

/* Inserts a new pointer to a record and its corresponding
 * key into a leaf.
 */
int insert_into_leaf(int fd, tableid_t table_id, pagenum_t leaf_num,
                     leaf_page_t* leaf_page, int64_t key, char* value) {
  int index, insertion_point;

  insertion_point = 0;
  while (insertion_point < leaf_page->num_of_keys &&
         leaf_page->records[insertion_point].key < key) {
    insertion_point++;
  }
  for (index = leaf_page->num_of_keys; index > insertion_point; index--) {
    leaf_page->records[index] = leaf_page->records[index - 1];
  }

  leaf_page->records[insertion_point].key = key;
  copy_value(leaf_page->records[insertion_point].value, value, VALUE_SIZE);
  leaf_page->num_of_keys++;

  write_buffer(table_id, leaf_num, (page_t*)leaf_page);
  unpin(table_id, leaf_num);
  return SUCCESS;
}

/**
 * helper function for insert_into_leaf_after_splitting
 * Create a temporary array by combining the existing record and the new record
 * and return it
 */
record_t* prepare_records_for_split(leaf_page_t* leaf_page, int64_t key,
                                    const char* value) {
  record_t* temp_records =
      (record_t*)malloc((RECORD_CNT + 1) * sizeof(record_t));
  if (temp_records == NULL) {
    perror("Memory allocation for temporary records failed.");
    exit(EXIT_FAILURE);
  }

  int insertion_index = 0;
  while (insertion_index < RECORD_CNT &&
         leaf_page->records[insertion_index].key < key) {
    insertion_index++;
  }

  int i, j;
  for (i = 0, j = 0; i < leaf_page->num_of_keys; i++, j++) {
    if (j == insertion_index) {
      j++;
    }
    temp_records[j] = leaf_page->records[i];
  }

  // insert new record
  temp_records[insertion_index].key = key;
  copy_value(temp_records[insertion_index].value, value, VALUE_SIZE);

  return temp_records;
}

/**
 * helper function for insert_into_leaf_after_splitting
 * Distributes records in the temporary array to old_leaf and new_leaf and
 * returns k_prime
 */
int64_t distribute_records_to_leaves(leaf_page_t* leaf_page,
                                     leaf_page_t* new_leaf_page,
                                     record_t* temp_records,
                                     pagenum_t new_leaf_num) {
  const int split = cut(RECORD_CNT);

  int i, j;

  // Allocate to old_leaf_page until split point
  leaf_page->num_of_keys = 0;
  for (i = 0; i < split; i++) {
    leaf_page->records[i] = temp_records[i];
    leaf_page->num_of_keys++;
  }
  for (int k = split; k < RECORD_CNT; k++) {
    memset(&(leaf_page->records[k]), 0, sizeof(record_t));
  }

  // Records after the split point are allocated to new_leaf_page
  new_leaf_page->num_of_keys = 0;
  for (j = 0; i < LEAF_ORDER; i++, j++) {
    new_leaf_page->records[j] = temp_records[i];
    new_leaf_page->num_of_keys++;
  }
  for (int k = new_leaf_page->num_of_keys; k < RECORD_CNT; k++) {
    memset(&(new_leaf_page->records[k]), 0, sizeof(record_t));
  }

  // Connect sibling nodes and set parent nodes
  new_leaf_page->right_sibling_page_num = leaf_page->right_sibling_page_num;
  leaf_page->right_sibling_page_num = new_leaf_num;
  new_leaf_page->parent_page_num = leaf_page->parent_page_num;

  return new_leaf_page->records[0].key;
}

/**
 * Splits a node into two by inserting a new key and record into the leaf and
 * passing the split information to the parent
 */
int insert_into_leaf_after_splitting(int fd, tableid_t table_id,
                                     pagenum_t leaf_num, int64_t key,
                                     char* value) {
  pagenum_t new_leaf_num;
  int64_t new_key;
  record_t* temp_records;

  leaf_page_t* leaf_page = (leaf_page_t*)read_buffer(fd, table_id, leaf_num);

  temp_records = prepare_records_for_split(leaf_page, key, value);

  new_leaf_num = make_node(fd, table_id, LEAF);
  leaf_page_t* new_leaf_page =
      (leaf_page_t*)read_buffer(fd, table_id, new_leaf_num);

  new_key = distribute_records_to_leaves(leaf_page, new_leaf_page, temp_records,
                                         new_leaf_num);

  free(temp_records);

  write_buffer(table_id, leaf_num, (page_t*)leaf_page);
  write_buffer(table_id, new_leaf_num, (page_t*)new_leaf_page);
  unpin(table_id, leaf_num);
  unpin(table_id, new_leaf_num);

  return insert_into_parent(fd, table_id, leaf_num, new_key, new_leaf_num);
}

/* Inserts a new key and pointer to a node
 * into a node into which these can fit
 * without violating the B+ tree properties.
 */
int insert_into_node(int fd, tableid_t table_id, pagenum_t page_num,
                     int64_t left_index, int64_t key, pagenum_t right) {
  int index;
  internal_page_t* page = (internal_page_t*)read_buffer(fd, table_id, page_num);

  for (index = page->num_of_keys; index > left_index; index--) {
    page->entries[index] = page->entries[index - 1];
  }
  page->entries[left_index].page_num = right;
  page->entries[left_index].key = key;
  page->num_of_keys++;

  write_buffer(table_id, page_num, (page_t*)page);
  unpin(table_id, page_num);

  return SUCCESS;
}

/**
 * helper function for insert_into_node_after_splitting
 * Returns a temporary array created by combining the existing entries and the
 * new entries
 */
entry_t* prepare_entries_for_split(internal_page_t* old_node_page,
                                   int64_t left_index, int64_t key,
                                   pagenum_t right) {
  entry_t* temp_entries = (entry_t*)malloc((ENTRY_CNT + 1) * sizeof(entry_t));
  if (temp_entries == NULL) {
    perror("Temporary entries array.");
    exit(EXIT_FAILURE);
  }

  int i;
  for (i = 0; i < old_node_page->num_of_keys; i++) {
    temp_entries[i] = old_node_page->entries[i];
  }

  // insert new entry
  for (i = old_node_page->num_of_keys; i > left_index; i--) {
    temp_entries[i] = temp_entries[i - 1];
  }
  temp_entries[left_index].key = key;
  temp_entries[left_index].page_num = right;

  return temp_entries;
}

/**
 * helper function for insert_into_node_after_splitting
 * Distribute temp_entries to old_node and new_node, and return k_prime
 */
int64_t distribute_entries_and_update_children(int fd, tableid_t table_id,
                                               pagenum_t old_node_num,
                                               internal_page_t* old_node_page,
                                               pagenum_t new_node_num,
                                               internal_page_t* new_node_page,
                                               entry_t* temp_entries) {
  const int split = cut(INTERNAL_ORDER);
  int i, j;

  // key to send to parents
  const int64_t k_prime = temp_entries[split - 1].key;

  // Reassign entries to Old Node
  old_node_page->num_of_keys = 0;
  for (i = 0; i < split - 1; i++) {
    old_node_page->entries[i] = temp_entries[i];
    old_node_page->num_of_keys++;
  }
  for (int k = split - 1; k < ENTRY_CNT; k++) {
    memset(&(old_node_page->entries[k]), 0, sizeof(entry_t));
  }

  // Set the P0 pointer of the new node (the right pointer of k_prime)
  pagenum_t new_node_p0 = temp_entries[split - 1].page_num;
  new_node_page->one_more_page_num = new_node_p0;

  // Assigning entries to new nodes
  new_node_page->num_of_keys = 0;
  for (i = split, j = 0; i < INTERNAL_ORDER; i++, j++) {
    new_node_page->entries[j] = temp_entries[i];
    new_node_page->num_of_keys++;
  }
  for (int k = new_node_page->num_of_keys; k < ENTRY_CNT; k++) {
    memset(&(new_node_page->entries[k]), 0, sizeof(entry_t));
  }

  new_node_page->parent_page_num = old_node_page->parent_page_num;

  // Update the parent of a child node
  pagenum_t child = new_node_page->one_more_page_num;
  if (child != PAGE_NULL) {
    page_t* child_page = read_buffer(fd, table_id, child);
    page_header_t* child_page_header = (page_header_t*)child_page;
    child_page_header->parent_page_num = new_node_num;
    write_buffer(table_id, child, child_page);
    unpin(table_id, child);
  }
  for (i = 0; i < new_node_page->num_of_keys; i++) {
    child = new_node_page->entries[i].page_num;
    if (child != PAGE_NULL) {
      page_t* child_page = read_buffer(fd, table_id, child);
      page_header_t* child_page_header = (page_header_t*)child_page;
      child_page_header->parent_page_num = new_node_num;
      write_buffer(table_id, child, child_page);
      unpin(table_id, child);
    }
  }

  return k_prime;
}

/**
 * Splits a node into two by inserting a new key and pointer into the internal
 * node and passes the split information to the parent
 */
int insert_into_node_after_splitting(int fd, tableid_t table_id,
                                     pagenum_t old_node, int64_t left_index,
                                     int64_t key, pagenum_t right) {
  pagenum_t new_node_num;
  int64_t k_prime;
  entry_t* temp_entries;

  internal_page_t* old_node_page =
      (internal_page_t*)read_buffer(fd, table_id, old_node);

  temp_entries =
      prepare_entries_for_split(old_node_page, left_index, key, right);

  new_node_num = make_node(fd, table_id, INTERNAL);
  internal_page_t* new_node_page =
      (internal_page_t*)read_buffer(fd, table_id, new_node_num);

  k_prime = distribute_entries_and_update_children(fd, table_id, old_node,
                                                   old_node_page, new_node_num,
                                                   new_node_page, temp_entries);

  free(temp_entries);

  write_buffer(table_id, old_node, (page_t*)old_node_page);
  write_buffer(table_id, new_node_num, (page_t*)new_node_page);
  unpin(table_id, old_node);
  unpin(table_id, new_node_num);

  return insert_into_parent(fd, table_id, old_node, k_prime, new_node_num);
}

/* Inserts a new node (leaf or internal node) into the B+ tree.
 * Returns the root of the tree after insertion.
 */
int insert_into_parent(int fd, tableid_t table_id, pagenum_t left, int64_t key,
                       pagenum_t right) {
  int left_index;
  pagenum_t parent;

  page_header_t* left_page_header =
      (page_header_t*)read_buffer(fd, table_id, left);

  parent = left_page_header->parent_page_num;
  unpin(table_id, left);

  /* Case: new root. */
  if (parent == PAGE_NULL) {
    return insert_into_new_root(fd, table_id, left, key, right);
  }

  /* Case: leaf or node. (Remainder of
   * function body.)
   */
  page_t* parent_page = read_buffer(fd, table_id, parent);
  page_header_t* parent_page_header = (page_header_t*)parent_page;

  /* Find the parent's pointer to the left
   * node.
   */
  left_index = get_index_after_left_child(parent_page, left);

  /* Simple case: the new key fits into the node.
   */
  if (parent_page_header->num_of_keys < INTERNAL_ORDER - 1) {
    unpin(table_id, parent);
    return insert_into_node(fd, table_id, parent, left_index, key, right);
  }

  /* Harder case:  split a node in order
   * to preserve the B+ tree properties.
   */
  unpin(table_id, parent);
  return insert_into_node_after_splitting(fd, table_id, parent, left_index, key,
                                          right);
}

/* Creates a new root for two subtrees
 * and inserts the appropriate key into
 * the new root.
 */
int insert_into_new_root(int fd, tableid_t table_id, pagenum_t left,
                         int64_t key, pagenum_t right) {
  pagenum_t root = make_node(fd, table_id, INTERNAL);

  // root 처리
  internal_page_t* root_page =
      (internal_page_t*)read_buffer(fd, table_id, root);

  root_page->one_more_page_num = left;
  root_page->entries[0].key = key;
  root_page->entries[0].page_num = right;
  root_page->num_of_keys = 1;
  root_page->parent_page_num = PAGE_NULL;

  write_buffer(table_id, root, (page_t*)root_page);
  unpin(table_id, root);

  // left right 처리
  page_t* left_page = read_buffer(fd, table_id, left);
  page_header_t* left_header = (page_header_t*)left_page;
  left_header->parent_page_num = root;
  write_buffer(table_id, left, left_page);
  unpin(table_id, left);

  page_t* right_page = read_buffer(fd, table_id, right);
  page_header_t* right_header = (page_header_t*)right_page;
  right_header->parent_page_num = root;
  write_buffer(table_id, right, right_page);
  unpin(table_id, right);

  // 헤더 페이지 갱신
  header_page_t* header_page = read_header_page(fd, table_id);
  header_page->root_page_num = root;
  write_buffer(table_id, HEADER_PAGE_POS, (page_t*)header_page);
  unpin(table_id, HEADER_PAGE_POS);

  return SUCCESS;
}

/* First insertion:
 * start a new tree.
 */
int start_new_tree(int fd, tableid_t table_id, int64_t key, char* value) {
  // make root page
  pagenum_t root = make_node(fd, table_id, LEAF);
  leaf_page_t* root_page = (leaf_page_t*)read_buffer(fd, table_id, root);

  root_page->parent_page_num = PAGE_NULL;
  root_page->is_leaf = LEAF;
  root_page->num_of_keys = 1;
  root_page->right_sibling_page_num = PAGE_NULL;
  root_page->records[0].key = key;
  copy_value(root_page->records[0].value, value, VALUE_SIZE);

  link_header_page(fd, table_id, root);

  write_buffer(table_id, root, (page_t*)root_page);
  unpin(table_id, root);
  return SUCCESS;
}

void link_header_page(int fd, tableid_t table_id, pagenum_t root) {
  header_page_t* header_page = (header_page_t*)read_header_page(fd, table_id);
  header_page->root_page_num = root;

  write_buffer(table_id, HEADER_PAGE_POS, (page_t*)header_page);
  unpin(table_id, HEADER_PAGE_POS);
}
