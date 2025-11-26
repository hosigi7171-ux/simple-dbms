#include "bpt.h"
#include "bpt_internal.h"

// DELETION.

/* Return the index of the key to the left
 * of the pointer in the parent pointing
 * to n. If not (the node is the leftmost child),
 * returns -1 to signify this special case.
 * If target_node has no parent, return -2 (CANNOT_ROOT)
 */
int get_kprime_index(pagenum_t target_node) {
  page_t target_buf, parent_buf;
  file_read_page(target_node, &target_buf);
  page_header_t *target_header = (page_header_t *)&target_buf;

  pagenum_t parent_num = target_header->parent_page_num;
  if (parent_num == PAGE_NULL) {
    return CANNOT_ROOT;
  }

  file_read_page(parent_num, &parent_buf);
  internal_page_t *parent_page = (internal_page_t *)&parent_buf;

  // 왼쪽 형제가 없는 경우
  if (parent_page->one_more_page_num == target_node) {
    return -1;
  }

  for (int index = 0; index < parent_page->num_of_keys; index++) {
    if (parent_page->entries[index].page_num == target_node) {
      return index;
    }
  }

  // Error state.
  printf("Search for nonexistent pointer to node in parent.\n");
  printf("Node:  %#lx\n", (unsigned long)target_node);
  exit(EXIT_FAILURE);
}

pagenum_t adjust_root(pagenum_t root) {
  page_t root_buf;
  file_read_page(root, &root_buf);
  page_header_t *root_header = (page_header_t *)&root_buf;

  /* Case: nonempty root.
   * Key and pointer have already been deleted,
   * so nothing to be done.
   */

  if (root_header->num_of_keys > 0) {
    return SUCCESS;
  }

  /* Case: empty root.
   */

  // If it has a child, promote
  // the first (only) child
  // as the new root.
  pagenum_t new_root;
  internal_page_t *root_internal = (internal_page_t *)&root_buf;
  if (root_header->is_leaf == INTERNAL) {
    new_root = root_internal->one_more_page_num;

    if (new_root != PAGE_NULL) {
      page_t new_root_buf;
      file_read_page(new_root, &new_root_buf);
      page_header_t *new_root_header = (page_header_t *)&new_root_buf;

      new_root_header->parent_page_num = PAGE_NULL;
      file_write_page(new_root, &new_root_buf);
    }
  } else {
    new_root = PAGE_NULL;
  }

  file_free_page(root);

  // update header
  page_t header_buf;
  file_read_page(HEADER_PAGE_POS, &header_buf);
  header_page_t *header_page = (header_page_t *)&header_buf;
  header_page->root_page_num = new_root;
  file_write_page(HEADER_PAGE_POS, &header_buf);

  return SUCCESS;
}

/**
 * helper function for coalesce nodes
 * @brief Handles the merging logic of internal nodes
 * Insert k_prime, copy target's entry, and update the child's parent pointer
 */
void coalesce_internal_nodes(page_t *neighbor_buf, page_t *target_buf,
                             int neighbor_num, int64_t k_prime) {
  page_header_t *neighbor_header = (page_header_t *)neighbor_buf;
  internal_page_t *neighbor_internal = (internal_page_t *)neighbor_buf;
  internal_page_t *target_internal = (internal_page_t *)target_buf;

  int neighbor_insertion_index = neighbor_header->num_of_keys;

  // Append k_prime and the target's one_more_page_num pointer
  neighbor_internal->entries[neighbor_insertion_index].key = k_prime;
  neighbor_internal->entries[neighbor_insertion_index].page_num =
      target_internal->one_more_page_num;
  neighbor_header->num_of_keys++;

  // Append all pointers and keys from target (excluding target's
  // one_more_page_num
  for (int i = neighbor_insertion_index + 1, j = 0;
       j < target_internal->num_of_keys; i++, j++) {
    neighbor_internal->entries[i] = target_internal->entries[j];
    neighbor_header->num_of_keys++;
  }
  target_internal->num_of_keys = 0;

  // Update parent pointers for all children copied from target
  pagenum_t child_num =
      neighbor_internal->entries[neighbor_insertion_index].page_num;
  if (child_num != PAGE_NULL) {
    page_t child_buf;
    file_read_page(child_num, &child_buf);
    ((page_header_t *)&child_buf)->parent_page_num = neighbor_num;
    file_write_page(child_num, &child_buf);
  }
  for (int i = neighbor_insertion_index + 1; i < neighbor_header->num_of_keys;
       i++) {
    child_num = neighbor_internal->entries[i].page_num;
    if (child_num != PAGE_NULL) {
      page_t child_buf;
      file_read_page(child_num, &child_buf);
      ((page_header_t *)&child_buf)->parent_page_num = neighbor_num;
      file_write_page(child_num, &child_buf);
    }
  }
}

/**
 * helper function for coalesce nodes
 * @brief Handles the merging logic of leaf nodes
 * Copy records from target and update right_sibling_page_num
 */
void coalesce_leaf_nodes(page_t *neighbor_buf, page_t *target_buf) {
  page_header_t *neighbor_header = (page_header_t *)neighbor_buf;
  leaf_page_t *neighbor_leaf = (leaf_page_t *)neighbor_buf;
  leaf_page_t *target_leaf = (leaf_page_t *)target_buf;

  int neighbor_insertion_index = neighbor_header->num_of_keys;

  // Append all records from target to neighbor
  for (int i = neighbor_insertion_index, j = 0; j < target_leaf->num_of_keys;
       i++, j++) {
    neighbor_leaf->records[i] = target_leaf->records[j];
    neighbor_header->num_of_keys++;
  }

  // Update neighbor's right sibling pointer
  neighbor_leaf->right_sibling_page_num = target_leaf->right_sibling_page_num;
}

/* Coalesces a node that has become
 * too small after deletion
 * with a neighboring node that
 * can accept the additional entries
 * without exceeding the maximum.
 */
int coalesce_nodes(pagenum_t target_num, pagenum_t neighbor_num,
                   int kprime_index_from_get, int64_t k_prime) {
  // Swap neighbor with target if target is on the extreme left
  if (kprime_index_from_get == -1) {
    pagenum_t tmp_num = target_num;
    target_num = neighbor_num;
    neighbor_num = tmp_num;
  }

  page_t neighbor_buf, target_buf;
  file_read_page(neighbor_num, &neighbor_buf);
  file_read_page(target_num, &target_buf);

  page_header_t *neighbor_header = (page_header_t *)&neighbor_buf;
  page_header_t *target_header = (page_header_t *)&target_buf;

  pagenum_t parent_num = target_header->parent_page_num;

  if (target_header->is_leaf == INTERNAL) {
    coalesce_internal_nodes(&neighbor_buf, &target_buf, neighbor_num, k_prime);
  } else {
    coalesce_leaf_nodes(&neighbor_buf, &target_buf);
  }

  file_write_page(neighbor_num, (page_t *)&neighbor_buf);
  file_free_page(target_num);

  // Remove the separator key from the parent
  return delete_entry(parent_num, k_prime, NULL);
}

/**
 * helper function for redistribute nodes
 * @brief Redistributes entries from the left neighbor node to the target node
 */
void redistribute_from_left(pagenum_t target_num, page_t *target_buf,
                            page_t *neighbor_buf, internal_page_t *parent_page,
                            int k_prime_index, int k_prime) {
  page_header_t *target_header = (page_header_t *)target_buf;

  if (target_header->is_leaf == INTERNAL) {
    redistribute_internal_from_left(target_num, target_buf, neighbor_buf,
                                    parent_page, k_prime_index, k_prime);
  } else {
    redistribute_leaf_from_left(target_buf, neighbor_buf, parent_page,
                                k_prime_index);
  }
}

/**
 * helper function for redistribute nodes
 * @brief Move the last entry of the left neighbor node from the internal node
 * to the first * position of the target node
 */
void redistribute_internal_from_left(pagenum_t target_num, page_t *target_buf,
                                     page_t *neighbor_buf,
                                     internal_page_t *parent_page,
                                     int k_prime_index, int k_prime) {
  page_header_t *target_header = (page_header_t *)target_buf;
  internal_page_t *target_internal = (internal_page_t *)target_buf;
  internal_page_t *neighbor_internal = (internal_page_t *)neighbor_buf;
  page_header_t *neighbor_header = (page_header_t *)neighbor_buf;

  for (int index = target_header->num_of_keys; index > 0; index--) {
    target_internal->entries[index] = target_internal->entries[index - 1];
  }
  target_internal->entries[0].page_num = target_internal->one_more_page_num;

  target_internal->entries[0].key = k_prime;

  pagenum_t last_num_neighbor =
      neighbor_internal->entries[neighbor_header->num_of_keys - 1].page_num;
  target_internal->one_more_page_num = last_num_neighbor;

  if (last_num_neighbor != PAGE_NULL) {
    page_t child_buf;
    file_read_page(last_num_neighbor, &child_buf);
    ((page_header_t *)&child_buf)->parent_page_num = target_num;
    file_write_page(last_num_neighbor, &child_buf);
  }

  parent_page->entries[k_prime_index].key =
      neighbor_internal->entries[neighbor_header->num_of_keys - 1].key;

  memset(&neighbor_internal->entries[neighbor_header->num_of_keys - 1], 0,
         sizeof(entry_t));
}

/**
 * helper function for redistribute nodes
 * @brief Move the last record of the left neighboring node from the leaf node
 * to the first position of the target node
 */
void redistribute_leaf_from_left(page_t *target_buf, page_t *neighbor_buf,
                                 internal_page_t *parent_page,
                                 int k_prime_index) {
  page_header_t *target_header = (page_header_t *)target_buf;
  leaf_page_t *target_leaf = (leaf_page_t *)target_buf;
  leaf_page_t *neighbor_leaf = (leaf_page_t *)neighbor_buf;
  page_header_t *neighbor_header = (page_header_t *)neighbor_buf;

  for (int i = target_header->num_of_keys; i > 0; i--) {
    target_leaf->records[i] = target_leaf->records[i - 1];
  }

  target_leaf->records[0] =
      neighbor_leaf->records[neighbor_header->num_of_keys - 1];

  parent_page->entries[k_prime_index].key = target_leaf->records[0].key;

  memset(&neighbor_leaf->records[neighbor_header->num_of_keys - 1], 0,
         sizeof(record_t));
}

/**
 * helper function for redistribute nodes
 * @brief Redistributes entries from the right neighbor node to the target node
 */
void redistribute_from_right(pagenum_t target_num, page_t *target_buf,
                             page_t *neighbor_buf, internal_page_t *parent_page,
                             int k_prime_index, int k_prime) {
  page_header_t *target_header = (page_header_t *)target_buf;

  if (target_header->is_leaf == INTERNAL) {
    redistribute_internal_from_right(target_num, target_buf, neighbor_buf,
                                     parent_page, k_prime_index, k_prime);
  } else {
    redistribute_leaf_from_right(target_buf, neighbor_buf, parent_page,
                                 k_prime_index);
  }
}

/**
 * helper function for redistribute nodes
 * @brief Move the first entry of the right neighbor node from the internal node
 * to the last position of the target node
 */
void redistribute_internal_from_right(pagenum_t target_num, page_t *target_buf,
                                      page_t *neighbor_buf,
                                      internal_page_t *parent_page,
                                      int k_prime_index, int k_prime) {
  page_header_t *target_header = (page_header_t *)target_buf;
  internal_page_t *target_internal = (internal_page_t *)target_buf;
  internal_page_t *neighbor_internal = (internal_page_t *)neighbor_buf;
  page_header_t *neighbor_header = (page_header_t *)neighbor_buf;

  target_internal->entries[target_header->num_of_keys].key = k_prime;

  pagenum_t num_from_neighbor = neighbor_internal->one_more_page_num;
  target_internal->entries[target_header->num_of_keys].page_num =
      num_from_neighbor;

  if (num_from_neighbor != PAGE_NULL) {
    page_t child_buf;
    file_read_page(num_from_neighbor, &child_buf);
    ((page_header_t *)&child_buf)->parent_page_num = target_num;
    file_write_page(num_from_neighbor, &child_buf);
  }

  parent_page->entries[k_prime_index].key = neighbor_internal->entries[0].key;

  neighbor_internal->one_more_page_num = neighbor_internal->entries[0].page_num;
  for (int i = 0; i < neighbor_header->num_of_keys - 1; i++) {
    neighbor_internal->entries[i] = neighbor_internal->entries[i + 1];
  }

  memset(&neighbor_internal->entries[neighbor_header->num_of_keys - 1], 0,
         sizeof(entry_t));
}

/**
 * helper function for redistribute nodes
 * @brief Move the first record of the right neighboring node from the leaf node
 * to the last position of the target node
 */
void redistribute_leaf_from_right(page_t *target_buf, page_t *neighbor_buf,
                                  internal_page_t *parent_page,
                                  int k_prime_index) {
  page_header_t *target_header = (page_header_t *)target_buf;
  leaf_page_t *target_leaf = (leaf_page_t *)target_buf;
  leaf_page_t *neighbor_leaf = (leaf_page_t *)neighbor_buf;
  page_header_t *neighbor_header = (page_header_t *)neighbor_buf;

  target_leaf->records[target_header->num_of_keys] = neighbor_leaf->records[0];

  parent_page->entries[k_prime_index].key = neighbor_leaf->records[1].key;

  for (int i = 0; i < neighbor_header->num_of_keys - 1; i++) {
    neighbor_leaf->records[i] = neighbor_leaf->records[i + 1];
  }

  memset(&neighbor_leaf->records[neighbor_header->num_of_keys - 1], 0,
         sizeof(record_t));
}

/* Redistributes entries between two nodes when
 * one has become too small after deletion
 * but its neighbor is too big to append the
 * small node's entries without exceeding the
 * maximum
 */
int redistribute_nodes(pagenum_t target_num, pagenum_t neighbor_num,
                       int kprime_index_from_get, int k_prime_index,
                       int k_prime) {
  page_t target_buf, neighbor_buf, parent_buf;
  file_read_page(target_num, &target_buf);
  file_read_page(neighbor_num, &neighbor_buf);

  page_header_t *target_header = (page_header_t *)&target_buf;
  page_header_t *neighbor_header = (page_header_t *)&neighbor_buf;
  pagenum_t parent_num = target_header->parent_page_num;

  file_read_page(parent_num, &parent_buf);
  internal_page_t *parent_page = (internal_page_t *)&parent_buf;

  /// target is not leftmost, so neighbor is to the left
  if (kprime_index_from_get != -1) {
    redistribute_from_left(target_num, &target_buf, &neighbor_buf, parent_page,
                           k_prime_index, k_prime);
  }
  // target is leftmost, so neighbor is to the right
  else {
    redistribute_from_right(target_num, &target_buf, &neighbor_buf, parent_page,
                            k_prime_index, k_prime);
  }

  // Update key counts and write back pages
  target_header->num_of_keys++;
  neighbor_header->num_of_keys--;

  file_write_page(target_num, &target_buf);
  file_write_page(neighbor_num, &neighbor_buf);
  file_write_page(parent_num, &parent_buf);

  return SUCCESS;
}

/**
 * @brief remove record from leaf node, if success return SUCCESS(0) else
 * FAILURE(-1)
 */
int remove_record_from_node(leaf_page_t *target_page, int64_t key,
                            const char *value) {
  // Remove the record and shift other records accordingly.
  int index = 0;
  while (target_page->records[index].key != key) {
    index++;
  }
  if (index == target_page->num_of_keys) {
    return FAILURE;
  }

  for (++index; index < target_page->num_of_keys; index++) {
    target_page->records[index - 1] = target_page->records[index];
  }
  // One key fewer.
  target_page->num_of_keys--;

  memset(&target_page->records[target_page->num_of_keys], 0, sizeof(record_t));

  return SUCCESS;
}

/**
 * @brief remove entry from internal node, if success return SUCCESS(0) else
 * FAILURE(-1)
 */
int remove_entry_from_node(internal_page_t *target_page, int64_t key) {
  // Remove the key and shift other keys accordingly.
  int index = 0;
  while (target_page->entries[index].key != key) {
    index++;
  }
  if (index == target_page->num_of_keys) {
    return FAILURE;
  }

  for (++index; index < target_page->num_of_keys; index++) {
    target_page->entries[index - 1] = target_page->entries[index];
  }
  // One key fewer.
  target_page->num_of_keys--;

  memset(&target_page->entries[target_page->num_of_keys], 0, sizeof(entry_t));

  return SUCCESS;
}

/**
 * helper function for delete entry
 * @brief Find the distinguishing key information of neighboring nodes and
 * parents to handle underflow
 */
int find_neighbor_and_kprime(pagenum_t target_node,
                             internal_page_t *parent_page,
                             page_header_t *target_header,
                             pagenum_t *neighbor_num_out,
                             int *k_prime_key_index_out) {

  int kprime_index_from_get = get_kprime_index(target_node);

  if (kprime_index_from_get == -1) {
    // target is P0 neighbor P1
    *neighbor_num_out = parent_page->entries[0].page_num;
    *k_prime_key_index_out = 0;
  } else {
    // target is Pi neighbor Pi-1.
    int target_pointer_index =
        kprime_index_from_get; // Index of the pointer to target

    *k_prime_key_index_out = target_pointer_index;

    if (target_pointer_index == 0) {
      // target is P1 (entries[0].page_num) neighbor P0
      *neighbor_num_out = parent_page->one_more_page_num;
    } else {
      // target is Pi+1 (entries[i].page_num, i > 0) neighbor is Pi
      *neighbor_num_out =
          parent_page->entries[target_pointer_index - 1].page_num;
    }
  }
  return kprime_index_from_get;
}

/**
 * helper function for delete entry
 * @brief Handles node underflow.
 * Finds neighboring nodes and decides whether to merge or redistribute them and
 * call
 */
int handle_underflow(pagenum_t target_node) {
  page_t node_buf, parent_buf;
  file_read_page(target_node, &node_buf);
  page_header_t *node_header = (page_header_t *)&node_buf;
  pagenum_t parent_num = node_header->parent_page_num;

  file_read_page(parent_num, &parent_buf);
  internal_page_t *parent_page = (internal_page_t *)&parent_buf;

  pagenum_t neighbor_num;
  int k_prime_key_index;

  int kprime_index_from_get = find_neighbor_and_kprime(
      target_node, parent_page, node_header, &neighbor_num, &k_prime_key_index);

  int64_t k_prime = parent_page->entries[k_prime_key_index].key;

  page_t neighbor_buf;
  file_read_page(neighbor_num, &neighbor_buf);
  page_header_t *neighbor_header = (page_header_t *)&neighbor_buf;

  int capacity = node_header->is_leaf ? RECORD_CNT : ENTRY_CNT - 1;

  if (neighbor_header->num_of_keys + node_header->num_of_keys < capacity) {
    return coalesce_nodes(target_node, neighbor_num, kprime_index_from_get,
                          k_prime);
  } else {
    return redistribute_nodes(target_node, neighbor_num, kprime_index_from_get,
                              k_prime_key_index, k_prime);
  }
}

/* Deletes an entry from the B+ tree.
 * Removes the record and its key and pointer
 * from the leaf, and then makes all appropriate
 * changes to preserve the B+ tree properties.
 */
int delete_entry(pagenum_t target_node, int64_t key, const char *value) {
  page_t node_buf;
  file_read_page(target_node, &node_buf);
  page_header_t *node_header = (page_header_t *)&node_buf;

  // Case: Remove key and pointer from node
  int remove_result = FAILURE;
  switch (node_header->is_leaf) {
  case LEAF:
    remove_result =
        remove_record_from_node((leaf_page_t *)&node_buf, key, value);
    break;
  case INTERNAL:
    remove_result = remove_entry_from_node((internal_page_t *)&node_buf, key);
    break;
  default:
    perror("delete_entry error: Unknown node type");
    return FAILURE;
  }

  if (remove_result != SUCCESS) {
    return FAILURE;
  }
  file_write_page(target_node, (page_t *)&node_buf);

  // Case: Deletion from the root
  page_t header_buf;
  file_read_page(HEADER_PAGE_POS, &header_buf);
  header_page_t *header_page = (header_page_t *)&header_buf;

  if (target_node == header_page->root_page_num) {
    return adjust_root(header_page->root_page_num);
  }

  // Case: Node stays at or above minimum. (The simple case)
  if (node_header->num_of_keys >= MIN_KEYS) {
    return SUCCESS;
  }

  // Case: Node falls below minimum (underflow)
  return handle_underflow(target_node);
}

void destroy_tree_nodes(pagenum_t root) {
  if (root == PAGE_NULL) {
    return;
  }

  page_t page_buf;
  file_read_page(root, &page_buf);
  page_header_t *page_header = (page_header_t *)&page_buf;

  if (page_header->is_leaf == INTERNAL) {
    internal_page_t *internal_page = (internal_page_t *)&page_buf;

    destroy_tree_nodes(internal_page->one_more_page_num);
    for (int index = 0; index < internal_page->num_of_keys; index++) {
      destroy_tree_nodes(internal_page->entries[index].page_num);
    }
  }

  file_free_page(root);
}

void destroy_tree() {
  page_t header_buf;
  file_read_page(HEADER_PAGE_POS, &header_buf);
  header_page_t *header_page = (header_page_t *)&header_buf;

  pagenum_t root_num = header_page->root_page_num;
  if (root_num != PAGE_NULL) {
    destroy_tree_nodes(root_num);
  }

  header_page->root_page_num = PAGE_NULL;
  file_write_page(HEADER_PAGE_POS, (page_t *)&header_buf);
}
