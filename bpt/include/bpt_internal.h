#ifndef BPT_INTERNAL_H
#define BPT_INTERNAL_H

#include "file.h"

/**
 * Declaration of helper functions used only bpt
 */
record_t *prepare_records_for_split(leaf_page_t *leaf_page, int64_t key,
                                    const char *value);
int64_t distribute_records_to_leaves(leaf_page_t *leaf_page,
                                     leaf_page_t *new_leaf_page,
                                     record_t *temp_records,
                                     pagenum_t new_leaf_num);
entry_t *prepare_entries_for_split(internal_page_t *old_node_page,
                                   int64_t left_index, int64_t key,
                                   pagenum_t right);
int64_t distribute_entries_and_update_children(pagenum_t old_node_num,
                                               internal_page_t *old_node_page,
                                               pagenum_t new_node_num,
                                               internal_page_t *new_node_page,
                                               entry_t *temp_entries);
void coalesce_internal_nodes(page_t *neighbor_buf, page_t *target_buf,
                             int neighbor_num, int64_t k_prime);
void coalesce_leaf_nodes(page_t *neighbor_buf, page_t *target_buf);
void redistribute_from_left(pagenum_t target_num, page_t *target_buf,
                            page_t *neighbor_buf, internal_page_t *parent_page,
                            int k_prime_index, int k_prime);
void redistribute_internal_from_left(pagenum_t target_num, page_t *target_buf,
                                     page_t *neighbor_buf,
                                     internal_page_t *parent_page,
                                     int k_prime_index, int k_prime);
void redistribute_leaf_from_left(page_t *target_buf, page_t *neighbor_buf,
                                 internal_page_t *parent_page,
                                 int k_prime_index);
void redistribute_from_right(pagenum_t target_num, page_t *target_buf,
                             page_t *neighbor_buf, internal_page_t *parent_page,
                             int k_prime_index, int k_prime);
void redistribute_internal_from_right(pagenum_t target_num, page_t *target_buf,
                                      page_t *neighbor_buf,
                                      internal_page_t *parent_page,
                                      int k_prime_index, int k_prime);
void redistribute_leaf_from_right(page_t *target_buf, page_t *neighbor_buf,
                                  internal_page_t *parent_page,
                                  int k_prime_index);
int find_neighbor_and_kprime(pagenum_t target_node,
                             internal_page_t *parent_page,
                             page_header_t *target_header,
                             pagenum_t *neighbor_num_out,
                             int *k_prime_key_index_out);
int handle_underflow(pagenum_t target_node);

#endif