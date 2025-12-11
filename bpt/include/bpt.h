#ifndef __BPT_H__
#define __BPT_H__

#include "common_config.h"
#include "page.h"

// Uncomment the line below if you are compiling on Windows.
// #define WINDOWS
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <cinttypes>
#ifdef WINDOWS
#define bool char
#define false 0
#define true 1
#endif

#ifndef LEAF_ORDER
#define LEAF_ORDER RECORD_CNT + 1
#endif
#ifndef INTERNAL_ORDER
#define INTERNAL_ORDER ENTRY_CNT + 1
#endif
#define CANNOT_ROOT -2
#define MAX_RANGE_SIZE 10000  // for finding range
#define MIN_KEYS 1            // for delayed merge

// Constants for printing part or all of the GPL license.
#define LICENSE_FILE "LICENSE.txt"
#define LICENSE_WARRANTEE 0
#define LICENSE_WARRANTEE_START 592
#define LICENSE_WARRANTEE_END 624
#define LICENSE_CONDITIONS 1
#define LICENSE_CONDITIONS_START 70
#define LICENSE_CONDITIONS_END 625

// TYPES.

// GLOBALS.

/* The queue is used to print the tree in
 * level order, starting from the root
 * printing each entire rank on a separate
 * line, finishing with the leaves.
 */
typedef struct queue {
  pagenum_t page_num;
  int level;
  struct queue* next;
} queue;

// FUNCTION PROTOTYPES.

// Output and utility.

void license_notice(void);
void print_license(int licence_part);
void usage(void);
void enqueue(pagenum_t new_page_num, int level);
queue* dequeue(void);
int height(int fd, tableid_t table_id, pagenum_t header_page_num);
void print_leaves(int fd, tableid_t table_id);
void print_tree(int fd, tableid_t table_id);
void find_and_print(int fd, tableid_t table_id, int64_t key);
int find_and_print_range(int fd, tableid_t table_id, int64_t key_start,
                         int64_t key_end);
int find_range(int fd, tableid_t table_id, int64_t key_start, int64_t key_end,
               int64_t returned_keys[], pagenum_t returned_pages[],
               int returned_indices[]);
pagenum_t find_leaf(int fd, tableid_t table_id, int64_t key);
int find(int fd, tableid_t table_id, int64_t key, char* result_buf);
int cut(int length);
void copy_value(char* dest, const char* src, size_t size);

// Insertion
pagenum_t make_node(int fd, tableid_t table_id, uint32_t isleaf);
int get_index_after_left_child(page_t* parent_buffer, pagenum_t left_num);
int insert_into_leaf(int fd, tableid_t table_id, pagenum_t leaf_num,
                     leaf_page_t* leaf_page, int64_t key, char* value);
int insert_into_leaf_after_splitting(int fd, tableid_t table_id, pagenum_t leaf,
                                     int64_t key, char* value);
int insert_into_node(int fd, tableid_t table_id, pagenum_t parent,
                     int64_t left_index, int64_t key, pagenum_t right);
int insert_into_node_after_splitting(int fd, tableid_t table_id,
                                     pagenum_t parent, int64_t left_index,
                                     int64_t key, pagenum_t right);
int insert_into_parent(int fd, tableid_t table_id, pagenum_t left, int64_t key,
                       pagenum_t right);
int insert_into_new_root(int fd, tableid_t table_id, pagenum_t left,
                         int64_t key, pagenum_t right);
int start_new_tree(int fd, tableid_t table_id, int64_t key, char* value);
void init_header_page(int fd, tableid_t table_id);
void link_header_page(int fd, tableid_t table_id, pagenum_t root);
int bpt_insert(int fd, tableid_t table_id, int64_t key, char* value);

// Deletion.
int get_kprime_index(int fd, tableid_t table_id, pagenum_t target_node,
                     internal_page_t* parent_page);
int remove_record_from_node(leaf_page_t* target_page, int64_t key,
                            const char* value);
int remove_entry_from_node(internal_page_t* target_page, int64_t key);
pagenum_t adjust_root(int fd, tableid_t table_id, pagenum_t root);
int coalesce_nodes(int fd, tableid_t table_id, pagenum_t target_num,
                   pagenum_t neighbor_num, int kprime_index_from_get,
                   int64_t k_prime);
int redistribute_nodes(int fd, tableid_t table_id, pagenum_t target_num,
                       pagenum_t neighbor_num, int kprime_index_from_get,
                       int k_prime_index, int k_prime);
int delete_entry(int fd, tableid_t table_id, pagenum_t target_node, int64_t key,
                 const char* value);
int bpt_delete(int fd, tableid_t table_id, int64_t key);

void destroy_tree_nodes(int fd, tableid_t table_id, pagenum_t root);
void destroy_tree(int fd, tableid_t table_id);

#endif /* __BPT_H__*/
