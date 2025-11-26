#ifndef PAGE_H
#define PAGE_H

#include <stdint.h>

typedef uint64_t pagenum_t;
typedef uint64_t magicnum_t;

#define PAGE_SIZE 4096
#define HEADER_PAGE_RESERVED 4072
#ifndef NON_HEADER_PAGE_RESERVED
#define NON_HEADER_PAGE_RESERVED 104
#endif
#define VALUE_SIZE 120
#ifndef RECORD_CNT
#define RECORD_CNT 31
#endif
#ifndef ENTRY_CNT
#define ENTRY_CNT 248
#endif
#define UNUSED_SIZE 4088
#define LEAF 1
#define INTERNAL 0
#define PAGE_NULL 0
#define HEADER_PAGE_POS 0

typedef struct {
  pagenum_t free_page_num;
  pagenum_t root_page_num;
  pagenum_t num_of_pages;
  char reserved[HEADER_PAGE_RESERVED]; // not used
} header_page_t;

// free page
typedef struct {
  pagenum_t next_free_page_num;
  char unused[UNUSED_SIZE];
} free_page_t;

// key-value record
typedef struct {
  int64_t key;
  char value[VALUE_SIZE];
} record_t;

// key-pagenum entry
typedef struct {
  int64_t key;
  pagenum_t page_num;
} entry_t;

// leaf page
typedef struct {
  // header
  pagenum_t parent_page_num;
  uint32_t is_leaf; // 1
  uint32_t num_of_keys;
  char reserved[NON_HEADER_PAGE_RESERVED]; // not used
  pagenum_t right_sibling_page_num;        // if rihgtmost, 0

  record_t records[RECORD_CNT];
} leaf_page_t;

// internal page
typedef struct {
  // header
  pagenum_t parent_page_num;
  int32_t is_leaf; // 0
  int32_t num_of_keys;
  char reserved[NON_HEADER_PAGE_RESERVED]; // not used
  pagenum_t one_more_page_num; // leftmost page num to know key ranges

  entry_t entries[ENTRY_CNT];
} internal_page_t;

// page header - for referencing header
typedef struct {
  pagenum_t parent_page_num;
  uint32_t is_leaf;
  uint32_t num_of_keys;
  char reserved[NON_HEADER_PAGE_RESERVED];
} page_header_t;

// raw page for type casting
typedef struct {
  char data[PAGE_SIZE];
} page_t;

#endif