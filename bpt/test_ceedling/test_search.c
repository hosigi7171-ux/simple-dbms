#include "bpt.h"
#include "bpt_internal.h"
#include "bptree.h"
#include "bptree_delete.h"
#include "bptree_find.h"
#include "bptree_insert.h"
#include "bptree_utils.h"
#include "helper_mock.h"
#include "mock_file.h"
#include "unity.h"
#include <stdio.h>
#include <string.h>

static char captured_output[4096];
static FILE *original_stdout;
static FILE *memstream;

static void start_capture(void) {
  memset(captured_output, 0, sizeof(captured_output));
  original_stdout = stdout;
  memstream = fmemopen(captured_output, sizeof(captured_output), "w");
  stdout = memstream;
}

static void stop_capture(void) {
  fflush(memstream);
  stdout = original_stdout;
  fclose(memstream);
}

void setUp() {
  setup_data_store();
  init_header_page_for_mock();
  file_read_page_Stub(MOCK_file_read_page);
  file_write_page_Stub(MOCK_file_write_page);
}

void tearDown() {}

void test_find_range() {
  header_page_t header = get_header_page();
  header.root_page_num = 3;
  MOCK_file_write_page(HEADER_PAGE_POS, (page_t *)&header, 0);

  page_t page3 = {0};
  leaf_page_t *leaf3 = (leaf_page_t *)&page3;
  leaf3->is_leaf = LEAF;
  leaf3->num_of_keys = 3;
  leaf3->records[0].key = 10;
  leaf3->records[1].key = 20;
  leaf3->records[2].key = 30;
  leaf3->right_sibling_page_num = PAGE_NULL;
  MOCK_file_write_page(3, &page3, 0);

  int64_t keys[10];
  pagenum_t pages[10];
  int idx[10];

  int n = find_range(15, 35, keys, pages, idx);

  TEST_ASSERT_EQUAL(2, n);
  TEST_ASSERT_EQUAL(20, keys[0]);
  TEST_ASSERT_EQUAL(3, pages[0]);
  TEST_ASSERT_EQUAL(1, idx[0]);
}

void test_find_and_print_range_output() {
  header_page_t header = get_header_page();
  header.root_page_num = 3;
  MOCK_file_write_page(HEADER_PAGE_POS, (page_t *)&header, 0);

  page_t page3 = {0};
  leaf_page_t *leaf = (leaf_page_t *)&page3;
  leaf->is_leaf = LEAF;
  leaf->num_of_keys = 2;
  leaf->records[0].key = 100;
  strcpy(leaf->records[0].value, "AAA");
  leaf->records[1].key = 150;
  strcpy(leaf->records[1].value, "BBB");
  leaf->right_sibling_page_num = PAGE_NULL;
  MOCK_file_write_page(3, &page3, 0);

  start_capture();
  find_and_print_range(90, 120);
  stop_capture();

  TEST_ASSERT_NOT_EQUAL(NULL, strstr(captured_output, "found 1 records"));
  TEST_ASSERT_NOT_EQUAL(NULL, strstr(captured_output, "100"));
  TEST_ASSERT_NOT_EQUAL(NULL, strstr(captured_output, "AAA"));
}

void test_print_tree_output() {
  header_page_t header = get_header_page();
  header.root_page_num = 3;
  MOCK_file_write_page(HEADER_PAGE_POS, (page_t *)&header, 0);

  page_t page3 = {0};
  leaf_page_t *leaf = (leaf_page_t *)&page3;
  leaf->is_leaf = LEAF;
  leaf->num_of_keys = 2;
  leaf->records[0].key = 10;
  leaf->records[1].key = 20;
  leaf->right_sibling_page_num = PAGE_NULL;
  MOCK_file_write_page(3, &page3, 0);

  start_capture();
  print_tree();
  stop_capture();

  TEST_ASSERT_NOT_EQUAL(NULL, strstr(captured_output, "10"));
  TEST_ASSERT_NOT_EQUAL(NULL, strstr(captured_output, "20"));
}
