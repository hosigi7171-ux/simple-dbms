#include "bpt.h"
#include "bpt_internal.h"
#include "bptree.h"
#include "bptree_delete.h"
#include "bptree_find.h"
#include "bptree_insert.h"
#include "bptree_utils.h"
#include "helper_mock.h"
#include "mock_file.h"
#include "page.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define RECORD_CNT 3
#define ENTRY_CNT 4
#define MIN_KEYS 1

#define SUCCESS 0
#define FAILURE -1

#define HEADER_PAGE_NUM HEADER_PAGE_POS
#define PAGE_SIZE 4096
#define PAGE_NULL 0
#define LEAF 1
#define INTERNAL 0

void setUp(void) {
  setup_data_store();
  file_read_page_Stub(MOCK_file_read_page);
  file_write_page_Stub(MOCK_file_write_page);
}
void tearDown(void) {}

/**
 * @brief Root(Leaf)에 키가 하나만 있을 때, 삭제 후 루트가 NULL이 되는지 확인
 */
void test_delete_root_becomes_empty(void) {
  pagenum_t ROOT_NUM = 1;
  const char *DELETE_VALUE = "val5";

  // 1. Initial Setup: Root(P2)
  header_page_t *h0 = (header_page_t *)&MOCK_PAGES[HEADER_PAGE_NUM];
  h0->root_page_num = ROOT_NUM;
  h0->num_of_pages = 3;

  leaf_page_t *l1 = (leaf_page_t *)&MOCK_PAGES[ROOT_NUM];
  l1->parent_page_num = PAGE_NULL;
  l1->is_leaf = LEAF;
  l1->num_of_keys = 1;
  l1->records[0].key = 5;
  strcpy(l1->records[0].value, DELETE_VALUE);

  // Mock Setup

  // P2는 루트이며 키 0개가 되므로 해제되어야 함
  file_free_page_Expect(ROOT_NUM);

  delete_entry(ROOT_NUM, 5, DELETE_VALUE);

  // Verification
  pagenum_t NEW_ROOT = h0->root_page_num;
  TEST_ASSERT_EQUAL_HEX64(PAGE_NULL, NEW_ROOT);

  file_read_page(HEADER_PAGE_NUM, (page_t *)h0);
  TEST_ASSERT_EQUAL_HEX64(PAGE_NULL, h0->root_page_num);
}

/**
 * @brief P4(Target)에서 삭제 후 P3(Neighbor)와 재분배
 */
void test_delete_leaf_redistribution_R_to_L(void) {
  pagenum_t ROOT_NUM = 1;
  pagenum_t P3 = 2; // neighbor(left)
  pagenum_t P4 = 3; // target
  const char *DELETE_VALUE = "val4";

  header_page_t *h0 = (header_page_t *)&MOCK_PAGES[HEADER_PAGE_NUM];
  h0->root_page_num = ROOT_NUM;
  h0->num_of_pages = 5;

  internal_page_t *i2 = (internal_page_t *)&MOCK_PAGES[ROOT_NUM];
  i2->is_leaf = INTERNAL;
  i2->num_of_keys = 1;
  i2->one_more_page_num = P3;
  i2->entries[0].key = 4;
  i2->entries[0].page_num = P4;

  // setup neighbor
  leaf_page_t *l3 = (leaf_page_t *)&MOCK_PAGES[P3];
  l3->parent_page_num = ROOT_NUM;
  l3->is_leaf = LEAF;
  l3->num_of_keys = RECORD_CNT;
  l3->records[0].key = 1;
  strcpy(l3->records[0].value, "val1");
  l3->records[1].key = 2;
  strcpy(l3->records[1].value, "val2");
  l3->records[2].key = 3;
  strcpy(l3->records[2].value, "val3");
  l3->right_sibling_page_num = P4;

  // setup target
  leaf_page_t *l4 = (leaf_page_t *)&MOCK_PAGES[P4];
  l4->parent_page_num = ROOT_NUM;
  l4->is_leaf = LEAF;
  l4->num_of_keys = 1;
  l4->records[0].key = 4;
  strcpy(l4->records[0].value, DELETE_VALUE);
  l4->right_sibling_page_num = PAGE_NULL;

  delete_entry(P4, 4, DELETE_VALUE);

  // Verification
  pagenum_t NEW_ROOT = h0->root_page_num;
  TEST_ASSERT_EQUAL_HEX64(ROOT_NUM, NEW_ROOT);

  // check target status
  leaf_page_t *l4_final = (leaf_page_t *)&MOCK_PAGES[P4];
  TEST_ASSERT_EQUAL_INT(1, l4_final->num_of_keys);
  TEST_ASSERT_EQUAL_INT64(3, l4_final->records[0].key);
  TEST_ASSERT_EQUAL_STRING("val3", l4_final->records[0].value);

  // check neighbor status
  leaf_page_t *l3_final = (leaf_page_t *)&MOCK_PAGES[P3];
  TEST_ASSERT_EQUAL_INT(2, l3_final->num_of_keys);
  TEST_ASSERT_EQUAL_INT64(1, l3_final->records[0].key);
  TEST_ASSERT_EQUAL_STRING("val1", l3_final->records[0].value);
  TEST_ASSERT_EQUAL_INT64(2, l3_final->records[1].key);
  TEST_ASSERT_EQUAL_STRING("val2", l3_final->records[1].value);
}

/**
 * @brief P3(Target)에서 3 삭제 후 P4(Neighbor)와 병합
 * P4 해제 및 P2(Root) 붕괴 후 P2 해제
 */
void test_delete_leaf_coalesce_swap_R_to_L(void) {
  pagenum_t ROOT_NUM = 1;
  pagenum_t P3 = 2; // target
  pagenum_t P4 = 3; // neighbor(right)
  pagenum_t P5 = 4;
  const char *DELETE_VALUE = "val3";

  header_page_t *h0 = (header_page_t *)&MOCK_PAGES[HEADER_PAGE_NUM];
  h0->root_page_num = ROOT_NUM;
  h0->num_of_pages = 6;

  // setup root
  internal_page_t *i2 = (internal_page_t *)&MOCK_PAGES[ROOT_NUM];
  i2->is_leaf = INTERNAL;
  i2->num_of_keys = 1;
  i2->one_more_page_num = P3;
  i2->entries[0].key = 5;
  i2->entries[0].page_num = P4;

  // setup target
  leaf_page_t *l3 = (leaf_page_t *)&MOCK_PAGES[P3];
  l3->parent_page_num = ROOT_NUM;
  l3->is_leaf = LEAF;
  l3->num_of_keys = 1;
  l3->records[0].key = 3;
  strcpy(l3->records[0].value, DELETE_VALUE);
  l3->right_sibling_page_num = P4;

  // setup neighbor
  leaf_page_t *l4 = (leaf_page_t *)&MOCK_PAGES[P4];
  l4->parent_page_num = ROOT_NUM;
  l4->is_leaf = LEAF;
  l4->num_of_keys = 2; // [6, 7]
  l4->records[0].key = 6;
  strcpy(l4->records[0].value, "val6");
  l4->records[1].key = 7;
  strcpy(l4->records[1].value, "val7");
  l4->right_sibling_page_num = P5;

  // EXPECTATION: P4가 병합으로 해제되고, P2(Root)가 붕괴하여 해제됨.
  file_free_page_Expect(P4);
  file_free_page_Expect(ROOT_NUM);

  int result = delete_entry(P3, 3, DELETE_VALUE);
  pagenum_t NEW_ROOT = h0->root_page_num;

  // 3. Verification
  TEST_ASSERT_EQUAL_INT(SUCCESS, result);
  TEST_ASSERT_EQUAL_HEX64(P3, NEW_ROOT);

  // check target status
  leaf_page_t *l3_final = (leaf_page_t *)&MOCK_PAGES[P3];
  TEST_ASSERT_EQUAL_INT(2, l3_final->num_of_keys);
  TEST_ASSERT_EQUAL_INT64(6, l3_final->records[0].key);
  TEST_ASSERT_EQUAL_STRING("val6", l3_final->records[0].value);
  TEST_ASSERT_EQUAL_INT64(7, l3_final->records[1].key);
  TEST_ASSERT_EQUAL_STRING("val7", l3_final->records[1].value);
  TEST_ASSERT_EQUAL_HEX64(P5, l3_final->right_sibling_page_num);

  // P2가 해제되었으므로 P3의 부모 포인터는 PAGE_NULL이어야 함
  TEST_ASSERT_EQUAL_HEX64(PAGE_NULL, l3_final->parent_page_num);
}

/**
 * @brief P4(Target)에서 삭제 후 P3(Neighbor)와 병합 P2(Root) 붕괴 후 P3가
 * 새로운 루트로 승격
 */
void test_delete_internal_coalesce(void) {
  pagenum_t ROOT_NUM = 1;
  pagenum_t P3 = 2; // neighbor(left)
  pagenum_t P4 = 3; // target
  pagenum_t L5 = 4; // P3 one_more_page_num3
  pagenum_t L8 = 7; // P3 entries[0].page_num
  pagenum_t L6 = 5; // P4 one_more_page_num
  pagenum_t L7 = 6; // P4 entries[0].page_num

  int64_t K_PRIME = 50; // separator key in root

  header_page_t *h0 = (header_page_t *)&MOCK_PAGES[HEADER_PAGE_NUM];
  h0->root_page_num = ROOT_NUM;
  h0->num_of_pages = 8;

  internal_page_t *i1 = (internal_page_t *)&MOCK_PAGES[ROOT_NUM];
  i1->is_leaf = INTERNAL;
  i1->num_of_keys = 1;
  i1->one_more_page_num = P3;
  i1->entries[0].key = K_PRIME; // 50
  i1->entries[0].page_num = P4;

  // setup neighbor
  internal_page_t *i3 = (internal_page_t *)&MOCK_PAGES[P3];
  i3->parent_page_num = ROOT_NUM;
  i3->is_leaf = INTERNAL;
  i3->num_of_keys = 1; // [10]
  i3->one_more_page_num = L5;
  i3->entries[0].key = 10;
  i3->entries[0].page_num = L8;

  // setup target
  internal_page_t *i4 = (internal_page_t *)&MOCK_PAGES[P4];
  i4->parent_page_num = ROOT_NUM;
  i4->is_leaf = INTERNAL;
  i4->num_of_keys = 1;
  i4->one_more_page_num = L6;
  i4->entries[0].key = 60;
  i4->entries[0].page_num = L7;

  // Dummy Leaf Pages (Child pages)
  page_header_t *l5 = (page_header_t *)&MOCK_PAGES[L5];
  l5->parent_page_num = P3;
  page_header_t *l8 = (page_header_t *)&MOCK_PAGES[L8];
  l8->parent_page_num = P3;
  page_header_t *l6 = (page_header_t *)&MOCK_PAGES[L6];
  l6->parent_page_num = P4;
  page_header_t *l7 = (page_header_t *)&MOCK_PAGES[L7];
  l7->parent_page_num = P4;

  // Expected Call Order:
  file_free_page_Expect(P4);
  file_free_page_Expect(ROOT_NUM);

  delete_entry(P4, 60, NULL);

  // Verification
  pagenum_t NEW_ROOT = h0->root_page_num;
  TEST_ASSERT_EQUAL_HEX64(P3, NEW_ROOT);

  // check target status
  internal_page_t *i3_final = (internal_page_t *)&MOCK_PAGES[P3];
  TEST_ASSERT_EQUAL_INT(2, i3_final->num_of_keys);
  TEST_ASSERT_EQUAL_INT64(10, i3_final->entries[0].key);
  TEST_ASSERT_EQUAL_INT64(K_PRIME, i3_final->entries[1].key); // 50

  TEST_ASSERT_EQUAL_HEX64(L6, i3_final->entries[1].page_num);

  TEST_ASSERT_EQUAL_HEX64(PAGE_NULL, i3_final->parent_page_num);

  page_header_t *l6_final = (page_header_t *)&MOCK_PAGES[L6];
  TEST_ASSERT_EQUAL_HEX64(P3, l6_final->parent_page_num);

  // l7은 체크 안해도 될듯
  // 원래라면 free 되었어야 하는 페이지지만 이 테스트케이스에선 중간 key만
  // 삭제해서 fre 과정이 누락됨
  page_header_t *l7_final = (page_header_t *)&MOCK_PAGES[L7];
}

/**
 * @brief P4(Target)에서 50 삭제 후 num_of_keys=0 이웃 노드 P3(Neighbor)가 꽉
 * 찼으므로 재분배를 시도하여 P4를 살림
 *
 */
void test_delete_leaf_redistribute_when_target_is_empty(void) {
  pagenum_t ROOT_NUM = 1;
  pagenum_t P3 = 2; // neighbor(left)
  pagenum_t P4 = 3; // target
  const char *DELETE_VALUE = "val50";

  header_page_t *h0 = (header_page_t *)&MOCK_PAGES[HEADER_PAGE_NUM];
  h0->root_page_num = ROOT_NUM;
  h0->num_of_pages = 4;

  internal_page_t *i2 = (internal_page_t *)&MOCK_PAGES[ROOT_NUM];
  i2->is_leaf = INTERNAL;
  i2->num_of_keys = 1;
  i2->one_more_page_num = P3;
  i2->entries[0].key = 50;
  i2->entries[0].page_num = P4;

  // setup neighbor
  leaf_page_t *l3 = (leaf_page_t *)&MOCK_PAGES[P3];
  l3->parent_page_num = ROOT_NUM;
  l3->is_leaf = LEAF;
  l3->num_of_keys = RECORD_CNT;
  l3->records[0].key = 10;
  strcpy(l3->records[0].value, "val10");
  l3->records[1].key = 20;
  strcpy(l3->records[1].value, "val20");
  l3->records[2].key = 30;
  strcpy(l3->records[2].value, "val30");
  l3->right_sibling_page_num = P4;

  // setup target
  leaf_page_t *l4 = (leaf_page_t *)&MOCK_PAGES[P4];
  l4->parent_page_num = ROOT_NUM;
  l4->is_leaf = LEAF;
  l4->num_of_keys = 1;
  l4->records[0].key = 50;
  strcpy(l4->records[0].value, DELETE_VALUE);
  l4->right_sibling_page_num = PAGE_NULL;

  // EXPECTATION: No pages should be freed (Redistribution).
  // file_free_page_Expect(...) 없음

  delete_entry(P4, 50, DELETE_VALUE);

  // Verification
  pagenum_t NEW_ROOT = h0->root_page_num;
  TEST_ASSERT_EQUAL_HEX64(ROOT_NUM, NEW_ROOT);

  // check target status
  leaf_page_t *l4_final = (leaf_page_t *)&MOCK_PAGES[P4];
  TEST_ASSERT_EQUAL_INT(1, l4_final->num_of_keys);
  TEST_ASSERT_EQUAL_INT64(30, l4_final->records[0].key);
  TEST_ASSERT_EQUAL_STRING("val30", l4_final->records[0].value);

  // check neighbor status
  leaf_page_t *l3_final = (leaf_page_t *)&MOCK_PAGES[P3];
  TEST_ASSERT_EQUAL_INT(2, l3_final->num_of_keys);
  TEST_ASSERT_EQUAL_INT64(10, l3_final->records[0].key);
  TEST_ASSERT_EQUAL_STRING("val10", l3_final->records[0].value);
  TEST_ASSERT_EQUAL_INT64(20, l3_final->records[1].key);
  TEST_ASSERT_EQUAL_STRING("val20", l3_final->records[1].value);

  // check parent status
  internal_page_t *i2_final = (internal_page_t *)&MOCK_PAGES[ROOT_NUM];
  TEST_ASSERT_EQUAL_INT64(30, i2_final->entries[0].key);
  TEST_ASSERT_EQUAL_HEX64(P4, i2_final->entries[0].page_num);
}
