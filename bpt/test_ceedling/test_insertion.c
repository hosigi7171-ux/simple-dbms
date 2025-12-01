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
#include "unity.h"

#define RECORD_CNT 2
#define ENTRY_CNT 16
#define NON_HEADER_PAGE_RESERVED 3816
#define LEAF_ORDER RECORD_CNT + 1
#define INTERNAL_ORDER ENTRY_CNT + 1

#define HEADER_PAGE_NUM HEADER_PAGE_POS

void setUp(void) {
  setup_data_store();
  file_read_page_Stub(MOCK_file_read_page);
  file_write_page_Stub(MOCK_file_write_page);
  file_alloc_page_Stub(MOCK_file_alloc_page);
  file_free_page_Stub(MOCK_file_free_page);
  init_header_page_for_mock();
}

void tearDown(void) {}

/**
 * test cases
 */

/**
 * @brief Case 1: 트리가 비어있을 때 첫 번째 키를 삽입하는 경우 (start_new_tree)
 */
void test_insert_start_new_tree(void) {
  int64_t key = 10;
  char value[] = "Value_10";

  // 초기 상태 체크: 루트 없음 (P0)
  header_page_t initial_header = get_header_page();
  TEST_ASSERT_EQUAL_INT64(PAGE_NULL, initial_header.root_page_num);
  TEST_ASSERT_EQUAL_UINT64(1, initial_header.num_of_pages);

  TEST_ASSERT_EQUAL(SUCCESS, insert(key, value));

  // 결과 검증
  // 헤더 페이지 검증: root_page_num이 새로 할당된 페이지 (P1)로
  // 업데이트되었는지 확인
  header_page_t final_header = get_header_page();
  TEST_ASSERT_EQUAL_INT64(1, final_header.root_page_num);
  TEST_ASSERT_EQUAL_UINT64(2, final_header.num_of_pages);

  // 루트 페이지 (P2) 검증: leaf 속성과 내용 확인
  leaf_page_t root_page = get_leaf_page(final_header.root_page_num);
  TEST_ASSERT_EQUAL(LEAF, root_page.is_leaf);
  TEST_ASSERT_EQUAL_INT(1, root_page.num_of_keys);
  TEST_ASSERT_EQUAL_INT64(PAGE_NULL, root_page.parent_page_num);
  TEST_ASSERT_EQUAL_INT64(key, root_page.records[0].key);
  TEST_ASSERT_EQUAL_STRING(value, root_page.records[0].value);
}

/**
 * @brief Case 2: 트리가 존재할 때, 기존 leaf에 공간이 있어 단순 삽입되는
 * 경우 (Max 2개 키 중 2개 채우기)
 */
void test_insert_into_leaf_simple(void) {
  test_insert_start_new_tree();

  int64_t key_new = 5;
  char value_new[] = "Value_05";
  TEST_ASSERT_EQUAL(SUCCESS, insert(key_new, value_new));

  // 결과 검증
  leaf_page_t leaf = get_leaf_page(1);
  TEST_ASSERT_EQUAL_INT(2, leaf.num_of_keys);
  TEST_ASSERT_EQUAL_INT64(PAGE_NULL, leaf.parent_page_num);

  TEST_ASSERT_EQUAL_INT64(5, leaf.records[0].key);
  TEST_ASSERT_EQUAL_STRING(value_new, leaf.records[0].value);
  TEST_ASSERT_EQUAL_INT64(10, leaf.records[1].key);
  TEST_ASSERT_EQUAL_STRING("Value_10", leaf.records[1].value);

  // 중복 삽입 시도
  TEST_ASSERT_EQUAL(FAILURE, insert(key_new, value_new));
}

/**
 * @brief Case 3: 단순 검색 (find_leaf, find)
 */
void test_find_simple(void) {
  test_insert_into_leaf_simple();
  char result_buf[VALUE_SIZE];

  // 존재하는 키 검색
  TEST_ASSERT_EQUAL(SUCCESS, find(5, result_buf));
  TEST_ASSERT_EQUAL_STRING("Value_05", result_buf);

  TEST_ASSERT_EQUAL(SUCCESS, find(10, result_buf));
  TEST_ASSERT_EQUAL_STRING("Value_10", result_buf);

  // 존재하지 않는 키 검색
  TEST_ASSERT_EQUAL(FAILURE, find(7, result_buf));
}

/**
 * @brief Case 4: Leaf 분할 및 New Root 생성
 * (insert_into_leaf_after_splitting)
 * - Max keys (2)를 채우고, 3번째 키 삽입 시 분할 발생
 * - cut(3) = 2 (Old: 1개, New: 2개)
 * - 승격 키 (New Key): 새 리프의 첫 번째 키 (3)
 */
void test_insert_split_leaf_and_new_root(void) {
  for (int64_t i = 1; i <= RECORD_CNT; i++) {
    char value[10];
    sprintf(value, "V_%lld", i);
    insert(i, value);
  }

  leaf_page_t leaf_full = get_leaf_page(1);
  TEST_ASSERT_EQUAL_INT(RECORD_CNT, leaf_full.num_of_keys);
  TEST_ASSERT_EQUAL_INT64(1, leaf_full.records[0].key);
  TEST_ASSERT_EQUAL_INT64(RECORD_CNT, leaf_full.records[RECORD_CNT - 1].key);

  // split 발생시키기
  int64_t key_new = 3;
  char value_new[] = "V_3";
  TEST_ASSERT_EQUAL(SUCCESS, insert(key_new, value_new));

  // 결과 검증
  // 헤더 검증: 새 루트 페이지 (P4) 설정 확인
  header_page_t final_header = get_header_page();
  // Pages: 0(유휴), 1(헤더), 2(Old Leaf), 3(New Leaf), 4(New Root)
  TEST_ASSERT_EQUAL_UINT64(4, final_header.num_of_pages);
  TEST_ASSERT_EQUAL_INT64(3, final_header.root_page_num);

  // Old Leaf (P2) 검증
  leaf_page_t old_leaf = get_leaf_page(1);
  TEST_ASSERT_EQUAL_INT(1, old_leaf.num_of_keys);
  TEST_ASSERT_EQUAL_INT64(3, old_leaf.parent_page_num);
  TEST_ASSERT_EQUAL_INT64(2, old_leaf.right_sibling_page_num);
  TEST_ASSERT_EQUAL_INT64(1, old_leaf.records[0].key);

  // New Leaf (P3) 검증
  leaf_page_t new_leaf = get_leaf_page(2);
  TEST_ASSERT_EQUAL_INT(2, new_leaf.num_of_keys);
  TEST_ASSERT_EQUAL_INT64(3, new_leaf.parent_page_num);
  TEST_ASSERT_EQUAL_INT64(PAGE_NULL, new_leaf.right_sibling_page_num);
  TEST_ASSERT_EQUAL_INT64(2, new_leaf.records[0].key);
  TEST_ASSERT_EQUAL_INT64(3, new_leaf.records[1].key);

  // New Root (P4) 검증
  internal_page_t new_root = get_internal_page(3);
  TEST_ASSERT_EQUAL(INTERNAL, new_root.is_leaf);
  TEST_ASSERT_EQUAL_INT(1, new_root.num_of_keys);
  TEST_ASSERT_EQUAL_INT64(PAGE_NULL, new_root.parent_page_num);

  TEST_ASSERT_EQUAL_INT64(1, new_root.one_more_page_num);
  TEST_ASSERT_EQUAL_INT64(2, new_root.entries[0].key);
  TEST_ASSERT_EQUAL_INT64(2, new_root.entries[0].page_num);
}

/**
 * @brief Case 5: Root가 이미 존재하고, Internal Node 분할이 없는 단순 Internal
 * Node 삽입 (Entry 2개 채우기)
 * - Case 4 결과: Root P4 (Key 2) -> [P2 (1), P3 (2, 3)]
 * - 2번째 Leaf Split 후: Root P4 (Key 2, 3) -> [P2 (1), P3 (2), P5 (3, 4)]
 * - Leaf Max keys = 2이므로, 이 테스트는 Internal Node에 Entry가 2개 채워짐
 */
void test_insert_into_internal_node_simple(void) {
  // Case 4까지의 상태: Root P4 (Key 2) -> [P2 (1), P3 (2, 3)]
  test_insert_split_leaf_and_new_root();

  int64_t key_to_split = 4;
  char value_to_split[] = "V_4";
  TEST_ASSERT_EQUAL(SUCCESS, insert(key_to_split, value_to_split));

  // 검증
  // 헤더 검증: 새 페이지 (P5) 할당 확인
  header_page_t final_header = get_header_page();
  // Pages: ..., 4(Root), 5(New Leaf)
  TEST_ASSERT_EQUAL_UINT64(5, final_header.num_of_pages);

  // Old Leaf (P3) 검증 (2)
  leaf_page_t old_leaf = get_leaf_page(2);
  TEST_ASSERT_EQUAL_INT(1, old_leaf.num_of_keys);
  TEST_ASSERT_EQUAL_INT64(3, old_leaf.parent_page_num);
  TEST_ASSERT_EQUAL_INT64(4, old_leaf.right_sibling_page_num);
  TEST_ASSERT_EQUAL_INT64(2, old_leaf.records[0].key);

  // New Leaf (P5) 검증 (3, 4)
  leaf_page_t new_leaf = get_leaf_page(4);
  TEST_ASSERT_EQUAL_INT(2, new_leaf.num_of_keys);
  TEST_ASSERT_EQUAL_INT64(3, new_leaf.parent_page_num);
  TEST_ASSERT_EQUAL_INT64(PAGE_NULL, new_leaf.right_sibling_page_num);
  TEST_ASSERT_EQUAL_INT64(3, new_leaf.records[0].key);
  TEST_ASSERT_EQUAL_INT64(4, new_leaf.records[1].key);

  // Root (P4) 검증
  internal_page_t root = get_internal_page(3);
  TEST_ASSERT_EQUAL(INTERNAL, root.is_leaf);
  TEST_ASSERT_EQUAL_INT(2, root.num_of_keys);
  TEST_ASSERT_EQUAL_INT64(PAGE_NULL, root.parent_page_num);

  TEST_ASSERT_EQUAL_INT64(2, root.entries[0].key);
  TEST_ASSERT_EQUAL_INT64(2, root.entries[0].page_num);

  TEST_ASSERT_EQUAL_INT64(3, root.entries[1].key);
  TEST_ASSERT_EQUAL_INT64(4, root.entries[1].page_num);

  TEST_ASSERT_EQUAL_INT64(1, root.one_more_page_num);
}

/**
 * @brief Case 6: Internal Node 분할 및 승격 (insert_into_node_after_splitting)
 */
void test_insert_split_internal_node(void) {
  // Case 5 결과 : P4는 현재 2 entries (키 2, 3). 14개의 엔트리 추가 필요.
  test_insert_into_internal_node_simple();
  // P2(1), P3(2), P5(3,4). Root P4(2,3

  // 14개의 엔트리 추가를 위해 14번의 split leaf
  int64_t i = 5;
  const int PROMOTIONS_NEEDED = 14;

  for (int j = 0; j < PROMOTIONS_NEEDED; j++) {
    insert(i, "V");
    i++;
  }
  // Root P4가 16 entries로 가득 찼는지 확인
  internal_page_t root_full = get_internal_page(3);
  TEST_ASSERT_EQUAL_INT(16, root_full.num_of_keys);
  TEST_ASSERT_EQUAL_INT64(17, root_full.entries[15].key);

  // Root 분할 (3단계 트리 생성)
  // 키 19 삽입 -> Leaf Split (Promoted Key 18)
  // Root P4가 17 entries (키 2~18)로 오버플로우 -> Root Split
  int64_t key_to_split_root = 19;
  TEST_ASSERT_EQUAL(SUCCESS, insert(key_to_split_root, "V_19"));

  // 페이지 번호 검증 및 새 Root (P22), New Internal (P21) 확인
  header_page_t header = get_header_page();
  pagenum_t new_root_num = header.root_page_num;
  pagenum_t old_root_num = 3;
  pagenum_t new_internal_num = 20;
  TEST_ASSERT_EQUAL_INT64(21, new_root_num);

  // Root Split 검증: cut(17) = 9.
  // Old Internal (P4): 8 entries (키 2, 3, ..., 9).
  // New Internal (P21): 8 entries (키 11, 12, ..., 18).
  // 승격 키: P4의 9번째 키 (10)

  internal_page_t new_root = get_internal_page(new_root_num);
  TEST_ASSERT_EQUAL_INT(1, new_root.num_of_keys);
  TEST_ASSERT_EQUAL_INT64(old_root_num, new_root.one_more_page_num);
  TEST_ASSERT_EQUAL_INT64(10, new_root.entries[0].key);
  TEST_ASSERT_EQUAL_INT64(new_internal_num, new_root.entries[0].page_num);

  // 내부 노드(P21)를 다시 채우기 (8 -> 16 entries)
  i = key_to_split_root + 1;
  const int PROMOTIONS_FOR_P21 = 8;

  for (int j = 0; j < PROMOTIONS_FOR_P21; j++) {
    insert(i, "V");
    i++;
  }

  // Internal P21이 16 entries로 가득 찼는지 확인
  internal_page_t p21_full = get_internal_page(new_internal_num);
  TEST_ASSERT_EQUAL_INT(16, p21_full.num_of_keys);
  TEST_ASSERT_EQUAL_INT64(26, p21_full.entries[15].key);

  // 내부 노드(P21) 분할 강제 (Key 28 삽입) -> split internal test
  int64_t key_to_split_internal = 28;
  TEST_ASSERT_EQUAL(SUCCESS, insert(key_to_split_internal, "V_28"));

  // New Internal Node(P32) 할당 확인
  header = get_header_page();
  pagenum_t p31_new_internal_num = 31;
  TEST_ASSERT_EQUAL_INT64(p31_new_internal_num, header.num_of_pages - 1);

  // Root(P20) 검증
  internal_page_t final_root = get_internal_page(new_root_num);
  TEST_ASSERT_EQUAL_INT(2, final_root.num_of_keys);
  TEST_ASSERT_EQUAL_INT64(10, final_root.entries[0].key);
  TEST_ASSERT_EQUAL_INT64(19, final_root.entries[1].key);
  TEST_ASSERT_EQUAL_INT64(p31_new_internal_num, final_root.entries[1].page_num);

  // Old Internal Node(P21) 검증
  internal_page_t p21_after_split = get_internal_page(new_internal_num);
  TEST_ASSERT_EQUAL_INT(8, p21_after_split.num_of_keys);
  TEST_ASSERT_EQUAL_INT64(new_root_num, p21_after_split.parent_page_num);
  TEST_ASSERT_EQUAL_INT64(18, p21_after_split.entries[7].key);

  // New Internal Node(P31) 검증
  internal_page_t p31_new = get_internal_page(p31_new_internal_num);
  TEST_ASSERT_EQUAL_INT(8, p31_new.num_of_keys);
  TEST_ASSERT_EQUAL_INT64(new_root_num, p31_new.parent_page_num);
  TEST_ASSERT_EQUAL_INT64(20, p31_new.entries[0].key);
}

/**
 * @brief Case 7: cut 함수 검증
 */
void test_cut(void) {
  TEST_ASSERT_EQUAL_INT(2, cut(4));
  TEST_ASSERT_EQUAL_INT(8, cut(16));

  TEST_ASSERT_EQUAL_INT(2, cut(3));
  TEST_ASSERT_EQUAL_INT(9, cut(17));
}