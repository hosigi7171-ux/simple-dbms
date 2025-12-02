#include <algorithm>  // std::max
#include <cstdlib>
#include <cstring>
#include <iostream>

#include "FileMock.h"
#include "bpt.h"
#include "buf_mgr.h"
#include "gtest/gtest.h"

extern buffer_manager_t buf_mgr;

static void init_buffer_manager(int buf_size) {
  buf_mgr.frames_size = buf_size;
  buf_mgr.frames =
      (buf_ctl_block_t*)std::calloc(buf_size, sizeof(buf_ctl_block_t));
  buf_mgr.clock_hand = 0;

  for (int i = 0; i < buf_size; ++i) {
    buf_mgr.frames[i].frame = std::calloc(1, PAGE_SIZE);
    buf_mgr.frames[i].table_id = INVALID_TABLE_ID;
    buf_mgr.frames[i].page_num = PAGE_NULL;
    buf_mgr.frames[i].is_dirty = false;
    buf_mgr.frames[i].pin_count = 0;
    buf_mgr.frames[i].ref_bit = false;
  }

  constexpr int MAX_TABLES = MAX_TABLE_COUNT + 1;
  for (int i = 0; i < MAX_TABLES; ++i) {
    buf_mgr.page_table[i].clear();
  }
}

static void shutdown_buffer_manager() {
  for (int i = 0; i < buf_mgr.frames_size; ++i) {
    std::free(buf_mgr.frames[i].frame);
  }
  std::free(buf_mgr.frames);

  constexpr int MAX_TABLES = 11;
  for (int i = 0; i < MAX_TABLES; ++i) {
    buf_mgr.page_table[i].clear();
  }
}

static leaf_page_t get_leaf_page(int fd, tableid_t table_id,
                                 pagenum_t pagenum) {
  page_t* page = read_buffer(fd, table_id, pagenum);
  leaf_page_t leaf;
  std::memcpy(&leaf, page, PAGE_SIZE);
  unpin(table_id, pagenum);
  return leaf;
}

static internal_page_t get_internal_page(int fd, tableid_t table_id,
                                         pagenum_t pagenum) {
  page_t* page = read_buffer(fd, table_id, pagenum);
  internal_page_t internal;
  std::memcpy(&internal, page, PAGE_SIZE);
  unpin(table_id, pagenum);
  return internal;
}

static header_page_t get_header_page(int fd, tableid_t table_id) {
  header_page_t* header_ptr = read_header_page(fd, table_id);
  header_page_t header;
  std::memcpy(&header, header_ptr, sizeof(header_page_t));
  unpin(table_id, HEADER_PAGE_POS);
  return header;
}

// GTest Fixture 정의
class HardInsertTest : public ::testing::Test {
 protected:
  tableid_t TEST_TID = 1;
  int BUFFER_SIZE = 100;

  void SetUp() override {
    FileMock::setup_data_store();
    FileMock::init_header_page_for_mock();

    init_buffer_manager(BUFFER_SIZE);

    init_header_page(FileMock::current_fd, TEST_TID);
  }

  void TearDown() override { shutdown_buffer_manager(); }
};

/**
 * test----------------------------------------------------------------------
 */

TEST_F(HardInsertTest, StartNewTree) {
  int64_t key = 10;
  char value[VALUE_SIZE];
  snprintf(value, VALUE_SIZE, "%ld_value", key);

  int result = bpt_insert(FileMock::current_fd, TEST_TID, key, value);
  ASSERT_EQ(result, SUCCESS);

  header_page_t header = get_header_page(FileMock::current_fd, TEST_TID);
  ASSERT_NE(header.root_page_num, PAGE_NULL);
  ASSERT_GT(header.num_of_pages, 1);

  leaf_page_t root =
      get_leaf_page(FileMock::current_fd, TEST_TID, header.root_page_num);
  ASSERT_EQ(root.is_leaf, LEAF);
  ASSERT_EQ(root.num_of_keys, 1);
  ASSERT_EQ(root.records[0].key, key);
  ASSERT_STREQ(root.records[0].value, value);
}

TEST_F(HardInsertTest, InsertIntoLeafWithRoom) {
  int64_t key1 = 10;
  char value1[VALUE_SIZE];
  snprintf(value1, VALUE_SIZE, "%ld_value", key1);
  bpt_insert(FileMock::current_fd, TEST_TID, key1, value1);

  int64_t key2 = 20;
  char value2[VALUE_SIZE];
  snprintf(value2, VALUE_SIZE, "%ld_value", key2);
  int result = bpt_insert(FileMock::current_fd, TEST_TID, key2, value2);
  ASSERT_EQ(result, SUCCESS);

  header_page_t header = get_header_page(FileMock::current_fd, TEST_TID);
  leaf_page_t root =
      get_leaf_page(FileMock::current_fd, TEST_TID, header.root_page_num);

  ASSERT_EQ(root.is_leaf, LEAF);
  ASSERT_EQ(root.num_of_keys, 2);
  ASSERT_EQ(root.records[0].key, key1);
  ASSERT_EQ(root.records[1].key, key2);
}

TEST_F(HardInsertTest, InsertDuplicateKey) {
  int64_t key = 10;
  char value[VALUE_SIZE];
  snprintf(value, VALUE_SIZE, "%ld_value", key);

  int result1 = bpt_insert(FileMock::current_fd, TEST_TID, key, value);
  ASSERT_EQ(result1, SUCCESS);

  int result2 = bpt_insert(FileMock::current_fd, TEST_TID, key, value);
  ASSERT_EQ(result2, FAILURE);
}

TEST_F(HardInsertTest, InsertMultipleKeysInOrder) {
  const int NUM_KEYS = 10;

  for (int i = 0; i < NUM_KEYS; i++) {
    int64_t key = i * 10;
    char value[VALUE_SIZE];
    snprintf(value, VALUE_SIZE, "%ld_value", key);

    int result = bpt_insert(FileMock::current_fd, TEST_TID, key, value);
    ASSERT_EQ(result, SUCCESS);
  }

  for (int i = 0; i < NUM_KEYS; i++) {
    int64_t key = i * 10;
    char result_buf[VALUE_SIZE];
    char expected[VALUE_SIZE];
    snprintf(expected, VALUE_SIZE, "%ld_value", key);

    int find_result = find(FileMock::current_fd, TEST_TID, key, result_buf);
    ASSERT_EQ(find_result, SUCCESS);
    ASSERT_STREQ(result_buf, expected);
  }
}

TEST_F(HardInsertTest, InsertMultipleKeysReverseOrder) {
  const int NUM_KEYS = 10;

  for (int i = NUM_KEYS - 1; i >= 0; i--) {
    int64_t key = i * 10;
    char value[VALUE_SIZE];
    snprintf(value, VALUE_SIZE, "%ld_value", key);

    int result = bpt_insert(FileMock::current_fd, TEST_TID, key, value);
    ASSERT_EQ(result, SUCCESS);
  }

  for (int i = 0; i < NUM_KEYS; i++) {
    int64_t key = i * 10;
    char result_buf[VALUE_SIZE];
    char expected[VALUE_SIZE];
    snprintf(expected, VALUE_SIZE, "%ld_value", key);

    int find_result = find(FileMock::current_fd, TEST_TID, key, result_buf);
    ASSERT_EQ(find_result, SUCCESS);
    ASSERT_STREQ(result_buf, expected);
  }
}

TEST_F(HardInsertTest, SplitLeafNode) {
  for (int i = 0; i <= RECORD_CNT; i++) {
    int64_t key = i * 10;
    char value[VALUE_SIZE];
    snprintf(value, VALUE_SIZE, "%ld_value", key);

    int result = bpt_insert(FileMock::current_fd, TEST_TID, key, value);
    ASSERT_EQ(result, SUCCESS);
  }

  header_page_t header = get_header_page(FileMock::current_fd, TEST_TID);
  ASSERT_NE(header.root_page_num, PAGE_NULL);

  internal_page_t root =
      get_internal_page(FileMock::current_fd, TEST_TID, header.root_page_num);
  ASSERT_EQ(root.is_leaf, INTERNAL);
  ASSERT_GT(root.num_of_keys, 0);

  for (int i = 0; i <= RECORD_CNT; i++) {
    int64_t key = i * 10;
    char result_buf[VALUE_SIZE];
    char expected[VALUE_SIZE];
    snprintf(expected, VALUE_SIZE, "%ld_value", key);

    int find_result = find(FileMock::current_fd, TEST_TID, key, result_buf);
    ASSERT_EQ(find_result, SUCCESS);
    ASSERT_STREQ(result_buf, expected);
  }
}

TEST_F(HardInsertTest, InsertManyKeysAndVerify) {
  const int NUM_KEYS = 100;

  for (int i = 0; i < NUM_KEYS; i++) {
    int64_t key = i;
    char value[VALUE_SIZE];
    snprintf(value, VALUE_SIZE, "value_%ld", key);

    int result = bpt_insert(FileMock::current_fd, TEST_TID, key, value);
    ASSERT_EQ(result, SUCCESS);
  }

  for (int i = 0; i < NUM_KEYS; i++) {
    int64_t key = i;
    char result_buf[VALUE_SIZE];
    char expected[VALUE_SIZE];
    snprintf(expected, VALUE_SIZE, "value_%ld", key);

    int find_result = find(FileMock::current_fd, TEST_TID, key, result_buf);
    ASSERT_EQ(find_result, SUCCESS);
    ASSERT_STREQ(result_buf, expected);
  }

  char result_buf[VALUE_SIZE];
  int find_result =
      find(FileMock::current_fd, TEST_TID, NUM_KEYS + 1000, result_buf);
  ASSERT_EQ(find_result, FAILURE);
}

TEST_F(HardInsertTest, InsertRandomOrder) {
  const int NUM_KEYS = 50;
  int64_t keys[] = {25, 5,  45, 15, 35, 10, 30, 20, 40, 0,  50, 48, 12,
                    8,  42, 18, 38, 28, 32, 22, 46, 16, 36, 26, 44, 14,
                    34, 24, 4,  2,  49, 47, 13, 11, 43, 19, 39, 29, 33,
                    23, 17, 37, 27, 7,  3,  1,  41, 31, 21, 9};

  for (int i = 0; i < NUM_KEYS; i++) {
    char value[VALUE_SIZE];
    snprintf(value, VALUE_SIZE, "val_%ld", keys[i]);

    int result = bpt_insert(FileMock::current_fd, TEST_TID, keys[i], value);
    ASSERT_EQ(result, SUCCESS);
  }

  for (int i = 0; i < NUM_KEYS; i++) {
    char result_buf[VALUE_SIZE];
    char expected[VALUE_SIZE];
    snprintf(expected, VALUE_SIZE, "val_%ld", keys[i]);

    int find_result = find(FileMock::current_fd, TEST_TID, keys[i], result_buf);
    ASSERT_EQ(find_result, SUCCESS);
    ASSERT_STREQ(result_buf, expected);
  }
}

TEST_F(HardInsertTest, TreeStructureAfterMultipleSplits) {
  const int NUM_KEYS = RECORD_CNT * 3;

  for (int i = 0; i < NUM_KEYS; i++) {
    int64_t key = i;
    char value[VALUE_SIZE];
    snprintf(value, VALUE_SIZE, "v%ld", key);

    bpt_insert(FileMock::current_fd, TEST_TID, key, value);
  }

  header_page_t header = get_header_page(FileMock::current_fd, TEST_TID);
  ASSERT_NE(header.root_page_num, PAGE_NULL);
  ASSERT_GE(header.num_of_pages, NUM_KEYS / RECORD_CNT);

  internal_page_t root =
      get_internal_page(FileMock::current_fd, TEST_TID, header.root_page_num);
  ASSERT_EQ(root.is_leaf, INTERNAL);

  for (int i = 0; i < NUM_KEYS; i++) {
    char result_buf[VALUE_SIZE];
    int find_result = find(FileMock::current_fd, TEST_TID, i, result_buf);
    ASSERT_EQ(find_result, SUCCESS);
  }
}
