#define RECORD_CNT 2
#define ENTRY_CNT 16
#define LEAF_ORDER 3
#define ENTRY_ORDER 17
#define NON_HEADER_PAGE_RESERVED 3816

#include <cstdlib>
#include <cstring>

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

static leaf_page_t get_leaf_page(tableid_t table_id, pagenum_t pagenum) {
  leaf_page_t leaf;

  page_t* page = read_buffer(FileMock::current_fd, table_id, pagenum);
  std::memcpy(&leaf, page, PAGE_SIZE);
  unpin(table_id, pagenum);
  return leaf;
}

static internal_page_t get_internal_page(tableid_t table_id,
                                         pagenum_t pagenum) {
  internal_page_t internal;

  page_t* page = read_buffer(FileMock::current_fd, table_id, pagenum);
  std::memcpy(&internal, page, PAGE_SIZE);
  unpin(table_id, pagenum);
  return internal;
}

static header_page_t get_header_page(tableid_t table_id) {
  header_page_t header;

  header_page_t* header_ptr = read_header_page(FileMock::current_fd, table_id);
  std::memcpy(&header, header_ptr, PAGE_SIZE);
  unpin(table_id, HEADER_PAGE_POS);
  return header;
}

// GTest Fixture 정의
class DeleteTest : public ::testing::Test {
 protected:
  tableid_t TEST_TID = 1;
  int BUFFER_SIZE = 200;

  void SetUp() override {
    FileMock::setup_data_store();
    FileMock::init_header_page_for_mock();

    init_buffer_manager(BUFFER_SIZE);

    init_header_page(FileMock::current_fd, TEST_TID);
  }

  void TearDown() override {
    flush_table_buffer(FileMock::current_fd, TEST_TID);
    shutdown_buffer_manager();
  }

  void insert_keys(const std::vector<int64_t>& keys) {
    for (int64_t key : keys) {
      char value[VALUE_SIZE];
      snprintf(value, VALUE_SIZE, "val%ld", key);
      int result = bpt_insert(FileMock::current_fd, TEST_TID, key, value);
      ASSERT_EQ(result, SUCCESS);
    }
  }

  bool key_exists(int64_t key) {
    char result_buf[VALUE_SIZE];
    return find(FileMock::current_fd, TEST_TID, key, result_buf) == SUCCESS;
  }
};

/**
 * test-----------------------------------------------------------------------
 * RECORD_CNT 2, ENTRY_CNT 16
 */

TEST_F(DeleteTest, DeleteRootBecomesEmpty) {
  int64_t key = 5;
  char value[VALUE_SIZE];
  snprintf(value, VALUE_SIZE, "val%ld", key);

  bpt_insert(FileMock::current_fd, TEST_TID, key, value);

  int result = bpt_delete(FileMock::current_fd, TEST_TID, key);
  ASSERT_EQ(result, SUCCESS);

  header_page_t header = get_header_page(TEST_TID);
  ASSERT_EQ(header.root_page_num, PAGE_NULL);

  ASSERT_FALSE(key_exists(key));
}

TEST_F(DeleteTest, DeleteFromLeafWithMultipleKeys) {
  insert_keys({10, 20});

  int result = bpt_delete(FileMock::current_fd, TEST_TID, 20);
  ASSERT_EQ(result, SUCCESS);

  ASSERT_TRUE(key_exists(10));
  ASSERT_FALSE(key_exists(20));

  header_page_t header = get_header_page(TEST_TID);
  leaf_page_t root = get_leaf_page(TEST_TID, header.root_page_num);
  ASSERT_EQ(root.num_of_keys, 1);
}

TEST_F(DeleteTest, DeleteNonExistentKey) {
  insert_keys({10, 20});

  int result = bpt_delete(FileMock::current_fd, TEST_TID, 999);
  ASSERT_EQ(result, FAILURE);

  ASSERT_TRUE(key_exists(10));
  ASSERT_TRUE(key_exists(20));
}

TEST_F(DeleteTest, DeleteFromEmptyTree) {
  int result = bpt_delete(FileMock::current_fd, TEST_TID, 10);
  ASSERT_EQ(result, FAILURE);
}

TEST_F(DeleteTest, LeafRedistributionRightToLeft) {
  // {10, 20, 30} 삽입 -> Left[10], Right[20,30]
  insert_keys({10, 20, 30});

  // Right[20,30]에서 재분배 발생 -> Left[20], Right[30]
  int result = bpt_delete(FileMock::current_fd, TEST_TID, 10);
  ASSERT_EQ(result, SUCCESS);

  ASSERT_FALSE(key_exists(10));
  ASSERT_TRUE(key_exists(20));
  ASSERT_TRUE(key_exists(30));

  header_page_t header = get_header_page(TEST_TID);
  internal_page_t root = get_internal_page(TEST_TID, header.root_page_num);
  ASSERT_EQ(RECORD_CNT, 2);
  ASSERT_EQ(root.is_leaf, INTERNAL);
}

TEST_F(DeleteTest, LeafCoalesceAndRootCollapse) {
  // {10, 20, 30} 삽입 -> Left[10], Right[20,30]
  insert_keys({10, 20, 30});

  // 20, 30 삭제 -> Right가 비어서 병합 발생
  bpt_delete(FileMock::current_fd, TEST_TID, 20);
  bpt_delete(FileMock::current_fd, TEST_TID, 30);

  ASSERT_TRUE(key_exists(10));
  ASSERT_FALSE(key_exists(20));
  ASSERT_FALSE(key_exists(30));

  header_page_t header = get_header_page(TEST_TID);
  ASSERT_NE(header.root_page_num, PAGE_NULL);

  leaf_page_t root = get_leaf_page(TEST_TID, header.root_page_num);
  ASSERT_EQ(root.is_leaf, LEAF);
  ASSERT_EQ(root.num_of_keys, 1);
}

TEST_F(DeleteTest, LeafCoalesceWithMultipleLeaves) {
  // {10, 20, 30, 40} 삽입 -> Left[10], Mid[20], Right[30,40]
  insert_keys({10, 20, 30, 40});

  // Mid[20] 삭제 -> Mid가 비어서 병합 발생
  int result = bpt_delete(FileMock::current_fd, TEST_TID, 20);
  ASSERT_EQ(result, SUCCESS);

  ASSERT_TRUE(key_exists(10));
  ASSERT_FALSE(key_exists(20));
  ASSERT_TRUE(key_exists(30));
  ASSERT_TRUE(key_exists(40));

  header_page_t header = get_header_page(TEST_TID);
  internal_page_t root = get_internal_page(TEST_TID, header.root_page_num);
  ASSERT_EQ(root.is_leaf, INTERNAL);
}

TEST_F(DeleteTest, LeafRedistributionLeftToRight) {
  insert_keys({10, 20, 30, 40, 50});

  int result = bpt_delete(FileMock::current_fd, TEST_TID, 50);
  ASSERT_EQ(result, SUCCESS);

  ASSERT_TRUE(key_exists(10));
  ASSERT_TRUE(key_exists(20));
  ASSERT_TRUE(key_exists(30));
  ASSERT_TRUE(key_exists(40));
  ASSERT_FALSE(key_exists(50));
}

TEST_F(DeleteTest, InternalNodeCoalesce) {
  const int NUM_KEYS = 20;
  std::vector<int64_t> keys;
  for (int i = 0; i < NUM_KEYS; i++) {
    keys.push_back(i);
  }
  insert_keys(keys);

  for (int i = 0; i < NUM_KEYS / 2; i++) {
    int result = bpt_delete(FileMock::current_fd, TEST_TID, keys[i]);
    ASSERT_EQ(result, SUCCESS);
  }

  for (int i = NUM_KEYS / 2; i < NUM_KEYS; i++) {
    ASSERT_TRUE(key_exists(keys[i]));
  }
}

TEST_F(DeleteTest, DeleteAllKeysSequentially) {
  const int NUM_KEYS = 20;
  std::vector<int64_t> keys;
  for (int i = 0; i < NUM_KEYS; i++) {
    keys.push_back(i);
  }
  insert_keys(keys);

  for (int64_t key : keys) {
    int result = bpt_delete(FileMock::current_fd, TEST_TID, key);
    ASSERT_EQ(result, SUCCESS);
    ASSERT_FALSE(key_exists(key));
  }

  header_page_t header = get_header_page(TEST_TID);
  ASSERT_EQ(header.root_page_num, PAGE_NULL);
}

TEST_F(DeleteTest, DeleteInReverseOrder) {
  const int NUM_KEYS = 20;
  std::vector<int64_t> keys;
  for (int i = 0; i < NUM_KEYS; i++) {
    keys.push_back(i);
  }
  insert_keys(keys);

  for (int i = NUM_KEYS - 1; i >= 0; i--) {
    int result = bpt_delete(FileMock::current_fd, TEST_TID, keys[i]);
    ASSERT_EQ(result, SUCCESS);
    ASSERT_FALSE(key_exists(keys[i]));
  }

  header_page_t header = get_header_page(TEST_TID);
  ASSERT_EQ(header.root_page_num, PAGE_NULL);
}

TEST_F(DeleteTest, DeleteRandomOrder) {
  std::vector<int64_t> keys = {25, 5,  45, 15, 35, 10, 30, 20, 40, 0,
                               50, 48, 12, 8,  42, 18, 38, 28, 32, 22};
  insert_keys(keys);

  std::vector<int64_t> to_delete = {25, 10, 48, 15, 40, 5, 30, 8, 50, 18};
  for (int64_t key : to_delete) {
    int result = bpt_delete(FileMock::current_fd, TEST_TID, key);
    ASSERT_EQ(result, SUCCESS);
    ASSERT_FALSE(key_exists(key));
  }

  for (int64_t key : keys) {
    bool should_exist =
        std::find(to_delete.begin(), to_delete.end(), key) == to_delete.end();
    ASSERT_EQ(key_exists(key), should_exist);
  }
}

TEST_F(DeleteTest, InsertDeleteMixed) {
  for (int i = 0; i < 30; i++) {
    insert_keys({i});
  }

  for (int i = 0; i < 30; i += 3) {
    bpt_delete(FileMock::current_fd, TEST_TID, i);
  }

  for (int i = 30; i < 40; i++) {
    insert_keys({i});
  }

  for (int i = 1; i < 30; i += 3) {
    bpt_delete(FileMock::current_fd, TEST_TID, i);
  }

  for (int i = 0; i < 40; i++) {
    bool should_exist = (i >= 30) || (i % 3 == 2);
    ASSERT_EQ(key_exists(i), should_exist);
  }
}

TEST_F(DeleteTest, StressTestManyDeletes) {
  const int NUM_KEYS = 100;
  std::vector<int64_t> keys;
  for (int i = 0; i < NUM_KEYS; i++) {
    keys.push_back(i);
  }
  insert_keys(keys);

  for (int i = 0; i < NUM_KEYS; i += 2) {
    int result = bpt_delete(FileMock::current_fd, TEST_TID, keys[i]);
    printf("test check: %d \n", i);
    ASSERT_EQ(result, SUCCESS);
  }

  for (int i = 0; i < NUM_KEYS; i++) {
    if (i % 2 == 0) {
      ASSERT_FALSE(key_exists(keys[i]));
    } else {
      ASSERT_TRUE(key_exists(keys[i]));
    }
  }

  for (int i = 1; i < NUM_KEYS; i += 2) {
    int result = bpt_delete(FileMock::current_fd, TEST_TID, keys[i]);
    ASSERT_EQ(result, SUCCESS);
  }

  header_page_t header = get_header_page(TEST_TID);
  ASSERT_EQ(header.root_page_num, PAGE_NULL);
}
