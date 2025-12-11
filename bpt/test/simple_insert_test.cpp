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

class SimpleInsertTest : public ::testing::Test {
 protected:
  tableid_t TEST_TID = 1;
  int BUFFER_SIZE = 100;

  void SetUp() override {
    FileMock::setup_data_store();
    FileMock::init_header_page_for_mock();
    init_buffer_manager(BUFFER_SIZE);
    init_header_page(FileMock::current_fd, TEST_TID);

    std::cout << "=== SetUp complete ===" << std::endl;
  }

  void TearDown() override {
    std::cout << "=== TearDown starting ===" << std::endl;
    shutdown_buffer_manager();
    std::cout << "=== TearDown complete ===" << std::endl;
  }
};

/**
 * test-------------------------------------------------------------------------
 */

TEST_F(SimpleInsertTest, Step1_ReadHeaderOnly) {
  std::cout << "\n=== Step 1: Read Header Only ===" << std::endl;

  header_page_t header = FileMock::get_header_page();
  std::cout << "Header - root_page_num: " << header.root_page_num << std::endl;
  std::cout << "Header - num_of_pages: " << header.num_of_pages << std::endl;

  ASSERT_EQ(header.root_page_num, PAGE_NULL);
  ASSERT_EQ(header.num_of_pages, HEADER_PAGE_POS + 1);

  std::cout << "=== Step 1 PASSED ===" << std::endl;
}

TEST_F(SimpleInsertTest, Step2_ReadHeaderFromBuffer) {
  std::cout << "\n=== Step 2: Read Header From Buffer ===" << std::endl;

  std::cout << "Calling read_header_page..." << std::endl;
  header_page_t* header_ptr = read_header_page(FileMock::current_fd, TEST_TID);
  std::cout << "read_header_page returned" << std::endl;

  ASSERT_NE(header_ptr, nullptr);
  std::cout << "Header ptr - root_page_num: " << header_ptr->root_page_num
            << std::endl;

  unpin(TEST_TID, HEADER_PAGE_POS);
  std::cout << "=== Step 2 PASSED ===" << std::endl;
}

TEST_F(SimpleInsertTest, Step3_FindInEmptyTree) {
  std::cout << "\n=== Step 3: Find in Empty Tree ===" << std::endl;

  char result_buf[VALUE_SIZE];
  std::cout << "Calling find (should fail on empty tree)..." << std::endl;
  int result = find(FileMock::current_fd, TEST_TID, 10, result_buf);
  std::cout << "find returned: " << result << std::endl;

  ASSERT_EQ(result, FAILURE);
  std::cout << "=== Step 3 PASSED ===" << std::endl;
}

TEST_F(SimpleInsertTest, Step4_MakeNode) {
  std::cout << "\n=== Step 4: Make Node ===" << std::endl;

  std::cout << "Calling make_node (LEAF)..." << std::endl;
  pagenum_t leaf_num = make_node(FileMock::current_fd, TEST_TID, LEAF);
  std::cout << "make_node returned page_num: " << leaf_num << std::endl;

  ASSERT_NE(leaf_num, PAGE_NULL);

  leaf_page_t leaf = get_leaf_page(FileMock::current_fd, TEST_TID, leaf_num);
  std::cout << "Leaf - is_leaf: " << leaf.is_leaf << std::endl;
  std::cout << "Leaf - num_of_keys: " << leaf.num_of_keys << std::endl;

  ASSERT_EQ(leaf.is_leaf, LEAF);
  ASSERT_EQ(leaf.num_of_keys, 0);

  std::cout << "=== Step 4 PASSED ===" << std::endl;
}

TEST_F(SimpleInsertTest, Step5_StartNewTreeDirect) {
  std::cout << "\n=== Step 5: Start New Tree Direct ===" << std::endl;

  int64_t key = 10;
  char value[VALUE_SIZE];
  snprintf(value, VALUE_SIZE, "%ld_value", key);

  std::cout << "Calling start_new_tree..." << std::endl;
  int result = start_new_tree(FileMock::current_fd, TEST_TID, key, value);
  std::cout << "start_new_tree returned: " << result << std::endl;

  ASSERT_EQ(result, SUCCESS);

  header_page_t header = get_header_page(FileMock::current_fd, TEST_TID);
  std::cout << "Header - root_page_num: " << header.root_page_num << std::endl;
  std::cout << "Header - num_of_pages: " << header.num_of_pages << std::endl;

  ASSERT_NE(header.root_page_num, PAGE_NULL);

  leaf_page_t root =
      get_leaf_page(FileMock::current_fd, TEST_TID, header.root_page_num);
  std::cout << "Root - is_leaf: " << root.is_leaf << std::endl;
  std::cout << "Root - num_of_keys: " << root.num_of_keys << std::endl;
  std::cout << "Root - records[0].key: " << root.records[0].key << std::endl;

  ASSERT_EQ(root.is_leaf, LEAF);
  ASSERT_EQ(root.num_of_keys, 1);
  ASSERT_EQ(root.records[0].key, key);

  std::cout << "=== Step 5 PASSED ===" << std::endl;
}

TEST_F(SimpleInsertTest, Step6_FullInsert) {
  std::cout << "\n=== Step 6: Full Insert ===" << std::endl;

  int64_t key = 10;
  char value[VALUE_SIZE];
  snprintf(value, VALUE_SIZE, "%ld_value", key);

  std::cout << "Before bpt_insert - checking buffer state..." << std::endl;
  std::cout << "Buffer frames_size: " << buf_mgr.frames_size << std::endl;
  std::cout << "Clock hand: " << buf_mgr.clock_hand << std::endl;

  std::cout << "Calling bpt_insert..." << std::endl;
  int result = bpt_insert(FileMock::current_fd, TEST_TID, key, value);
  std::cout << "bpt_insert returned: " << result << std::endl;

  ASSERT_EQ(result, SUCCESS);

  header_page_t header = get_header_page(FileMock::current_fd, TEST_TID);
  std::cout << "Header - root_page_num: " << header.root_page_num << std::endl;
  ASSERT_NE(header.root_page_num, PAGE_NULL);
  ASSERT_EQ(header.num_of_pages, 2);

  leaf_page_t root =
      get_leaf_page(FileMock::current_fd, TEST_TID, header.root_page_num);
  std::cout << "Root - is_leaf: " << root.is_leaf << std::endl;
  std::cout << "Root - num_of_keys: " << root.num_of_keys << std::endl;
  std::cout << "Root - key[0]: " << root.records[0].key << std::endl;

  ASSERT_EQ(root.is_leaf, LEAF);
  ASSERT_EQ(root.num_of_keys, 1);
  ASSERT_EQ(root.records[0].key, 10);

  std::cout << "=== Step 6 PASSED ===" << std::endl;
}
