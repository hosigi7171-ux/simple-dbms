#include "buf_mgr.h"

#include <cstdlib>
#include <cstring>

#include "FileMock.h"
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

// GTest Fixture 정의
class BufferManagerTest : public ::testing::Test {
 protected:
  tableid_t TEST_TID = 1;
  int BUFFER_SIZE = 5;

  void SetUp() override {
    FileMock::setup_data_store();
    FileMock::init_header_page_for_mock();

    init_buffer_manager(BUFFER_SIZE);
  }

  void TearDown() override { shutdown_buffer_manager(); }
};

/**
 * test-----------------------------------------------------------------------
 */

TEST_F(BufferManagerTest, ReadHeaderPageWorks) {
  header_page_t* header_ptr = read_header_page(FileMock::current_fd, TEST_TID);

  ASSERT_NE(header_ptr, nullptr);
  ASSERT_EQ(header_ptr->num_of_pages, HEADER_PAGE_POS + 1);

  unpin(TEST_TID, HEADER_PAGE_POS);
}

TEST_F(BufferManagerTest, PinAndUnpinIncrementsAndDecrements) {
  allocated_page_info_t info =
      make_and_pin_page(FileMock::current_fd, TEST_TID);
  frame_idx_t fidx = get_frame_index_by_page(TEST_TID, info.page_num);

  ASSERT_EQ(buf_mgr.frames[fidx].pin_count, 1);

  pin(TEST_TID, info.page_num);
  ASSERT_EQ(buf_mgr.frames[fidx].pin_count, 2);

  unpin(TEST_TID, info.page_num);
  ASSERT_EQ(buf_mgr.frames[fidx].pin_count, 1);

  unpin(TEST_TID, info.page_num);
  ASSERT_EQ(buf_mgr.frames[fidx].pin_count, 0);
}

TEST_F(BufferManagerTest, LoadPageIntoBufferAndEviction) {
  read_header_page(FileMock::current_fd, TEST_TID);
  unpin(TEST_TID, HEADER_PAGE_POS);

  std::vector<pagenum_t> allocated_pages;
  for (int i = 1; i < BUFFER_SIZE; ++i) {
    pagenum_t pnum = file_alloc_page(FileMock::current_fd);
    allocated_pages.push_back(pnum);
    load_page_into_buffer(FileMock::current_fd, TEST_TID, pnum);
    unpin(TEST_TID, pnum);
  }

  for (int i = 0; i < BUFFER_SIZE; ++i) {
    buf_mgr.frames[i].ref_bit = false;
  }

  buf_mgr.clock_hand = 0;

  pagenum_t dirty_page_num = allocated_pages[0];
  frame_idx_t dirty_fidx = get_frame_index_by_page(TEST_TID, dirty_page_num);

  buf_mgr.frames[dirty_fidx].is_dirty = true;

  pagenum_t new_page_num = file_alloc_page(FileMock::current_fd);

  frame_idx_t evicted_fidx =
      load_page_into_buffer(FileMock::current_fd, TEST_TID, new_page_num);

  ASSERT_EQ(buf_mgr.frames[evicted_fidx].page_num, new_page_num);
  ASSERT_FALSE(buf_mgr.frames[evicted_fidx].is_dirty);

  unpin(TEST_TID, new_page_num);
}

TEST_F(BufferManagerTest, FreePageInBufferAndDisk) {
  pagenum_t pnum = file_alloc_page(FileMock::current_fd);
  frame_idx_t fidx =
      load_page_into_buffer(FileMock::current_fd, TEST_TID, pnum);
  unpin(TEST_TID, pnum);

  ASSERT_NE(get_frame_index_by_page(TEST_TID, pnum), INVALID_FRAME);

  free_page_in_buffer(FileMock::current_fd, TEST_TID, pnum);

  ASSERT_EQ(get_frame_index_by_page(TEST_TID, pnum), INVALID_FRAME);

  page_t zero_page;
  std::memset(&zero_page, 0, PAGE_SIZE);

  ASSERT_EQ(std::memcmp(&FileMock::MOCK_PAGES[pnum], &zero_page, PAGE_SIZE), 0);
}
