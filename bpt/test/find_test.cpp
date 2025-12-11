#include <fcntl.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <sstream>
#include <vector>

#include "FileMock.h"
#include "bpt.h"
#include "buf_mgr.h"

extern buffer_manager_t buf_mgr;

#define PAGE_SIZE 4096
#define HEADER_PAGE_POS 0
#define LEAF 1

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
}

static leaf_page_t get_leaf_page(int fd, tableid_t table_id,
                                 pagenum_t pagenum) {
  page_t* page = read_buffer(fd, table_id, pagenum);
  if (!page) {
    return {};
  }
  leaf_page_t leaf;
  std::memcpy(&leaf, page, PAGE_SIZE);
  unpin(table_id, pagenum);
  return leaf;
}

static header_page_t get_header_page_from_buffer(int fd, tableid_t table_id) {
  header_page_t* header_ptr = read_header_page(fd, table_id);
  if (!header_ptr) {
    return {};
  }
  header_page_t header;
  std::memcpy(&header, header_ptr, sizeof(header_page_t));
  unpin(table_id, HEADER_PAGE_POS);
  return header;
}

// GTest Fixture 정의
class FindTest : public ::testing::Test {
 protected:
  tableid_t TEST_TID = 1;
  int BUFFER_SIZE = 100;

  // 파일 디스크립터 캡처를 위한 변수
  int saved_stdout_fd = -1;
  int saved_stderr_fd = -1;
  int pipefd[2] = {-1, -1};
  std::string captured_output;

  // B+ Tree에 데이터 삽입 (find 테스트를 위한 선행 작업)
  void insert_data(const std::vector<std::pair<int64_t, std::string>>& data) {
    for (const auto& entry : data) {
      int result = bpt_insert(FileMock::current_fd, TEST_TID, entry.first,
                              (char*)entry.second.c_str());
      ASSERT_EQ(SUCCESS, result) << "Insert failed for key: " << entry.first;
    }
  }

  void SetUp() override {
    FileMock::setup_data_store();
    FileMock::init_header_page_for_mock();

    init_buffer_manager(BUFFER_SIZE);

    init_header_page(FileMock::current_fd, TEST_TID);
  }

  void TearDown() override {
    shutdown_buffer_manager();

    cleanup_pipe_fds();
  }

  // 캡처 시작: stdout 및 stderr을 파이프로 리디렉션
  void start_capture() {
    saved_stdout_fd = dup(STDOUT_FILENO);
    saved_stderr_fd = dup(STDERR_FILENO);

    if (pipe(pipefd) == -1) {
      FAIL() << "Failed to create pipe for output capture.";
    }

    dup2(pipefd[1], STDOUT_FILENO);
    dup2(pipefd[1], STDERR_FILENO);

    std::ios_base::sync_with_stdio(true);

    captured_output.clear();
  }

  // 캡처 종료: 파이프에서 내용을 읽고 원본 디스크립터 복구
  std::string stop_capture() {
    std::cout.flush();
    std::cerr.flush();
    fflush(stdout);
    fflush(stderr);

    // stdout stderr 을 원래 상태로 복구
    if (saved_stdout_fd != -1) {
      dup2(saved_stdout_fd, STDOUT_FILENO);
      close(saved_stdout_fd);
      saved_stdout_fd = -1;
    }
    if (saved_stderr_fd != -1) {
      dup2(saved_stderr_fd, STDERR_FILENO);
      close(saved_stderr_fd);
      saved_stderr_fd = -1;
    }

    // close pipe write
    if (pipefd[1] != -1) {
      close(pipefd[1]);
      pipefd[1] = -1;
    }

    // 파이프의 읽기 쪽(pipefd[0])에서 모든 데이터 읽기
    char buffer[4096];
    ssize_t bytes_read;

    while ((pipefd[0] != -1) &&
           ((bytes_read = read(pipefd[0], buffer, sizeof(buffer) - 1)) > 0)) {
      buffer[bytes_read] = '\0';
      captured_output += buffer;
    }

    // close pipe read
    if (pipefd[0] != -1) {
      close(pipefd[0]);
      pipefd[0] = -1;
    }

    return captured_output;
  }

  // 파이프 디스크립터만 정리 (TearDown에서 사용)
  void cleanup_pipe_fds() {
    if (pipefd[0] != -1) {
      close(pipefd[0]);
      pipefd[0] = -1;
    }
    if (pipefd[1] != -1) {
      close(pipefd[1]);
      pipefd[1] = -1;
    }
  }
};

/**
 * test---------------------------------------------------------------------------
 */

TEST_F(FindTest, FindRangeSingleLeaf) {
  insert_data({{10, "v10"}, {20, "v20"}, {30, "v30"}});

  header_page_t header =
      get_header_page_from_buffer(FileMock::current_fd, TEST_TID);
  ASSERT_NE(header.root_page_num, PAGE_NULL);
  pagenum_t root_page_num = header.root_page_num;

  int64_t keys[10];
  pagenum_t pages[10];
  int idx[10];

  int n = find_range(FileMock::current_fd, TEST_TID, 15, 35, keys, pages, idx);

  ASSERT_EQ(2, n);

  ASSERT_EQ(20, keys[0]);
  ASSERT_EQ(root_page_num, pages[0]);
  ASSERT_EQ(1, idx[0]);

  ASSERT_EQ(30, keys[1]);
  ASSERT_EQ(root_page_num, pages[1]);
  ASSERT_EQ(2, idx[1]);
}

TEST_F(FindTest, FindAndPrintRangeOutput) {
  insert_data({{100, "AAA"}, {150, "BBB"}, {50, "CCC"}});

  start_capture();

  find_and_print_range(FileMock::current_fd, TEST_TID, 90, 120);

  std::string captured_output = stop_capture();

  if (captured_output.empty()) {
    FAIL() << "Output capture failed. The function may not be printing "
              "anything. Captured output size: "
           << captured_output.size();
  }

  EXPECT_NE(std::string::npos, captured_output.find("found 1 records"))
      << "Output: [" << captured_output << "]";
  EXPECT_NE(std::string::npos, captured_output.find("100"))
      << "Output: [" << captured_output << "]";
  EXPECT_NE(std::string::npos, captured_output.find("AAA"))
      << "Output: [" << captured_output << "]";
  EXPECT_EQ(std::string::npos, captured_output.find("150"));
  EXPECT_EQ(std::string::npos, captured_output.find("50"));
}

TEST_F(FindTest, PrintTreeOutput) {
  insert_data({{10, "v10"}, {20, "v20"}});

  start_capture();

  print_tree(FileMock::current_fd, TEST_TID);

  std::string captured_output = stop_capture();

  if (captured_output.empty()) {
    FAIL() << "Output capture failed. The function may not be printing "
              "anything. Captured output size: "
           << captured_output.size();
  }

  EXPECT_NE(std::string::npos, captured_output.find("--- level 0 ---"))
      << "Output: [" << captured_output << "]";
  EXPECT_NE(std::string::npos, captured_output.find("10"))
      << "Output: [" << captured_output << "]";
  EXPECT_NE(std::string::npos, captured_output.find("20"))
      << "Output: [" << captured_output << "]";
}
