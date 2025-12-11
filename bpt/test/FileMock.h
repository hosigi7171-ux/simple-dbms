#pragma once

#include <gtest/gtest.h>

#include <cstdlib>
#include <cstring>

#include "common_config.h"
#include "file.h"
#include "page.h"

#define MAX_MOCK_PAGES 300

/**
 * @brief Mock File Manager
 */
class FileMock {
 public:
  static page_t MOCK_PAGES[MAX_MOCK_PAGES];
  static int current_fd;
  static pagenum_t mock_next_page_num;

  static void setup_data_store();
  static void init_header_page_for_mock();

  static header_page_t get_header_page();
};
