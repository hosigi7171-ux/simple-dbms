#include "FileMock.h"

#include <cstdlib>
#include <cstring>
#include <iostream>

page_t FileMock::MOCK_PAGES[MAX_MOCK_PAGES];
int FileMock::current_fd = 100;
pagenum_t FileMock::mock_next_page_num = HEADER_PAGE_POS + 1;

void FileMock::setup_data_store() {
  std::memset(MOCK_PAGES, 0, sizeof(page_t) * MAX_MOCK_PAGES);

  mock_next_page_num = HEADER_PAGE_POS + 1;
}

void FileMock::init_header_page_for_mock() {
  setup_data_store();

  header_page_t* header_ptr = (header_page_t*)&MOCK_PAGES[HEADER_PAGE_POS];

  header_ptr->free_page_num = PAGE_NULL;
  header_ptr->num_of_pages = HEADER_PAGE_POS + 1;
  header_ptr->root_page_num = PAGE_NULL;
}

header_page_t FileMock::get_header_page() {
  header_page_t header;
  std::memcpy(&header, &MOCK_PAGES[HEADER_PAGE_POS], PAGE_SIZE);
  return header;
}

/**
 * Mock function for File Manager
 */

#ifdef TEST_ENV

pagenum_t file_alloc_page(int fd) {
  if (fd != FileMock::current_fd) {
    return PAGE_NULL;
  }

  pagenum_t pagenum = FileMock::mock_next_page_num++;
  if (pagenum >= MAX_MOCK_PAGES) {
    return PAGE_NULL;
  }

  std::memset(FileMock::MOCK_PAGES[pagenum].data, 0, PAGE_SIZE);

  return pagenum;
}

void file_free_page(int fd, pagenum_t pagenum) {
  if (fd != FileMock::current_fd || pagenum < HEADER_PAGE_POS ||
      pagenum >= MAX_MOCK_PAGES) {
    return;
  }

  std::memset(FileMock::MOCK_PAGES[pagenum].data, 0, PAGE_SIZE);
}

void file_read_page(int fd, pagenum_t pagenum, page_t* dest) {
  if (fd != FileMock::current_fd || pagenum < HEADER_PAGE_POS ||
      pagenum >= MAX_MOCK_PAGES) {
    return;
  }
  std::memcpy(dest, &FileMock::MOCK_PAGES[pagenum], PAGE_SIZE);
}

void file_write_page(int fd, pagenum_t pagenum, const page_t* src) {
  if (fd != FileMock::current_fd || pagenum < HEADER_PAGE_POS ||
      pagenum >= MAX_MOCK_PAGES) {
    return;
  }
  std::memcpy(&FileMock::MOCK_PAGES[pagenum], src, PAGE_SIZE);
}

#endif
