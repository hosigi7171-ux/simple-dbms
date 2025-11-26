#include "helper_mock.h"
#include <string.h>

page_t MOCK_PAGES[MAX_MOCK_PAGES];

void setup_data_store(void) { memset(MOCK_PAGES, 0, sizeof(MOCK_PAGES)); }

void MOCK_file_read_page(pagenum_t pagenum, page_t *dest, int num_calls) {
  if (pagenum < MAX_MOCK_PAGES) {
    memcpy(dest, &MOCK_PAGES[pagenum], PAGE_SIZE);
  }
}

void MOCK_file_write_page(pagenum_t pagenum, const page_t *src, int num_calls) {
  if (pagenum < MAX_MOCK_PAGES) {
    memcpy(&MOCK_PAGES[pagenum], src, PAGE_SIZE);
  }
}

pagenum_t MOCK_file_alloc_page(int num_calls) {
  header_page_t header;
  MOCK_file_read_page(HEADER_PAGE_POS, (page_t *)&header, 0);

  pagenum_t new_page_num = header.num_of_pages;

  if (new_page_num >= MAX_MOCK_PAGES) {
    return 0;
  }
  // 처음 헤더에 루트 페이지 설정
  if (header.num_of_pages == 1) {
    header.root_page_num = new_page_num;
  }

  header.num_of_pages += 1;
  MOCK_file_write_page(HEADER_PAGE_POS, (page_t *)&header, 0);

  memset(&MOCK_PAGES[new_page_num], 0, PAGE_SIZE);

  return new_page_num;
}

void MOCK_file_free_page(pagenum_t pagenum, int num_calls) {
  if (pagenum >= MAX_MOCK_PAGES || pagenum <= HEADER_PAGE_POS) {
    return;
  }

  memset(&MOCK_PAGES[pagenum], 0, PAGE_SIZE);

  header_page_t header;
  free_page_t *free_page = (free_page_t *)&MOCK_PAGES[pagenum];

  MOCK_file_read_page(HEADER_PAGE_POS, (page_t *)&header, 0);

  free_page->next_free_page_num = header.free_page_num;
  header.free_page_num = pagenum;

  MOCK_file_write_page(HEADER_PAGE_POS, (page_t *)&header, 0);
}

void init_header_page_for_mock(void) {
  page_t header_buf;
  memset(&header_buf, 0, PAGE_SIZE);
  header_page_t *header_page = (header_page_t *)&header_buf;

  header_page->root_page_num = PAGE_NULL;
  header_page->num_of_pages = HEADER_PAGE_POS + 1;

  memcpy(&MOCK_PAGES[HEADER_PAGE_POS], &header_buf, PAGE_SIZE);
}

// ---------------utils for mock-------------------

leaf_page_t get_leaf_page(pagenum_t pagenum) {
  page_t buf;
  MOCK_file_read_page(pagenum, &buf, 0);
  return *(leaf_page_t *)&buf;
}

internal_page_t get_internal_page(pagenum_t pagenum) {
  page_t buf;
  MOCK_file_read_page(pagenum, &buf, 0);
  return *(internal_page_t *)&buf;
}

header_page_t get_header_page(void) {
  page_t buf;
  MOCK_file_read_page(HEADER_PAGE_POS, &buf, 0);
  return *(header_page_t *)&buf;
}