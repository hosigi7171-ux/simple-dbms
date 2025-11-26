#include "file.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>


int fd = -1; // temp file discripter

off_t get_offset(pagenum_t pagenum) { return (off_t)pagenum * PAGE_SIZE; }

uint32_t get_isleaf_flag(const page_t *page) {
  return *(uint32_t *)(page->data + sizeof(pagenum_t));
}

void handle_error(const char *msg) {
  perror(msg);
  exit(EXIT_FAILURE);
}

/**
 * @brief Allocate an on-disk page from the free page list
 */
pagenum_t file_alloc_page() {
  header_page_t header;
  free_page_t free_page;
  pagenum_t allocated_page_num;

  file_read_page(HEADER_PAGE_POS, (page_t *)&header);
  allocated_page_num = header.free_page_num;

  if (allocated_page_num == PAGE_NULL) {
    pagenum_t new_page_num = header.num_of_pages;
    header.num_of_pages += 1;
    file_write_page(HEADER_PAGE_POS, (page_t *)&header);

    return new_page_num;
  }

  file_read_page(allocated_page_num, (page_t *)&free_page);
  header.free_page_num = free_page.next_free_page_num;
  file_write_page(HEADER_PAGE_POS, (page_t *)&header);

  return allocated_page_num;
}

/**
 * @brief Free an on-disk page to the free page list
 */
void file_free_page(pagenum_t pagenum) {
  header_page_t header;
  page_t removing_page;
  free_page_t new_free_page;
  memset(&new_free_page, 0, PAGE_SIZE);

  // 헤더 페이지를 읽어와서 프리 페이지 리스트 참조
  file_read_page(HEADER_PAGE_POS, (page_t *)&header);
  new_free_page.next_free_page_num = header.free_page_num;
  header.free_page_num = pagenum;

  // 삭제할 자리에 새로 작성할 프리 페이지 작성
  file_write_page(pagenum, (page_t *)&new_free_page);
  file_write_page(HEADER_PAGE_POS, (page_t *)&header);
}

/**
 * @brief Read an on-disk page into the in-memory page structure(dest)
 */
void file_read_page(pagenum_t pagenum, page_t *dest) {
  off_t offset = get_offset(pagenum);

  if (lseek(fd, offset, SEEK_SET) == (off_t)-1) {
    handle_error("lseek error");
  }

  if (read(fd, dest, PAGE_SIZE) != PAGE_SIZE) {
    handle_error("read error");
  }
}

/**
 * @brief Write an in-memory page(src) to the on-disk page
 */
void file_write_page(pagenum_t pagenum, const page_t *src) {
  off_t offset = get_offset(pagenum);

  if (lseek(fd, offset, SEEK_SET) == (off_t)-1) {
    handle_error("lseek error");
  }

  if (write(fd, src, PAGE_SIZE) != PAGE_SIZE) {
    handle_error("write error");
  }

  if (fsync(fd) != 0) {
    handle_error("fsync error");
  }
}
