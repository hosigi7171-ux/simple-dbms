#include "file.h"

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

off_t get_offset(pagenum_t pagenum) { return (off_t)pagenum * PAGE_SIZE; }

uint32_t get_isleaf_flag(const page_t* page) {
  return *(uint32_t*)(page->data + sizeof(pagenum_t));
}

void handle_error(const char* msg) {
  perror(msg);
  exit(EXIT_FAILURE);
}

/**
 * @brief Allocate an on-disk page from the free page list
 * not used
 */
pagenum_t file_alloc_page(int fd) {
  // header_page_t header;
  // free_page_t free_page;
  // pagenum_t allocated_page_num;

  // file_read_page(fd, HEADER_PAGE_POS, (page_t*)&header);
  // allocated_page_num = header.free_page_num;

  // if (allocated_page_num == PAGE_NULL) {
  //   pagenum_t new_page_num = header.num_of_pages;
  //   header.num_of_pages += 1;
  //   file_write_page(fd, HEADER_PAGE_POS, (page_t*)&header);

  //   return new_page_num;
  // }

  // file_read_page(fd, allocated_page_num, (page_t*)&free_page);
  // header.free_page_num = free_page.next_free_page_num;
  // file_write_page(fd, HEADER_PAGE_POS, (page_t*)&header);

  // return allocated_page_num;

  // 이제 새 페이지의 할당은 버퍼 매니저가 인메모리로 처리
  // 이후, write할때만 버퍼 매니적 알아서 처리한다
  // 혹시 몰라서 주석으로 남겨둠
  return PAGE_NULL;
}

/**
 * @brief Free an on-disk page to the free page list
 */
void file_free_page(int fd, pagenum_t pagenum) {
  // 프리페이지 리스트에 추가하는 것은 버퍼 매니저에서 담당함
  // 위에는 혹시 몰라서 남겨둔 주석

  page_t empty_page;
  memset(&empty_page, 0, PAGE_SIZE);

  file_write_page(fd, pagenum, &empty_page);
}

/**
 * @brief Read an on-disk page into the in-memory page structure(dest)
 */
void file_read_page(int fd, pagenum_t pagenum, page_t* dest) {
  off_t offset = get_offset(pagenum);

  if (lseek(fd, offset, SEEK_SET) == (off_t)-1) {
    handle_error("lseek error");
  }

  ssize_t bytes_read = read(fd, dest, PAGE_SIZE);

  if (bytes_read == -1) {
    handle_error("read error (I/O failure)");
  } else if (bytes_read < PAGE_SIZE) {
    fprintf(stderr, "EOF reached or partial read (%zd bytes) for page %d.\n",
            bytes_read, pagenum);
    exit(EXIT_FAILURE);
  }
}

/**
 * @brief Write an in-memory page(src) to the on-disk page
 */
void file_write_page(int fd, pagenum_t pagenum, const page_t* src) {
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
