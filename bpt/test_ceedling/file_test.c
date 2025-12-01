// gcc -I../include ../src/file.c file_test.c -o file_test

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

#include "file.h"
#include "page.h"

extern int fd;
const char *TEST_DB_FILE = "test_db.dat";

void print_header_status(const char *stage) {
  header_page_t header;
  file_read_page(HEADER_PAGE_POS, (page_t *)&header);

  printf("\n[%s]\n", stage);
  printf("Free Page Head (free_page_num): %ld\n", header.free_page_num);
  printf("Total Pages (num_of_pages): %ld\n", header.num_of_pages);
  printf("--------------------------------------\n");
}

void setup_test_file(const char *filename) {
  fd = open(filename, O_RDWR | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) {
    printf("Error opening test file\n");
    exit(EXIT_FAILURE);
  }

  header_page_t initial_header;
  page_t header_buffer;
  memset(&initial_header, 0, sizeof(header_page_t));

  initial_header.free_page_num = PAGE_NULL;
  initial_header.root_page_num = PAGE_NULL;
  initial_header.num_of_pages = HEADER_PAGE_POS + 1; // 1

  memset(&header_buffer, 0, PAGE_SIZE);
  memcpy(header_buffer.data, &initial_header, sizeof(header_page_t));
  file_write_page(HEADER_PAGE_POS, &header_buffer);
}

void cleanup_test_file(const char *filename) {
  if (fd != -1) {
    close(fd);
    fd = -1;
  }
  remove(filename);
}

int main() {
  setup_test_file(TEST_DB_FILE);
  printf("TEST START: Simple File Manager\n");

  pagenum_t p1, p2, p3, p4, p5;

  // Sequential Allocation
  print_header_status("Initial State"); // Head: 0, Total: 1

  p1 = file_alloc_page();
  p2 = file_alloc_page();
  p3 = file_alloc_page();

  printf(
      "[1] Allocated Pages (Sequential): %ld, %ld, %ld (Expected: 1, 2, 3)\n",
      p1, p2, p3);

  print_header_status("After Sequential Allocation"); // Head: 0, Total: 4

  // Deallocation
  file_free_page(p2);
  file_free_page(p3);

  printf("[2] Freed Pages: %ld, %ld (Freed in order: 2 then 3)\n", p2, p3);

  print_header_status("After Deallocation");

  // Allocation with Recycle
  p4 = file_alloc_page();
  printf("[3] Recycled Page 1: %ld (Expected: 3)\n", p4);

  print_header_status("After First Recycle");

  p5 = file_alloc_page();
  printf("[3] Recycled Page 2: %ld (Expected: 2)\n", p5);

  print_header_status("After Second Recycle");

  // Sequential Allocation Check (After Recycle)
  pagenum_t p6 = file_alloc_page();
  printf("[4] Allocated Page (Extension): %ld (Expected: 4)\n", p6);

  print_header_status("After Final Extension");

  cleanup_test_file(TEST_DB_FILE);

  return 0;
}