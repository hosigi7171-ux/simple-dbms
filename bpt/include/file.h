#ifndef FILE_H
#define FILE_H

#include "common_config.h"
#include "page.h"

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(int fd);
// Free an on-disk page to the free page list
void file_free_page(int fd, pagenum_t pagenum);
// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int fd, pagenum_t pagenum, page_t *dest);
// Write an in-memory page(src) to the on-disk page
void file_write_page(int fd, pagenum_t pagenum, const page_t *src);

#endif