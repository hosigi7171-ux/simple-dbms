#ifndef FILE_H
#define FILE_H

#include "common_config.h"
#include "page.h"

pagenum_t file_alloc_page(int fd);
void file_free_page(int fd, pagenum_t pagenum);
void file_read_page(int fd, pagenum_t pagenum, page_t *dest);
void file_write_page(int fd, pagenum_t pagenum, const page_t *src);

#endif