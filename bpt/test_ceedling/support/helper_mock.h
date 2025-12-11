#include "page.h"

#define MAX_MOCK_PAGES 300

// mock data store
extern page_t MOCK_PAGES[MAX_MOCK_PAGES];

// must called within test's setUp function for using mock read/write page
void setup_data_store(void);

void MOCK_file_read_page(pagenum_t pagenum, page_t *dest, int num_calls);
void MOCK_file_write_page(pagenum_t pagenum, const page_t *src, int num_calls);
pagenum_t MOCK_file_alloc_page(int num_calls);
void MOCK_file_free_page(pagenum_t pagenum, int num_calls);

// ---------------utils for mock-------------------
void init_header_page_for_mock(void);
leaf_page_t get_leaf_page(pagenum_t pagenum);
internal_page_t get_internal_page(pagenum_t pagenum);
header_page_t get_header_page(void);