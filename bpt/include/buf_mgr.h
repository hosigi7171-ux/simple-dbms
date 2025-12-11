#ifndef BUF_MGR_H
#define BUF_MGR_H

#include <unordered_map>

#include "common_config.h"
#include "page.h"

#define PREFETCH_SIZE 3
#define INVALID_FRAME -1
#define INVALID_TABLE_ID -1

/**
 * dto for make_and_pin_page
 */
typedef struct {
  page_t* page_ptr;
  pagenum_t page_num;
} allocated_page_info_t;

typedef struct {
  void* frame;
  tableid_t table_id;
  pagenum_t page_num;
  bool is_dirty;
  int pin_count;
  bool ref_bit;
  // fields will be added later
} buf_ctl_block_t;

typedef struct {
  buf_ctl_block_t* frames;
  int frames_size;
  int clock_hand;
  std::unordered_map<pagenum_t, frame_idx_t> page_table[MAX_TABLE_COUNT + 1];
} buffer_manager_t;

extern buffer_manager_t buf_mgr;

// flush buffer
void flush_table_buffer(int fd, tableid_t table_id);
void flush_all_buffers(void);
void flush_frame(int fd, tableid_t table_id, frame_idx_t frame_idx);

// read/write buffer
header_page_t* read_header_page(int fd, tableid_t table_id);
page_t* get_page_from_buffer(
    pagenum_t page_num,
    std::unordered_map<pagenum_t, frame_idx_t>& frame_mapper);
page_t* read_buffer(int fd, tableid_t table_id, pagenum_t page_num);
frame_idx_t load_page_into_buffer(int fd, tableid_t table_id,
                                  pagenum_t page_num);
void set_new_bcb(tableid_t table_id, pagenum_t page_num, frame_idx_t frame_idx,
                 page_t* page_buf);
void set_new_prefetched_bcb(tableid_t table_id, pagenum_t page_num,
                            frame_idx_t frame_idx, page_t* page_buf);
void prefetch(int fd, pagenum_t page_num, tableid_t table_id,
              frame_idx_t frame_idx,
              std::unordered_map<pagenum_t, frame_idx_t>& frame_mapper);
void write_buffer(tableid_t table_id, pagenum_t page_num, page_t* page);
allocated_page_info_t make_and_pin_page(int fd, tableid_t table_id);
frame_idx_t get_frame_index_by_page(tableid_t table_id, pagenum_t page_num);
void clear_frame_and_page_table(tableid_t table_id, pagenum_t page_num,
                                frame_idx_t frame_idx);
void free_page_in_buffer(int fd, tableid_t table_id, pagenum_t page_num);

// set pin count
void pin(tableid_t table_id, pagenum_t page_num);
void pin_frame(frame_idx_t frame_idx);
void unpin(tableid_t table_id, pagenum_t page_num);
void unpin_frame(frame_idx_t frame_idx);

void mark_dirty(tableid_t table_id, pagenum_t page_num);

// clock algorithm
void update_clock_hand(void);
frame_idx_t find_free_frame_index(int fd, tableid_t table_id,
                                  pagenum_t page_num);

#endif