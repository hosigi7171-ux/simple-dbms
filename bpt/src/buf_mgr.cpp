#include "buf_mgr.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <unordered_map>

#include "file.h"

buffer_manager_t buf_mgr = {0};  // temp buffer manager

/**
 * flush buffer-----------------------------------------------------
 */

void flush_table_buffer(int fd, tableid_t table_id) {
  std::unordered_map<pagenum_t, frame_idx_t>& frame_mapper =
      buf_mgr.page_table[table_id];

  for (auto it = frame_mapper.begin(); it != frame_mapper.end(); it++) {
    frame_idx_t fidx = it->second;
    flush_frame(fd, table_id, fidx);
  }
}

void flush_frame(int fd, tableid_t table_id, frame_idx_t frame_idx) {
  buf_ctl_block_t* bcb = &buf_mgr.frames[frame_idx];

  if (bcb->is_dirty && bcb->pin_count == 0) {
    file_write_page(fd, bcb->page_num, (page_t*)bcb->frame);
    bcb->is_dirty = false;
    bcb->ref_bit = false;
    bcb->frame = NULL;
    bcb->page_num = 0;
    bcb->table_id = 0;
  }
}

/**
 * read/write buffer-----------------------------------------------------
 */

/**
 * helper function for read_buffer
 * @brief Get the page from buffer object
 */
page_t* get_page_from_buffer(
    pagenum_t page_num,
    std::unordered_map<pagenum_t, frame_idx_t>& frame_mapper) {
  frame_idx_t frame_idx = frame_mapper[page_num];
  buf_ctl_block_t* bcb = &buf_mgr.frames[frame_idx];

  pin_frame(frame_idx);

  return (page_t*)bcb->frame;
}

/**
 * 버퍼에서 페이지를 읽기
 */
page_t* read_buffer(int fd, tableid_t table_id, pagenum_t page_num) {
  std::unordered_map<pagenum_t, frame_idx_t>& frame_mapper =
      buf_mgr.page_table[table_id];

  // Case: if page exists in buffer
  if (frame_mapper.count(page_num)) {
    return get_page_from_buffer(page_num, frame_mapper);
  }

  // Case: not exsits, read page from disk and write buffer
  frame_idx_t frame_idx = load_page_into_buffer(fd, table_id, page_num);

  prefetch(fd, page_num, table_id, frame_idx, frame_mapper);

  return (page_t*)buf_mgr.frames[frame_idx].frame;
}

/**
 * 버퍼에 페이지를 작성한다
 */
void write_buffer(tableid_t table_id, pagenum_t page_num, page_t* page) {
  frame_idx_t frame_idx = buf_mgr.page_table[table_id][page_num];
  buf_ctl_block_t* bcb = &buf_mgr.frames[frame_idx];
  bcb->frame = page;
  bcb->is_dirty = true;
  bcb->ref_bit = true;
}

header_page_t* read_header_page(int fd, tableid_t table_id) {
  page_t* header_page_buff = read_buffer(fd, table_id, HEADER_PAGE_POS);
  header_page_t* header_page_ptr = (header_page_t*)header_page_buff;

  return header_page_ptr;
}

/**
 * PREFETCH_SIZE만큼 더 페이지를 미리 버퍼에 가져오는 함수
 */
void prefetch(int fd, pagenum_t page_num, tableid_t table_id,
              frame_idx_t frame_idx,
              std::unordered_map<pagenum_t, frame_idx_t>& frame_mapper) {
  header_page_t* header_page_ptr = read_header_page(fd, table_id);
  int total_pages = header_page_ptr->num_of_pages;

  for (int index = 1; index <= PREFETCH_SIZE; index++) {
    pagenum_t prefetched_page_num = page_num + index;

    // invalid
    if (prefetched_page_num >= total_pages) {
      break;
    }

    // if already exists in buffer
    if (frame_mapper.count(prefetched_page_num)) {
      continue;
    }

    // valid case
    frame_idx_t prefetched_index =
        find_free_frame_index(fd, table_id, page_num);

    page_t* frame_ptr = (page_t*)buf_mgr.frames[prefetched_index].frame;
    file_read_page(fd, prefetched_page_num, frame_ptr);
    buf_mgr.page_table[table_id].insert(
        std::make_pair(prefetched_page_num, prefetched_index));
    set_new_prefetched_bcb(table_id, prefetched_page_num, prefetched_index,
                           frame_ptr);
  }
}

/**
 * @brief Set the new bcb object
 */
void set_new_bcb(tableid_t table_id, pagenum_t page_num, frame_idx_t frame_idx,
                 page_t* page_buf) {
  buf_mgr.frames[frame_idx].frame = page_buf;
  buf_mgr.frames[frame_idx].table_id = table_id;
  buf_mgr.frames[frame_idx].page_num = page_num;
  buf_mgr.frames[frame_idx].is_dirty = false;
  buf_mgr.frames[frame_idx].pin_count = 1;
  buf_mgr.frames[frame_idx].ref_bit = true;
}

/**
 * @brief Set the new prefetched bcb object
 */
void set_new_prefetched_bcb(tableid_t table_id, pagenum_t page_num,
                            frame_idx_t frame_idx, page_t* page_buf) {
  buf_mgr.frames[frame_idx].frame = page_buf;
  buf_mgr.frames[frame_idx].table_id = table_id;
  buf_mgr.frames[frame_idx].page_num = page_num;
  buf_mgr.frames[frame_idx].is_dirty = false;
  buf_mgr.frames[frame_idx].pin_count = 0;
  buf_mgr.frames[frame_idx].ref_bit = true;
}

/**
 * 디스크에서 페이지를 읽어서 버퍼에 올림
 */
frame_idx_t load_page_into_buffer(int fd, tableid_t table_id,
                                  pagenum_t page_num) {
  frame_idx_t frame_idx = find_free_frame_index(fd, table_id, page_num);

  page_t* frame_ptr = (page_t*)buf_mgr.frames[frame_idx].frame;
  file_read_page(fd, page_num, frame_ptr);

  buf_mgr.page_table[table_id].insert(std::make_pair(page_num, frame_idx));

  set_new_bcb(table_id, page_num, frame_idx, frame_ptr);

  return frame_idx;
}

/**
 * 디스크에 페이지를 할당하고 버퍼에 올림
 * @return allocated_page_info_t = {page_t*, pagenum_t}
 */
allocated_page_info_t make_and_pin_page(int fd, tableid_t table_id) {
  pagenum_t page_num = file_alloc_page(fd);
  if (page_num == PAGE_NULL) {
    perror("Node creation.");
    exit(EXIT_FAILURE);
  }

  frame_idx_t frame_idx = find_free_frame_index(fd, table_id, page_num);
  page_t* frame_ptr = (page_t*)buf_mgr.frames[frame_idx].frame;
  buf_mgr.page_table[table_id].insert(std::make_pair(page_num, frame_idx));

  set_new_bcb(table_id, page_num, frame_idx, frame_ptr);

  return {frame_ptr, page_num};
}

frame_idx_t get_frame_index_by_page(tableid_t table_id, pagenum_t page_num) {
  auto& page_map = buf_mgr.page_table[table_id];
  auto it = page_map.find(page_num);

  if (it == page_map.end()) {
    return INVALID_FRAME;
  }
  return it->second;
}

/**
 * helper function for free_page_in_buffer
 * remove frame in buffer and mapper
 */
void clear_frame_and_page_table(tableid_t table_id, pagenum_t page_num,
                                frame_idx_t frame_idx) {
  buf_mgr.frames[frame_idx].table_id = INVALID_TABLE_ID;
  buf_mgr.frames[frame_idx].page_num = PAGE_NULL;
  buf_mgr.frames[frame_idx].is_dirty = false;
  buf_mgr.frames[frame_idx].pin_count = 0;
  buf_mgr.frames[frame_idx].ref_bit = false;

  buf_mgr.page_table[table_id].erase(page_num);
}

/**
 * 버퍼 내부의 해당하는 프레임을 제거하고 디스크 상에서 free까지 함
 * 호출시 추가로 unpin할 필요는 없음
 */
void free_page_in_buffer(int fd, tableid_t table_id, pagenum_t page_num) {
  frame_idx_t frame_idx = get_frame_index_by_page(table_id, page_num);

  if (frame_idx != INVALID_FRAME) {
    clear_frame_and_page_table(table_id, page_num, frame_idx);
  }

  file_free_page(fd, page_num);
}

/**
 * clock alogorithm---------------------------------------------------
 */

void update_clock_hand() {
  buf_mgr.clock_hand = (buf_mgr.clock_hand + 1) % buf_mgr.frames_size;
}

/**
 * clock algorithm main function
 * frame배열에서 빈 페이지를 가져온다
 */
frame_idx_t find_free_frame_index(int fd, tableid_t table_id,
                                  pagenum_t page_num) {
  while (true) {
    buf_ctl_block_t* bcb = &buf_mgr.frames[buf_mgr.clock_hand];
    // Case: if used, skip
    if (bcb->pin_count > 0) {
      update_clock_hand();
      continue;
    }
    // Case: if not used and not ref, found target
    if (!bcb->ref_bit) {
      // dirty page must be written
      if (bcb->is_dirty) {
        file_write_page(fd, bcb->page_num, (page_t*)bcb->frame);
      }

      // if other page already exists, eviction
      if (bcb->page_num != 0) {
        buf_mgr.page_table[bcb->table_id].erase(bcb->page_num);
      }

      frame_idx_t target_idx = buf_mgr.clock_hand;
      update_clock_hand();
      return target_idx;
    }

    // Case: give chance
    bcb->ref_bit = false;
    update_clock_hand();
  }
}

/**
 * set/unset pin count---------------------------------------------------
 */

void pin(tableid_t table_id, pagenum_t page_num) {
  auto it = buf_mgr.page_table[table_id].find(page_num);

  if (it != buf_mgr.page_table[table_id].end()) {
    frame_idx_t frame_idx = it->second;
    pin_frame(frame_idx);
  }
}

void pin_frame(frame_idx_t frame_idx) {
  buf_ctl_block_t* bcb = &buf_mgr.frames[frame_idx];

  bcb->pin_count += 1;
  bcb->ref_bit = true;
}

void unpin(tableid_t table_id, pagenum_t page_num) {
  auto it = buf_mgr.page_table[table_id].find(page_num);

  if (it != buf_mgr.page_table[table_id].end()) {
    frame_idx_t frame_idx = it->second;
    unpin_frame(frame_idx);
  }
}

void unpin_frame(frame_idx_t frame_idx) {
  buf_ctl_block_t* bcb = &buf_mgr.frames[frame_idx];

  int next_pin_count = bcb->pin_count - 1;
  if (next_pin_count < 0) {
    next_pin_count = 0;
  }
  bcb->pin_count = next_pin_count;
}
