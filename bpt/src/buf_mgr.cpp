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
  // test
  static int read_buffer_call_count = 0;
  read_buffer_call_count++;

#ifdef TEST_ENV
  // if (read_buffer_call_count % 10 == 0) {
  //   fprintf(stderr, "DEBUG: read_buffer called %d times (page_num=%lu)\n",
  //           read_buffer_call_count, page_num);
  // }

  if (read_buffer_call_count > 10000) {
    fprintf(stderr,
            "ERROR: read_buffer called too many times (%d)! Possible infinite "
            "loop.\n",
            read_buffer_call_count);
    fprintf(stderr, "Last page_num: %lu, table_id: %d\n", page_num, table_id);
    exit(EXIT_FAILURE);
  }
#endif
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
#ifdef TEST_ENV
  static int header_call_count = 0;
  header_call_count++;

  if (header_call_count > 10000) {
    fprintf(stderr, "ERROR: read_header_page called too many times (%d)!\n",
            header_call_count);
    exit(EXIT_FAILURE);
  }
#endif
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
#ifdef TEST_ENV
  return;
#endif
  header_page_t* header_page_ptr = read_header_page(fd, table_id);
  int total_pages = header_page_ptr->num_of_pages;
  unpin(table_id, HEADER_PAGE_POS);

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
#ifdef TEST_ENV
  static int load_count = 0;
  load_count++;
  if (load_count % 10 == 0) {
    fprintf(stderr,
            "DEBUG: load_page_into_buffer called %d times (page_num=%lu)\n",
            load_count, page_num);
  }
#endif
  frame_idx_t frame_idx = find_free_frame_index(fd, table_id, page_num);

  page_t* frame_ptr = (page_t*)buf_mgr.frames[frame_idx].frame;
  file_read_page(fd, page_num, frame_ptr);

  buf_mgr.page_table[table_id].insert(std::make_pair(page_num, frame_idx));

  set_new_bcb(table_id, page_num, frame_idx, frame_ptr);

  return frame_idx;
}

/**
 * @brief Mark the page in the buffer as dirty.
 */
void mark_dirty(tableid_t table_id, pagenum_t page_num) {
  frame_idx_t frame_idx = get_frame_index_by_page(table_id, page_num);
  if (frame_idx != INVALID_FRAME) {
    buf_mgr.frames[frame_idx].is_dirty = true;
  } else {
    fprintf(stderr,
            "WARNING: Attempted to mark unbuffered page %lu as dirty.\n",
            page_num);
  }
}

/**
 * 디스크에 페이지를 할당하고 버퍼에 올림
 * @return allocated_page_info_t = {page_t*, pagenum_t}
 */
allocated_page_info_t make_and_pin_page(int fd, tableid_t table_id) {
  pagenum_t page_num;

  header_page_t* header = read_header_page(fd, table_id);

  if (header == NULL) {
    perror("Failed to read header page in make_and_pin_page.");
    exit(EXIT_FAILURE);
  }

  if (header->free_page_num != PAGE_NULL) {
    // use free page list
    page_num = header->free_page_num;
    free_page_t* free_page = (free_page_t*)read_buffer(fd, table_id, page_num);
    header->free_page_num = free_page->next_free_page_num;
    unpin(table_id, page_num);
  } else {
    // allocate in order
    page_num = header->num_of_pages;
    header->num_of_pages += 1;
  }

  mark_dirty(table_id, HEADER_PAGE_POS);
  unpin(table_id, HEADER_PAGE_POS);

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
 * Header Page 업데이트 및 Free Page 링크 구성
 */
void free_page_in_buffer(int fd, tableid_t table_id, pagenum_t page_num) {
  header_page_t* header = read_header_page(fd, table_id);

  if (header == NULL) {
    perror("Failed to read header page in free_page_in_buffer.");
    exit(EXIT_FAILURE);
  }

  free_page_t new_free_page;
  memset(&new_free_page, 0, PAGE_SIZE);
  new_free_page.next_free_page_num = header->free_page_num;

  header->free_page_num = page_num;
  mark_dirty(table_id, HEADER_PAGE_POS);
  unpin(table_id, HEADER_PAGE_POS);

  frame_idx_t frame_idx = get_frame_index_by_page(table_id, page_num);

  if (frame_idx != INVALID_FRAME) {
    clear_frame_and_page_table(table_id, page_num, frame_idx);
  }

  file_write_page(fd, page_num, (page_t*)&new_free_page);
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
#ifdef TEST_ENV
  static int find_frame_call_count = 0;
  find_frame_call_count++;

  if (find_frame_call_count > 50000) {
    fprintf(stderr,
            "ERROR: find_free_frame_index called too many times (%d)!\n",
            find_frame_call_count);
    fprintf(stderr, "Requested for page_num=%lu, table_id=%d\n", page_num,
            table_id);
    fprintf(stderr, "Buffer size: %d\n", buf_mgr.frames_size);

    // 프레임 상태 출력
    int pinned_count = 0;
    for (int i = 0; i < buf_mgr.frames_size; i++) {
      if (buf_mgr.frames[i].pin_count > 0) {
        pinned_count++;
        fprintf(stderr,
                "Frame[%d]: PINNED (pin_count=%d, page_num=%lu, table_id=%d)\n",
                i, buf_mgr.frames[i].pin_count, buf_mgr.frames[i].page_num,
                buf_mgr.frames[i].table_id);
      }
    }
    fprintf(stderr, "Total pinned frames: %d / %d\n", pinned_count,
            buf_mgr.frames_size);
    exit(EXIT_FAILURE);
  }
#endif
  int iterations = 0;
  const int MAX_ITERATIONS =
      buf_mgr.frames_size * 3;  // 일단 최대 3바퀴만 나중에 수정할지도?

  while (true) {
    buf_ctl_block_t* bcb = &buf_mgr.frames[buf_mgr.clock_hand];

    if (iterations >= MAX_ITERATIONS) {
      fprintf(stderr,
              "ERROR: find_free_frame_index - all frames are pinned!\n");
      fprintf(stderr, "Buffer size: %d, Iterations: %d\n", buf_mgr.frames_size,
              iterations);

      for (int i = 0; i < buf_mgr.frames_size; i++) {
        fprintf(
            stderr,
            "Frame[%d]: pin_count=%d, ref_bit=%d, table_id=%d, page_num=%lu\n",
            i, buf_mgr.frames[i].pin_count, buf_mgr.frames[i].ref_bit,
            buf_mgr.frames[i].table_id, buf_mgr.frames[i].page_num);
      }

      exit(EXIT_FAILURE);
    }
    iterations++;

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
