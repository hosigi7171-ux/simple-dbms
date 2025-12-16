#include "buf_mgr.h"

#include <db_api.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <unordered_map>

#include "file.h"

buffer_manager_t buf_mgr = {0};  // temp buffer manager

pthread_mutex_t buffer_manager_latch = PTHREAD_MUTEX_INITIALIZER;

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
 * 버퍼에서 페이지를 읽기 with buffer manager latch
 * return hold page latch
 */
buf_ctl_block_t* read_buffer_with_txn(int fd, tableid_t table_id,
                                      pagenum_t page_num) {
  buf_ctl_block_t* bcb = nullptr;

  // buffer_manager_latch로 보호하면서 pin_count 증가
  pthread_mutex_lock(&buffer_manager_latch);

  std::unordered_map<pagenum_t, frame_idx_t>& frame_mapper =
      buf_mgr.page_table[table_id];

  if (frame_mapper.count(page_num)) {
    frame_idx_t fidx = frame_mapper[page_num];
    bcb = &buf_mgr.frames[fidx];
    bcb->pin_count++;  // eviction 방지
  } else {
    frame_idx_t frame_idx = load_page_into_buffer(fd, table_id, page_num);
    prefetch_with_txn(
        fd, page_num, table_id, frame_idx, frame_mapper,
        (header_page_t*)buf_mgr.frames[frame_mapper[HEADER_PAGE_POS]].frame);

    frame_idx_t fidx = frame_mapper[page_num];
    bcb = &buf_mgr.frames[fidx];
    bcb->pin_count++;  // eviction 방지
  }

  pthread_mutex_unlock(&buffer_manager_latch);

  pthread_mutex_lock(&bcb->page_latch);

  return bcb;
}

/**
 * 버퍼에 페이지를 작성한다
 */
void write_buffer(tableid_t table_id, pagenum_t page_num, page_t* page) {
  frame_idx_t frame_idx = get_frame_index_by_page(table_id, page_num);
  if (frame_idx == INVALID_FRAME) {
    fprintf(stderr,
            "ERROR: write_buffer called on non-resident page %lu (table %d). ",
            page_num, table_id);
    return;
  }
  buf_ctl_block_t* bcb = &buf_mgr.frames[frame_idx];
  memcpy(bcb->frame, page, PAGE_SIZE);
  bcb->is_dirty = true;
  bcb->ref_bit = true;
}

header_page_t* read_header_page(int fd, tableid_t table_id) {
  page_t* header_page_buff = read_buffer(fd, table_id, HEADER_PAGE_POS);
  header_page_t* header_page_ptr = (header_page_t*)header_page_buff;

  return header_page_ptr;
}

buf_ctl_block_t* read_header_page_with_txn(int fd, tableid_t table_id) {
  buf_ctl_block_t* header_page_buff =
      read_buffer_with_txn(fd, table_id, HEADER_PAGE_POS);

  return header_page_buff;
}

/**
 * PREFETCH_SIZE만큼 더 페이지를 미리 버퍼에 가져오는 함수
 */
void prefetch(int fd, pagenum_t page_num, tableid_t table_id,
              frame_idx_t frame_idx,
              std::unordered_map<pagenum_t, frame_idx_t>& frame_mapper) {
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
 * PREFETCH_SIZE만큼 더 페이지를 미리 버퍼에 가져오는 함수
 */
void prefetch_with_txn(int fd, pagenum_t page_num, tableid_t table_id,
                       frame_idx_t frame_idx,
                       std::unordered_map<pagenum_t, frame_idx_t>& frame_mapper,
                       header_page_t* header_page_ptr) {
  int total_pages = header_page_ptr->num_of_pages;
  // unpin(table_id, HEADER_PAGE_POS);

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
        find_free_frame_index(fd, table_id, prefetched_page_num);

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
  memset(frame_ptr, 0, PAGE_SIZE);
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
    frame_idx_t current_frame_idx = buf_mgr.clock_hand;
    buf_ctl_block_t* bcb = &buf_mgr.frames[buf_mgr.clock_hand];
    // 헤더 페이지는 eviction 대상이 아님
    if (bcb->page_num == HEADER_PAGE_POS && bcb->ref_bit) {
      update_clock_hand();
      continue;
    }
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
      tableid_t old_table_id = bcb->table_id;
      pagenum_t old_page_num = bcb->page_num;

#ifdef TEST_ENV
      printf("EVICTION: frame_idx=%d, old_table_id=%d, old_page_num=%lu\n",
             current_frame_idx, old_table_id, old_page_num);
#endif

      // write dirty page if needed
      if (bcb->is_dirty) {
        if (old_table_id >= 1 && old_table_id <= MAX_TABLE_COUNT &&
            table_infos[old_table_id].fd > 0) {
#ifdef TEST_ENV
          printf("  -> Writing dirty page to disk\n");
#endif
          file_write_page(table_infos[old_table_id].fd, old_page_num,
                          (page_t*)bcb->frame);
        }
      }

      // remove old page_table mapping
      if (old_table_id >= 1 && old_table_id <= MAX_TABLE_COUNT) {
        auto& page_map = buf_mgr.page_table[old_table_id];
        auto it = page_map.find(old_page_num);

        if (it != page_map.end() && it->second == current_frame_idx) {
#ifdef TEST_ENV
          printf("  -> Removing page_table[%d][%lu] (frame_idx=%d)\n",
                 old_table_id, old_page_num, current_frame_idx);
#endif
          page_map.erase(it);
        } else if (it != page_map.end()) {
#ifdef TEST_ENV
          printf(
              "  -> WARNING: page_table[%d][%lu] points to frame %d, not %d\n",
              old_table_id, old_page_num, it->second, current_frame_idx);
#endif
        }
      }

      frame_idx_t target_idx = current_frame_idx;
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

/**
 * unpin
 */
void unpin(tableid_t table_id, pagenum_t page_num) {
  auto it = buf_mgr.page_table[table_id].find(page_num);

  if (it != buf_mgr.page_table[table_id].end()) {
    frame_idx_t frame_idx = it->second;
    buf_ctl_block_t* bcb = &buf_mgr.frames[frame_idx];

    int next_pin_count = bcb->pin_count - 1;
    if (next_pin_count < 0) {
      next_pin_count = 0;
    }
    bcb->pin_count = next_pin_count;
  }
}

void unpin_bcb(buf_ctl_block_t* bcb) {
  pthread_mutex_lock(&buffer_manager_latch);
  int next_pin_count = bcb->pin_count - 1;
  if (next_pin_count < 0) {
    next_pin_count = 0;
  }
  bcb->pin_count = next_pin_count;
  pthread_mutex_unlock(&buffer_manager_latch);
}
