//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Create a new page in the buffer pool. Set page_id to the new page's id, or nullptr if all frames
 * are currently in use and not evictable (in another word, pinned).
 *
 * You should pick the replacement frame from either the free list or the replacer (always find from the free list
 * first), and then call the AllocatePage() method to get a new page id. If the replacement frame has a dirty page,
 * you should write it back to the disk first. You also need to reset the memory and metadata for the new page.
 *
 * Remember to "Pin" the frame by calling replacer.SetEvictable(frame_id, false), "UnPin the frame by calling
 * replacer.SetEvictable(frame_id, true)" so that the replacer wouldn't evict the frame before the buffer pool manager
 * "Unpin"s it. Also, remember to record the access history of the frame in the replacer for the lru-k algorithm to
 * work.
 *
 * @param[out] page_id id of created page
 * @return nullptr if no new pages could be created, otherwise pointer to new page
 */

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> guard(latch_);
  Page *res_page = nullptr;
  /** 1. ????????? free list ??????????????? */
  if (!free_list_.empty()) {
    frame_id_t frame_index = free_list_.front();
    free_list_.pop_front();
    page_id_t new_page_id = AllocatePage();
    *page_id = new_page_id;
    res_page = new (pages_ + frame_index) Page(); /** ?????????????????? frame_index ????????????????????? */

    res_page->page_id_ = new_page_id;  // ??? page_id ?????????????????????
    res_page->pin_count_ = 1;          // ???????????????????????? NewPage, pin_count ??????????????? 1 ??????
    page_table_->Insert(new_page_id, frame_index);
    /** LRU-k replacer ???????????? */
    replacer_->RecordAccess(frame_index);
    replacer_->SetEvictable(frame_index, false);
    return res_page;
  }
  /** 2. ?????? free list ???????????? ???????????????????????????????????? page */
  frame_id_t frame_index;
  if (!replacer_->Evict(&frame_index)) {
    return nullptr;
  }
  res_page = pages_ + frame_index; /** ????????????????????? frame page */
  if (res_page->IsDirty()) {
    disk_manager_->WritePage(res_page->GetPageId(), res_page->GetData());
    res_page->is_dirty_ = false;
  }
  page_id_t new_page_id = AllocatePage();

  *page_id = new_page_id; /** ??????????????? ???Page ??? Id */
  res_page->ResetMemory();
  res_page->pin_count_ = 1;

  page_table_->Remove(res_page->GetPageId()); /** ??? Hash ????????????????????? ????????????????????????????????? */
  page_table_->Insert(new_page_id, frame_index);

  res_page->page_id_ = new_page_id;

  replacer_->RecordAccess(frame_index);        /** ?????? LRU-k ???????????????  */
  replacer_->SetEvictable(frame_index, false); /** ???????????????????????????(Pin) */
  return res_page;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Fetch the requested page from the buffer pool. Return nullptr if page_id needs to be fetched from the disk
 * but all frames are currently in use and not evictable (in another word, pinned).
 *
 * First search for page_id in the buffer pool. If not found, pick a replacement frame from either the free list or
 * the replacer (always find from the free list first), read the page from disk by calling disk_manager_->ReadPage(),
 * and replace the old page in the frame. Similar to NewPgImp(), if the old page is dirty, you need to write it back
 * to disk and update the metadata of the new page
 *
 * In addition, remember to disable eviction and record the access history of the frame like you did for NewPgImp().
 *
 * @param page_id id of page to be fetched
 * @return nullptr if page_id cannot be fetched, otherwise pointer to the requested page
 */

/** ????????? page ????????????????????????????????? buffer pool ???????????????????????????????????????????????????????????? */
auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::lock_guard<std::mutex> guard(latch_);
  Page *res_page = nullptr;
  /** 1. ????????? Pages ???????????????????????????????????????????????? pin  */
  frame_id_t frame_index;
  if (page_table_->Find(page_id, frame_index)) {  // ??????????????????????????? frame_id
    res_page = pages_ + frame_index;
    res_page->pin_count_++; /** pin count++, ??????????????????????????? */
    replacer_->SetEvictable(frame_index, false);
    replacer_->RecordAccess(frame_index);  // ?????? LRU ???, ???????????????????????????
    return res_page;
  }
  /** 2. ???????????????????????????????????????????????????????????????????????????????????????????????? free list ??????????????? */
  if (!free_list_.empty()) {
    frame_index = free_list_.back();
    free_list_.pop_back();
    res_page = pages_ + frame_index;
  } else { /** 3. ?????????????????? free_list ?????? ???????????????????????????????????? */
    if (replacer_->Evict(&frame_index)) {
      res_page = pages_ + frame_index;
    } else {
      return nullptr;
    }
    /** Run here means there is a page is evicted, If the page is dirty, flush to disk first  */
    if (res_page->IsDirty()) {
      disk_manager_->WritePage(res_page->GetPageId(), res_page->GetData());
      res_page->is_dirty_ = false;
    }
    res_page->pin_count_ = 0; /** ?????? pin_count */
  }
  /** Run here means the page we have determined, and the page is null now */
  page_table_->Remove(res_page->GetPageId());
  page_table_->Insert(page_id, frame_index);
  replacer_->RecordAccess(frame_index);
  replacer_->SetEvictable(frame_index, false); /** ???????????????????????????????????? */
  res_page->page_id_ = page_id;
  res_page->ResetMemory(); /** ??????????????????????????? ?????????????????????????????? Reset ??????*/
  disk_manager_->ReadPage(page_id, res_page->GetData());
  res_page->pin_count_++;
  return res_page;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Unpin the target page from the buffer pool. If page_id is not in the buffer pool or its pin count is already
 * 0, return false.
 *
 * Decrement the pin count of a page. If the pin count reaches 0, the frame should be evictable by the replacer.
 * Also, set the dirty flag on the page to indicate if the page was modified.
 *
 * @param page_id id of page to be unpinned
 * @param is_dirty true if the page should be marked as dirty, false otherwise
 * @return false if the page is not in the page table or its pin count is <= 0 before this call, true otherwise
 */
auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  frame_id_t frame_index;
  if (!page_table_->Find(page_id, frame_index)) {
    return false; /** ?????????????????? page_table ??????????????? */
  }
  Page *res_page = pages_ + frame_index;
  if (res_page->pin_count_ <= 0) {
    return false; /** ????????? pin_count ???0*/
  }
  res_page->pin_count_--;  // ????????? pin_count = 0
  if (res_page->pin_count_ == 0) {
    replacer_->SetEvictable(frame_index, true);
  }
  if (is_dirty) { /** ?????????????????? dirty ?????????????????? ???????????????????????????????????? */
    res_page->is_dirty_ = is_dirty;
  }
  return true;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Flush the target page to disk.
 *
 * Use the DiskManager::WritePage() method to flush a page to disk, REGARDLESS of the dirty flag.
 * Unset the dirty flag of the page after flushing.
 *
 * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
 * @return false if the page could not be found in the page table, true otherwise
 */

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  frame_id_t frame_index;
  if (!page_table_->Find(page_id, frame_index)) {
    return false;
  }
  Page *res_page = pages_ + frame_index;
  disk_manager_->WritePage(res_page->GetPageId(), res_page->GetData());
  res_page->is_dirty_ = false;
  return true;
}

/** Flush all of the pages, passed the num of pages, from id [0 ~ pool_size - 1] */
void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::lock_guard<std::mutex> guard(latch_);
  size_t pool_size = GetPoolSize();
  Page *page = nullptr;
  for (size_t i = 0; i < pool_size; i++) {
    page = pages_ + i;
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Delete a page from the buffer pool. If page_id is not in the buffer pool, do nothing and return true. If the
 * page is pinned and cannot be deleted, return false immediately.
 *
 * After deleting the page from the page table, stop tracking the frame in the replacer and add the frame
 * back to the free list. Also, reset the page's memory and metadata. Finally, you should call DeallocatePage() to
 * imitate freeing the page on the disk.
 *
 * @param page_id id of page to be deleted
 * @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
 */

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  frame_id_t frame_index;
  if (!page_table_->Find(page_id, frame_index)) {
    return true;
  } /** ????????? page_id ???????????? buffer_pool ??? */
  Page *res_page = pages_ + frame_index;
  if (res_page->GetPinCount() != 0) {
    return false;
  } /** ????????? Pinned, ??????????????????????????? */

  /** ???page_table???lru ??????????????? */
  page_table_->Remove(page_id);
  replacer_->Remove(frame_index);
  free_list_.push_back(frame_index);

  /** Reset data */
  res_page->ResetMemory();
  res_page->pin_count_ = 0;
  res_page->is_dirty_ = false;
  res_page->page_id_ = INVALID_PAGE_ID;
  DeallocatePage(page_id); /** Deallocate Page ??????????????????????????? ???*/
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
