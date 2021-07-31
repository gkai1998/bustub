//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  latch_.lock();
  Page *fetch_page;
  frame_id_t fetch_frame_id;
  if (page_table_.find(page_id) != page_table_.end()) {
    fetch_frame_id = page_table_[page_id];
    fetch_page = pages_ + fetch_frame_id;
    fetch_page->pin_count_++;
    latch_.unlock();
    return pages_ + fetch_frame_id;
  }
  if (!free_list_.empty()) {
    fetch_frame_id = free_list_.front();
    free_list_.pop_front();
    fetch_page = pages_ + fetch_frame_id;

  } else if (replacer_->Size() > 0) {
    bool issuccess = replacer_->Victim(&fetch_frame_id);
    if (!issuccess) {
      latch_.unlock();
      return nullptr;
    }
    fetch_page = pages_ + fetch_frame_id;
    page_id_t fetch_page_id = fetch_page->GetPageId();
    if (fetch_page->IsDirty()) {
      disk_manager_->WritePage(fetch_page_id, fetch_page->GetData());
    }
    page_table_.erase(fetch_frame_id);
  } else {
    latch_.unlock();
    return nullptr;
  }
  page_table_.insert(std::pair<page_id_t, frame_id_t>(page_id, fetch_frame_id));
  fetch_page->pin_count_ = 1;
  fetch_page->is_dirty_ = false;
  fetch_page->page_id_ = page_id;
  disk_manager_->ReadPage(fetch_page->page_id_, fetch_page->GetData());
  latch_.unlock();
  return fetch_page;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  latch_.lock();
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page *unpin_page = pages_ + frame_id;
  unpin_page->is_dirty_ = is_dirty;
  if (unpin_page->pin_count_ <= 0) {
    latch_.unlock();
    return false;
  }
  unpin_page->pin_count_--;
  if (unpin_page->pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }
  latch_.unlock();
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  latch_.lock();
  frame_id_t frame_id;
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return false;
  }
  frame_id = page_table_[page_id];
  Page *flush_page = pages_ + frame_id;
  if (flush_page->IsDirty()) {
    flush_page->is_dirty_ = false;
    disk_manager_->WritePage(flush_page->GetPageId(), flush_page->GetData());
  }
  page_table_.erase(page_id);
  free_list_.push_back(frame_id);
  replacer_->Pin(frame_id);
  latch_.unlock();
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  latch_.lock();
  frame_id_t frame_id;
  Page *out_page;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    out_page = pages_ + frame_id;
  } else if (replacer_->Size() > 0) {
    bool issuccess = replacer_->Victim(&frame_id);
    if (!issuccess) {
      *page_id = INVALID_PAGE_ID;
      latch_.unlock();
      return nullptr;
    }
    out_page = pages_ + frame_id;
    page_id_t out_page_id = out_page->GetPageId();
    if (out_page->IsDirty()) {
      disk_manager_->WritePage(out_page_id, out_page->GetData());
    }
    page_table_.erase(out_page_id);
  } else {
    *page_id = INVALID_PAGE_ID;
    latch_.unlock();
    return nullptr;
  }
  page_id_t new_page_id;
  new_page_id = disk_manager_->AllocatePage();
  out_page->ResetMemory();
  out_page->page_id_ = new_page_id;
  out_page->pin_count_ = 1;
  page_table_.insert(std::pair<page_id_t, frame_id_t>(new_page_id, frame_id));
  *page_id = new_page_id;
  latch_.unlock();
  return out_page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  latch_.lock();
  if (page_table_.find(page_id) == page_table_.end()) {
    disk_manager_->DeallocatePage(page_id);
    latch_.unlock();
    return true;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page *delete_page = pages_ + frame_id;
  if (delete_page->pin_count_ > 0) {
    latch_.lock();
    return false;
  }
  page_table_.erase(page_id);
  replacer_->Pin(frame_id);
  delete_page->ResetMemory();
  free_list_.push_back(frame_id);
  disk_manager_->DeallocatePage(page_id);
  latch_.unlock();
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  for (auto iter : page_table_) {
    FetchPageImpl(iter.first);
  }
}

}  // namespace bustub
