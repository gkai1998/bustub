//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : size(num_pages), timestamp(1) { hash_map.reserve(size); }

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  lock.lock();
  if (hash_map.empty()) {
    lock.unlock();
    return false;
  }

  frame_id_t result = hash_map.begin()->first;
  int result_timestamp = hash_map.begin()->second;
  auto it = hash_map.begin();
  for (it++; it != hash_map.end(); it++) {
    if ((*it).second < result_timestamp) {
      result = (*it).first;
      result_timestamp = (*it).second;
    }
  }
  *frame_id = result;
  lock.unlock();
  Pin(result);
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  lock.lock();
  hash_map.erase(frame_id);
  lock.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  lock.lock();
  if (hash_map.find(frame_id) != hash_map.end()) {
    hash_map[frame_id] = timestamp++;
    lock.unlock();
    return;
  }
  hash_map.insert({frame_id, timestamp++});
  lock.unlock();
}

size_t LRUReplacer::Size() { return hash_map.size(); }

}  // namespace bustub
