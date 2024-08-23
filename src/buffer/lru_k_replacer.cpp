//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

#include "common/logger.h"
namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  // LOG_INFO("create. replacer_size_: %ld, k_: %ld", num_frames, k);
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  // LOG_INFO("evict set size: %ld. total frames: %ld.", evictable_coll_.size(), frames_coll_enforced_.size());
  if (evictable_coll_.empty()) {
    return false;
  }

  frame_id_t track_frame = INT32_MAX;
  bool is_success = false;

  size_t damn = 0;
  if (!total_first_.empty()) {
    damn = total_first_.begin()->first;
    track_frame = total_first_.begin()->second;
    is_success = true;
  } else if (!record_first_.empty()) {
    damn = record_first_.begin()->first;
    track_frame = record_first_.begin()->second;
    is_success = true;
  }

  if (is_success) {
    *frame_id = track_frame;

    total_first_.erase(damn);
    record_first_.erase(damn);

    evictable_coll_.erase(track_frame);
    replacer_size_--;
    frames_coll_enforced_.erase(track_frame);
    frames_st_end_.erase(track_frame);
  }

  if (!first_time_list_.empty()) {
    auto itx = first_time_list_.front();
    track_frame = itx.first;
    for (const auto& x : frames_coll_iters_[itx.second]) {
      first_time_list_.erase(x);
    }

    frames_coll_iters_.erase(itx.second);
    evictable_coll_.erase(track_frame);
    replacer_size_--;
    frames_coll_enforced_.erase(track_frame);
    frames_st_end_.erase(track_frame);
    return true;
  }

  return is_success;
  //   return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  current_timestamp_++;
  //   auto now = std::chrono::system_clock::now();
  //   auto milliseconds_since_epoch = std::chrono::time_point_cast<std::chrono::milliseconds>(now)
  //   .time_since_epoch().count();
  auto it = frames_coll_enforced_.find(frame_id);
  if (it == frames_coll_enforced_.end()) {
    frames_coll_enforced_.insert({frame_id, std::vector<int64_t>(k_ + 1)});
    frames_coll_iters_.insert({frame_id, std::list<std::list<std::pair<size_t, frame_id_t>>::iterator>()});
    // fuck, `std::vector<int64_t>(k_)` only results in size == capacity == k_.
    it = frames_coll_enforced_.find(frame_id);
    it->second.at(k_) = 0;
    frames_st_end_.insert({frame_id, {0, 0}});
  }

  size_t prev_save_time = it->second.at(frames_st_end_[frame_id].first);
  it->second.at(frames_st_end_[frame_id].second) = current_timestamp_;

  frames_st_end_[frame_id].second = (frames_st_end_[frame_id].second + 1) % k_;

  if (it->second.at(k_) == 0 && frames_st_end_[frame_id].first == frames_st_end_[frame_id].second) {
    it->second.at(k_) = 1;
    if (evictable_coll_.find(frame_id) != evictable_coll_.end()) {
      total_first_.erase(prev_save_time);
      record_first_.insert({prev_save_time, frame_id});
    }
  } else if (it->second.at(k_) == 1) {
    frames_st_end_[frame_id].first = (frames_st_end_[frame_id].first + 1) % k_;
    if (evictable_coll_.find(frame_id) != evictable_coll_.end()) {
      record_first_.erase(prev_save_time);
      record_first_.insert({it->second.at(frames_st_end_[frame_id].first), frame_id});
    }
  }

  if (evictable_coll_.find(frame_id) != evictable_coll_.end()) {
    first_time_list_.push_back({current_timestamp_, frame_id});
    frames_coll_iters_[frame_id].push_back(--first_time_list_.end());
    if (frames_coll_iters_[frame_id].size() > k_) {
      auto itx = frames_coll_iters_[frame_id].front();
      first_time_list_.erase(itx);
      frames_coll_iters_[frame_id].pop_front();
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frames_coll_enforced_.find(frame_id) == frames_coll_enforced_.end()) {
    return;
  }

  auto it = evictable_coll_.find(frame_id);
  size_t saved_first_tsp = frames_coll_enforced_[frame_id][frames_st_end_[frame_id].first];
  if (set_evictable && (it == evictable_coll_.end())) {
    if (frames_coll_enforced_[frame_id].at(k_) == 1) {
      record_first_[saved_first_tsp] = frame_id;
    } else {
      total_first_[saved_first_tsp] = frame_id;
    }
    for (int i = frames_st_end_[frame_id].first; i != frames_st_end_[frame_id].second; i = (i + 1) % k_) {
      size_t tmp_time = frames_coll_enforced_[frame_id][i];
    }
    evictable_coll_.insert({frame_id, true});

    size_t first_idx = frames_st_end_[frame_id].first;
    record_first_.insert({frames_coll_enforced_[frame_id].at(first_idx), frame_id});
    if (frames_coll_enforced_[frame_id].at(k_) == 0) {
      total_first_.insert({frames_coll_enforced_[frame_id].at(first_idx), frame_id});
    }
    replacer_size_++;

  } else if (!set_evictable && (it != evictable_coll_.end())) {
    record_first_.erase(saved_first_tsp);
    total_first_.erase(saved_first_tsp);
    for (const auto& x : frames_coll_iters_[frame_id]) {
      first_time_list_.erase(x);
    }
    frames_coll_iters_.find(frame_id)->second.clear();
    evictable_coll_.erase(it);
    replacer_size_--;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);

  auto it = evictable_coll_.find(frame_id);
  if (it != evictable_coll_.end()) {
    evictable_coll_.erase(it);
    // `along with its access history`.
    record_first_.erase(frames_coll_enforced_[frame_id].at(frames_st_end_[frame_id].first));
    total_first_.erase(frames_coll_enforced_[frame_id].at(frames_st_end_[frame_id].first));
    for (const auto &x : frames_coll_iters_[frame_id]) {
      first_time_list_.erase(x);
    }
    frames_coll_iters_.find(frame_id)->second.clear();

    frames_coll_enforced_.erase(frame_id);
    frames_st_end_.erase(frame_id);
    replacer_size_--;
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return evictable_coll_.size();
  //   return 0;
}

}  // namespace bustub
