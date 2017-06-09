#pragma once
#ifndef MICA_POOL_CIRCULAR_LOG_IMPL_H_
#define MICA_POOL_CIRCULAR_LOG_IMPL_H_

namespace mica {
namespace pool {
template <class StaticConfig>
CircularLog<StaticConfig>::CircularLog(const ::mica::util::Config& config,
                                       Alloc* alloc)
    : config_(config), alloc_(alloc) {
  uint64_t size = config.get("size").get_uint64();
  bool concurrent_read = config.get("concurrent_read").get_bool();
  bool concurrent_write = config.get("concurrent_write").get_bool();
  size_t numa_node = config.get("numa_node").get_uint64();

  if (size < kMinimumSize) size = kMinimumSize;

  size = Alloc::roundup(size);
  size = ::mica::util::next_power_of_two(size);
  assert(size <= (kOffsetMask >>
                  1));  // ">> 1" is for sufficient garbage collection time
  assert(size == Alloc::roundup(size));

  if (!concurrent_read)
    concurrent_access_mode_ = 0;
  else if (!concurrent_write)
    concurrent_access_mode_ = 1;
  else
    concurrent_access_mode_ = 2;

  size_ = size;
  mask_ = size - 1;

  lock_ = 0;
  head_ = tail_ = 0;

  size_t alloc_id = alloc_->alloc(size, numa_node);
  if (alloc_id == Alloc::kInvalidId) {
    fprintf(stderr, "error: failed to allocate memory\n");
    assert(false);
    return;
  }
  while (true) {
    data_ = reinterpret_cast<char*>(
        alloc->find_free_address(size + kWrapAroundSize));
    if (data_ == nullptr) {
      fprintf(stderr, "error: failed to find free memory region\n");
      assert(false);
      return;
    }

    if (!alloc_->map(alloc_id, data_, 0, size_)) {
      // TODO: Give up after some trials.
      continue;
    }

    // aliased access across pool end boundary
    if (!alloc_->map(alloc_id, reinterpret_cast<void*>(
                                   reinterpret_cast<char*>(data_) + size_),
                     0, kWrapAroundSize)) {
      alloc_->unmap(data_);
      // TODO: Give up after some trials.
      continue;
    }

    data_ = reinterpret_cast<char*>(data_);
    break;
  }

  if (!alloc_->schedule_release(alloc_id)) {
    perror("");
    fprintf(stderr, "error: failed to schedule releasing memory\n");
    assert(false);
    return;
  }
}

template <class StaticConfig>
CircularLog<StaticConfig>::~CircularLog() {
  if (!alloc_->unmap(data_)) assert(false);
  if (!alloc_->unmap(data_ + size_)) assert(false);
}

template <class StaticConfig>
typename CircularLog<StaticConfig>::Offset CircularLog<StaticConfig>::allocate(
    size_t item_size) {
  assert(item_size <= std::numeric_limits<uint32_t>::max());
  return push_tail(item_size + sizeof(Item));
}

template <class StaticConfig>
void CircularLog<StaticConfig>::release(Offset offset) {
  (void)offset;
}

template <class StaticConfig>
bool CircularLog<StaticConfig>::is_valid(Offset offset) const {
  if (concurrent_access_mode_ == 0)
    return ((tail_ - offset) & kOffsetMask) <= size_;
  else {
    ::mica::util::memory_barrier();
    return ((*(volatile uint64_t*)&tail_ - offset) & kOffsetMask) <= size_;
  }
}

// template <class StaticConfig>
// uint64_t CircularLog<StaticConfig>::get_head() const {
//   return head_;
// }

template <class StaticConfig>
uint64_t CircularLog<StaticConfig>::get_tail() const {
  return tail_;
}

template <class StaticConfig>
uint64_t CircularLog<StaticConfig>::get_mask() const {
  return mask_;
}

template <class StaticConfig>
uint64_t CircularLog<StaticConfig>::get_size() const {
  return size_;
}

template <class StaticConfig>
void CircularLog<StaticConfig>::prefetch_item(Offset offset) const {
  size_t addr =
      reinterpret_cast<size_t>(data_ + (offset & mask_)) & ~(size_t)63;

  // prefetch the item's cache line and the subsequence cache line
  __builtin_prefetch(reinterpret_cast<const char*>(addr), 0, 0);
  __builtin_prefetch(reinterpret_cast<const char*>(addr + 64), 0, 0);
}

template <class StaticConfig>
char* CircularLog<StaticConfig>::get_item(Offset offset) {
  Item* item = reinterpret_cast<Item*>(data_ + (offset & mask_));
  return item->data;
}

template <class StaticConfig>
const char* CircularLog<StaticConfig>::get_item(Offset offset) const {
  const Item* item = reinterpret_cast<const Item*>(data_ + (offset & mask_));
  return item->data;
}

template <class StaticConfig>
char* CircularLog<StaticConfig>::get_item(Offset offset, std::size_t* out_len) {
  Item* item = reinterpret_cast<Item*>(data_ + (offset & mask_));
  *out_len = item->size;
  return item->data;
}

template <class StaticConfig>
const char* CircularLog<StaticConfig>::get_item(Offset offset,
                                                std::size_t* out_len) const {
  const Item* item = reinterpret_cast<const Item*>(data_ + (offset & mask_));
  *out_len = item->size;
  return item->data;
}

template <class StaticConfig>
void CircularLog<StaticConfig>::reset() {
  head_ = tail_ = 0;
}

template <class StaticConfig>
void CircularLog<StaticConfig>::lock() {
  if (!StaticConfig::kConcurrent) return;

  if (concurrent_access_mode_ == 2) {
    while (true) {
      if (__sync_bool_compare_and_swap((volatile uint8_t*)&lock_, 0U, 1U))
        break;
    }
  }
}

template <class StaticConfig>
void CircularLog<StaticConfig>::unlock() {
  if (!StaticConfig::kConcurrent) return;

  if (concurrent_access_mode_ == 2) {
    ::mica::util::memory_barrier();
    assert((*(volatile uint8_t*)&lock_ & 1U) == 1U);
    // no need to use atomic add because this thread is the only one writing
    // to
    // version
    *(volatile uint8_t*)&lock_ = 0U;
  }
}

template <class StaticConfig>
void CircularLog<StaticConfig>::check_invariants() const {
  assert(tail_ - head_ <= size_);
}

template <class StaticConfig>
void CircularLog<StaticConfig>::pop_head() {
  Item* item = reinterpret_cast<Item*>(data_ + (head_ & mask_));

  if (StaticConfig::kVerbose)
    printf("popping item size = %" PRIu64 " at head = %" PRIu64 "\n",
           item->size, head_ & kOffsetMask);

  head_ += item->size;
  check_invariants();
}

template <class StaticConfig>
uint64_t CircularLog<StaticConfig>::push_tail(uint64_t item_size) {
  assert(item_size == ::mica::util::roundup<8>(item_size));
  assert(item_size <= size_);

  uint64_t offset = tail_;

  uint64_t v = offset + sizeof(Item) + item_size;
  while (v > head_ + size_) pop_head();

  Item* item = reinterpret_cast<Item*>(data_ + (offset & mask_));
  item->size = item_size;

  if (concurrent_access_mode_ == 0)
    tail_ += item_size;
  else {
    *reinterpret_cast<volatile uint64_t*>(&tail_) += item_size;
    ::mica::util::memory_barrier();
  }

  check_invariants();

  if (StaticConfig::kVerbose)
    printf("pushing item size = %" PRIu64 " at tail = %" PRIu64 "\n", item_size,
           offset & kOffsetMask);

  return offset & kOffsetMask;
}
}
}

#endif
