#pragma once
#ifndef MICA_POOL_SEGREGATED_FIT_IMPL_H_
#define MICA_POOL_SEGREGATED_FIT_IMPL_H_

namespace mica {
namespace pool {
// data structure layout

// free_head[class] -> the first free chunk of the class (nullptr if none
// exists)

// free chunk (of size N) - N is the same or larger than the size of the class
// 8-byte: status (1 bit), size (63 bit)
// 8-byte: prev free chunk of the same class (nullptr if head)
// 8-byte: next free chunk of the same class (nullptr if tail)
// (N - 32 bytes)
// 8-byte: status (1 bit), size (63 bit)

// occupied chunk (of size N) - overhead of 16 bytes
// 8-byte: status (1 bit), size (63 bit)
// (N - 16 bytes)
// 8-byte: status (1 bit), size (63 bit)

// TODO: use address order for each freelist to reduce fragmentation and improve
// locality
// TODO: use the LSB (not MSB) to store status as all sizes are aligned to
// 8-byte boundary

template <class StaticConfig>
SegregatedFit<StaticConfig>::SegregatedFit(const ::mica::util::Config& config,
                                           Alloc* alloc)
    : config_(config), alloc_(alloc) {
  uint64_t size = config.get("size").get_uint64();
  bool concurrent_read = config.get("concurrent_read").get_bool();
  bool concurrent_write = config.get("concurrent_write").get_bool();
  size_t numa_node = config.get("numa_node").get_uint64();

  if (size < kMinimumSize) size = kMinimumSize;

  size = Alloc::roundup(size);
  size = ::mica::util::next_power_of_two(size);
  assert(size == Alloc::roundup(size));

  if (!concurrent_read)
    concurrent_access_mode_ = 0;
  else if (!concurrent_write)
    concurrent_access_mode_ = 1;
  else
    concurrent_access_mode_ = 2;

  lock_ = 0;

  size = Alloc::roundup(size);
  assert(size <= kMaximumSize);

  size_ = size;

  size_t alloc_id = alloc_->alloc(size, numa_node);
  if (alloc_id == Alloc::kInvalidId) {
    fprintf(stderr, "error: failed to allocate memory\n");
    assert(false);
    return;
  }

  while (true) {
    data_ = reinterpret_cast<char*>(alloc_->find_free_address(size));
    if (data_ == nullptr) {
      fprintf(stderr, "error: failed to find free memory region\n");
      assert(false);
      return;
    }

    if (!alloc_->map(alloc_id, data_, 0, size)) {
      // TODO: Give up after some trials.
      continue;
    }

    break;
  }

  if (!alloc_->schedule_release(alloc_id)) {
    perror("");
    fprintf(stderr, "error: failed to schedule releasing memory\n");
    assert(false);
    return;
  }

  reset();
}

template <class StaticConfig>
SegregatedFit<StaticConfig>::~SegregatedFit() {
  if (!alloc_->unmap(data_)) assert(false);
}

template <class StaticConfig>
void SegregatedFit<StaticConfig>::lock() {
  if (StaticConfig::kConcurrent) {
    if (concurrent_access_mode_ == 2) {
      while (true) {
        if (__sync_bool_compare_and_swap((volatile uint8_t*)&lock_, 0U, 1U))
          break;
      }
    }
  }
}

template <class StaticConfig>
void SegregatedFit<StaticConfig>::unlock() {
  if (StaticConfig::kConcurrent) {
    if (concurrent_access_mode_ == 2) {
      ::mica::util::memory_barrier();
      assert((*(volatile uint8_t*)&lock_ & 1U) == 1U);
      // no need to use atomic add because this thread is the only one writing
      // to
      // version
      *(volatile uint8_t*)&lock_ = 0U;
    }
  }
}

template <class StaticConfig>
constexpr size_t SegregatedFit<StaticConfig>::size_to_class_roundup(
    uint64_t size) {
  assert(size <= kMaximumSize);

  if (size <=
      kMinimumSize +
          (StaticConfig::kNumClasses - 1) * StaticConfig::kClassIncrement)
    return (size - kMinimumSize + StaticConfig::kClassIncrement - 1) /
           StaticConfig::kClassIncrement;
  else
    return StaticConfig::kNumClasses - 1;
}

template <class StaticConfig>
constexpr size_t SegregatedFit<StaticConfig>::size_to_class_rounddown(
    uint64_t size) {
  assert(size <= kMaximumSize);
  assert(size >= kMinimumSize);

  if (size <
      kMinimumSize + StaticConfig::kNumClasses * StaticConfig::kClassIncrement)
    return (size - kMinimumSize) / StaticConfig::kClassIncrement;
  else
    return StaticConfig::kNumClasses - 1;
}

template <class StaticConfig>
void SegregatedFit<StaticConfig>::insert_free_chunk(char* chunk_start,
                                                    uint64_t chunk_size) {
  if (StaticConfig::kVerbose)
    printf("insert_free_chunk: start=%p size=%" PRIu64 "\n", chunk_start,
           chunk_size);
  size_t chunk_class = size_to_class_rounddown(chunk_size);
  *reinterpret_cast<uint64_t*>(chunk_start) =
      *reinterpret_cast<uint64_t*>(chunk_start + chunk_size - 8) =
          make_tag_vec(chunk_size, kStatusFree);
  *reinterpret_cast<char**>(chunk_start + 8) =
      nullptr;  // the head has no previous free chunk
  *reinterpret_cast<char**>(chunk_start + 16) =
      free_head_[chunk_class];  // point to the old head

  if (free_head_[chunk_class] != nullptr) {
    assert(*reinterpret_cast<char**>(free_head_[chunk_class] + 8) == nullptr);
    *reinterpret_cast<char**>(free_head_[chunk_class] + 8) =
        chunk_start;  // update the previous head's prev pointer
  }

  free_head_[chunk_class] = chunk_start;  // set as a new head
}

template <class StaticConfig>
void SegregatedFit<StaticConfig>::remove_free_chunk_from_free_list(
    char* chunk_start, uint64_t chunk_size) {
  if (StaticConfig::kVerbose)
    printf(
        "remove_free_chunk_from_free_list: start=%p "
        "size=%" PRIu64 "\n",
        chunk_start, chunk_size);

  char* prev_chunk_start = *reinterpret_cast<char**>(chunk_start + 8);
  char* next_chunk_start = *reinterpret_cast<char**>(chunk_start + 16);

  if (prev_chunk_start != nullptr)
    *reinterpret_cast<char**>(prev_chunk_start + 16) = next_chunk_start;
  else {
    size_t chunk_class = size_to_class_rounddown(chunk_size);
    assert(free_head_[chunk_class] == chunk_start);
    free_head_[chunk_class] =
        next_chunk_start;  // set the next free chunk as the head
  }

  if (next_chunk_start != nullptr)
    *reinterpret_cast<char**>(next_chunk_start + 8) = prev_chunk_start;
}

template <class StaticConfig>
bool SegregatedFit<StaticConfig>::remove_free_chunk_from_head(
    uint64_t minimum_chunk_size, char** out_chunk_start,
    uint64_t* out_chunk_size) {
  size_t chunk_class = size_to_class_roundup(minimum_chunk_size);

  // determine the size class to use (best fit)
  for (; chunk_class < StaticConfig::kNumClasses; chunk_class++)
    if (free_head_[chunk_class] != nullptr) break;

  if (chunk_class == StaticConfig::kNumClasses) {
    if (StaticConfig::kVerbose)
      printf("remove_free_chunk_from_head: minsize=%" PRIu64 " no space\n",
             minimum_chunk_size);
    return false;
  }

  // use the first valid free chunk in the class; the overall policy is still
  // approximately best fit (which is good) due to segregation
  char* chunk_start = free_head_[chunk_class];
  uint64_t chunk_size;

  while (chunk_start != nullptr) {
    assert(get_tag_status(*reinterpret_cast<uint64_t*>(chunk_start)) ==
           kStatusFree);
    chunk_size = get_tag_size(*reinterpret_cast<uint64_t*>(chunk_start));
    assert(*reinterpret_cast<uint64_t*>(chunk_start) ==
           *reinterpret_cast<uint64_t*>(chunk_start + chunk_size - 8));

    if (chunk_size >= minimum_chunk_size) break;

    // Too small chunk size.  This can occur for the last class that can
    // contain very different chunk sizes.
    chunk_start = *reinterpret_cast<char**>(chunk_start + 16);
  }

  if (chunk_start == nullptr) {
    if (StaticConfig::kVerbose)
      printf("remove_free_chunk_from_head: minsize=%" PRIu64 " no space\n",
             minimum_chunk_size);
    return false;
  }

  assert(chunk_size >= minimum_chunk_size);

  remove_free_chunk_from_free_list(chunk_start, chunk_size);

  *out_chunk_start = chunk_start;
  *out_chunk_size = chunk_size;
  if (StaticConfig::kVerbose)
    printf("remove_free_chunk_from_head: minsize=%" PRIu64
           " start=%p size=%" PRIu64 "\n",
           minimum_chunk_size, *out_chunk_start, *out_chunk_size);
  return true;
}

template <class StaticConfig>
void SegregatedFit<StaticConfig>::reset() {
  ::mica::util::memset(free_head_, 0, sizeof(free_head_));

  // set the entire free space as a free chunk
  insert_free_chunk(data_, size_);
}

template <class StaticConfig>
void SegregatedFit<StaticConfig>::prefetch_item(Offset offset) const {
  size_t addr = reinterpret_cast<size_t>(data_ + offset) & ~(size_t)63;

  // prefetch the item's cache line and the subsequence cache line
  __builtin_prefetch(reinterpret_cast<const char*>(addr), 0, 0);
  __builtin_prefetch(reinterpret_cast<const char*>(addr + 64), 0, 0);
}

template <class StaticConfig>
char* SegregatedFit<StaticConfig>::get_item(Offset offset) {
  return data_ + offset + 8;
}

template <class StaticConfig>
const char* SegregatedFit<StaticConfig>::get_item(Offset offset) const {
  return data_ + offset + 8;
}

template <class StaticConfig>
char* SegregatedFit<StaticConfig>::get_item(Offset offset,
                                            std::size_t* out_len) {
  *out_len = get_tag_size(*reinterpret_cast<const uint64_t*>(data_ + offset));
  return data_ + offset + 8;
}

template <class StaticConfig>
const char* SegregatedFit<StaticConfig>::get_item(Offset offset,
                                                  std::size_t* out_len) const {
  *out_len = get_tag_size(*reinterpret_cast<const uint64_t*>(data_ + offset));
  return data_ + offset + 8;
}

template <class StaticConfig>
void SegregatedFit<StaticConfig>::coalese_free_chunk_left(
    char** chunk_start, uint64_t* chunk_size) {
  if (*chunk_start == data_) return;
  assert(*chunk_start > data_);

  if (get_tag_status(*reinterpret_cast<uint64_t*>(*chunk_start - 8)) ==
      kStatusOccupied)
    return;

  uint64_t adj_chunk_size =
      get_tag_size(*reinterpret_cast<uint64_t*>(*chunk_start - 8));
  char* adj_chunk_start = *chunk_start - adj_chunk_size;
  assert(*reinterpret_cast<uint64_t*>(adj_chunk_start) ==
         *reinterpret_cast<uint64_t*>(adj_chunk_start + adj_chunk_size - 8));

  if (StaticConfig::kVerbose)
    printf("coalese_free_chunk_left: start=%p size=%" PRIu64 " left=%" PRIu64
           "\n",
           *chunk_start, *chunk_size, adj_chunk_size);

  remove_free_chunk_from_free_list(adj_chunk_start, adj_chunk_size);
  *chunk_start = adj_chunk_start;
  *chunk_size = *chunk_size + adj_chunk_size;
}

template <class StaticConfig>
void SegregatedFit<StaticConfig>::coalese_free_chunk_right(
    char** chunk_start, uint64_t* chunk_size) {
  if (*chunk_start + *chunk_size == data_ + size_) return;
  assert(*chunk_start + *chunk_size < data_ + size_);

  if (get_tag_status(*reinterpret_cast<uint64_t*>(
                         *chunk_start + *chunk_size)) == kStatusOccupied)
    return;

  char* adj_chunk_start = *chunk_start + *chunk_size;
  uint64_t adj_chunk_size =
      get_tag_size(*reinterpret_cast<uint64_t*>(adj_chunk_start));
  assert(*reinterpret_cast<uint64_t*>(adj_chunk_start) ==
         *reinterpret_cast<uint64_t*>(adj_chunk_start + adj_chunk_size - 8));

  if (StaticConfig::kVerbose)
    printf("coalese_free_chunk_right: start=%p size=%" PRIu64 " right=%" PRIu64
           "\n",
           *chunk_start, *chunk_size, adj_chunk_size);

  remove_free_chunk_from_free_list(adj_chunk_start, adj_chunk_size);
  // chunk_start is unchanged
  *chunk_size = *chunk_size + adj_chunk_size;
}

template <class StaticConfig>
typename SegregatedFit<StaticConfig>::Offset
SegregatedFit<StaticConfig>::allocate(size_t item_size) {
  uint64_t minimum_chunk_size = ::mica::util::roundup<8>(item_size) + kOverhead;

  char* chunk_start;
  uint64_t chunk_size;
  if (!remove_free_chunk_from_head(minimum_chunk_size, &chunk_start,
                                   &chunk_size))
    return kInsufficientSpace;

  // see if we can make a leftover free chunk
  uint64_t leftover_chunk_size = chunk_size - minimum_chunk_size;
  if (leftover_chunk_size >= kMinimumSize) {
    // create a leftover free chunk and insert it to the freelist
    insert_free_chunk(chunk_start + minimum_chunk_size, leftover_chunk_size);
    // coalescing is not required here because the previous chunk already used
    // to be a big coalesced free chunk

    // adjust the free chunk to avoid overlapping
    chunk_size = minimum_chunk_size;
  } else
    leftover_chunk_size = 0;

  if (StaticConfig::kVerbose)
    printf("allocate: item_size=%zu minsize=%" PRIu64 " start=%p size=%" PRIu64
           " (leftover=%" PRIu64 ")\n",
           item_size, minimum_chunk_size, chunk_start, chunk_size,
           leftover_chunk_size);

  *reinterpret_cast<uint64_t*>(chunk_start) =
      *reinterpret_cast<uint64_t*>(chunk_start + chunk_size - 8) =
          make_tag_vec(chunk_size, kStatusOccupied);

  // TODO: We are wasting 4 bytes for struct mehcached_alloc_item for
  // compatibility.  Need to implement an allocator-specific method to obtain
  // the item size
  *reinterpret_cast<uint64_t*>(chunk_start + 8) = item_size;

  return static_cast<Offset>((chunk_start + 8) - data_);
}

template <class StaticConfig>
void SegregatedFit<StaticConfig>::release(Offset offset) {
  char* chunk_start = reinterpret_cast<char*>(data_ + offset - 8);

  assert(get_tag_status(*reinterpret_cast<uint64_t*>(chunk_start)) ==
         kStatusOccupied);
  uint64_t chunk_size = get_tag_size(*reinterpret_cast<uint64_t*>(chunk_start));

  if (StaticConfig::kVerbose)
    printf("deallocate: start=%p size=%" PRIu64 "\n", chunk_start, chunk_size);

  coalese_free_chunk_left(&chunk_start, &chunk_size);
  coalese_free_chunk_right(&chunk_start, &chunk_size);
  insert_free_chunk(chunk_start, chunk_size);
}

template <class StaticConfig>
char* SegregatedFit<StaticConfig>::allocate_raw(size_t item_size) {
  Offset offset = allocate(item_size);
  if (offset == kInsufficientSpace)
    return nullptr;
  else
    return data_ + static_cast<size_t>(offset) + 8;
}

template <class StaticConfig>
void SegregatedFit<StaticConfig>::release_raw(char* p) {
  if (!p) return;
  Offset offset = static_cast<Offset>(p - 8 - data_);
  release(offset);
}

template <class StaticConfig>
constexpr uint64_t SegregatedFit<StaticConfig>::get_tag_size(uint64_t vec) {
  return vec & ((1UL << 63UL) - 1UL);
}

template <class StaticConfig>
constexpr uint64_t SegregatedFit<StaticConfig>::get_tag_status(uint64_t vec) {
  return vec >> 63UL;
}

template <class StaticConfig>
constexpr uint64_t SegregatedFit<StaticConfig>::make_tag_vec(uint64_t size,
                                                             uint64_t status) {
  return size | (status << 63UL);
}
}
}

#endif