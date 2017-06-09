#pragma once
#ifndef MICA_POOL_SEGREGATED_FIT_H_
#define MICA_POOL_SEGREGATED_FIT_H_

#include <cstdio>
#include "mica/pool/pool.h"
#include "mica/util/config.h"
#include "mica/util/barrier.h"
#include "mica/util/memcpy.h"
#include "mica/util/roundup.h"
#include "mica/util/safe_cast.h"
#include "mica/alloc/hugetlbfs_shm.h"

// Configuration file entries for SegregatedFit:
//
//  * size (integer): The total size in bytes to use.
//  * concurrent_read (bool): If true, enable concurrent reads by multiple
//    threads.
//  * concurrent_write (bool): If true, enable concurrent writes by multiple
//    threads.
//  * numa_node (integer): The ID of the NUMA node to store the data.

namespace mica {
namespace pool {
struct BasicSegregatedFitConfig {
  // Support concurrent access.  The actual concurrent access is enabled by
  // concurrent_read and concurrent_write in the configuration.
  static constexpr bool kConcurrent = true;

  // Be verbose.
  static constexpr bool kVerbose = false;

  // The number of size classes for freelists.
  static constexpr size_t kNumClasses = 32;
  // The byte increment between freelist size classes.
  // The maximum object size supported without linear scanning is
  // kMinimumSize + (kNumClasses - 1) * kClassIncrement - kOverhead
  static constexpr size_t kClassIncrement = 8;

  // The memory allocator type.
  typedef ::mica::alloc::HugeTLBFS_SHM Alloc;
};

class SegregatedFitTag {};

// memory allocation using segregated fit (similar to Lea)
template <class StaticConfig = BasicSegregatedFitConfig>
class SegregatedFit : public PoolInterface {
 public:
  typedef SegregatedFitTag Tag;

  typedef typename StaticConfig::Alloc Alloc;

  SegregatedFit(const ::mica::util::Config& config, Alloc* alloc);
  ~SegregatedFit();

  typedef uint64_t Offset;
  static constexpr size_t kOffsetWidth = 40;
  static constexpr Offset kInsufficientSpace =
      std::numeric_limits<Offset>::max();

  void lock();
  void unlock();

  void reset();
  Offset allocate(size_t item_size);
  void release(Offset offset);

  char* allocate_raw(size_t item_size);
  void release_raw(char* p);

  void prefetch_item(Offset offset) const;

  char* get_item(Offset offset);
  const char* get_item(Offset offset) const;

  char* get_item(Offset offset, std::size_t* out_len);
  const char* get_item(Offset offset, std::size_t* out_len) const;

 private:
  static constexpr size_t size_to_class_roundup(uint64_t size);
  static constexpr size_t size_to_class_rounddown(uint64_t size);

  void insert_free_chunk(char* chunk_start, uint64_t chunk_size);

  void remove_free_chunk_from_free_list(char* chunk_start, uint64_t chunk_size);
  bool remove_free_chunk_from_head(uint64_t minimum_chunk_size,
                                   char** out_chunk_start,
                                   uint64_t* out_chunk_size);

  void coalese_free_chunk_left(char** chunk_start, uint64_t* chunk_size);
  void coalese_free_chunk_right(char** chunk_start, uint64_t* chunk_size);

  static constexpr uint64_t get_tag_size(uint64_t vec);
  static constexpr uint64_t get_tag_status(uint64_t vec);
  static constexpr uint64_t make_tag_vec(uint64_t size, uint64_t status);

  // Per-item space overhead solely caused by SegregatedFit
  static constexpr size_t kOverhead = 24;
  // Minimum pool size = 32 bytes (must be able to hold 4 size_t variables)
  static constexpr size_t kMinimumSize = 32;
  // Maximum pool size = 40-bit wide size (can be up to 63-bit wide)
  static constexpr size_t kMaximumSize = (1UL << 40) - 1;

  static constexpr uint64_t kStatusFree = 0;
  static constexpr uint64_t kStatusOccupied = 1;

  ::mica::util::Config config_;
  Alloc* alloc_;

  uint8_t concurrent_access_mode_;
  char* data_;  // the base address of the reserved memory

  // Padding to separate static and dynamic fields.
  char padding0[128];

  uint8_t lock_;
  char* free_head_[StaticConfig::kNumClasses];  // the head free pointer
                                                // of each class
  uint64_t size_;                               // the total size
} __attribute__((aligned(128)));  // To prevent false sharing caused by
                                  // adjacent cacheline prefetching.
}
}

#include "mica/pool/segregated_fit_impl.h"

#endif
