#pragma once
#ifndef MICA_PROCESSOR_PARTITIONS_H_
#define MICA_PROCESSOR_PARTITIONS_H_

#include <cassert>
#include <string>
#include <iostream>
#include <sstream>
#include <algorithm>
#include "mica/processor/processor.h"
#include "mica/table/ltable.h"
#include "mica/util/config.h"
#include "mica/util/lcore.h"
#include "mica/util/tsc.h"
#include "mica/util/queue.h"

// Configuration file entries for Partitions:
//
//  * lcores (array of integers): A list of LCore IDs to use.
//  * partition_count (integer): The partition count.  Each partition has a
//    table and an item pool.
//  * total_size (integer): The total approximate size of key-value item data to
//    store.  This excludes the space occupied by indexing.
//  * total_item_count (integer): The total approximate number of key-value
//    items to store.
//  * extra_collision_avoidance (float): The amount of additional memory to
//    resolve excessive hash collisions as a fraction of the main hash table.
//    Default = (none - see LTable)
//  * mth_threshold (double): The move-to-head threshold.  See LTable's
//    mth_threshold.  Default = 0.5
//  * concurrent_read (bool): If true, enable concurrent reads by multiple
//    threads.
//  * concurrent_write (bool): If true, enable concurrent writes by multiple
//    threads.
//  * stage_gap (integer): The number of requests between two adjacent stages of
//    the processing pipeline.  The time gap between two stages should be
//    ideally slightly larger than the memory access latency to maximize the
//    effectiveness of prefetching.  Default = 2
//  * target_stage_gap_time (integer): Automatically adjust stage_gap to make
//    the average gap time no smaller than this gap time expressed in cycles. An
//    appropriate cycle count can be obtained using test/test_prefetch program.
//    Specify 0 to disable.  Requires kAutoStageGap == true.  Overrides
//    stage_gap.  Default = 0

namespace mica {
namespace processor {
struct BasicPartitionsConfig {
  // The maximum number of LCore to support.
  static constexpr size_t kMaxLCoreCount = 64;
  // The maximum number of partitions to support.
  static constexpr size_t kMaxPartitionCount = 256;

  // Enable table prefetching.
  static constexpr bool kPrefetchTable = true;
  // Enable item pool prefetching.
  static constexpr bool kPrefetchPool = true;

  // Enable skipping prefetching for recent key hashes.
  static constexpr bool kSkipPrefetchingForRecentKeyHashes = true;
  // The number of buckets to keep track of recent key hashes.
  // Must be a power of two.
  static constexpr size_t kRecentKeyHashBuckets = 128;
  // The associativity of recent key hash buckets.
  // Must be a power of two.
  static constexpr size_t kRecentKeyHashAssociativity = 1;

  // Enable automatic stage_gap adjustment.
  static constexpr bool kAutoStageGap = false;

  // Be verbose.
  static constexpr bool kVerbose = false;

  // The table type.  The item pool is inferred from Table::Pool.
  typedef ::mica::table::LTable<> Table;

  // The memory allocator type.
  typedef typename Table::Alloc Alloc;
};

template <class StaticConfig = BasicPartitionsConfig>
class Partitions : public ProcessorInterface<typename StaticConfig::Table> {
 public:
  typedef typename StaticConfig::Table Table;
  typedef typename StaticConfig::Alloc Alloc;
  typedef typename StaticConfig::Table::Pool Pool;

  // partitions_impl/init.h
  Partitions(const ::mica::util::Config& config, Alloc* alloc);
  ~Partitions();

  // partitions_impl/process.h
  template <class RequestAccessor>
  void process(RequestAccessor& ra);

  // template <class RequestAccessor>
  // void process(RequestAccessor& ra) const;

  // partitions_impl/info.h
  size_t get_table_count() const;

  const Table* get_table(size_t index) const;
  Table* get_table(size_t index);

  bool get_concurrent_read() const;
  bool get_concurrent_write() const;

  uint16_t get_owner_lcore_id(size_t index) const;
  void set_new_owner_lcore_id(size_t index, uint16_t lcore_id);
  void apply_pending_owner_lcore_changes();
  void wait_for_pending_owner_lcore_changes();

  void rebalance_load();

  uint16_t get_partition_id(uint64_t key_hash) const;

  void reset_load_stats();
  uint32_t get_request_count(uint16_t lcore_id, uint16_t index) const;
  uint64_t get_processing_time(uint16_t lcore_id) const;

 private:
  // partitions_impl/init.h
  void initialize();

  // partitions_impl/recent_key_hash.h
  bool is_recent_key_hash(uint16_t lcore_id, uint64_t key_hash) const;
  void update_recent_key_hash(uint16_t lcore_id, uint64_t key_hash);

  ::mica::util::Config config_;
  Alloc* alloc_;

  uint8_t concurrent_read_;
  uint8_t concurrent_write_;
  // uint8_t pipeline_size_;
  // uint8_t stage_gap_;
  uint16_t target_stage_gap_time_;
  uint16_t partition_count_;
  size_t total_size_;
  size_t total_item_count_;
  double extra_collision_avoidance_;
  double mth_threshold_;
  std::vector<uint16_t> lcores_;

  uint16_t owner_lcore_ids_[StaticConfig::kMaxPartitionCount];
  Table* tables_[StaticConfig::kMaxPartitionCount];
  Pool* pools_[StaticConfig::kMaxPartitionCount];

  struct PendingOwnerLCoreChange {
    uint16_t partition_id;
    uint16_t new_owner_lcore_id;
  };
  ::mica::util::Queue<PendingOwnerLCoreChange, 16, true, false>
      pending_owner_lcore_changes_[StaticConfig::kMaxLCoreCount];

  // Padding to separate static and dynamic fields.
  char padding0[128];

  struct LoadStats {
    uint32_t request_count[StaticConfig::kMaxPartitionCount];
    uint64_t processing_time;
    uint64_t stage_gap;
  } __attribute__((aligned(128)));  // To prevent false sharing caused by
                                    // adjacent cacheline prefetching.
  LoadStats load_stats_[StaticConfig::kMaxLCoreCount];

  // Padding to separate two dynamic fields.
  char padding1[128];

  struct RecentKeyHashes {
    uint16_t v[StaticConfig::kRecentKeyHashBuckets *
               StaticConfig::kRecentKeyHashAssociativity];
  } __attribute__((aligned(128)));  // To prevent false sharing caused by
                                    // adjacent cacheline prefetching.
  RecentKeyHashes recent_key_hashes_[StaticConfig::kMaxLCoreCount];
};
}
}

#include "mica/processor/partitions_impl/info.h"
#include "mica/processor/partitions_impl/init.h"
#include "mica/processor/partitions_impl/process.h"
#include "mica/processor/partitions_impl/recent_key_hash.h"

#endif