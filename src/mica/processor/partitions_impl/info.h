#pragma once
#ifndef MICA_PROCESSOR_PARTITIONS_IMPL_INFO_H_
#define MICA_PROCESSOR_PARTITIONS_IMPL_INFO_H_

namespace mica {
namespace processor {
template <class StaticConfig>
size_t Partitions<StaticConfig>::get_table_count() const {
  return partition_count_;
}

template <class StaticConfig>
const typename Partitions<StaticConfig>::Table*
Partitions<StaticConfig>::get_table(size_t index) const {
  assert(index < partition_count_);
  return tables_[index];
}

template <class StaticConfig>
bool Partitions<StaticConfig>::get_concurrent_read() const {
  return concurrent_read_ != 0;
}

template <class StaticConfig>
bool Partitions<StaticConfig>::get_concurrent_write() const {
  return concurrent_write_ != 0;
}

template <class StaticConfig>
typename Partitions<StaticConfig>::Table* Partitions<StaticConfig>::get_table(
    size_t index) {
  assert(index < partition_count_);
  return tables_[index];
}

template <class StaticConfig>
uint16_t Partitions<StaticConfig>::get_owner_lcore_id(size_t index) const {
  assert(index < partition_count_);
  return owner_lcore_ids_[index];
}

template <class StaticConfig>
void Partitions<StaticConfig>::set_new_owner_lcore_id(size_t index,
                                                      uint16_t lcore_id) {
  assert(index < partition_count_);
  assert(lcore_id < ::mica::util::lcore.lcore_count());

  if (owner_lcore_ids_[index] == lcore_id) return;

  // TODO: Validate lcore_id to see if it is in lcores_.

  PendingOwnerLCoreChange c{static_cast<uint16_t>(index), lcore_id};
  pending_owner_lcore_changes_[owner_lcore_ids_[index]].enqueue(c);
}

template <class StaticConfig>
void Partitions<StaticConfig>::wait_for_pending_owner_lcore_changes() {
  while (true) {
    bool any_pending_changes = false;

    apply_pending_owner_lcore_changes();

    // TODO: Skip unused lcores.
    for (uint16_t lcore_id = 0; lcore_id < ::mica::util::lcore.lcore_count();
         lcore_id++)
      if (!pending_owner_lcore_changes_[lcore_id].approximate_empty()) {
        if (StaticConfig::kVerbose)
          printf("lcore %2" PRIu16 ": lcore %2" PRIu16
                 " has pending owner lcore changes\n",
                 static_cast<uint16_t>(::mica::util::lcore.lcore_id()),
                 lcore_id);
        any_pending_changes = true;
        break;
      }

    if (!any_pending_changes) break;

    ::mica::util::pause();
  }
}

template <class StaticConfig>
void Partitions<StaticConfig>::apply_pending_owner_lcore_changes() {
  uint16_t lcore_id = static_cast<uint16_t>(::mica::util::lcore.lcore_id());

  // Only the current owner lcore can give this partition to another
  // partition.
  if (!pending_owner_lcore_changes_[lcore_id].approximate_empty()) {
    PendingOwnerLCoreChange c;
    while (pending_owner_lcore_changes_[lcore_id].dequeue(&c)) {
      auto& owner_lcore_id = owner_lcore_ids_[c.partition_id];
      if (owner_lcore_id == lcore_id) {
        if (StaticConfig::kVerbose)
          printf("lcore %2" PRIu16 ": give partition %" PRIu16
                 " to lcore %" PRIu16 "\n",
                 lcore_id, c.partition_id, c.new_owner_lcore_id);
        owner_lcore_id = c.new_owner_lcore_id;
      } else {
        if (StaticConfig::kVerbose)
          printf("lcore %2" PRIu16
                 ": owner already changed; redirecting the change to lcore "
                 "%" PRIu16 "\n",
                 lcore_id, owner_lcore_id);
        // Rediect the pending change to the current owner lcore.
        pending_owner_lcore_changes_[owner_lcore_id].enqueue(c);
      }
    }
  }
}

template <class StaticConfig>
void Partitions<StaticConfig>::rebalance_load() {
  // Using a simple greedy algorithm ignoring affinity.

  for (size_t numa_id = 0; numa_id < ::mica::util::lcore.numa_count();
       numa_id++) {
    // Find lcores in this NUMA domain.
    std::vector<uint16_t> lcores;
    for (size_t i = 0; i < lcores_.size(); i++) {
      if (::mica::util::lcore.numa_id(lcores_[i]) == numa_id)
        lcores.push_back(lcores_[i]);
    }

    // Calculate the load of partitions owned by the lcores of this NUMA domain.
    std::vector<std::pair<uint16_t, uint64_t>> bins;
    for (uint16_t partition_id = 0; partition_id < partition_count_;
         partition_id++) {
      if (::mica::util::lcore.numa_id(owner_lcore_ids_[partition_id]) !=
          numa_id)
        continue;

      uint64_t count = 0;
      for (auto lcore_id : lcores)
        count += get_request_count(lcore_id, partition_id);

      bins.push_back(std::make_pair(partition_id, count));

      if (StaticConfig::kVerbose)
        printf("partition %2" PRIu16 ": %" PRIu64 " requests\n", partition_id,
               count);
    }

    // Sort bins by request count in descending order.
    std::stable_sort(bins.begin(), bins.end(),
                     [](const std::pair<uint16_t, uint64_t>& a,
                        const std::pair<uint16_t, uint64_t>& b) {
                       return a.second > b.second;
                     });

    // Do not rebalance if there were no requests.
    if (bins.size() == 0 || bins[0].second == 0) continue;

    // Perform greedy bin packing.
    std::vector<uint64_t> estimated_load(lcores.size(), 0);

    for (size_t i = 0; i < bins.size(); i++) {
      uint64_t min_estimated_load = std::numeric_limits<uint64_t>::max();
      size_t min_lcore_index = 0;
      // TODO: Skip unused lcores.
      for (size_t lcore_index = 0; lcore_index < lcores.size(); lcore_index++) {
        if (min_estimated_load > estimated_load[lcore_index]) {
          min_estimated_load = estimated_load[lcore_index];
          min_lcore_index = lcore_index;
        }
      }
      estimated_load[min_lcore_index] += bins[i].second;
      set_new_owner_lcore_id(bins[i].first, lcores[min_lcore_index]);
    }
  }

  reset_load_stats();
}

template <class StaticConfig>
uint16_t Partitions<StaticConfig>::get_partition_id(uint64_t key_hash) const {
  return static_cast<uint16_t>((key_hash >> 48) % partition_count_);
}

template <class StaticConfig>
void Partitions<StaticConfig>::reset_load_stats() {
  for (size_t lcore_id = 0; lcore_id < StaticConfig::kMaxLCoreCount;
       lcore_id++) {
    for (size_t partition_id = 0;
         partition_id < StaticConfig::kMaxPartitionCount; partition_id++) {
      load_stats_[lcore_id].request_count[partition_id] = 0;
    }
    load_stats_[lcore_id].processing_time = 0;
  }
}

template <class StaticConfig>
uint32_t Partitions<StaticConfig>::get_request_count(uint16_t lcore_id,
                                                     uint16_t index) const {
  return load_stats_[lcore_id].request_count[index];
}

template <class StaticConfig>
uint64_t Partitions<StaticConfig>::get_processing_time(
    uint16_t lcore_id) const {
  return load_stats_[lcore_id].processing_time;
}
}
}

#endif