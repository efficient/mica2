#pragma once
#ifndef MICA_PROCESSOR_PARTITIONS_IMPL_INIT_H_
#define MICA_PROCESSOR_PARTITIONS_IMPL_INIT_H_

namespace mica {
namespace processor {
template <class StaticConfig>
Partitions<StaticConfig>::Partitions(const ::mica::util::Config& config,
                                     Alloc* alloc)
    : config_(config), alloc_(alloc) {
  {
    auto c = config.get("lcores");
    for (size_t i = 0; i < c.size(); i++) {
      uint16_t lcore_id =
          ::mica::util::safe_cast<uint16_t>(c.get(i).get_uint64());
      assert(lcore_id < StaticConfig::kMaxLCoreCount);
      lcores_.push_back(lcore_id);
    }
  }
  assert(lcores_.size() > 0);

  partition_count_ = ::mica::util::safe_cast<uint16_t>(
      config.get("partition_count").get_uint64());
  assert(static_cast<size_t>(partition_count_) <=
         StaticConfig::kMaxPartitionCount);

  total_size_ = config.get("total_size").get_uint64();
  total_item_count_ = config.get("total_item_count").get_uint64();

  extra_collision_avoidance_ =
      config.get("extra_collision_avoidance").get_double(-1.);

  mth_threshold_ = config.get("mth_threshold").get_double(0.5);

  concurrent_read_ = config.get("concurrent_read").get_bool() ? 1 : 0;
  concurrent_write_ = config.get("concurrent_write").get_bool() ? 1 : 0;

  // pipeline_size_ =
  //     ::mica::util::safe_cast<uint8_t>(config.get("pipeline_size").get_uint64());
  uint8_t stage_gap =
      ::mica::util::safe_cast<uint8_t>(config.get("stage_gap").get_uint64(2));
  // assert(pipeline_size_ > stage_gap_ * (1 + 1 + 1));
  for (size_t lcore_id = 0; lcore_id < StaticConfig::kMaxLCoreCount; lcore_id++)
    load_stats_[lcore_id].stage_gap = stage_gap;

  target_stage_gap_time_ = ::mica::util::safe_cast<uint16_t>(
      config.get("target_stage_gap_time").get_uint64(0));

  initialize();
}

template <class StaticConfig>
Partitions<StaticConfig>::~Partitions() {
  for (size_t i = 0; i < partition_count_; i++) {
    delete tables_[i];
    delete pools_[i];
  }
}

template <class StaticConfig>
void Partitions<StaticConfig>::initialize() {
  reset_load_stats();

  assert(::mica::util::next_power_of_two(StaticConfig::kRecentKeyHashBuckets) ==
         StaticConfig::kRecentKeyHashBuckets);
  assert(::mica::util::next_power_of_two(
             StaticConfig::kRecentKeyHashAssociativity) ==
         StaticConfig::kRecentKeyHashAssociativity);
  ::mica::util::memset(recent_key_hashes_, 0, sizeof(recent_key_hashes_));

  size_t current_lcore_index = 0;
  for (size_t i = 0; i < partition_count_; i++) {
    uint16_t current_lcore_id = lcores_[current_lcore_index];
    current_lcore_index = (current_lcore_index + 1) % lcores_.size();

    auto pool_config = ::mica::util::Config::empty_dict(
        std::string() + "[derived from " + config_.get_path() +
        " by Partitions]");
    pool_config.insert_uint64("size", total_size_ / partition_count_);
    pool_config.insert_bool("concurrent_read", concurrent_read_);
    pool_config.insert_bool("concurrent_write", concurrent_write_);
    pool_config.insert_uint64("numa_node",
                              ::mica::util::lcore.numa_id(current_lcore_id));

    auto table_config = ::mica::util::Config::empty_dict(
        std::string() + "[derived from " + config_.get_path() +
        " by Partitions]");
    size_t item_count = total_item_count_ / partition_count_;
    table_config.insert_uint64("item_count", item_count);
    if (extra_collision_avoidance_ >= 0.)
      table_config.insert_double("extra_collision_avoidance",
                                 extra_collision_avoidance_);
    table_config.insert_bool("concurrent_read", concurrent_read_);
    table_config.insert_bool("concurrent_write", concurrent_write_);
    table_config.insert_uint64("numa_node",
                               ::mica::util::lcore.numa_id(current_lcore_id));
    table_config.insert_double("mth_threshold", mth_threshold_);

    owner_lcore_ids_[i] = current_lcore_id;
    pools_[i] = new Pool(pool_config, alloc_);
    tables_[i] = new Table(table_config, alloc_, pools_[i]);

    if (StaticConfig::kVerbose) {
      printf("= Partition %zu at lcore %hu\n", i, current_lcore_id);
      printf("== Pool config\n");
      printf("%s\n", pool_config.dump().c_str());
      printf("== Table config\n");
      printf("%s\n", table_config.dump().c_str());
      printf("\n");
    }
  }
}
}
}

#endif