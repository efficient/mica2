#pragma once
#ifndef MICA_PROCESSOR_SIMPLE_PROCESSOR_H_
#define MICA_PROCESSOR_SIMPLE_PROCESSOR_H_

#include <cassert>
#include "mica/processor/processor.h"
#include "mica/table/ltable.h"
#include "mica/util/config.h"
#include "mica/util/lcore.h"

// Configuration file entries for SimpleProcessor:
//
//  Nothing.

namespace mica {
namespace processor {
struct BasicSimpleProcessorConfig {
  // Be verbose.
  static constexpr bool kVerbose = false;

  // The table type.
  typedef ::mica::table::LTable<> Table;
};

template <class StaticConfig = BasicSimpleProcessorConfig>
class SimpleProcessor
    : public ProcessorInterface<typename StaticConfig::Table> {
 public:
  typedef typename StaticConfig::Table Table;

  SimpleProcessor(const ::mica::util::Config& config, Table* table)
      : config_(config), table_(table) {}

  ~SimpleProcessor() {}

  template <class RequestAccessor>
  void process(RequestAccessor& ra) {
    assert(::mica::util::lcore.lcore_id() != ::mica::util::LCore::kUnknown);
    uint16_t lcore_id = static_cast<uint16_t>(::mica::util::lcore.lcore_id());

    for (uint64_t index = 0;; index++) {
      if (StaticConfig::kVerbose)
        printf("lcore %2" PRIu16 ": prepare index %" PRIu64 "\n", lcore_id,
               index);
      if (!ra.prepare(index)) break;

      if (StaticConfig::kVerbose)
        printf("lcore %2" PRIu16 ": process index %" PRIu64 "\n", lcore_id,
               index);
      auto operation = ra.get_operation(index);
      auto key_hash = ra.get_key_hash(index);
      auto table = table_;

      Result result;

      switch (operation) {
        case Operation::kReset:
          table->reset();
          result = Result::kSuccess;
          ra.set_out_value_length(index, 0);
          break;
        case Operation::kNoopRead:
        case Operation::kNoopWrite:
          result = Result::kSuccess;
          ra.set_out_value_length(index, 0);
          break;
        case Operation::kAdd: {
          result = table->set(key_hash, ra.get_key(index),
                              ra.get_key_length(index), ra.get_value(index),
                              ra.get_value_length(index), false);
          ra.set_out_value_length(index, 0);
        } break;
        case Operation::kSet: {
          result =
              table->set(key_hash, ra.get_key(index), ra.get_key_length(index),
                         ra.get_value(index), ra.get_value_length(index), true);
          ra.set_out_value_length(index, 0);
        } break;
        case Operation::kGet: {
          auto out_value = ra.get_out_value(index);
          auto out_value_length = ra.get_out_value_length(index);
          result =
              table->get(key_hash, ra.get_key(index), ra.get_key_length(index),
                         out_value, out_value_length, &out_value_length, true);
          if (result == Result::kSuccess || result == Result::kPartialValue)
            ra.set_out_value_length(index, out_value_length);
          else
            ra.set_out_value_length(index, 0);
        } break;
        case Operation::kTest: {
          result = table->test(key_hash, ra.get_key(index),
                               ra.get_key_length(index));
          ra.set_out_value_length(index, 0);
        } break;
        case Operation::kDelete: {
          result =
              table->del(key_hash, ra.get_key(index), ra.get_key_length(index));
          ra.set_out_value_length(index, 0);
        } break;
        case Operation::kIncrement: {
          auto out_value = ra.get_out_value(index);
          auto in_value_length = ra.get_value_length(index);
          auto out_value_length = ra.get_out_value_length(index);
          if (in_value_length != sizeof(uint64_t) ||
              out_value_length < sizeof(uint64_t)) {
            result = Result::kError;
            ra.set_out_value_length(index, 0);
          } else {
            auto increment =
                *reinterpret_cast<const uint64_t*>(ra.get_value(index));
            result = table->increment(key_hash, ra.get_key(index),
                                      ra.get_key_length(index), increment,
                                      reinterpret_cast<uint64_t*>(out_value));
            if (result == Result::kSuccess)
              ra.set_out_value_length(index, sizeof(uint64_t));
            else
              ra.set_out_value_length(index, 0);
          }
        } break;
        default:
          assert(false);
          result = Result::kError;
          ra.set_out_value_length(index, 0);
          break;
      }

      ra.set_result(index, result);

      if (StaticConfig::kVerbose)
        printf("lcore %2" PRIu16 ": retire index %" PRIu64 "\n", lcore_id,
               index);

      ra.retire(index);
    }
  }

  // template <class RequestAccessor>
  // void process(RequestAccessor& ra) const;

  size_t get_table_count() const { return 1; }

  const Table* get_table(size_t index) const {
    (void)index;
    return table_;
  }
  Table* get_table(size_t index) {
    (void)index;
    return table_;
  };

  bool get_concurrent_read() const { return true; }
  bool get_concurrent_write() const { return true; }

  uint16_t get_owner_lcore_id(size_t index) const {
    (void)index;
    return 0;
  }
  void set_new_owner_lcore_id(size_t index, uint16_t lcore_id) {
    (void)index;
    (void)lcore_id;
  }
  void apply_pending_owner_lcore_changes() {}
  void wait_for_pending_owner_lcore_changes() {}

  void rebalance_load(){}

 private:
  ::mica::util::Config config_;
  Table* table_;
};
}
}

#endif