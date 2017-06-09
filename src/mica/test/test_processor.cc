#include "mica/processor/partitions.h"
#include "mica/util/hash.h"
#include "mica/util/lcore.h"

struct LTableConfig : public ::mica::table::BasicLossyLTableConfig {
  // struct LTableConfig : public ::mica::table::BasicLosslessLTableConfig {
  // static constexpr bool kVerbose = false;
  // static constexpr bool kCollectStats = false;
};

struct PartitionsConfig : public ::mica::processor::BasicPartitionsConfig {
  static constexpr bool kVerbose = true;
  typedef ::mica::table::LTable<LTableConfig> Table;
  typedef Table::Alloc Alloc;
};

typedef ::mica::processor::Partitions<PartitionsConfig> Processor;

typedef ::mica::table::Result Result;
typedef ::mica::processor::Operation Operation;

template <typename T>
static uint64_t hash(const T* key, size_t key_length) {
  return ::mica::util::hash(key, key_length);
}

int main() {
  ::mica::util::lcore.pin_thread(0);

  auto config = ::mica::util::Config::load_file("test_processor.json");

  PartitionsConfig::Alloc alloc(config.get("alloc"));
  Processor processor(config.get("processor"), &alloc);

  uint64_t key_i;
  uint64_t value_i;
  const char* key = reinterpret_cast<const char*>(&key_i);
  char* value = reinterpret_cast<char*>(&value_i);
  size_t key_length = sizeof(key_i);
  size_t value_length = sizeof(value_i);

  Result out_result;
  size_t out_value_length;

  Operation ops[1] = {Operation::kReset};  // To be filled.
  uint64_t key_hashes[1] = {0};            // To be filled.
  const char* keys[1] = {key};
  uint64_t key_lengths[1] = {key_length};
  const char* values[1] = {value};
  uint64_t value_lengths[1] = {value_length};
  Result* out_results[1] = {&out_result};
  char* out_values[1] = {value};
  size_t* out_value_lengths[1] = {&out_value_length};

  ::mica::processor::RequestArrayAccessor ra(
      1, ops, key_hashes, keys, key_lengths, values, value_lengths, out_results,
      out_values, out_value_lengths);

  {
    ops[0] = Operation::kSet;
    key_i = 10;
    key_hashes[0] = hash(&key_i, sizeof(key_i));
    value_i = 200;
    processor.process(ra);
    assert(out_result == Result::kSuccess);
  }

  {
    ops[0] = Operation::kGet;
    key_i = 10;
    key_hashes[0] = hash(&key_i, sizeof(key_i));
    value_i = 0;
    out_value_length = sizeof(value);
    processor.process(ra);
    assert(out_result == Result::kSuccess);
    assert(out_value_length == value_length);
    assert(value_i == 200);
  }

  {
    ops[0] = Operation::kSet;
    key_i = 10;
    key_hashes[0] = hash(&key_i, sizeof(key_i));
    value_i = 300;
    processor.process(ra);
    assert(out_result == Result::kSuccess);
  }

  {
    ops[0] = Operation::kGet;
    key_i = 10;
    key_hashes[0] = hash(&key_i, sizeof(key_i));
    value_i = 0;
    out_value_length = sizeof(value);
    processor.process(ra);
    assert(out_result == Result::kSuccess);
    assert(out_value_length == value_length);
    assert(value_i == 300);
  }

  return EXIT_SUCCESS;
}
