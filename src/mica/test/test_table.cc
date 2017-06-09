#include "mica/table/ltable.h"
#include "mica/util/hash.h"
#include "mica/util/lcore.h"

struct LTableConfig : public ::mica::table::BasicLossyLTableConfig {
  // struct LTableConfig : public ::mica::table::BasicLosslessLTableConfig {
  static constexpr bool kVerbose = true;
  static constexpr bool kCollectStats = true;
};

typedef ::mica::table::LTable<LTableConfig> Table;
typedef ::mica::table::Result Result;

template <typename T>
static uint64_t hash(const T* key, size_t key_length) {
  return ::mica::util::hash(key, key_length);
}

int main() {
  ::mica::util::lcore.pin_thread(0);

  auto config = ::mica::util::Config::load_file("test_table.json");

  LTableConfig::Alloc alloc(config.get("alloc"));
  LTableConfig::Pool pool(config.get("pool"), &alloc);
  Table table(config.get("table"), &alloc, &pool);

  uint64_t key_hash;
  uint64_t key_i;
  uint64_t value_i;
  const char* key = reinterpret_cast<const char*>(&key_i);
  char* value = reinterpret_cast<char*>(&value_i);
  size_t key_length = sizeof(key_i);
  size_t value_length = sizeof(value_i);

  Result out_result;
  size_t out_value_length;

  {
    key_i = 10;
    key_hash = hash(&key_i, sizeof(key_i));
    value_i = 200;
    out_result =
        table.set(key_hash, key, key_length, value, value_length, true);
    table.print_stats();
    assert(out_result == Result::kSuccess);
    // table.print_buckets();
  }

  {
    key_i = 10;
    key_hash = hash(&key_i, sizeof(key_i));
    value_i = 0;
    out_value_length = 0;
    out_result = table.get(key_hash, key, key_length, value, value_length,
                           &out_value_length, false);
    table.print_stats();
    assert(out_result == Result::kSuccess);
    assert(out_value_length == value_length);
    assert(value_i == 200);
  }

  {
    key_i = 10;
    key_hash = hash(&key_i, sizeof(key_i));
    value_i = 300;
    out_result =
        table.set(key_hash, key, key_length, value, value_length, true);
    table.print_stats();
    assert(out_result == Result::kSuccess);
    // table.print_buckets();
  }

  {
    key_i = 10;
    key_hash = hash(&key_i, sizeof(key_i));
    value_i = 0;
    out_result = table.get(key_hash, key, key_length, value, value_length,
                           &out_value_length, false);
    table.print_stats();
    assert(out_result == Result::kSuccess);
    assert(out_value_length == value_length);
    assert(value_i == 300);
  }

  {
    key_i = 10;
    key_hash = hash(&key_i, sizeof(key_i));
    out_result = table.del(key_hash, key, key_length);
    table.print_stats();
    assert(out_result == Result::kSuccess);
  }

  (void)out_result;

  return EXIT_SUCCESS;
}
