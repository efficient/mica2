#include "mica/table/ltable.h"
#include "mica/util/hash.h"
#include "mica/util/lcore.h"

struct LTableConfig : public ::mica::table::BasicLossyLTableConfig {
  // struct LTableConfig : public ::mica::table::BasicLosslessLTableConfig {
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

  auto config = ::mica::util::Config::load_file("test_load.json");

  LTableConfig::Alloc alloc(config.get("alloc"));
  LTableConfig::Pool pool(config.get("pool"), &alloc);
  Table table(config.get("table"), &alloc, &pool);

  size_t num_items = 16 * 1048576;

  size_t key_i;
  size_t value_i;
  const char* key = reinterpret_cast<const char*>(&key);
  char* value = reinterpret_cast<char*>(&value);

  bool first_failure = false;
  size_t first_failure_i = 0;
  size_t last_failure_i = 0;
  size_t success_count = 0;

  for (size_t i = 0; i < num_items; i++) {
    key_i = i;
    value_i = i;
    uint64_t key_hash = hash(&key_i, sizeof(key_i));
    table.set(key_hash, key, sizeof(key_i), value, sizeof(value_i), false);
  }

  for (size_t i = 0; i < num_items; i++) {
    key_i = i;
    size_t value_len;
    uint64_t key_hash = hash(&key_i, sizeof(key_i));

    if (table.get(key_hash, key, sizeof(key_i), value, sizeof(value_i),
                  &value_len, false) == Result::kSuccess)
      success_count++;
    else {
      if (!first_failure) {
        first_failure = true;
        first_failure_i = i;
      }
      last_failure_i = i;
    }
  }

  printf("first_failure: %zu (%.2f%%)\n", first_failure_i,
         100. * (double)first_failure_i / (double)num_items);
  printf("last_failure: %zu (%.2f%%)\n", last_failure_i,
         100. * (double)last_failure_i / (double)num_items);
  printf("success_count: %zu (%.2f%%)\n", success_count,
         100. * (double)success_count / (double)num_items);

  table.print_stats();

  return EXIT_SUCCESS;
}
