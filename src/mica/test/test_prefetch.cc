#include "mica/alloc/hugetlbfs_shm.h"
#include "mica/util/config.h"
#include "mica/util/lcore.h"
#include "mica/util/rand.h"
#include "mica/util/roundup.h"
#include "mica/util/tsc.h"
#include <cstdio>

typedef ::mica::alloc::HugeTLBFS_SHM Alloc;

int main() {
  auto config = ::mica::util::Config::load_file("microbench.json");

  Alloc alloc(config.get("alloc"));

  ::mica::util::lcore.pin_thread(0);

  size_t request_count = 16 * 1048576;
  size_t table_location_count = 128 * 1048576;
  size_t pool_location_count = 1024 * 1048576;

  size_t* requests = reinterpret_cast<size_t*>(
      alloc.malloc_contiguous(request_count * sizeof(uint64_t), 0));
  assert(requests);

  size_t* table_locations = reinterpret_cast<size_t*>(
      alloc.malloc_contiguous(table_location_count * sizeof(size_t), 0));
  assert(table_locations);

  uint64_t* pool_locations = reinterpret_cast<uint64_t*>(
      alloc.malloc_contiguous(pool_location_count * sizeof(uint64_t), 0));
  assert(pool_locations);

  ::mica::util::Rand r(1);

  printf("initializing data\n");

  for (size_t i = 0; i < request_count; i++)
    requests[i] =
        static_cast<size_t>(r.next_u32() & (table_location_count - 1));

  for (size_t i = 0; i < table_location_count; i++)
    table_locations[i] =
        static_cast<size_t>(r.next_u32() & (pool_location_count - 1));

  for (size_t i = 0; i < pool_location_count; i++)
    pool_locations[i] = static_cast<uint64_t>(r.next_u32());

  size_t gap = 0;
  uint64_t v = 0;

  uint64_t best_elapsed = static_cast<uint64_t>(-1);
  size_t best_gap = 0;

  while (gap <= 16) {
    printf("gap = %zu\n", gap);

    uint64_t start_t = ::mica::util::rdtsc();

    for (size_t i_ = 0; i_ < request_count + gap * 2; i_++) {
      size_t index = i_;

      if (index < request_count)
        __builtin_prefetch(&table_locations[requests[index]], 0, 0);

      index -= gap;

      if (index < request_count)
        __builtin_prefetch(&pool_locations[table_locations[requests[index]]], 0,
                           0);

      index -= gap;

      if (index < request_count)
        v ^= pool_locations[table_locations[requests[index]]];
    }

    uint64_t diff = ::mica::util::rdtsc() - start_t;

    printf("  elapsed = %" PRIu64 " clocks (%" PRIu64 " clocks/req)\n", diff,
           diff / request_count);
    printf("  gap time = %" PRIu64 " clocks\n", gap * diff / request_count);

    if (best_elapsed > diff) {
      best_elapsed = diff;
      best_gap = gap;
    }

    gap++;
  }

  printf("v = %lu (ignore this)\n", v);
  printf("\n");

  printf("best gap = %zu\n", best_gap);
  printf("  elapsed = %" PRIu64 " clocks (%" PRIu64 " clocks/req)\n",
         best_elapsed, best_elapsed / request_count);
  printf("  gap time = %" PRIu64 " clocks\n",
         best_gap * best_elapsed / request_count);

  return EXIT_SUCCESS;
}
