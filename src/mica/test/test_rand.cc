#include <cstdio>
#include "mica/util/lcore.h"
#include "mica/util/stopwatch.h"
#include "mica/util/rand.h"
#include "mica/util/rand_pcg.h"
#include "mica/util/rand_philox.h"

static ::mica::util::Stopwatch sw;

template <class T>
void benchmark(const char* name) {
  const uint64_t count = 1000000000LU;

  T rand;
  uint64_t v = 0;

  uint64_t start = sw.now();

  for (uint64_t i = 0; i < count; i++)
    v += static_cast<uint64_t>(rand.next_u32());

  uint64_t end = sw.now();

  double diff = sw.diff(end, start);

  printf("Using %s\n", name);
  printf("count: %" PRIu64 "\n", count);
  printf("elapsed: %.2lf seconds\n", diff);
  printf("throughput: %.2lf M/s\n",
         static_cast<double>(count) / diff / 1000000.);
  printf("v: %" PRIu64 "\n", v);
  printf("\n");
}

int main(int argc, const char* argv[]) {
  (void)argc;
  (void)argv;

  ::mica::util::lcore.pin_thread(0);

  sw.init_start();
  sw.init_end();

  // For warming up
  benchmark<::mica::util::Rand>("Rand (for warming up; ignore this)");

  benchmark<::mica::util::Rand>("Rand");
  benchmark<::mica::util::RandPCG>("RandPCG");
  benchmark<::mica::util::RandPhilox>("RandPhilox");

  return EXIT_SUCCESS;
}