#include <vector>
#include <thread>
#include <cstdio>
#include <cstdlib>
#include <cassert>
#include <sys/time.h>
#include "mica/processor/partitions.h"
#include "mica/processor/request_accessor.h"
#include "mica/util/hash.h"
#include "mica/util/zipf.h"
#include "mica/util/tsc.h"

struct LTableConfig : public ::mica::table::BasicLossyLTableConfig {
  // struct LTableConfig : public ::mica::table::BasicLosslessLTableConfig {
  // static constexpr bool kVerbose = true;
};

struct PartitionsConfig : public ::mica::processor::BasicPartitionsConfig {
  // static constexpr bool kVerbose = true;

  typedef ::mica::table::LTable<LTableConfig> Table;
  typedef typename Table::Alloc Alloc;
};

typedef ::mica::processor::Partitions<PartitionsConfig> Processor;

typedef ::mica::table::Result Result;
typedef ::mica::processor::Operation Operation;

template <typename T>
static uint64_t hash(const T* key, size_t key_length) {
  return ::mica::util::hash(key, key_length);
}

enum class BenchmarkMode {
  kAdd = 0,
  kSet,
  kGetHit,
  kGetMiss,
  kGetSet95,
  kGetSet50,
  kDelete,
  kSet1,
  kGet1,
};

struct Task {
  uint16_t lcore_id;
  uint16_t num_threads;

  Processor* processor;

  uint8_t key_length;
  uint8_t value_length;

  size_t count;
  uint8_t* types;
  char* keys;
  uint64_t* key_hashes;
  char* values;

  struct timeval tv_start;
  struct timeval tv_end;

  uint64_t success_count;
  uint64_t total_operation_count;
  // size_t num_existing_items;
  // size_t* existing_items;
} __attribute__((aligned(128)));  // To prevent false sharing caused by
                                  // adjacent cacheline prefetching.

class RequestAccessor : public ::mica::processor::RequestAccessorInterface {
 public:
  RequestAccessor(const Task& task) : task_(task) {
    assert(sizeof(out_value_) >= task_.value_length);
  }

  Task& get_task() { return task_; }

  size_t count() { return task_.count; }

  bool prepare(size_t index) {
    if (index >= task_.count) return false;
    // HW prefetching is usually enough.
    // __builtin_prefetch(task_.key_hashes + index, 0, 0);
    // __builtin_prefetch(task_.keys + index * task_.key_length, 0, 0);
    // __builtin_prefetch(task_.values + index * task_.value_length, 0, 0);
    return true;
  }

  Operation get_operation(size_t index) {
    assert(index < task_.count);
    return static_cast<Operation>(task_.types[index]);
  }

  uint64_t get_key_hash(size_t index) {
    assert(index < task_.count);
    return task_.key_hashes[index];
  }

  const char* get_key(size_t index) {
    assert(index < task_.count);
    return task_.keys + index * task_.key_length;
  }

  size_t get_key_length(size_t index) {
    assert(index < task_.count);
    (void)index;
    return task_.key_length;
  }

  const char* get_value(size_t index) {
    assert(index < task_.count);
    return task_.values + index * task_.value_length;
  }

  size_t get_value_length(size_t index) {
    assert(index < task_.count);
    (void)index;
    return task_.value_length;
  }

  char* get_out_value(size_t index) {
    assert(index < task_.count);
    (void)index;
    return out_value_;
  }

  size_t get_out_value_length(size_t index) {
    (void)index;
    return sizeof(out_value_);
  }

  void set_out_value_length(size_t index, size_t len) {
    (void)index;
    (void)len;
  }

  void set_result(size_t index, Result result) {
    assert(index < task_.count);
    (void)index;
    if (result == Result::kSuccess) task_.success_count++;
  }

  void retire(size_t index) {
    assert(index < task_.count);
    (void)index;
  }

 private:
  Task task_;
  size_t out_value_length_;
  char out_value_[2048]
      __attribute__((aligned(8)));  // For 8-byte aligned access.
} __attribute__((aligned(128)));    // To prevent false sharing caused by
                                    // adjacent cacheline prefetching.

static volatile uint16_t running_threads;

void benchmark_proc(Task* task) {
  ::mica::util::lcore.pin_thread(task->lcore_id);

  RequestAccessor ra(*task);

  // warm up to increase CPU clock and cache (about 3-5 seconds)
  {
    uint64_t start_t = ::mica::util::rdtsc();
    while (::mica::util::rdtsc() - start_t < 10 * 1000 * 1000 * 1000LU) {
      volatile size_t v = 0;
      for (size_t i = 0; i < ra.count(); i++) {
        v ^= static_cast<size_t>(ra.get_operation(i));
        v ^= static_cast<size_t>(ra.get_key_hash(i));
        v ^= static_cast<size_t>(ra.get_key(i)[0]);
        v ^= static_cast<size_t>(ra.get_value(i)[0]);
      }
    }
  }

  __sync_add_and_fetch(&running_threads, 1);
  while (running_threads < task->num_threads) ::mica::util::pause();

  gettimeofday(&task->tv_start, nullptr);

  // if (task->lcore_id == 0) {
  //   auto ret = system("perf record -a -g sleep 5 &");
  //   (void)ret;
  // }

  for (auto i = 0; i < 10; i++) {
    task->processor->process(ra);
    task->total_operation_count += ra.count();
  }

  task->success_count = ra.get_task().success_count;

  gettimeofday(&task->tv_end, nullptr);
}

void benchmark(double zipf_theta) {
  ::mica::util::lcore.pin_thread(0);

  printf("zipf_theta = %lf\n", zipf_theta);

  size_t num_items = 16 * 1048576;

  auto config = ::mica::util::Config::load_file("microbench.json");

  uint16_t num_threads =
      static_cast<uint16_t>(config.get("processor").get("lcores").size());
  size_t num_operations = 16 * 1048576;
  size_t max_num_operations_per_thread = num_operations;

  size_t key_length = ::mica::util::roundup<8>(sizeof(uint64_t));
  size_t value_length = ::mica::util::roundup<8>(sizeof(uint64_t));

  PartitionsConfig::Alloc alloc(config.get("alloc"));

  char* keys =
      reinterpret_cast<char*>(alloc.malloc_striped(key_length * num_items * 2));
  assert(keys);
  uint64_t* key_hashes = reinterpret_cast<uint64_t*>(
      alloc.malloc_striped(sizeof(uint64_t) * num_items * 2));
  assert(key_hashes);
  uint16_t* key_parts = reinterpret_cast<uint16_t*>(
      alloc.malloc_striped(sizeof(uint16_t) * num_items * 2));
  assert(key_parts);
  char* values = reinterpret_cast<char*>(
      alloc.malloc_striped(value_length * num_items * 2));
  assert(values);

  uint64_t* op_count = new uint64_t[num_threads];
  assert(op_count);
  uint8_t** op_types = new uint8_t*[num_threads];
  assert(op_types);
  char** op_keys = new char*[num_threads];
  assert(op_keys);
  uint64_t** op_key_hashes = new uint64_t*[num_threads];
  assert(op_key_hashes);
  char** op_values = new char*[num_threads];
  assert(op_values);

  for (size_t thread_id = 0; thread_id < num_threads; thread_id++) {
    op_types[thread_id] = reinterpret_cast<uint8_t*>(
        alloc.malloc_contiguous(num_operations, thread_id));
    assert(op_types[thread_id]);
    op_keys[thread_id] = reinterpret_cast<char*>(
        alloc.malloc_contiguous(key_length * num_operations, thread_id));
    assert(op_keys[thread_id]);
    op_key_hashes[thread_id] = reinterpret_cast<uint64_t*>(
        alloc.malloc_contiguous(sizeof(uint64_t) * num_operations, thread_id));
    assert(op_key_hashes[thread_id]);
    op_values[thread_id] = reinterpret_cast<char*>(
        alloc.malloc_contiguous(value_length * num_operations, thread_id));
    assert(op_values[thread_id]);
  }

  size_t mem_start = alloc.get_memuse();

  Processor processor(config.get("processor"), &alloc);

  bool concurrent_read = processor.get_concurrent_read();
  bool concurrent_write = processor.get_concurrent_write();

  size_t mem_diff = (size_t)-1;
  double add_ops = -1.;
  double set_ops = -1.;
  double get_hit_ops = -1.;
  double get_miss_ops = -1.;
  double get_set_95_ops = -1.;
  double get_set_50_ops = -1.;
  double delete_ops = -1.;
  double set_1_ops = -1.;
  double get_1_ops = -1.;

  printf("generating %zu items (including %zu miss items)\n", num_items,
         num_items);
  for (size_t i = 0; i < num_items * 2; i++) {
    *(uint64_t*)(keys + i * key_length) = i;
    *(key_hashes + i) = hash(keys + i * key_length, key_length);
    *(key_parts + i) = processor.get_partition_id(*(key_hashes + i));
    *(uint64_t*)(values + i * value_length) = i;
  }
  printf("\n");

  Task tasks[num_threads];

  for (uint16_t thread_id = 0; thread_id < num_threads; thread_id++) {
    Task& task = tasks[thread_id];

    task.lcore_id = thread_id;
    task.num_threads = num_threads;

    task.processor = &processor;

    task.key_length = ::mica::util::safe_cast<uint8_t>(key_length);
    task.value_length = ::mica::util::safe_cast<uint8_t>(value_length);

    // task.count
    task.types = op_types[thread_id];
    task.keys = op_keys[thread_id];
    task.key_hashes = op_key_hashes[thread_id];
    task.values = op_values[thread_id];

    // task.success_count
  }

  BenchmarkMode benchmark_modes[] = {
      // clang-format off
      BenchmarkMode::kAdd,
      BenchmarkMode::kSet,
      BenchmarkMode::kGetHit,
      BenchmarkMode::kGetMiss,
      BenchmarkMode::kGetSet95,
      BenchmarkMode::kGetSet50,
      BenchmarkMode::kDelete,
      BenchmarkMode::kSet1,
      BenchmarkMode::kGet1,
      // clang-format on
  };

  for (auto& benchmark_mode : benchmark_modes) {
    switch (benchmark_mode) {
      case BenchmarkMode::kAdd:
        printf("adding %zu items\n", num_items);
        break;
      case BenchmarkMode::kSet:
        printf("setting %zu items\n", num_items);
        break;
      case BenchmarkMode::kGetHit:
        printf("getting %zu items (hit)\n", num_items);
        break;
      case BenchmarkMode::kGetMiss:
        printf("getting %zu items (miss)\n", num_items);
        break;
      case BenchmarkMode::kGetSet95:
        printf("getting/setting %zu items (95%% get)\n", num_items);
        break;
      case BenchmarkMode::kGetSet50:
        printf("getting/setting %zu items (50%% get)\n", num_items);
        break;
      case BenchmarkMode::kDelete:
        printf("deleting %zu items\n", num_items);
        break;
      case BenchmarkMode::kSet1:
        printf("setting 1 item\n");
        break;
      case BenchmarkMode::kGet1:
        printf("getting 1 item\n");
        break;
      default:
        assert(false);
    }

    printf("generating workload\n");
    ::mica::util::Rand thread_rand(1);
    // ::mica::util::Rand key_rand(2);
    ::mica::util::Rand op_type_rand(3);

    uint32_t get_threshold = 0;
    if (benchmark_mode == BenchmarkMode::kAdd ||
        benchmark_mode == BenchmarkMode::kSet ||
        benchmark_mode == BenchmarkMode::kDelete ||
        benchmark_mode == BenchmarkMode::kSet1)
      get_threshold = (uint32_t)(0.0 * (double)((uint32_t)-1));
    else if (benchmark_mode == BenchmarkMode::kGetHit ||
             benchmark_mode == BenchmarkMode::kGetMiss ||
             benchmark_mode == BenchmarkMode::kGet1)
      get_threshold = (uint32_t)(1.0 * (double)((uint32_t)-1));
    else if (benchmark_mode == BenchmarkMode::kGetSet95)
      get_threshold = (uint32_t)(0.95 * (double)((uint32_t)-1));
    else if (benchmark_mode == BenchmarkMode::kGetSet50)
      get_threshold = (uint32_t)(0.5 * (double)((uint32_t)-1));
    else
      assert(false);

    ::mica::util::ZipfGen zg(num_items, zipf_theta,
                             static_cast<uint64_t>(benchmark_mode));

    for (size_t thread_id = 0; thread_id < num_threads; thread_id++)
      op_count[thread_id] = 0;

    for (size_t j = 0; j < num_operations; j++) {
      size_t i;
      if (benchmark_mode == BenchmarkMode::kAdd ||
          benchmark_mode == BenchmarkMode::kDelete) {
        if (j >= num_items) break;
        i = j;
      } else if (benchmark_mode == BenchmarkMode::kGet1 ||
                 benchmark_mode == BenchmarkMode::kSet1)
        i = 0;
      else {
        // i = key_rand.next_u32() % num_items;
        i = zg.next();
        if (benchmark_mode == BenchmarkMode::kGetMiss) i += num_items;
      }

      uint16_t partition_id = key_parts[i];

      uint32_t op_r = op_type_rand.next_u32();
      bool is_get = op_r <= get_threshold;

      uint16_t thread_id;
      if (is_get) {
        if (concurrent_read == false)
          thread_id = processor.get_owner_lcore_id(partition_id);
        else
          thread_id =
              static_cast<uint16_t>(thread_rand.next_u32() % num_threads);
      } else {
        if (concurrent_write == false)
          thread_id = processor.get_owner_lcore_id(partition_id);
        else
          thread_id =
              static_cast<uint16_t>(thread_rand.next_u32() % num_threads);
      }

      if (op_count[thread_id] < max_num_operations_per_thread) {
        uint8_t op_type;
        switch (benchmark_mode) {
          case BenchmarkMode::kAdd:
            op_type = static_cast<uint8_t>(Operation::kAdd);
            break;
          case BenchmarkMode::kSet:
          case BenchmarkMode::kSet1:
            op_type = static_cast<uint8_t>(Operation::kSet);
            break;
          case BenchmarkMode::kGetHit:
          case BenchmarkMode::kGet1:
          case BenchmarkMode::kGetMiss:
            op_type = static_cast<uint8_t>(Operation::kGet);
            break;
          case BenchmarkMode::kGetSet95:
          case BenchmarkMode::kGetSet50:
            op_type = static_cast<uint8_t>(is_get ? Operation::kGet
                                                  : Operation::kSet);
            break;
          case BenchmarkMode::kDelete:
            op_type = static_cast<uint8_t>(Operation::kDelete);
            break;
          default:
            assert(false);
            op_type = static_cast<uint8_t>(Operation::kNoopRead);
            break;
        }
        op_types[thread_id][op_count[thread_id]] = op_type;
        ::mica::util::memcpy(
            op_keys[thread_id] + key_length * op_count[thread_id],
            keys + key_length * i, key_length);
        op_key_hashes[thread_id][op_count[thread_id]] = key_hashes[i];
        ::mica::util::memcpy(
            op_values[thread_id] + value_length * op_count[thread_id],
            values + value_length * i, value_length);
        op_count[thread_id]++;
      } else
        break;
    }

    printf("executing workload\n");

    for (size_t thread_id = 0; thread_id < num_threads; thread_id++) {
      Task& task = tasks[thread_id];

      task.count = op_count[thread_id];
      task.total_operation_count = 0;
      task.success_count = 0;
    }

    processor.reset_load_stats();

    running_threads = 0;
    ::mica::util::memory_barrier();

    std::vector<std::thread> threads;
    for (size_t thread_id = 1; thread_id < num_threads; thread_id++)
      threads.emplace_back(benchmark_proc, &tasks[thread_id]);

    benchmark_proc(&tasks[0]);

    while (threads.size() > 0) {
      threads.back().join();
      threads.pop_back();
    }

    double diff;
    {
      double min_start = 0.;
      double max_end = 0.;
      for (size_t thread_id = 0; thread_id < num_threads; thread_id++) {
        double start = (double)tasks[thread_id].tv_start.tv_sec * 1. +
                       (double)tasks[thread_id].tv_start.tv_usec * 0.000001;
        double end = (double)tasks[thread_id].tv_end.tv_sec * 1. +
                     (double)tasks[thread_id].tv_end.tv_usec * 0.000001;
        if (thread_id == 0 || min_start > start) min_start = start;
        if (thread_id == 0 || max_end < end) max_end = end;
      }

      diff = max_end - min_start;
    }

    size_t success_count = 0;
    size_t total_operation_count = 0;
    uint64_t max_operation_count = 0;
    for (size_t thread_id = 0; thread_id < num_threads; thread_id++) {
      total_operation_count += tasks[thread_id].total_operation_count;
      success_count += tasks[thread_id].success_count;
      if (max_operation_count < tasks[thread_id].total_operation_count)
        max_operation_count = tasks[thread_id].total_operation_count;
    }

    printf("operations: %zu\n", total_operation_count);
    printf("success_count: %zu\n", success_count);

    for (uint16_t thread_id = 0; thread_id < num_threads; thread_id++) {
      uint32_t request_count_sum = 0;
      uint64_t processing_time = processor.get_processing_time(thread_id);
      for (uint16_t index = 0; index < processor.get_table_count(); index++) {
        uint32_t request_count = processor.get_request_count(thread_id, index);
        request_count_sum += request_count;
      }
      if (request_count_sum == 0) request_count_sum = 1;

      printf("lcore %2hu:", thread_id);
      printf(" %4.0lf clocks/req ", static_cast<double>(processing_time) /
                                        static_cast<double>(request_count_sum));
      for (uint16_t index = 0; index < processor.get_table_count(); index++) {
        uint32_t request_count = processor.get_request_count(thread_id, index);
        printf(" %3.0lf", 100. * static_cast<double>(request_count) /
                              static_cast<double>(max_operation_count));
      }
      printf("\n");
    }

    switch (benchmark_mode) {
      case BenchmarkMode::kAdd:
        add_ops = (double)total_operation_count / diff;
        mem_diff = alloc.get_memuse() - mem_start;
        break;
      case BenchmarkMode::kSet:
        set_ops = (double)total_operation_count / diff;
        break;
      case BenchmarkMode::kGetHit:
        get_hit_ops = (double)total_operation_count / diff;
        break;
      case BenchmarkMode::kGetMiss:
        get_miss_ops = (double)total_operation_count / diff;
        break;
      case BenchmarkMode::kGetSet95:
        get_set_95_ops = (double)total_operation_count / diff;
        break;
      case BenchmarkMode::kGetSet50:
        get_set_50_ops = (double)total_operation_count / diff;
        break;
      case BenchmarkMode::kDelete:
        delete_ops = (double)total_operation_count / diff;
        break;
      case BenchmarkMode::kSet1:
        set_1_ops = (double)total_operation_count / diff;
        break;
      case BenchmarkMode::kGet1:
        get_1_ops = (double)total_operation_count / diff;
        break;
      default:
        assert(false);
    }

    printf("\n");
  }

  printf("memory:     %10.2lf MB\n", (double)mem_diff * 0.000001);
  printf("add:        %10.2lf Mops\n", add_ops * 0.000001);
  printf("set:        %10.2lf Mops\n", set_ops * 0.000001);
  printf("get_hit:    %10.2lf Mops\n", get_hit_ops * 0.000001);
  printf("get_miss:   %10.2lf Mops\n", get_miss_ops * 0.000001);
  printf("get_set_95: %10.2lf Mops\n", get_set_95_ops * 0.000001);
  printf("get_set_50: %10.2lf Mops\n", get_set_50_ops * 0.000001);
  printf("delete:     %10.2lf Mops\n", delete_ops * 0.000001);
  printf("set_1:      %10.2lf Mops\n", set_1_ops * 0.000001);
  printf("get_1:      %10.2lf Mops\n", get_1_ops * 0.000001);
}

int main(int argc, const char* argv[]) {
  if (argc < 2) {
    printf("%s ZIPF-THETA\n", argv[0]);
    return EXIT_FAILURE;
  }

  benchmark(atof(argv[1]));

  return EXIT_SUCCESS;
}
