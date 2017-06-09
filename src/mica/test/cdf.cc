#include <unistd.h>
#include <ctype.h>
#include <errno.h>
#include <sys/mman.h>

#include "mica/util/latency.h"

typedef uint16_t binary_time_t;

int main(int argc, const char* argv[]) {
  FILE* in_file = stdin;
  FILE* out_file = stdout;
  FILE* binary_file = nullptr;
  size_t skip = 0;
  size_t n = static_cast<size_t>(-1);

  bool create_binary = false;

  int c;
  opterr = 0;
  while ((c = getopt(argc, const_cast<char**>(argv), "i:o:b:s:n:")) != -1) {
    switch (c) {
      case 'i':
        in_file = fopen(optarg, "r");
        if (in_file == nullptr) {
          perror("fopen");
          return EXIT_FAILURE;
        }
        break;
      case 'o':
        out_file = fopen(optarg, "w");
        if (out_file == nullptr) {
          perror("fopen");
          return EXIT_FAILURE;
        }
        break;
      case 'b':
        binary_file = fopen(optarg, "rb");
        if (binary_file == nullptr) {
          binary_file = fopen(optarg, "w+b");
          if (binary_file == nullptr) {
            perror("fopen");
            return EXIT_FAILURE;
          }
          create_binary = true;
        }
        break;
      case 's':
        skip = static_cast<size_t>(atol(optarg));
        break;
      case 'n':
        n = static_cast<size_t>(atol(optarg));
        break;
      case '?':
        if (isprint(optopt))
          fprintf(stderr, "incomplete option -%c\n", optopt);
        else
          fprintf(stderr, "error parsing arguments\n");
        return EXIT_FAILURE;
    }
  }

  if (optind == argc) {
    printf(
        "%s [-i IN-FILE] [-o OUT-FILE] [-b BINARY-IN-OUT-FILE] [-s SKIP] [-n "
        "COUNT] "
        "(min|max|avg|cnt|0.5|0.99)...\n",
        argv[0]);
    return EXIT_FAILURE;
  }

  ::mica::util::Latency lat;

  if (create_binary) {
    char buf[64];
    while (true) {
      if (fgets(buf, sizeof(buf), in_file) == nullptr) break;

      auto raw_l = atol(buf);
      auto l = static_cast<binary_time_t>(raw_l);
      if (static_cast<long>(l) != raw_l) {
        fprintf(stderr, "error: binary_time_t overflew!\n");
        return EXIT_FAILURE;
      }
      while (true) {
        size_t wrote = fwrite(&l, sizeof(l), 1, binary_file);
        if (wrote == 1) break;
      }
    }
    fflush(binary_file);
  }

  if (binary_file == nullptr) {
    char buf[64];
    while (true) {
      if (fgets(buf, sizeof(buf), in_file) == nullptr) break;
      if (skip > 0) {
        skip--;
        continue;
      }
      if (--n == 0) break;
      if (buf[0] == '\n') continue;

      auto l = static_cast<uint64_t>(atol(buf));
      lat.update(l);
    }
  } else {
    int ret = fseek(binary_file, 0, SEEK_END);
    if (ret == EBADF || ret == EINVAL) {
      perror("fseek");
      return EXIT_FAILURE;
    }
    long pos = ftell(binary_file);
    if (pos == EBADF || pos == EINVAL) {
      perror("ftell");
      return EXIT_FAILURE;
    }
    size_t file_size = static_cast<size_t>(pos);
    size_t max_n = file_size / sizeof(binary_time_t);

    auto p = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE,
                  fileno(binary_file), 0);
    if (p == MAP_FAILED) {
      perror("mmap");
      return EXIT_FAILURE;
    }

    size_t i = skip;
    while (i < max_n) {
      if (--n == 0) break;

      auto l =
          static_cast<uint64_t>(reinterpret_cast<const binary_time_t*>(p)[i]);
      lat.update(l);
      i++;
    }

    munmap(p, file_size);
  }

  for (auto i = optind; i < argc; i++) {
    const char* arg = argv[i];
    if (strcasecmp(arg, "min") == 0)
      fprintf(out_file, "%s: %" PRIu64 "\n", arg, lat.min());
    else if (strcasecmp(arg, "max") == 0)
      fprintf(out_file, "%s: %" PRIu64 "\n", arg, lat.max());
    else if (strcasecmp(arg, "avg") == 0)
      fprintf(out_file, "%s: %lf\n", arg, lat.avg_f());
    else if (strcasecmp(arg, "cnt") == 0)
      fprintf(out_file, "%s: %" PRIu64 "\n", arg, lat.count());
    else
      fprintf(out_file, "%s: %" PRIu64 "\n", arg, lat.perc(atof(arg)));
  }

  fflush(out_file);
  return EXIT_SUCCESS;
}
