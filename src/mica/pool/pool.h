#pragma once
#ifndef MICA_POOL_POOL_H_
#define MICA_POOL_POOL_H_

#include <limits>
#include "mica/common.h"

namespace mica {
namespace pool {
class PoolInterface {
 public:
  typedef void Tag;

  typedef uint64_t Offset;
  static constexpr size_t kOffsetWidth =
      static_cast<size_t>(std::numeric_limits<Offset>::digits);
  static constexpr Offset kInsufficientSpace =
      std::numeric_limits<Offset>::max();

  void lock();
  void unlock();

  void reset();
  Offset allocate(size_t size);
  void release(Offset offset);

  void prefetch_item(Offset offset) const;

  char* get_item(Offset offset);
  const char* get_item(Offset offset) const;

  char* get_item(Offset offset, std::size_t* out_len);
  const char* get_item(Offset offset, std::size_t* out_len) const;
};
}
}

#endif