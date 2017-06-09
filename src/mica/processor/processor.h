#pragma once
#ifndef MICA_PROCESSOR_PROCESSOR_H_
#define MICA_PROCESSOR_PROCESSOR_H_

#include "mica/processor/request_accessor.h"

namespace mica {
namespace processor {
template <class Table>
class ProcessorInterface {
 public:
  template <class RequestAccessor>
  void process(RequestAccessor& ra);

  // template <class RequestAccessor>
  // void process(RequestAccessor& ra) const;

  size_t get_table_count() const;

  const Table* get_table(size_t index) const;
  Table* get_table(size_t index);

  bool get_concurrent_read() const;
  bool get_concurrent_write() const;
};
}
}
#endif