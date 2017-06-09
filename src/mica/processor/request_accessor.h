#pragma once
#ifndef MICA_PROCESSOR_REQUEST_ACCESSOR_H_
#define MICA_PROCESSOR_REQUEST_ACCESSOR_H_

#include "mica/common.h"
#include "mica/processor/types.h"
#include "mica/table/types.h"

namespace mica {
namespace processor {
class RequestAccessorInterface {};

typedef ::mica::table::Result Result;

class RandomAccessRequestAccessorInterface : public RequestAccessorInterface {
 public:
  bool prepare(size_t index);

  Operation get_operation(size_t index);

  uint64_t get_key_hash(size_t index);
  const char* get_key(size_t index);
  size_t get_key_length(size_t index);

  const char* get_value(size_t index);
  size_t get_value_length(size_t index);

  char* get_out_value(size_t index);
  size_t get_out_value_length(size_t index);

  void set_out_value_length(size_t index, size_t len);

  void set_result(size_t index, Result result);

  void retire(size_t index);
};

class RequestArrayAccessor : public RandomAccessRequestAccessorInterface {
 public:
  RequestArrayAccessor(size_t count, const Operation* req_types,
                       const uint64_t* key_hashes, const char** keys,
                       const size_t* key_lengths, const char** values,
                       const size_t* value_lengths, Result** out_results,
                       char** out_values, size_t** out_value_lengths)
      : count_(count),
        req_types_(req_types),
        key_hashes_(key_hashes),
        keys_(keys),
        key_lengths_(key_lengths),
        values_(values),
        value_lengths_(value_lengths),
        out_results_(out_results),
        out_values_(out_values),
        out_value_lengths_(out_value_lengths) {
    assert(req_types_);
    assert(key_hashes_);
    assert(keys_);
    assert(key_lengths_);
    assert(values_);
    assert(value_lengths_);
    assert(out_values_);
    assert(out_value_lengths_);
  }

  bool prepare(size_t index) {
    if (index >= count_) return false;
    // __builtin_prefetch(keys_[index], 0, 0);
    // __builtin_prefetch(values_[index], 0, 0);
    // __builtin_prefetch(out_values_[index], 0, 0);
    return true;
  }

  Operation get_operation(size_t index) {
    assert(index < count_);
    return req_types_[index];
  }

  uint64_t get_key_hash(size_t index) {
    assert(index < count_);
    return key_hashes_[index];
  }

  const char* get_key(size_t index) {
    assert(index < count_);
    return keys_[index];
  }

  size_t get_key_length(size_t index) {
    assert(index < count_);
    return key_lengths_[index];
  }

  const char* get_value(size_t index) {
    assert(index < count_);
    return values_[index];
  }

  size_t get_value_length(size_t index) {
    assert(index < count_);
    return value_lengths_[index];
  }

  char* get_out_value(size_t index) {
    assert(index < count_);
    return out_values_[index];
  }

  size_t get_out_value_length(size_t index) {
    assert(index < count_);
    return *out_value_lengths_[index];
  }

  void set_out_value_length(size_t index, size_t len) {
    assert(index < count_);
    *out_value_lengths_[index] = len;
  }

  void set_result(size_t index, Result result) {
    assert(index < count_);
    *out_results_[index] = result;
  }

  void retire(size_t index) {
    assert(index < count_);
    (void)index;
  }

 private:
  size_t count_;
  const Operation* req_types_;
  const uint64_t* key_hashes_;
  const char** keys_;
  const size_t* key_lengths_;
  const char** values_;
  const size_t* value_lengths_;
  Result** out_results_;
  char** out_values_;
  size_t** out_value_lengths_;
};
}
}

#endif