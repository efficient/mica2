#pragma once
#ifndef MICA_TABLE_TYPES_H_
#define MICA_TABLE_TYPES_H_

#include "mica/common.h"

namespace mica {
namespace table {
enum class Result {
  kSuccess = 0,
  kError,
  kInsufficientSpace,
  kExists,
  kNotFound,
  kPartialValue,
  kNotProcessed,
  kNotSupported,
  kTimedOut,
  kRejected,
};
}
}

#endif