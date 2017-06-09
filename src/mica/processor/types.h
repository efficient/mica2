#pragma once
#ifndef MICA_PROCESSOR_TYPES_H_
#define MICA_PROCESSOR_TYPES_H_

#include "mica/common.h"

namespace mica {
namespace processor {
enum class Operation {
  kReset = 0,
  kNoopRead,
  kNoopWrite,
  kAdd,
  kSet,
  kGet,
  kTest,
  kDelete,
  kIncrement,
};
}
}

#endif