// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "common/object-pool.h"
#include "runtime/runtime-state.h"

namespace impala {

RuntimeState::RuntimeState()
  : obj_pool_(new ObjectPool()) {
}

}
