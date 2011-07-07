// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "common/object-pool.h"
#include "runtime/runtime-state.h"

namespace impala {

RuntimeState::RuntimeState(const DescriptorTbl& descs)
  : descs_(descs),
    obj_pool_(new ObjectPool()),
    batch_size_(DEFAULT_BATCH_SIZE),
    file_buf_size_(DEFAULT_FILE_BUF_SIZE) {
}

}
