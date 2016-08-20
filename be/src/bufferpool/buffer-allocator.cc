// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "bufferpool/buffer-allocator.h"

#include "util/bit-util.h"

namespace impala {

BufferAllocator::BufferAllocator(int64_t min_buffer_len)
  : min_buffer_len_(min_buffer_len) {}

Status BufferAllocator::Allocate(int64_t len, uint8_t** buffer) {
  DCHECK_GE(len, min_buffer_len_);
  DCHECK_EQ(len, BitUtil::RoundUpToPowerOfTwo(len));

  *buffer = reinterpret_cast<uint8_t*>(malloc(len));
  if (*buffer == NULL) return Status(TErrorCode::BUFFER_ALLOCATION_FAILED, len);
  return Status::OK();
}

void BufferAllocator::Free(uint8_t* buffer, int64_t len) {
  free(buffer);
}
}
