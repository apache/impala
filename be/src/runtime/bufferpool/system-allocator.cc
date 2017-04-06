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

#include "runtime/bufferpool/system-allocator.h"

#include "util/bit-util.h"

namespace impala {

SystemAllocator::SystemAllocator(int64_t min_buffer_len)
  : min_buffer_len_(min_buffer_len) {
  DCHECK(BitUtil::IsPowerOf2(min_buffer_len));
}

Status SystemAllocator::Allocate(int64_t len, BufferPool::BufferHandle* buffer) {
  DCHECK_GE(len, min_buffer_len_);
  DCHECK_LE(len, BufferPool::MAX_BUFFER_BYTES);
  DCHECK(BitUtil::IsPowerOf2(len)) << len;

  uint8_t* alloc = reinterpret_cast<uint8_t*>(malloc(len));
  if (alloc == NULL) return Status(TErrorCode::BUFFER_ALLOCATION_FAILED, len);
  buffer->Open(alloc, len, CpuInfo::GetCurrentCore());
  return Status::OK();
}

void SystemAllocator::Free(BufferPool::BufferHandle&& buffer) {
  free(buffer.data());
  buffer.Reset(); // Avoid DCHECK in ~BufferHandle().
}
}
