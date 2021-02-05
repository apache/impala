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

#include "util/impala-bloom-filter-buffer-allocator.h"
#include "runtime/exec-env.h"

namespace impala {

ImpalaBloomFilterBufferAllocator::ImpalaBloomFilterBufferAllocator()
  : buffer_pool_client_(nullptr), is_allocated_(false) {
  // Default constructor, which is defined to support the virtual function Clone().
  // Impala code should not hit this function.
  LOG(DFATAL) << "Unsupported code path.";
}

ImpalaBloomFilterBufferAllocator::ImpalaBloomFilterBufferAllocator(
    BufferPool::ClientHandle* client)
  : buffer_pool_client_(DCHECK_NOTNULL(client)), is_allocated_(false) {}

ImpalaBloomFilterBufferAllocator::~ImpalaBloomFilterBufferAllocator() {
  if (is_allocated_) {
    LOG(DFATAL) << "Close() should have been called before the object is destroyed.";
    Close();
  }
}

void ImpalaBloomFilterBufferAllocator::Close() {
  if (!is_allocated_) return;
  DCHECK(buffer_pool_client_ != nullptr);
  BufferPool* buffer_pool = ExecEnv::GetInstance()->buffer_pool();
  buffer_pool->FreeBuffer(buffer_pool_client_, &buffer_handle_);
  is_allocated_ = false;
}

kudu::Status ImpalaBloomFilterBufferAllocator::AllocateBuffer(size_t bytes, void** ptr) {
  Close(); // Ensure that any previously allocated memory is released.

  BufferPool* buffer_pool = ExecEnv::GetInstance()->buffer_pool();
  DCHECK(buffer_pool_client_ != nullptr);
  const size_t min_buffer_len = buffer_pool_client_->min_buffer_len();
  impala::Status status = buffer_pool->AllocateBuffer(buffer_pool_client_,
      std::max(bytes, min_buffer_len), &buffer_handle_);
  if (!status.ok()) {
    return kudu::Status::RuntimeError(
        strings::Substitute("BufferPool bad_alloc, bytes: $0", bytes));
  }
  *ptr = reinterpret_cast<void*>(buffer_handle_.data());
  is_allocated_ = true;
  return kudu::Status::OK();
}

void ImpalaBloomFilterBufferAllocator::FreeBuffer(void* ptr) {
  if (ptr == nullptr) return;
  DCHECK_EQ(ptr, buffer_handle_.data());
  Close();
}

} // namespace impala
