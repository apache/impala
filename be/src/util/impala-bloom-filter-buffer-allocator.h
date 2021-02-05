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

#pragma once

#include "kudu/util/block_bloom_filter.h"
#include "runtime/bufferpool/buffer-pool.h"

namespace impala {

// Buffer allocator to allocate and de-allocate memory for the BlockBloomFilter
// from buffer pool.
class ImpalaBloomFilterBufferAllocator : public kudu::BlockBloomFilterBufferAllocatorIf {
 public:
  // Default constructor, which is defined to support the virtual function Clone().
  // It uses kudu::DefaultBlockBloomFilterBufferAllocator to allocate/de-allocate
  // memory. Since Clone function is only used for internal testing, so that
  // memory allocation don't need to be tracked.
  ImpalaBloomFilterBufferAllocator();

  // Constructor with client handle of the buffer pool, which is created for
  // runtime filters in runtime-filter-bank.
  explicit ImpalaBloomFilterBufferAllocator(BufferPool::ClientHandle* client);

  ~ImpalaBloomFilterBufferAllocator() override;

  kudu::Status AllocateBuffer(size_t bytes, void** ptr) override;
  void FreeBuffer(void* ptr) override;

  // This virtual function is only defined for Kudu internal testing.
  // Impala code should not hit this function.
  std::shared_ptr<kudu::BlockBloomFilterBufferAllocatorIf> Clone() const override {
    LOG(DFATAL) << "Unsupported code path.";
    return std::make_shared<ImpalaBloomFilterBufferAllocator>();
  }

  bool IsAllocated() { return is_allocated_; }

 private:
  void Close();

  /// Bufferpool client and handle used for allocating and freeing directory memory.
  /// Client is not owned by the buffer allocator.
  BufferPool::ClientHandle* buffer_pool_client_;
  BufferPool::BufferHandle buffer_handle_;
  bool is_allocated_;

  DISALLOW_COPY_AND_ASSIGN(ImpalaBloomFilterBufferAllocator);
};

} // namespace impala
