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

#include "util/bloom-filter.h"
#include <math.h>
#include <string.h>

#include <cmath>
#include <cstdint>
#include <memory>
#include <ostream>

#include "gen-cpp/data_stream_service.pb.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_sidecar.h"
#include "kudu/util/block_bloom_filter.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "runtime/exec-env.h"
#include "util/kudu-status-util.h"

using namespace std;

namespace impala {

constexpr BloomFilter* const BloomFilter::ALWAYS_TRUE_FILTER;

BloomFilter::BloomFilter(BufferPool::ClientHandle* client)
  : buffer_allocator_(client), block_bloom_filter_(&buffer_allocator_) {}

BloomFilter::~BloomFilter() {}

Status BloomFilter::Init(const int log_bufferpool_space, uint32_t hash_seed) {
  KUDU_RETURN_IF_ERROR(
      block_bloom_filter_.Init(log_bufferpool_space, kudu::FAST_HASH, hash_seed),
      "Failed to init Block Bloom Filter");
  return Status::OK();
}

Status BloomFilter::Init(const BloomFilterPB& protobuf, const uint8_t* directory_in,
    size_t directory_in_size, uint32_t hash_seed) {
  if (protobuf.always_false() || directory_in_size == 0) {
    // Directory size equal 0 only when it's always false.
    KUDU_RETURN_IF_ERROR(block_bloom_filter_.Init(
                             protobuf.log_bufferpool_space(), kudu::FAST_HASH, hash_seed),
        "Failed to init Block Bloom Filter");
  } else {
    kudu::Slice slice(directory_in, directory_in_size);
    KUDU_RETURN_IF_ERROR(
        block_bloom_filter_.InitFromDirectory(
            protobuf.log_bufferpool_space(), slice, false, kudu::FAST_HASH, hash_seed),
        "Failed to init Block Bloom Filter");
  }
  return Status::OK();
}

void BloomFilter::Close() {
  block_bloom_filter_.Close();
}

void BloomFilter::AddDirectorySidecar(BloomFilterPB* rpc_params,
    kudu::rpc::RpcController* controller, const char* directory,
    unsigned long directory_size) {
  DCHECK(rpc_params != nullptr);
  DCHECK(!rpc_params->always_false());
  DCHECK(!rpc_params->always_true());
  kudu::Slice dir_slice(directory, directory_size);
  unique_ptr<kudu::rpc::RpcSidecar> rpc_sidecar =
      kudu::rpc::RpcSidecar::FromSlice(dir_slice);

  int sidecar_idx = -1;
  kudu::Status sidecar_status =
      controller->AddOutboundSidecar(std::move(rpc_sidecar), &sidecar_idx);
  if (!sidecar_status.ok()) {
    LOG(ERROR) << "Cannot add outbound sidecar: " << sidecar_status.message().ToString();
    // If AddOutboundSidecar() fails, we 'disable' the BloomFilterPB by setting it to
    // an always true filter.
    rpc_params->set_always_false(false);
    rpc_params->set_always_true(true);
    return;
  }
  rpc_params->set_directory_sidecar_idx(sidecar_idx);
  rpc_params->set_always_false(false);
  rpc_params->set_always_true(false);
}

void BloomFilter::AddDirectorySidecar(BloomFilterPB* rpc_params,
    kudu::rpc::RpcController* controller, const string& directory) {
  AddDirectorySidecar(rpc_params, controller,
      reinterpret_cast<const char*>(&(directory[0])),
      static_cast<unsigned long>(directory.size()));
}

void BloomFilter::ToProtobuf(
    BloomFilterPB* protobuf, kudu::rpc::RpcController* controller) const {
  protobuf->set_log_bufferpool_space(block_bloom_filter_.log_space_bytes());
  if (AlwaysFalse()) {
    protobuf->set_always_false(true);
    protobuf->set_always_true(false);
    return;
  }
  kudu::Slice directory = block_bloom_filter_.directory();
  BloomFilter::AddDirectorySidecar(protobuf, controller,
      reinterpret_cast<const char*>(directory.data()),
      static_cast<unsigned long>(directory.size()));
}

void BloomFilter::ToProtobuf(const BloomFilter* filter,
    kudu::rpc::RpcController* controller, BloomFilterPB* protobuf) {
  DCHECK(protobuf != nullptr);
  // If filter == nullptr, then this BloomFilter is an always true filter.
  if (filter == nullptr) {
    protobuf->set_always_true(true);
    DCHECK(!protobuf->always_false());
    return;
  }
  filter->ToProtobuf(protobuf, controller);
}

int64_t BloomFilter::GetBufferPoolSpaceUsed() {
  return buffer_allocator_.IsAllocated() ? block_bloom_filter_.GetSpaceUsed() : -1;
}

void BloomFilter::Or(const BloomFilter& other) {
  DCHECK_NE(this, &other);
  DCHECK_NE(&other, ALWAYS_TRUE_FILTER);
  if (other.AlwaysFalse()) return;
  DCHECK_EQ(
      block_bloom_filter_.log_space_bytes(), other.block_bloom_filter_.log_space_bytes());
  block_bloom_filter_.Or(other.block_bloom_filter_);
}

void BloomFilter::RawOr(const BloomFilter& other) {
  DCHECK_NE(this, &other);
  DCHECK_NE(&other, ALWAYS_TRUE_FILTER);
  DCHECK_EQ(
      block_bloom_filter_.log_space_bytes(), other.block_bloom_filter_.log_space_bytes());
  kudu::Slice target_slice = block_bloom_filter_.directory();
  kudu::Slice input_slice = other.block_bloom_filter_.directory();
  kudu::BlockBloomFilter::OrEqualArray(
      target_slice.size(), input_slice.data(), const_cast<uint8_t*>(target_slice.data()));
}

void BloomFilter::Or(const BloomFilterPB& in, const kudu::Slice& input_slice) {
  DCHECK_NE(this, BloomFilter::ALWAYS_TRUE_FILTER);
  DCHECK(!in.always_true());
  if (in.always_false()) return;
  DCHECK_EQ(in.log_bufferpool_space(), block_bloom_filter_.log_space_bytes());
  kudu::Slice target_slice = block_bloom_filter_.directory();
  DCHECK_EQ(input_slice.size(), target_slice.size());
  kudu::BlockBloomFilter::OrEqualArray(
      target_slice.size(), input_slice.data(), const_cast<uint8_t*>(target_slice.data()));
}

void BloomFilter::Or(const BloomFilterPB& in, const uint8_t* directory_in,
    BloomFilterPB* out, uint8_t* directory_out, size_t directory_size) {
  DCHECK(out != nullptr);
  DCHECK_NE(&in, out);
  // These cases are impossible in current code. If they become possible in the future,
  // memory usage should be tracked accordingly.
  DCHECK(!out->always_false());
  DCHECK(!out->always_true());
  DCHECK(!in.always_true());
  if (in.always_false()) return;
  DCHECK_EQ(in.log_bufferpool_space(), out->log_bufferpool_space());
  kudu::BlockBloomFilter::OrEqualArray(directory_size, directory_in, directory_out);
}

} // namespace impala
