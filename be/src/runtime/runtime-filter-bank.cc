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

#include "runtime/runtime-filter-bank.h"

#include "gen-cpp/ImpalaInternalService_types.h"
#include "gutil/strings/substitute.h"
#include "runtime/client-cache.h"
#include "runtime/exec-env.h"
#include "runtime/backend-client.h"
#include "runtime/mem-tracker.h"
#include "runtime/runtime-filter.inline.h"
#include "service/impala-server.h"
#include "util/bit-util.h"
#include "util/bloom-filter.h"

#include "common/names.h"

using namespace impala;
using namespace boost;
using namespace strings;

DEFINE_double(max_filter_error_rate, 0.75, "(Advanced) The maximum probability of false "
    "positives in a runtime filter before it is disabled.");

const int64_t RuntimeFilterBank::MIN_BLOOM_FILTER_SIZE;
const int64_t RuntimeFilterBank::MAX_BLOOM_FILTER_SIZE;

RuntimeFilterBank::RuntimeFilterBank(const TQueryCtx& query_ctx, RuntimeState* state)
    : state_(state), closed_(false) {
  memory_allocated_ =
      state->runtime_profile()->AddCounter("BloomFilterBytes", TUnit::BYTES);

  // Clamp bloom filter size down to the limits {MIN,MAX}_BLOOM_FILTER_SIZE
  max_filter_size_ = query_ctx.client_request.query_options.runtime_filter_max_size;
  max_filter_size_ = max<int64_t>(max_filter_size_, MIN_BLOOM_FILTER_SIZE);
  max_filter_size_ =
      BitUtil::RoundUpToPowerOfTwo(min<int64_t>(max_filter_size_, MAX_BLOOM_FILTER_SIZE));

  min_filter_size_ = query_ctx.client_request.query_options.runtime_filter_min_size;
  min_filter_size_ = max<int64_t>(min_filter_size_, MIN_BLOOM_FILTER_SIZE);
  min_filter_size_ =
      BitUtil::RoundUpToPowerOfTwo(min<int64_t>(min_filter_size_, MAX_BLOOM_FILTER_SIZE));

  // Make sure that min <= max
  min_filter_size_ = min<int64_t>(min_filter_size_, max_filter_size_);

  DCHECK_GT(min_filter_size_, 0);
  DCHECK_GT(max_filter_size_, 0);

  default_filter_size_ = query_ctx.client_request.query_options.runtime_bloom_filter_size;
  default_filter_size_ = max<int64_t>(default_filter_size_, min_filter_size_);
  default_filter_size_ =
      BitUtil::RoundUpToPowerOfTwo(min<int64_t>(default_filter_size_, max_filter_size_));

  filter_mem_tracker_.reset(
      new MemTracker(-1, "Runtime Filter Bank", state->instance_mem_tracker(), false));
}

RuntimeFilter* RuntimeFilterBank::RegisterFilter(const TRuntimeFilterDesc& filter_desc,
    bool is_producer) {
  RuntimeFilter* ret = obj_pool_.Add(
      new RuntimeFilter(filter_desc, GetFilterSizeForNdv(filter_desc.ndv_estimate)));
  lock_guard<mutex> l(runtime_filter_lock_);
  if (is_producer) {
    DCHECK(produced_filters_.find(filter_desc.filter_id) == produced_filters_.end());
    produced_filters_[filter_desc.filter_id] = ret;
  } else {
    if (consumed_filters_.find(filter_desc.filter_id) == consumed_filters_.end()) {
      consumed_filters_[filter_desc.filter_id] = ret;
      VLOG_QUERY << "registered consumer filter " << filter_desc.filter_id;
    } else {
      // The filter has already been registered in this filter bank by another
      // target node.
      DCHECK_GT(filter_desc.targets.size(), 1);
      ret = consumed_filters_[filter_desc.filter_id];
      VLOG_QUERY << "re-registered consumer filter " << filter_desc.filter_id;
    }
  }
  return ret;
}

namespace {

/// Sends a filter to the coordinator. Executed asynchronously in the context of
/// ExecEnv::rpc_pool().
void SendFilterToCoordinator(TNetworkAddress address, TUpdateFilterParams params,
    ImpalaBackendClientCache* client_cache) {
  Status status;
  ImpalaBackendConnection coord(client_cache, address, &status);
  if (!status.ok()) {
    // Failing to send a filter is not a query-wide error - the remote fragment will
    // continue regardless.
    // TODO: Retry.
    LOG(INFO) << "Couldn't send filter to coordinator: " << status.msg().msg();
    return;
  }
  TUpdateFilterResult res;
  status = coord.DoRpc(&ImpalaBackendClient::UpdateFilter, params, &res);
}

}

void RuntimeFilterBank::UpdateFilterFromLocal(int32_t filter_id,
    BloomFilter* bloom_filter) {
  DCHECK_NE(state_->query_options().runtime_filter_mode, TRuntimeFilterMode::OFF)
      << "Should not be calling UpdateFilterFromLocal() if filtering is disabled";
  TUpdateFilterParams params;
  // A runtime filter may have both local and remote targets.
  bool has_local_target = false;
  bool has_remote_target = false;
  {
    lock_guard<mutex> l(runtime_filter_lock_);
    RuntimeFilterMap::iterator it = produced_filters_.find(filter_id);
    DCHECK(it != produced_filters_.end()) << "Tried to update unregistered filter: "
                                          << filter_id;
    it->second->SetBloomFilter(bloom_filter);
    has_local_target = it->second->filter_desc().has_local_targets;
    has_remote_target = it->second->filter_desc().has_remote_targets;
  }

  if (has_local_target) {
    // Do a short circuit publication by pushing the same BloomFilter to the consumer
    // side.
    RuntimeFilter* filter;
    {
      lock_guard<mutex> l(runtime_filter_lock_);
      RuntimeFilterMap::iterator it = consumed_filters_.find(filter_id);
      if (it == consumed_filters_.end()) return;
      filter = it->second;
    }
    filter->SetBloomFilter(bloom_filter);
    state_->runtime_profile()->AddInfoString(
        Substitute("Filter $0 arrival", filter_id),
        PrettyPrinter::Print(filter->arrival_delay(), TUnit::TIME_MS));
  }

  if (has_remote_target
      && state_->query_options().runtime_filter_mode == TRuntimeFilterMode::GLOBAL) {
    params.__set_filter_id(filter_id);
    params.__set_query_id(state_->query_id());
    BloomFilter::ToThrift(bloom_filter, &params.bloom_filter);
    params.__isset.bloom_filter = true;

    ExecEnv::GetInstance()->rpc_pool()->Offer(bind<void>(
        SendFilterToCoordinator, state_->query_ctx().coord_address, params,
        ExecEnv::GetInstance()->impalad_client_cache()));
  }
}

void RuntimeFilterBank::PublishGlobalFilter(int32_t filter_id,
    const TBloomFilter& thrift_filter) {
  lock_guard<mutex> l(runtime_filter_lock_);
  if (closed_) return;
  RuntimeFilterMap::iterator it = consumed_filters_.find(filter_id);
  DCHECK(it != consumed_filters_.end()) << "Tried to publish unregistered filter: "
                                        << filter_id;
  if (thrift_filter.always_true) {
    it->second->SetBloomFilter(BloomFilter::ALWAYS_TRUE_FILTER);
  } else {
    int64_t required_space =
        BloomFilter::GetExpectedHeapSpaceUsed(thrift_filter.log_heap_space);
    // Silently fail to publish the filter (replacing it with a 0-byte complete one) if
    // there's not enough memory for it.
    if (!filter_mem_tracker_->TryConsume(required_space)) {
      VLOG_QUERY << "No memory for global filter: " << filter_id
                 << " (fragment instance: " << state_->fragment_instance_id() << ")";
      it->second->SetBloomFilter(BloomFilter::ALWAYS_TRUE_FILTER);
    } else {
      BloomFilter* bloom_filter = obj_pool_.Add(new BloomFilter(thrift_filter));
      DCHECK_EQ(required_space, bloom_filter->GetHeapSpaceUsed());
      memory_allocated_->Add(bloom_filter->GetHeapSpaceUsed());
      it->second->SetBloomFilter(bloom_filter);
    }
  }
  state_->runtime_profile()->AddInfoString(Substitute("Filter $0 arrival", filter_id),
      PrettyPrinter::Print(it->second->arrival_delay(), TUnit::TIME_MS));
}

BloomFilter* RuntimeFilterBank::AllocateScratchBloomFilter(int32_t filter_id) {
  lock_guard<mutex> l(runtime_filter_lock_);
  if (closed_) return NULL;

  RuntimeFilterMap::iterator it = produced_filters_.find(filter_id);
  DCHECK(it != produced_filters_.end()) << "Filter ID " << filter_id << " not registered";

  // Track required space
  int64_t log_filter_size = BitUtil::Log2Ceiling64(it->second->filter_size());
  int64_t required_space = BloomFilter::GetExpectedHeapSpaceUsed(log_filter_size);
  if (!filter_mem_tracker_->TryConsume(required_space)) return NULL;
  BloomFilter* bloom_filter = obj_pool_.Add(new BloomFilter(log_filter_size));
  DCHECK_EQ(required_space, bloom_filter->GetHeapSpaceUsed());
  memory_allocated_->Add(bloom_filter->GetHeapSpaceUsed());
  return bloom_filter;
}

int64_t RuntimeFilterBank::GetFilterSizeForNdv(int64_t ndv) {
  if (ndv == -1) return default_filter_size_;
  int64_t required_space =
      1LL << BloomFilter::MinLogSpace(ndv, FLAGS_max_filter_error_rate);
  required_space = max<int64_t>(required_space, min_filter_size_);
  required_space = min<int64_t>(required_space, max_filter_size_);
  return required_space;
}

bool RuntimeFilterBank::FpRateTooHigh(int64_t filter_size, int64_t observed_ndv) {
  double fpp =
      BloomFilter::FalsePositiveProb(observed_ndv, BitUtil::Log2Ceiling64(filter_size));
  return fpp > FLAGS_max_filter_error_rate;
}

void RuntimeFilterBank::Close() {
  lock_guard<mutex> l(runtime_filter_lock_);
  closed_ = true;
  obj_pool_.Clear();
  filter_mem_tracker_->Release(memory_allocated_->value());
  filter_mem_tracker_->UnregisterFromParent();
}
