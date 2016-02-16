// Copyright 2016 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "runtime/runtime-filter.h"

#include <gutil/strings/substitute.h>

#include "common/names.h"
#include "runtime/client-cache.h"
#include "runtime/exec-env.h"
#include "service/impala-server.h"
#include "util/bloom-filter.h"

using namespace impala;
using namespace boost;
using namespace strings;

DEFINE_double(max_filter_error_rate, 0.75, "(Advanced) The maximum probability of false "
    "positives in a runtime filter before it is disabled.");

const int RuntimeFilter::SLEEP_PERIOD_MS = 20;

const int32_t RuntimeFilterBank::MIN_BLOOM_FILTER_SIZE;
const int32_t RuntimeFilterBank::MAX_BLOOM_FILTER_SIZE;

RuntimeFilterBank::RuntimeFilterBank(const TQueryCtx& query_ctx, RuntimeState* state)
    : query_ctx_(query_ctx), state_(state), closed_(false) {
  memory_allocated_ =
      ADD_COUNTER(state->runtime_profile(), "BloomFilterBytes", TUnit::BYTES);

  // Clamp bloom filter size down to the limits {MIN,MAX}_BLOOM_FILTER_SIZE
  int32_t bloom_filter_size = query_ctx_.request.query_options.runtime_bloom_filter_size;
  bloom_filter_size = std::max(bloom_filter_size, MIN_BLOOM_FILTER_SIZE);
  bloom_filter_size = std::min(bloom_filter_size, MAX_BLOOM_FILTER_SIZE);
  log_filter_size_ = BitUtil::Log2(bloom_filter_size);
}

RuntimeFilter* RuntimeFilterBank::RegisterFilter(const TRuntimeFilterDesc& filter_desc,
    bool is_producer) {
  RuntimeFilter* ret = obj_pool_.Add(new RuntimeFilter(filter_desc));
  lock_guard<SpinLock> l(runtime_filter_lock_);
  if (is_producer) {
    DCHECK(produced_filters_.find(filter_desc.filter_id) == produced_filters_.end());
    produced_filters_[filter_desc.filter_id] = ret;
  } else {
    DCHECK(consumed_filters_.find(filter_desc.filter_id) == consumed_filters_.end());
    consumed_filters_[filter_desc.filter_id] = ret;
  }
  return ret;
}

namespace {

/// Sends a filter to the coordinator. Executed asynchronously in the context of
/// ExecEnv::rpc_pool().
void SendFilterToCoordinator(TNetworkAddress address, TUpdateFilterParams params,
    ImpalaInternalServiceClientCache* client_cache) {
  Status status;
  ImpalaInternalServiceConnection coord(client_cache, address, &status);
  if (!status.ok()) {
    // Failing to send a filter is not a query-wide error - the remote fragment will
    // continue regardless.
    // TODO: Retry.
    LOG(INFO) << "Couldn't send filter to coordinator: " << status.msg().msg();
    return;
  }
  TUpdateFilterResult res;
  status = coord.DoRpc(&ImpalaInternalServiceClient::UpdateFilter, params, &res);
}

}

void RuntimeFilterBank::UpdateFilterFromLocal(uint32_t filter_id,
    BloomFilter* bloom_filter) {
  DCHECK_NE(state_->query_options().runtime_filter_mode, TRuntimeFilterMode::OFF)
      << "Should not be calling UpdateFilterFromLocal() if filtering is disabled";
  TUpdateFilterParams params;
  bool is_broadcast = false;
  {
    lock_guard<SpinLock> l(runtime_filter_lock_);
    RuntimeFilterMap::iterator it = produced_filters_.find(filter_id);
    DCHECK(it != produced_filters_.end()) << "Tried to update unregistered filter: "
                                          << filter_id;
    it->second->SetBloomFilter(bloom_filter);
    is_broadcast = it->second->filter_desc().is_broadcast_join;
  }

  if (state_->query_options().runtime_filter_mode == TRuntimeFilterMode::GLOBAL) {
    bloom_filter->ToThrift(&params.bloom_filter);
    params.filter_id = filter_id;
    params.query_id = query_ctx_.query_id;

    ExecEnv::GetInstance()->rpc_pool()->Offer(bind<void>(
        SendFilterToCoordinator, query_ctx_.coord_address, params,
        ExecEnv::GetInstance()->impalad_client_cache()));
  }

  if (is_broadcast) {
    // Do a short circuit publication by pushing the same BloomFilter to the consumer
    // side.
    RuntimeFilter* filter;
    {
      lock_guard<SpinLock> l(runtime_filter_lock_);
      RuntimeFilterMap::iterator it = consumed_filters_.find(filter_id);
      if (it == consumed_filters_.end()) return;
      filter = it->second;
      // Check if the filter already showed up.
      if (filter->GetBloomFilter() != NULL) return;
    }
    // TODO: Avoid need for this copy.
    BloomFilter* copy = AllocateScratchBloomFilter();
    if (copy == NULL) return;
    copy->Or(*bloom_filter);
    {
      // Take lock only to ensure no race with PublishGlobalFilter() - there's no need for
      // coordination with readers of the filter.
      lock_guard<SpinLock> l(runtime_filter_lock_);
      if (filter->GetBloomFilter() == NULL) {
        filter->SetBloomFilter(copy);
        state_->runtime_profile()->AddInfoString(
            Substitute("Filter $0 arrival", filter_id),
            PrettyPrinter::Print(filter->arrival_delay(), TUnit::TIME_MS));
      }
    }
  }
}

void RuntimeFilterBank::PublishGlobalFilter(uint32_t filter_id,
    const TBloomFilter& thrift_filter) {
  lock_guard<SpinLock> l(runtime_filter_lock_);
  if (closed_) return;
  RuntimeFilterMap::iterator it = consumed_filters_.find(filter_id);
  DCHECK(it != consumed_filters_.end()) << "Tried to publish unregistered filter: "
                                        << filter_id;
  if (it->second->filter_desc().is_broadcast_join &&
      it->second->GetBloomFilter() != NULL) {
    // Already showed up from local filter.
    return;
  }
  if (!state_->query_mem_tracker()->TryConsume(
          BloomFilter::GetExpectedHeapSpaceUsed(thrift_filter.log_heap_space))) {
    // Silently fail to publish the filter if there's not enough memory for it.
    return;
  }
  BloomFilter* bloom_filter = obj_pool_.Add(new BloomFilter(thrift_filter, NULL, NULL));
  memory_allocated_->Add(bloom_filter->GetHeapSpaceUsed());
  it->second->SetBloomFilter(bloom_filter);
  state_->runtime_profile()->AddInfoString(Substitute("Filter $0 arrival", filter_id),
      PrettyPrinter::Print(it->second->arrival_delay(), TUnit::TIME_MS));
}

BloomFilter* RuntimeFilterBank::AllocateScratchBloomFilter() {
  lock_guard<SpinLock> l(runtime_filter_lock_);
  if (closed_) return NULL;

  // Track required space
  uint32_t required_space = BloomFilter::GetExpectedHeapSpaceUsed(log_filter_size_);
  if (!state_->query_mem_tracker()->TryConsume(required_space)) return NULL;
  BloomFilter* bloom_filter =
      obj_pool_.Add(new BloomFilter(log_filter_size_, NULL, NULL));
  DCHECK_EQ(required_space, bloom_filter->GetHeapSpaceUsed());
  memory_allocated_->Add(bloom_filter->GetHeapSpaceUsed());
  return bloom_filter;
}

bool RuntimeFilterBank::ShouldDisableFilter(uint64_t max_ndv) {
  double fpp = BloomFilter::FalsePositiveProb(max_ndv, log_filter_size_);
  return fpp > FLAGS_max_filter_error_rate;
}

void RuntimeFilterBank::Close() {
  lock_guard<SpinLock> l(runtime_filter_lock_);
  closed_ = true;
  BOOST_FOREACH(RuntimeFilterMap::value_type v, produced_filters_) {
    const BloomFilter* bloom_filter = v.second->GetBloomFilter();
    if (bloom_filter != NULL) {
      state_->query_mem_tracker()->Release(bloom_filter->GetHeapSpaceUsed());
    }
  }

  BOOST_FOREACH(RuntimeFilterMap::value_type v, consumed_filters_) {
    const BloomFilter* bloom_filter = v.second->GetBloomFilter();
    if (bloom_filter != NULL) {
      state_->query_mem_tracker()->Release(bloom_filter->GetHeapSpaceUsed());
    }
  }
  obj_pool_.Clear();
}

bool RuntimeFilter::WaitForArrival(int32_t timeout_ms) const {
  if (GetBloomFilter() != NULL) return true;
  while ((MonotonicMillis() - registration_time_) < timeout_ms) {
    SleepForMs(SLEEP_PERIOD_MS);
    if (GetBloomFilter() != NULL) return true;
  }

  return false;
}
