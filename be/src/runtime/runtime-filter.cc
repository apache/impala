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

#include "runtime/runtime-filter.inline.h"

#include "common/names.h"
#include "gutil/bits.h"
#include "gutil/strings/substitute.h"
#include "runtime/client-cache.h"
#include "runtime/exec-env.h"
#include "runtime/backend-client.h"
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
      state->runtime_profile()->AddCounter("BloomFilterBytes", TUnit::BYTES);

  // Clamp bloom filter size down to the limits {MIN,MAX}_BLOOM_FILTER_SIZE
  int32_t bloom_filter_size = query_ctx_.request.query_options.runtime_bloom_filter_size;
  bloom_filter_size = std::max(bloom_filter_size, MIN_BLOOM_FILTER_SIZE);
  bloom_filter_size = std::min(bloom_filter_size, MAX_BLOOM_FILTER_SIZE);
  default_log_filter_size_ = Bits::Log2Ceiling64(bloom_filter_size);
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
    DCHECK(consumed_filters_.find(filter_desc.filter_id) == consumed_filters_.end());
    consumed_filters_[filter_desc.filter_id] = ret;
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
  bool has_local_target = false;
  {
    lock_guard<mutex> l(runtime_filter_lock_);
    RuntimeFilterMap::iterator it = produced_filters_.find(filter_id);
    DCHECK(it != produced_filters_.end()) << "Tried to update unregistered filter: "
                                          << filter_id;
    it->second->SetBloomFilter(bloom_filter);
    has_local_target = it->second->filter_desc().has_local_target;
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
      // Check if the filter already showed up.
      DCHECK(!filter->HasBloomFilter());
    }
    filter->SetBloomFilter(bloom_filter);
    state_->runtime_profile()->AddInfoString(
        Substitute("Filter $0 arrival", filter_id),
        PrettyPrinter::Print(filter->arrival_delay(), TUnit::TIME_MS));
  } else if (state_->query_options().runtime_filter_mode == TRuntimeFilterMode::GLOBAL) {
    BloomFilter::ToThrift(bloom_filter, &params.bloom_filter);
    params.filter_id = filter_id;
    params.query_id = query_ctx_.query_id;

    ExecEnv::GetInstance()->rpc_pool()->Offer(bind<void>(
        SendFilterToCoordinator, query_ctx_.coord_address, params,
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
    if (!state_->query_mem_tracker()->TryConsume(required_space)) {
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
  int64_t log_filter_size = Bits::Log2Ceiling64(it->second->filter_size());
  int64_t required_space = BloomFilter::GetExpectedHeapSpaceUsed(log_filter_size);
  if (!state_->query_mem_tracker()->TryConsume(required_space)) return NULL;
  BloomFilter* bloom_filter = obj_pool_.Add(new BloomFilter(log_filter_size));
  DCHECK_EQ(required_space, bloom_filter->GetHeapSpaceUsed());
  memory_allocated_->Add(bloom_filter->GetHeapSpaceUsed());
  return bloom_filter;
}

int64_t RuntimeFilterBank::GetFilterSizeForNdv(int64_t ndv) {
  if (ndv == -1) return 1LL << default_log_filter_size_;
  int64_t required_space =
      1LL << BloomFilter::MinLogSpace(ndv, FLAGS_max_filter_error_rate);
  if (required_space > MAX_BLOOM_FILTER_SIZE) required_space = MAX_BLOOM_FILTER_SIZE;
  if (required_space < MIN_BLOOM_FILTER_SIZE) required_space = MIN_BLOOM_FILTER_SIZE;
  return required_space;
}

bool RuntimeFilterBank::FpRateTooHigh(int64_t filter_size, int64_t observed_ndv) {
  double fpp =
      BloomFilter::FalsePositiveProb(observed_ndv, Bits::Log2Ceiling64(filter_size));
  return fpp > FLAGS_max_filter_error_rate;
}

void RuntimeFilterBank::Close() {
  lock_guard<mutex> l(runtime_filter_lock_);
  closed_ = true;
  obj_pool_.Clear();
  state_->query_mem_tracker()->Release(memory_allocated_->value());
}

bool RuntimeFilter::WaitForArrival(int32_t timeout_ms) const {
  do {
    if (HasBloomFilter()) return true;
    SleepForMs(SLEEP_PERIOD_MS);
  } while ((MonotonicMillis() - registration_time_) < timeout_ms);

  return HasBloomFilter();
}
