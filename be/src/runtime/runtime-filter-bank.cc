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
#include "runtime/bufferpool/reservation-tracker.h"
#include "runtime/initial-reservations.h"
#include "runtime/mem-tracker.h"
#include "runtime/query-state.h"
#include "runtime/runtime-filter.inline.h"
#include "service/impala-server.h"
#include "util/bit-util.h"
#include "util/bloom-filter.h"
#include "util/min-max-filter.h"

#include "common/names.h"

using namespace impala;
using namespace boost;
using namespace strings;

DEFINE_double(max_filter_error_rate, 0.75, "(Advanced) The maximum probability of false "
    "positives in a runtime filter before it is disabled.");

const int64_t RuntimeFilterBank::MIN_BLOOM_FILTER_SIZE;
const int64_t RuntimeFilterBank::MAX_BLOOM_FILTER_SIZE;

RuntimeFilterBank::RuntimeFilterBank(const TQueryCtx& query_ctx, RuntimeState* state,
    long total_filter_mem_required)
  : state_(state),
    filter_mem_tracker_(
        new MemTracker(-1, "Runtime Filter Bank", state->instance_mem_tracker(), false)),
    mem_pool_(filter_mem_tracker_.get()),
    closed_(false),
    total_bloom_filter_mem_required_(total_filter_mem_required) {
  bloom_memory_allocated_ =
      state->runtime_profile()->AddCounter("BloomFilterBytes", TUnit::BYTES);
}

Status RuntimeFilterBank::ClaimBufferReservation() {
  DCHECK(!buffer_pool_client_.is_registered());
  string filter_bank_name = Substitute(
      "RuntimeFilterBank (Fragment Id: $0)", PrintId(state_->fragment_instance_id()));
  RETURN_IF_ERROR(state_->exec_env()->buffer_pool()->RegisterClient(filter_bank_name,
      state_->query_state()->file_group(), state_->instance_buffer_reservation(),
      filter_mem_tracker_.get(), total_bloom_filter_mem_required_,
      state_->runtime_profile(), &buffer_pool_client_));
  VLOG_FILE << filter_bank_name << " claiming reservation "
            << total_bloom_filter_mem_required_;
  state_->query_state()->initial_reservations()->Claim(
      &buffer_pool_client_, total_bloom_filter_mem_required_);
  return Status::OK();
}

RuntimeFilter* RuntimeFilterBank::RegisterFilter(const TRuntimeFilterDesc& filter_desc,
    bool is_producer) {
  RuntimeFilter* ret = nullptr;
  lock_guard<mutex> l(runtime_filter_lock_);
  if (is_producer) {
    DCHECK(produced_filters_.find(filter_desc.filter_id) == produced_filters_.end());
    ret = obj_pool_.Add(new RuntimeFilter(filter_desc, filter_desc.filter_size_bytes));
    produced_filters_[filter_desc.filter_id] = ret;
  } else {
    if (consumed_filters_.find(filter_desc.filter_id) == consumed_filters_.end()) {
      ret = obj_pool_.Add(new RuntimeFilter(filter_desc, filter_desc.filter_size_bytes));
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

void RuntimeFilterBank::UpdateFilterFromLocal(
    int32_t filter_id, BloomFilter* bloom_filter, MinMaxFilter* min_max_filter) {
  DCHECK_NE(state_->query_options().runtime_filter_mode, TRuntimeFilterMode::OFF)
      << "Should not be calling UpdateFilterFromLocal() if filtering is disabled";
  TUpdateFilterParams params;
  // A runtime filter may have both local and remote targets.
  bool has_local_target = false;
  bool has_remote_target = false;
  TRuntimeFilterType::type type;
  {
    lock_guard<mutex> l(runtime_filter_lock_);
    RuntimeFilterMap::iterator it = produced_filters_.find(filter_id);
    DCHECK(it != produced_filters_.end()) << "Tried to update unregistered filter: "
                                          << filter_id;
    it->second->SetFilter(bloom_filter, min_max_filter);
    has_local_target = it->second->filter_desc().has_local_targets;
    has_remote_target = it->second->filter_desc().has_remote_targets;
    type = it->second->filter_desc().type;
  }

  if (has_local_target) {
    // Do a short circuit publication by pushing the same filter to the consumer side.
    RuntimeFilter* filter;
    {
      lock_guard<mutex> l(runtime_filter_lock_);
      RuntimeFilterMap::iterator it = consumed_filters_.find(filter_id);
      if (it == consumed_filters_.end()) return;
      filter = it->second;
    }
    filter->SetFilter(bloom_filter, min_max_filter);
    state_->runtime_profile()->AddInfoString(
        Substitute("Filter $0 arrival", filter_id),
        PrettyPrinter::Print(filter->arrival_delay(), TUnit::TIME_MS));
  }

  if (has_remote_target
      && state_->query_options().runtime_filter_mode == TRuntimeFilterMode::GLOBAL) {
    params.__set_filter_id(filter_id);
    params.__set_query_id(state_->query_id());
    if (type == TRuntimeFilterType::BLOOM) {
      BloomFilter::ToThrift(bloom_filter, &params.bloom_filter);
      params.__isset.bloom_filter = true;
    } else {
      DCHECK(type == TRuntimeFilterType::MIN_MAX);
      min_max_filter->ToThrift(&params.min_max_filter);
      params.__isset.min_max_filter = true;
    }

    ExecEnv::GetInstance()->rpc_pool()->Offer(bind<void>(
        SendFilterToCoordinator, state_->query_ctx().coord_address, params,
        ExecEnv::GetInstance()->impalad_client_cache()));
  }
}

void RuntimeFilterBank::PublishGlobalFilter(const TPublishFilterParams& params) {
  lock_guard<mutex> l(runtime_filter_lock_);
  if (closed_) return;
  RuntimeFilterMap::iterator it = consumed_filters_.find(params.filter_id);
  DCHECK(it != consumed_filters_.end()) << "Tried to publish unregistered filter: "
                                        << params.filter_id;

  BloomFilter* bloom_filter = nullptr;
  MinMaxFilter* min_max_filter = nullptr;
  if (it->second->is_bloom_filter()) {
    DCHECK(params.__isset.bloom_filter);
    if (params.bloom_filter.always_true) {
      bloom_filter = BloomFilter::ALWAYS_TRUE_FILTER;
    } else {
      int64_t required_space =
          BloomFilter::GetExpectedMemoryUsed(params.bloom_filter.log_bufferpool_space);
      DCHECK_GE(buffer_pool_client_.GetUnusedReservation(), required_space)
          << "BufferPool Client should have enough reservation to fulfill bloom filter "
             "allocation";
      bloom_filter = obj_pool_.Add(new BloomFilter(&buffer_pool_client_));
      Status status = bloom_filter->Init(params.bloom_filter);
      if (!status.ok()) {
        LOG(ERROR) << "Unable to allocate memory for bloom filter: "
                   << status.GetDetail();
        bloom_filter = BloomFilter::ALWAYS_TRUE_FILTER;
      } else {
        bloom_filters_.push_back(bloom_filter);
        DCHECK_EQ(required_space, bloom_filter->GetBufferPoolSpaceUsed());
        bloom_memory_allocated_->Add(bloom_filter->GetBufferPoolSpaceUsed());
      }
    }
  } else {
    DCHECK(it->second->is_min_max_filter());
    DCHECK(params.__isset.min_max_filter);
    min_max_filter = MinMaxFilter::Create(
        params.min_max_filter, it->second->type(), &obj_pool_, &mem_pool_);
  }
  it->second->SetFilter(bloom_filter, min_max_filter);
  state_->runtime_profile()->AddInfoString(
      Substitute("Filter $0 arrival", params.filter_id),
      PrettyPrinter::Print(it->second->arrival_delay(), TUnit::TIME_MS));
}

BloomFilter* RuntimeFilterBank::AllocateScratchBloomFilter(int32_t filter_id) {
  lock_guard<mutex> l(runtime_filter_lock_);
  if (closed_) return nullptr;

  RuntimeFilterMap::iterator it = produced_filters_.find(filter_id);
  DCHECK(it != produced_filters_.end()) << "Filter ID " << filter_id << " not registered";

  // Track required space
  int64_t log_filter_size = BitUtil::Log2Ceiling64(it->second->filter_size());
  int64_t required_space = BloomFilter::GetExpectedMemoryUsed(log_filter_size);
  DCHECK_GE(buffer_pool_client_.GetUnusedReservation(), required_space)
      << "BufferPool Client should have enough reservation to fulfill bloom filter "
         "allocation";
  BloomFilter* bloom_filter = obj_pool_.Add(new BloomFilter(&buffer_pool_client_));
  Status status = bloom_filter->Init(log_filter_size);
  if (!status.ok()) {
    LOG(ERROR) << "Unable to allocate memory for bloom filter: " << status.GetDetail();
    return nullptr;
  }
  bloom_filters_.push_back(bloom_filter);
  DCHECK_EQ(required_space, bloom_filter->GetBufferPoolSpaceUsed());
  bloom_memory_allocated_->Add(bloom_filter->GetBufferPoolSpaceUsed());
  return bloom_filter;
}

MinMaxFilter* RuntimeFilterBank::AllocateScratchMinMaxFilter(
    int32_t filter_id, ColumnType type) {
  lock_guard<mutex> l(runtime_filter_lock_);
  if (closed_) return nullptr;

  RuntimeFilterMap::iterator it = produced_filters_.find(filter_id);
  DCHECK(it != produced_filters_.end()) << "Filter ID " << filter_id << " not registered";

  return MinMaxFilter::Create(type, &obj_pool_, &mem_pool_);
}

bool RuntimeFilterBank::FpRateTooHigh(int64_t filter_size, int64_t observed_ndv) {
  double fpp =
      BloomFilter::FalsePositiveProb(observed_ndv, BitUtil::Log2Ceiling64(filter_size));
  return fpp > FLAGS_max_filter_error_rate;
}

void RuntimeFilterBank::Close() {
  lock_guard<mutex> l(runtime_filter_lock_);
  closed_ = true;
  for (BloomFilter* filter : bloom_filters_) filter->Close();
  obj_pool_.Clear();
  mem_pool_.FreeAll();
  if (buffer_pool_client_.is_registered()) {
    VLOG_FILE << "RuntimeFilterBank (Fragment Id: " << state_->fragment_instance_id()
              << ") returning reservation " << total_bloom_filter_mem_required_;
    state_->query_state()->initial_reservations()->Return(
        &buffer_pool_client_, total_bloom_filter_mem_required_);
    state_->exec_env()->buffer_pool()->DeregisterClient(&buffer_pool_client_);
  }
  DCHECK_EQ(filter_mem_tracker_->consumption(), 0);
  filter_mem_tracker_->Close();
}
