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

#include <chrono>

#include <boost/algorithm/string/join.hpp>

#include "gen-cpp/ImpalaInternalService_types.h"
#include "gen-cpp/data_stream_service.proxy.h"
#include "gutil/strings/substitute.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_sidecar.h"
#include "runtime/backend-client.h"
#include "runtime/bufferpool/reservation-tracker.h"
#include "runtime/client-cache.h"
#include "runtime/exec-env.h"
#include "runtime/initial-reservations.h"
#include "runtime/mem-tracker.h"
#include "runtime/query-state.h"
#include "runtime/runtime-filter.inline.h"
#include "runtime/runtime-state.h"
#include "service/data-stream-service.h"
#include "service/impala-server.h"
#include "util/bit-util.h"
#include "util/bloom-filter.h"
#include "util/min-max-filter.h"
#include "util/pretty-printer.h"

#include "common/names.h"

using kudu::rpc::RpcContext;
using kudu::rpc::RpcController;
using kudu::rpc::RpcSidecar;
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
    closed_(false),
    total_bloom_filter_mem_required_(total_filter_mem_required) {
  bloom_memory_allocated_ =
      state->runtime_profile()->AddCounter("BloomFilterBytes", TUnit::BYTES);
}

Status RuntimeFilterBank::ClaimBufferReservation() {
  DCHECK(!buffer_pool_client_.is_registered());
  string filter_bank_name = Substitute(
      "RuntimeFilterBank (Fragment Id: $0)", PrintId(state_->fragment_instance_id()));
  RETURN_IF_ERROR(ExecEnv::GetInstance()->buffer_pool()->RegisterClient(filter_bank_name,
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
      // The filter bank may have already been cancelled. In that case, still allocate the
      // filter but cancel it immediately, so that callers of RuntimeFilterBank don't need
      // to have separate handling of that case.
      if (cancelled_) ret->Cancel();
      consumed_filters_[filter_desc.filter_id] = ret;
      VLOG(2) << "registered consumer filter " << filter_desc.filter_id;
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

void RuntimeFilterBank::UpdateFilterCompleteCb(
    const RpcController* rpc_controller, const UpdateFilterResultPB* res) {
  const kudu::Status controller_status = rpc_controller->status();

  // In the case of an unsuccessful KRPC call, e.g., request dropped due to
  // backpressure, we only log this event w/o retrying. Failing to send a
  // filter is not a query-wide error - the remote fragment will continue
  // regardless.
  if (!controller_status.ok()) {
    LOG(ERROR) << "UpdateFilter() failed: " << controller_status.message().ToString();
  }
  // DataStreamService::UpdateFilter() should never set an error status
  DCHECK_EQ(res->status().status_code(), TErrorCode::OK);

  {
    std::unique_lock<SpinLock> l(num_inflight_rpcs_lock_);
    DCHECK_GT(num_inflight_rpcs_, 0);
    --num_inflight_rpcs_;
  }
  krpcs_done_cv_.notify_one();
}

void RuntimeFilterBank::UpdateFilterFromLocal(
    int32_t filter_id, BloomFilter* bloom_filter, MinMaxFilter* min_max_filter) {
  DCHECK_NE(state_->query_options().runtime_filter_mode, TRuntimeFilterMode::OFF)
      << "Should not be calling UpdateFilterFromLocal() if filtering is disabled";
  // This function is only called from ExecNode::Open() or more specifically
  // PartitionedHashJoinNode::Open().
  DCHECK(!closed_);
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
    state_->runtime_profile()->AddInfoString(Substitute("Filter $0 arrival", filter_id),
        PrettyPrinter::Print(filter->arrival_delay_ms(), TUnit::TIME_MS));
  }

  if (has_remote_target
      && state_->query_options().runtime_filter_mode == TRuntimeFilterMode::GLOBAL) {
    UpdateFilterParamsPB params;
    // The memory associated with the following 2 objects needs to live until
    // the asynchronous KRPC call proxy->UpdateFilterAsync() is completed.
    // Hence, we allocate these 2 objects in 'obj_pool_'.
    UpdateFilterResultPB* res = obj_pool_.Add(new UpdateFilterResultPB);
    RpcController* controller = obj_pool_.Add(new RpcController);

    TUniqueIdToUniqueIdPB(state_->query_id(), params.mutable_query_id());
    params.set_filter_id(filter_id);
    if (type == TRuntimeFilterType::BLOOM) {
      BloomFilter::ToProtobuf(bloom_filter, controller, params.mutable_bloom_filter());
    } else {
      DCHECK_EQ(type, TRuntimeFilterType::MIN_MAX);
      min_max_filter->ToProtobuf(params.mutable_min_max_filter());
    }
    const TNetworkAddress& krpc_address = state_->query_ctx().coord_krpc_address;
    const TNetworkAddress& host_address = state_->query_ctx().coord_address;

    // Use 'proxy' to send the filter to the coordinator.
    unique_ptr<DataStreamServiceProxy> proxy;
    Status get_proxy_status =
        DataStreamService::GetProxy(krpc_address, host_address.hostname, &proxy);
    if (!get_proxy_status.ok()) {
      // Failing to send a filter is not a query-wide error - the remote fragment will
      // continue regardless.
      LOG(INFO) << Substitute("Failed to get proxy to coordinator $0: $1",
          host_address.hostname, get_proxy_status.msg().msg());
      return;
    }

    // Increment 'num_inflight_rpcs_' to make sure that the filter will not be deallocated
    // in Close() until all in-flight RPCs complete.
    {
      unique_lock<SpinLock> l(num_inflight_rpcs_lock_);
      DCHECK_GE(num_inflight_rpcs_, 0);
      ++num_inflight_rpcs_;
    }

    proxy->UpdateFilterAsync(params, res, controller,
        boost::bind(&RuntimeFilterBank::UpdateFilterCompleteCb, this, controller, res));
  }
}

void RuntimeFilterBank::PublishGlobalFilter(
    const PublishFilterParamsPB& params, RpcContext* context) {
  lock_guard<mutex> l(runtime_filter_lock_);
  if (closed_) return;
  RuntimeFilterMap::iterator it = consumed_filters_.find(params.filter_id());
  DCHECK(it != consumed_filters_.end()) << "Tried to publish unregistered filter: "
                                        << params.filter_id();

  BloomFilter* bloom_filter = nullptr;
  MinMaxFilter* min_max_filter = nullptr;
  if (it->second->is_bloom_filter()) {
    DCHECK(params.has_bloom_filter());
    if (params.bloom_filter().always_true()) {
      bloom_filter = BloomFilter::ALWAYS_TRUE_FILTER;
    } else {
      int64_t required_space = BloomFilter::GetExpectedMemoryUsed(
          params.bloom_filter().log_bufferpool_space());
      DCHECK_GE(buffer_pool_client_.GetUnusedReservation(), required_space)
          << "BufferPool Client should have enough reservation to fulfill bloom filter "
             "allocation";
      bloom_filter = obj_pool_.Add(new BloomFilter(&buffer_pool_client_));

      kudu::Slice sidecar_slice;
      if (params.bloom_filter().has_directory_sidecar_idx()) {
        kudu::Status status = context->GetInboundSidecar(
            params.bloom_filter().directory_sidecar_idx(), &sidecar_slice);
        if (!status.ok()) {
          LOG(ERROR) << "Failed to get Bloom filter sidecar: "
                     << status.message().ToString();
          bloom_filter = BloomFilter::ALWAYS_TRUE_FILTER;
        }
      } else {
        DCHECK(params.bloom_filter().always_false());
      }

      if (bloom_filter != BloomFilter::ALWAYS_TRUE_FILTER) {
        Status status = bloom_filter->Init(
            params.bloom_filter(), sidecar_slice.data(), sidecar_slice.size());
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
    }
  } else {
    DCHECK(it->second->is_min_max_filter());
    DCHECK(params.has_min_max_filter());
    min_max_filter = MinMaxFilter::Create(params.min_max_filter(), it->second->type(),
        &obj_pool_, filter_mem_tracker_.get());
    min_max_filters_.push_back(min_max_filter);
  }
  it->second->SetFilter(bloom_filter, min_max_filter);
  state_->runtime_profile()->AddInfoString(
      Substitute("Filter $0 arrival", params.filter_id()),
      PrettyPrinter::Print(it->second->arrival_delay_ms(), TUnit::TIME_MS));
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

  MinMaxFilter* min_max_filter =
      MinMaxFilter::Create(type, &obj_pool_, filter_mem_tracker_.get());
  min_max_filters_.push_back(min_max_filter);
  return min_max_filter;
}

bool RuntimeFilterBank::FpRateTooHigh(int64_t filter_size, int64_t observed_ndv) {
  double fpp =
      BloomFilter::FalsePositiveProb(observed_ndv, BitUtil::Log2Ceiling64(filter_size));
  return fpp > FLAGS_max_filter_error_rate;
}

void RuntimeFilterBank::Cancel() {
  lock_guard<mutex> l(runtime_filter_lock_);
  CancelLocked();
}

void RuntimeFilterBank::CancelLocked() {
  if (cancelled_) return;
  // Cancel all filters that a thread might be waiting on.
  for (auto& entry : consumed_filters_) entry.second->Cancel();
  cancelled_ = true;
}

void RuntimeFilterBank::Close() {
  // Wait for all in-flight RPCs to complete before closing the filters.
  {
    unique_lock<SpinLock> l1(num_inflight_rpcs_lock_);
    while (num_inflight_rpcs_ > 0) {
      krpcs_done_cv_.wait(l1);
    }
  }

  lock_guard<mutex> l2(runtime_filter_lock_);
  CancelLocked();
  // We do not have to set 'closed_' to true before waiting for all in-flight RPCs to
  // drain because the async build thread in
  // BlockingJoinNode::ProcessBuildInputAndOpenProbe() should have exited by the time
  // Close() is called so there shouldn't be any new RPCs being issued when this function
  // is called.
  closed_ = true;
  for (BloomFilter* filter : bloom_filters_) filter->Close();
  for (MinMaxFilter* filter : min_max_filters_) filter->Close();
  obj_pool_.Clear();
  if (buffer_pool_client_.is_registered()) {
    VLOG_FILE << "RuntimeFilterBank (Fragment Id: "
              << PrintId(state_->fragment_instance_id())
              << ") returning reservation " << total_bloom_filter_mem_required_;
    state_->query_state()->initial_reservations()->Return(
        &buffer_pool_client_, total_bloom_filter_mem_required_);
    ExecEnv::GetInstance()->buffer_pool()->DeregisterClient(&buffer_pool_client_);
  }
  DCHECK_EQ(filter_mem_tracker_->consumption(), 0);
  filter_mem_tracker_->Close();
}
