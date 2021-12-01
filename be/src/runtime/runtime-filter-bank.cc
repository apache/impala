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
#include "util/debug-util.h"
#include "util/min-max-filter.h"
#include "util/pretty-printer.h"
#include "util/uid-util.h"

#include "common/names.h"

using kudu::rpc::RpcContext;
using kudu::rpc::RpcController;
using kudu::rpc::RpcSidecar;
using namespace impala;
using namespace strings;

DEFINE_double(max_filter_error_rate, 0.75, "(Advanced) The target false positive "
    "probability used to determine the ideal size for each bloom filter size. This value "
    "can be overriden by the RUNTIME_FILTER_ERROR_RATE query option.");

const int64_t RuntimeFilterBank::MIN_BLOOM_FILTER_SIZE;
const int64_t RuntimeFilterBank::MAX_BLOOM_FILTER_SIZE;

RuntimeFilterBank::RuntimeFilterBank(QueryState* query_state,
    const unordered_map<int32_t, FilterRegistration>& filters,
    long total_filter_mem_required)
  : filters_(BuildFilterMap(filters, &obj_pool_)),
    query_state_(query_state),
    filter_mem_tracker_(query_state->obj_pool()->Add(new MemTracker(
        -1, "Runtime Filter Bank", query_state->query_mem_tracker(), false))),
    bloom_memory_allocated_(
        query_state->host_profile()->AddCounter("BloomFilterBytes", TUnit::BYTES)),
    total_in_list_filter_items_(
        query_state->host_profile()->AddCounter("InListFilterItems", TUnit::UNIT)),
    total_bloom_filter_mem_required_(total_filter_mem_required) {}

RuntimeFilterBank::~RuntimeFilterBank() {}

unordered_map<int32_t, unique_ptr<RuntimeFilterBank::PerFilterState>>
RuntimeFilterBank::BuildFilterMap(
    const unordered_map<int32_t, FilterRegistration>& filters, ObjectPool* obj_pool) {
  unordered_map<int32_t, unique_ptr<PerFilterState>> result;
  for (auto& entry : filters) {
    const FilterRegistration reg = entry.second;
    RuntimeFilter* result_filter = nullptr;
    RuntimeFilter* consumed_filter = nullptr;
    if (reg.has_consumer) {
      VLOG(3) << "registered consumer filter " << reg.desc.filter_id;
      consumed_filter =
          obj_pool->Add(new RuntimeFilter(reg.desc, reg.desc.filter_size_bytes));
    }
    if (reg.num_producers > 0) {
      result_filter =
          obj_pool->Add(new RuntimeFilter(reg.desc, reg.desc.filter_size_bytes));
    }
    result.emplace(entry.first,
        make_unique<PerFilterState>(reg.num_producers, result_filter, consumed_filter));
  }
  return result;
}

Status RuntimeFilterBank::ClaimBufferReservation() {
  DCHECK(!buffer_pool_client_.is_registered());
  string filter_bank_name =
      Substitute("RuntimeFilterBank (Query Id: $0)", PrintId(query_state_->query_id()));
  RETURN_IF_ERROR(ExecEnv::GetInstance()->buffer_pool()->RegisterClient(filter_bank_name,
      query_state_->file_group(), query_state_->buffer_reservation(),
      filter_mem_tracker_, total_bloom_filter_mem_required_,
      query_state_->host_profile(), &buffer_pool_client_));
  VLOG_FILE << filter_bank_name << " claiming reservation "
            << total_bloom_filter_mem_required_;
  query_state_->initial_reservations()->Claim(
      &buffer_pool_client_, total_bloom_filter_mem_required_);
  return Status::OK();
}

RuntimeFilter* RuntimeFilterBank::RegisterProducer(
    const TRuntimeFilterDesc& filter_desc) {
  auto it = filters_.find(filter_desc.filter_id);
  DCHECK(it != filters_.end()) << "Filter ID " << filter_desc.filter_id
                               << " not registered";
  PerFilterState* fs = it->second.get();
  DCHECK(fs->produced_filter.result_filter != nullptr);
  RuntimeFilter* ret = fs->produced_filter.result_filter;
  DCHECK_EQ(filter_desc.filter_size_bytes, ret->filter_size());
  return ret;
}

RuntimeFilter* RuntimeFilterBank::RegisterConsumer(
    const TRuntimeFilterDesc& filter_desc) {
  auto it = filters_.find(filter_desc.filter_id);
  DCHECK(it != filters_.end()) << "Filter ID " << filter_desc.filter_id
                               << " not registered";
  PerFilterState* fs = it->second.get();
  DCHECK(fs->consumed_filter != nullptr)
      << "Consumed filters must be created in constructor";
  VLOG(3) << "Consumer registered for filter " << filter_desc.filter_id;
  DCHECK_EQ(filter_desc.filter_size_bytes, fs->consumed_filter->filter_size());
  return fs->consumed_filter;
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
    int32_t filter_id, BloomFilter* bloom_filter, MinMaxFilter* min_max_filter,
    InListFilter* in_list_filter) {
  DCHECK_NE(query_state_->query_options().runtime_filter_mode, TRuntimeFilterMode::OFF)
      << "Should not be calling UpdateFilterFromLocal() if filtering is disabled";
  // This function is only called from ExecNode::Open() or more specifically
  // PartitionedHashJoinNode::Open().
  DCHECK(!closed_);
  // A runtime filter may have both local and remote targets.
  bool has_local_target = false;
  bool has_remote_target = false;
  RuntimeFilter* complete_filter = nullptr; // Set if the filter should be sent out.
  auto it = filters_.find(filter_id);
  DCHECK(it != filters_.end()) << "Tried to update unregistered filter: " << filter_id;
  PerFilterState* fs = it->second.get();
  {
    unique_lock<SpinLock> l(fs->lock);
    ProducedFilter& produced_filter = fs->produced_filter;
    RuntimeFilter* result_filter = produced_filter.result_filter;
    DCHECK(result_filter != nullptr)
        << "Tried to update unregistered filter: " << filter_id;
    DCHECK_GT(produced_filter.pending_producers, 0);
    if (result_filter->filter_desc().is_broadcast_join) {
      // For broadcast joins, the first filter to arrived is used, and the rest are
      // ignored (because they should have identical contents).
      if (result_filter->HasFilter()) {
        // Don't need to merge, the previous broadcast filter contained all values.
        VLOG(3) << "Dropping redundant broadcast filter " << filter_id;
        return;
      }
      VLOG(3) << "Setting broadcast filter " << filter_id;
      result_filter->SetFilter(bloom_filter, min_max_filter, in_list_filter);
      complete_filter = result_filter;
    } else {
      DCHECK(in_list_filter == nullptr)
          << "InListFilter should only be generated for broadcast joins";
      // Merge partitioned join filters in parallel - each thread setting the filter will
      // try to merge its filter with a previously merged filter, looping until either
      // it has produced the final filter or it runs out of other filters to merge.
      unique_ptr<RuntimeFilter> tmp_filter = make_unique<RuntimeFilter>(
          result_filter->filter_desc(), result_filter->filter_size());
      tmp_filter->SetFilter(bloom_filter, min_max_filter, nullptr);
      while (produced_filter.pending_merge_filter != nullptr) {
        unique_ptr<RuntimeFilter> pending_merge =
            std::move(produced_filter.pending_merge_filter);
        // Drop the lock while doing the merge so that other merges can proceed in
        // parallel.
        l.unlock();
        VLOG(3) << "Merging partitioned join filter " << filter_id;
        tmp_filter->Or(pending_merge.get());
        l.lock();
      }
      // At this point, either we've merged all the filters or we're waiting for more
      // filters.
      if (produced_filter.pending_producers > 1) {
        // A subsequent caller of UpdateFilterFromLocal() is responsible for merging this
        // filter into the final one.
        produced_filter.pending_merge_filter = std::move(tmp_filter);
      } else {
        // Everything was merged into 'tmp_filter'. It is therefore the result filter.
        result_filter->SetFilter(tmp_filter.get());
        complete_filter = result_filter;
        VLOG(3) << "Partitioned join filter " << filter_id << " is locally complete.";
      }
    }
    int remaining_producers = --produced_filter.pending_producers;
    VLOG(3) << "Filter " << filter_id << " updated. " << remaining_producers
            << " producers left on the backend.";
    DCHECK(remaining_producers > 0 || result_filter != nullptr);
    has_local_target = result_filter->filter_desc().has_local_targets;
    has_remote_target = result_filter->filter_desc().has_remote_targets;
  }

  if (complete_filter != nullptr && has_local_target) {
    // Do a short circuit publication by pushing the same filter to the consumer side.
    RuntimeFilter* consumed_filter;
    {
      lock_guard<SpinLock> l(fs->lock);
      if (fs->consumed_filter == nullptr) return;
      consumed_filter = fs->consumed_filter;
      // Update the filter while still holding the lock to avoid racing with the
      // SetFilter() call in PublishGlobalFilter().
      if (consumed_filter->HasFilter()) {
        // Multiple instances may produce the same filter for broadcast joins.
        // TODO: we would ideally update the coordinator logic to avoid creating duplicates
        // on the same node, but sending out a few duplicate filters is relatively
        // inconsequential for performance.
        DCHECK(consumed_filter->filter_desc().is_broadcast_join)
            << consumed_filter->filter_desc();
      } else {
        consumed_filter->SetFilter(complete_filter);
        string into_key;
        if (in_list_filter != nullptr) {
          into_key = Substitute("Filter $0 arrival with $1 items",
              filter_id, in_list_filter->NumItems());
        } else {
          into_key = Substitute("Filter $0 arrival", filter_id);
        }
        query_state_->host_profile()->AddInfoString(into_key,
            PrettyPrinter::Print(consumed_filter->arrival_delay_ms(), TUnit::TIME_MS));
      }
    }
  }

  if (complete_filter != nullptr && has_remote_target &&
      query_state_->query_options().runtime_filter_mode == TRuntimeFilterMode::GLOBAL) {
    UpdateFilterParamsPB params;
    // The memory associated with the following 2 objects needs to live until
    // the asynchronous KRPC call proxy->UpdateFilterAsync() is completed.
    // Hence, we allocate these 2 objects in 'obj_pool_'.
    UpdateFilterResultPB* res = obj_pool_.Add(new UpdateFilterResultPB);
    RpcController* controller = obj_pool_.Add(new RpcController);

    TUniqueIdToUniqueIdPB(query_state_->query_id(), params.mutable_query_id());
    params.set_filter_id(filter_id);
    TRuntimeFilterType::type type = complete_filter->filter_desc().type;
    if (type == TRuntimeFilterType::BLOOM) {
      BloomFilter::ToProtobuf(bloom_filter, controller, params.mutable_bloom_filter());
    } else if (type == TRuntimeFilterType::MIN_MAX) {
      min_max_filter->ToProtobuf(params.mutable_min_max_filter());
    } else {
      DCHECK_EQ(type, TRuntimeFilterType::IN_LIST);
      InListFilter::ToProtobuf(in_list_filter, params.mutable_in_list_filter());
    }
    const TNetworkAddress& krpc_address = query_state_->query_ctx().coord_ip_address;
    const std::string& hostname = query_state_->query_ctx().coord_hostname;

    // Use 'proxy' to send the filter to the coordinator.
    unique_ptr<DataStreamServiceProxy> proxy;
    Status get_proxy_status = DataStreamService::GetProxy(krpc_address, hostname, &proxy);
    if (!get_proxy_status.ok()) {
      // Failing to send a filter is not a query-wide error - the remote fragment will
      // continue regardless.
      LOG(INFO) << Substitute("Failed to get proxy to coordinator $0: $1", hostname,
          get_proxy_status.msg().msg());
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
  VLOG(3) << "PublishGlobalFilter(filter_id=" << params.filter_id() << ")";
  auto it = filters_.find(params.filter_id());
  DCHECK(it != filters_.end()) << "Filter ID " << params.filter_id() << " not registered";
  PerFilterState* fs = it->second.get();
  lock_guard<SpinLock> l(fs->lock);
  if (closed_) return;
  if (fs->consumed_filter->HasFilter()) {
    // The filter routing in the Coordinator sometimes can redundantly send broadcast
    // filters that were already produced on this backend and consumed locally.
    // It is safe to drop the filter because we already have a filter with the same
    // contents.
    DCHECK(fs->consumed_filter->filter_desc().is_broadcast_join)
        << "Got duplicate partitioned join filter";
    return;
  }
  BloomFilter* bloom_filter = nullptr;
  MinMaxFilter* min_max_filter = nullptr;
  InListFilter* in_list_filter = nullptr;
  string details;
  if (fs->consumed_filter->is_bloom_filter()) {
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
        Status status = bloom_filter->Init(params.bloom_filter(), sidecar_slice.data(),
            sidecar_slice.size(), DefaultHashSeed());
        if (!status.ok()) {
          LOG(ERROR) << "Unable to allocate memory for bloom filter: "
                     << status.GetDetail();
          bloom_filter = BloomFilter::ALWAYS_TRUE_FILTER;
        } else {
          fs->bloom_filters.push_back(bloom_filter);
          DCHECK_EQ(required_space, bloom_filter->GetBufferPoolSpaceUsed());
          bloom_memory_allocated_->Add(bloom_filter->GetBufferPoolSpaceUsed());
        }
      }
    }
  } else if (fs->consumed_filter->is_min_max_filter()) {
    DCHECK(params.has_min_max_filter());
    min_max_filter = MinMaxFilter::Create(params.min_max_filter(),
        fs->consumed_filter->type(), &obj_pool_, filter_mem_tracker_);
    fs->min_max_filters.push_back(min_max_filter);
  } else {
    DCHECK(fs->consumed_filter->is_in_list_filter());
    DCHECK(params.has_in_list_filter());
    DCHECK(query_state_->query_options().__isset.runtime_in_list_filter_entry_limit);
    int entry_limit = query_state_->query_options().runtime_in_list_filter_entry_limit;
    in_list_filter = InListFilter::Create(params.in_list_filter(),
        fs->consumed_filter->type(), entry_limit, &obj_pool_);
    fs->in_list_filters.push_back(in_list_filter);
    total_in_list_filter_items_->Add(params.in_list_filter().value_size());
    details = Substitute(" with $0 items", params.in_list_filter().value_size());
  }
  fs->consumed_filter->SetFilter(bloom_filter, min_max_filter, in_list_filter);
  query_state_->host_profile()->AddInfoString(
      Substitute("Filter $0 arrival$1", params.filter_id(), details),
      PrettyPrinter::Print(fs->consumed_filter->arrival_delay_ms(), TUnit::TIME_MS));
}

BloomFilter* RuntimeFilterBank::AllocateScratchBloomFilter(int32_t filter_id) {
  auto it = filters_.find(filter_id);
  DCHECK(it != filters_.end()) << "Filter ID " << filter_id << " not registered";
  PerFilterState* fs = it->second.get();
  lock_guard<SpinLock> l(fs->lock);
  if (closed_) return nullptr;

  // Track required space
  int64_t log_filter_size =
      BitUtil::Log2Ceiling64(fs->produced_filter.result_filter->filter_size());
  int64_t required_space = BloomFilter::GetExpectedMemoryUsed(log_filter_size);
  DCHECK_GE(buffer_pool_client_.GetUnusedReservation(), required_space)
      << "BufferPool Client should have enough reservation to fulfill bloom filter "
         "allocation";
  BloomFilter* bloom_filter = obj_pool_.Add(new BloomFilter(&buffer_pool_client_));
  Status status = bloom_filter->Init(log_filter_size, DefaultHashSeed());
  if (!status.ok()) {
    LOG(ERROR) << "Unable to allocate memory for bloom filter: " << status.GetDetail();
    return nullptr;
  }
  fs->bloom_filters.push_back(bloom_filter);
  DCHECK_EQ(required_space, bloom_filter->GetBufferPoolSpaceUsed());
  bloom_memory_allocated_->Add(bloom_filter->GetBufferPoolSpaceUsed());
  return bloom_filter;
}

MinMaxFilter* RuntimeFilterBank::AllocateScratchMinMaxFilter(
    int32_t filter_id, ColumnType type) {
  auto it = filters_.find(filter_id);
  DCHECK(it != filters_.end()) << "Filter ID " << filter_id << " not registered";
  PerFilterState* fs = it->second.get();
  lock_guard<SpinLock> l(fs->lock);
  if (closed_) return nullptr;

  MinMaxFilter* min_max_filter =
      MinMaxFilter::Create(type, &obj_pool_, filter_mem_tracker_);
  fs->min_max_filters.push_back(min_max_filter);
  return min_max_filter;
}

InListFilter* RuntimeFilterBank::AllocateScratchInListFilter(
    int32_t filter_id, ColumnType type) {
  auto it = filters_.find(filter_id);
  DCHECK(it != filters_.end()) << "Filter ID " << filter_id << " not registered";
  PerFilterState* fs = it->second.get();
  lock_guard<SpinLock> l(fs->lock);
  if (closed_) return nullptr;

  DCHECK(query_state_->query_options().__isset.runtime_in_list_filter_entry_limit);
  int32_t entry_limit = query_state_->query_options().runtime_in_list_filter_entry_limit;
  InListFilter* in_list_filter =
      InListFilter::Create(type, entry_limit, &obj_pool_);
  fs->in_list_filters.push_back(in_list_filter);
  return in_list_filter;
}

vector<unique_lock<SpinLock>> RuntimeFilterBank::LockAllFilters() {
  vector<unique_lock<SpinLock>> locks;
  for (auto& entry : filters_) locks.emplace_back(entry.second->lock);
  return locks;
}

void RuntimeFilterBank::Cancel() {
  auto all_locks = LockAllFilters();
  CancelLocked();
}

void RuntimeFilterBank::CancelLocked() {
  // IMPALA-9730: if no filters are present, we did not acquire any filter locks. Avoid a
  // TSAN data race on 'cancelled_' by exiting early.
  if (filters_.empty() || cancelled_) return;
  // Cancel all filters that a thread might be waiting on.
  for (auto& entry : filters_) {
    if (entry.second->consumed_filter != nullptr) entry.second->consumed_filter->Cancel();
  }
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
  auto all_locks = LockAllFilters();
  CancelLocked();
  // We do not have to set 'closed_' to true before waiting for all in-flight RPCs to
  // drain because the async build thread in
  // BlockingJoinNode::ProcessBuildInputAndOpenProbe() should have exited by the time
  // Close() is called so there shouldn't be any new RPCs being issued when this function
  // is called.
  if (closed_) return;
  closed_ = true;
  for (auto& entry : filters_) {
    for (BloomFilter* filter : entry.second->bloom_filters) filter->Close();
    for (MinMaxFilter* filter : entry.second->min_max_filters) filter->Close();
    for (InListFilter* filter : entry.second->in_list_filters) filter->Close();
  }
  obj_pool_.Clear();
  if (buffer_pool_client_.is_registered()) {
    VLOG_FILE << "RuntimeFilterBank (Query Id: " << PrintId(query_state_->query_id())
              << ") returning reservation " << total_bloom_filter_mem_required_;
    query_state_->initial_reservations()->Return(
        &buffer_pool_client_, total_bloom_filter_mem_required_);
    ExecEnv::GetInstance()->buffer_pool()->DeregisterClient(&buffer_pool_client_);
  }
  DCHECK_EQ(filter_mem_tracker_->consumption(), 0);
  filter_mem_tracker_->Close();
}

RuntimeFilterBank::ProducedFilter::ProducedFilter(
    int pending_producers, RuntimeFilter* result_filter)
  : result_filter(result_filter), pending_producers(pending_producers) {}

RuntimeFilterBank::PerFilterState::PerFilterState(
    int pending_producers, RuntimeFilter* result_filter, RuntimeFilter* consumed_filter)
  : produced_filter(pending_producers, result_filter), consumed_filter(consumed_filter) {}
