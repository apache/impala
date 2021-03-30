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

#include "runtime/coordinator-backend-state.h"

#include <boost/lexical_cast.hpp>

#include "common/object-pool.h"
#include "exec/exec-node.h"
#include "exec/kudu-util.h"
#include "exec/scan-node.h"
#include "gen-cpp/data_stream_service.proxy.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_sidecar.h"
#include "kudu/util/monotime.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "rpc/rpc-mgr.inline.h"
#include "runtime/backend-client.h"
#include "runtime/client-cache.h"
#include "runtime/coordinator-filter-state.h"
#include "runtime/debug-options.h"
#include "runtime/exec-env.h"
#include "runtime/fragment-instance-state.h"
#include "runtime/krpc-data-stream-sender.h"
#include "runtime/mem-tracker.h"
#include "service/control-service.h"
#include "service/data-stream-service.h"
#include "util/counting-barrier.h"
#include "util/error-util-internal.h"
#include "util/network-util.h"
#include "util/pretty-printer.h"
#include "util/runtime-profile-counters.h"
#include "util/scope-exit-trigger.h"
#include "util/uid-util.h"

#include "common/names.h"

using kudu::MonoDelta;
using kudu::rpc::RpcController;
using kudu::rpc::RpcSidecar;
using namespace rapidjson;
namespace accumulators = boost::accumulators;

DECLARE_int32(backend_client_rpc_timeout_ms);
DECLARE_int64(rpc_max_message_size);
PROFILE_DECLARE_COUNTER(ScanRangesComplete);

namespace impala {
PROFILE_DECLARE_COUNTER(BytesRead);

const char* Coordinator::BackendState::InstanceStats::LAST_REPORT_TIME_DESC =
    "Last report received time";

Coordinator::BackendState::BackendState(const QuerySchedule& schedule,
    const TQueryCtx& query_ctx, int state_idx, TRuntimeFilterMode::type filter_mode,
    const BackendExecParams& exec_params)
  : schedule_(schedule),
    state_idx_(state_idx),
    filter_mode_(filter_mode),
    backend_exec_params_(&exec_params),
    host_(backend_exec_params_->be_desc.address()),
    krpc_host_(backend_exec_params_->be_desc.krpc_address()),
    query_ctx_(query_ctx),
    query_id_(schedule.query_id()),
    num_remaining_instances_(backend_exec_params_->instance_params.size()) {}

void Coordinator::BackendState::Init(const vector<FragmentStats*>& fragment_stats,
    RuntimeProfile* host_profile_parent, ObjectPool* obj_pool) {
  host_profile_ = RuntimeProfile::Create(obj_pool, NetworkAddressPBToString(host_));
  host_profile_parent->AddChild(host_profile_);
  RuntimeProfile::Counter* admission_slots =
      ADD_COUNTER(host_profile_, "AdmissionSlots", TUnit::UNIT);
  admission_slots->Set(backend_exec_params_->slots_to_use);

  // populate instance_stats_map_ and install instance
  // profiles as child profiles in fragment_stats' profile
  int prev_fragment_idx = -1;
  for (const FInstanceExecParams* instance_params:
       backend_exec_params_->instance_params) {
    DCHECK_EQ(
        FromNetworkAddressPB(host_), instance_params->host); // all hosts must be the same
    int fragment_idx = instance_params->fragment().idx;
    DCHECK_LT(fragment_idx, fragment_stats.size());
    if (prev_fragment_idx != -1 && fragment_idx != prev_fragment_idx) {
      // all instances of a fragment are contiguous
      DCHECK_EQ(fragments_.count(fragment_idx), 0);
      prev_fragment_idx = fragment_idx;
    }
    fragments_.insert(fragment_idx);

    instance_stats_map_.emplace(
        GetInstanceIdx(instance_params->instance_id),
        obj_pool->Add(
          new InstanceStats(*instance_params, fragment_stats[fragment_idx], obj_pool)));
  }
}

void Coordinator::BackendState::SetRpcParams(const DebugOptions& debug_options,
    const FilterRoutingTable& filter_routing_table, ExecQueryFInstancesRequestPB* request,
    TExecPlanFragmentInfo* fragment_info) {
  request->set_coord_state_idx(state_idx_);
  request->set_min_mem_reservation_bytes(backend_exec_params_->min_mem_reservation_bytes);
  request->set_initial_mem_reservation_total_claims(
      backend_exec_params_->initial_mem_reservation_total_claims);
  request->set_per_backend_mem_limit(schedule_.per_backend_mem_limit());

  // set fragment_ctxs and fragment_instance_ctxs
  fragment_info->__isset.fragment_ctxs = true;
  fragment_info->__isset.fragment_instance_ctxs = true;
  fragment_info->fragment_instance_ctxs.resize(
      backend_exec_params_->instance_params.size());
  for (int i = 0; i < backend_exec_params_->instance_params.size(); ++i) {
    TPlanFragmentInstanceCtx& instance_ctx = fragment_info->fragment_instance_ctxs[i];
    const FInstanceExecParams& params = *backend_exec_params_->instance_params[i];
    int fragment_idx = params.fragment_exec_params.fragment.idx;

    // add a TPlanFragmentCtx, if we don't already have it
    if (fragment_info->fragment_ctxs.empty()
        || fragment_info->fragment_ctxs.back().fragment.idx != fragment_idx) {
      fragment_info->fragment_ctxs.emplace_back();
      TPlanFragmentCtx& fragment_ctx = fragment_info->fragment_ctxs.back();
      fragment_ctx.__set_fragment(params.fragment_exec_params.fragment);
      fragment_ctx.__set_destinations(params.fragment_exec_params.destinations);
    }

    instance_ctx.fragment_idx = fragment_idx;
    instance_ctx.fragment_instance_id = params.instance_id;
    instance_ctx.per_fragment_instance_idx = params.per_fragment_instance_idx;
    instance_ctx.__set_per_node_scan_ranges(params.per_node_scan_ranges);
    instance_ctx.__set_per_exch_num_senders(
        params.fragment_exec_params.per_exch_num_senders);
    instance_ctx.__set_sender_id(params.sender_id);
    instance_ctx.__set_join_build_inputs(params.join_build_inputs);
    if (params.num_join_build_outputs != -1) {
      instance_ctx.__set_num_join_build_outputs(params.num_join_build_outputs);
    }
    if (debug_options.enabled()
        && (debug_options.instance_idx() == -1
            || debug_options.instance_idx() == GetInstanceIdx(params.instance_id))) {
      instance_ctx.__set_debug_options(debug_options.ToThrift());
    }

    if (filter_mode_ == TRuntimeFilterMode::OFF) continue;

    int instance_idx = GetInstanceIdx(params.instance_id);
    auto& produced_map = filter_routing_table.finstance_filters_produced;
    auto produced_it = produced_map.find(instance_idx);
    if (produced_it == produced_map.end()) continue;
    // Finstance needs list of source filters that were selected during filter routing
    // table construction.
    instance_ctx.__set_filters_produced(produced_it->second);
  }
}

void Coordinator::BackendState::SetExecError(
    const Status& status, TypedCountingBarrier<Status>* exec_status_barrier) {
  const string ERR_TEMPLATE = "ExecQueryFInstances rpc query_id=$0 failed: $1";
  const string& err_msg =
      Substitute(ERR_TEMPLATE, PrintId(query_id_), status.msg().GetFullMessageDetails());
  LOG(ERROR) << err_msg;
  status_ = Status::Expected(err_msg);
  exec_done_ = true;
  exec_status_barrier->NotifyRemaining(status);
}

void Coordinator::BackendState::WaitOnExecRpc() {
  unique_lock<mutex> l(lock_);
  WaitOnExecLocked(&l);
}

void Coordinator::BackendState::WaitOnExecLocked(unique_lock<mutex>* l) {
  DCHECK(l->owns_lock());
  while (!exec_done_) {
    exec_done_cv_.Wait(*l);
  }
}

void Coordinator::BackendState::ExecCompleteCb(
    TypedCountingBarrier<Status>* exec_status_barrier, int64_t start_ms) {
  {
    lock_guard<mutex> l(lock_);
    exec_rpc_status_ = exec_rpc_controller_.status();
    rpc_latency_ = MonotonicMillis() - start_ms;

    if (!exec_rpc_status_.ok()) {
      SetExecError(
          FromKuduStatus(exec_rpc_status_, "Exec() rpc failed"), exec_status_barrier);
      goto done;
    }

    Status exec_status = Status(exec_response_.status());
    if (!exec_status.ok()) {
      SetExecError(exec_status, exec_status_barrier);
      goto done;
    }

    for (const auto& entry : instance_stats_map_) entry.second->stopwatch_.Start();
    VLOG_FILE << "rpc succeeded: ExecQueryFInstances query_id=" << PrintId(query_id_);
    exec_done_ = true;
    last_report_time_ms_ = GenerateReportTimestamp();
    exec_status_barrier->Notify(Status::OK());
  }
done:
  // Notify after releasing 'lock_' so that we don't wake up a thread just to have it
  // immediately block again.
  exec_done_cv_.NotifyAll();
}

void Coordinator::BackendState::ExecAsync(const DebugOptions& debug_options,
    const FilterRoutingTable& filter_routing_table,
    const kudu::Slice& serialized_query_ctx,
    TypedCountingBarrier<Status>* exec_status_barrier) {
  {
    lock_guard<mutex> l(lock_);
    DCHECK(!exec_done_);
    DCHECK(status_.ok());
    // Do not issue an ExecQueryFInstances RPC if there are no fragment instances
    // scheduled to run on this backend.
    if (IsEmptyBackend()) {
      DCHECK(backend_exec_params_->is_coord_backend);
      exec_done_ = true;
      exec_status_barrier->Notify(Status::OK());
      goto done;
    }

    std::unique_ptr<ControlServiceProxy> proxy;
    Status get_proxy_status = ControlService::GetProxy(
        FromNetworkAddressPB(krpc_host_), host_.hostname(), &proxy);
    if (!get_proxy_status.ok()) {
      SetExecError(get_proxy_status, exec_status_barrier);
      goto done;
    }

    ExecQueryFInstancesRequestPB request;
    TExecPlanFragmentInfo fragment_info;
    SetRpcParams(debug_options, filter_routing_table, &request, &fragment_info);

    exec_rpc_controller_.set_timeout(
        MonoDelta::FromMilliseconds(FLAGS_backend_client_rpc_timeout_ms));

    // Serialize the sidecar and add it to the rpc controller. The serialized buffer is
    // owned by 'serializer' and is freed when it is destructed.
    ThriftSerializer serializer(true);
    uint8_t* serialized_buf = nullptr;
    uint32_t serialized_len = 0;
    Status serialize_status =
        DebugAction(schedule_.query_options(), "EXEC_SERIALIZE_FRAGMENT_INFO");
    if (LIKELY(serialize_status.ok())) {
      serialize_status =
          serializer.SerializeToBuffer(&fragment_info, &serialized_len, &serialized_buf);
    }
    if (UNLIKELY(!serialize_status.ok())) {
      SetExecError(serialize_status, exec_status_barrier);
      goto done;
    } else if (serialized_len > FLAGS_rpc_max_message_size) {
      SetExecError(
          Status::Expected("Serialized Exec() request exceeds --rpc_max_message_size."),
          exec_status_barrier);
      goto done;
    }

    // TODO: eliminate the extra copy here by using a Slice
    unique_ptr<kudu::faststring> sidecar_buf = make_unique<kudu::faststring>();
    sidecar_buf->assign_copy(serialized_buf, serialized_len);
    unique_ptr<RpcSidecar> rpc_sidecar = RpcSidecar::FromFaststring(move(sidecar_buf));

    int sidecar_idx;
    kudu::Status sidecar_status =
        exec_rpc_controller_.AddOutboundSidecar(move(rpc_sidecar), &sidecar_idx);
    if (!sidecar_status.ok()) {
      SetExecError(
          FromKuduStatus(sidecar_status, "Failed to add sidecar"), exec_status_barrier);
      goto done;
    }
    request.set_plan_fragment_info_sidecar_idx(sidecar_idx);

    // Add the serialized TQueryCtx as a sidecar.
    unique_ptr<RpcSidecar> query_ctx_sidecar =
        RpcSidecar::FromSlice(serialized_query_ctx);
    int query_ctx_sidecar_idx;
    kudu::Status query_ctx_sidecar_status = exec_rpc_controller_.AddOutboundSidecar(
        move(query_ctx_sidecar), &query_ctx_sidecar_idx);
    if (!query_ctx_sidecar_status.ok()) {
      SetExecError(
          FromKuduStatus(query_ctx_sidecar_status, "Failed to add TQueryCtx sidecar"),
          exec_status_barrier);
      goto done;
    }
    request.set_query_ctx_sidecar_idx(query_ctx_sidecar_idx);

    VLOG_FILE << "making rpc: ExecQueryFInstances"
              << " host=" << impalad_address() << " query_id=" << PrintId(query_id_);

    proxy->ExecQueryFInstancesAsync(request, &exec_response_, &exec_rpc_controller_,
        std::bind(&Coordinator::BackendState::ExecCompleteCb, this, exec_status_barrier,
            MonotonicMillis()));
    exec_rpc_sent_ = true;
    return;
  }
done:
  // Notify after releasing 'lock_' so that we don't wake up a thread just to have it
  // immediately block again.
  exec_done_cv_.NotifyAll();
}

Status Coordinator::BackendState::GetStatus(bool* is_fragment_failure,
    TUniqueId* failed_instance_id) {
  lock_guard<mutex> l(lock_);
  DCHECK_EQ(is_fragment_failure == nullptr, failed_instance_id == nullptr);
  if (!status_.ok() && failed_instance_id != nullptr) {
    *is_fragment_failure = is_fragment_failure_;
    *failed_instance_id = failed_instance_id_;
  }
  return status_;
}

Coordinator::ResourceUtilization Coordinator::BackendState::ComputeResourceUtilization() {
  lock_guard<mutex> l(lock_);
  DCHECK(exec_done_) << "May only be called after WaitOnExecRpc() completes.";
  return ComputeResourceUtilizationLocked();
}

Coordinator::ResourceUtilization
Coordinator::BackendState::ComputeResourceUtilizationLocked() {
  ResourceUtilization result;
  for (const auto& entry : instance_stats_map_) {
    RuntimeProfile* profile = entry.second->profile_;
    ResourceUtilization instance_utilization;
    // Update resource utilization and apply delta.
    RuntimeProfile::Counter* user_time = profile->GetCounter("TotalThreadsUserTime");
    if (user_time != nullptr) instance_utilization.cpu_user_ns = user_time->value();

    RuntimeProfile::Counter* system_time = profile->GetCounter("TotalThreadsSysTime");
    if (system_time != nullptr) instance_utilization.cpu_sys_ns = system_time->value();

    for (RuntimeProfile::Counter* c : entry.second->bytes_read_counters_) {
      instance_utilization.bytes_read += c->value();
    }

    int64_t bytes_sent = 0;
    for (RuntimeProfile::Counter* c : entry.second->bytes_sent_counters_) {
      bytes_sent += c->value();
    }

    // Determine whether this instance had a scan node in its plan.
    if (instance_utilization.bytes_read > 0) {
      instance_utilization.scan_bytes_sent = bytes_sent;
    } else {
      instance_utilization.exchange_bytes_sent = bytes_sent;
    }

    RuntimeProfile::Counter* peak_mem =
        profile->GetCounter(FragmentInstanceState::PER_HOST_PEAK_MEM_COUNTER);
    if (peak_mem != nullptr)
      instance_utilization.peak_per_host_mem_consumption = peak_mem->value();
    result.Merge(instance_utilization);
  }
  return result;
}

void Coordinator::BackendState::MergeErrorLog(ErrorLogMap* merged) {
  lock_guard<mutex> l(lock_);
  DCHECK(exec_done_) << "May only be called after WaitOnExecRpc() completes.";
  if (error_log_.size() > 0)  MergeErrorMaps(error_log_, merged);
}

bool Coordinator::BackendState::HasFragmentIdx(int fragment_idx) const {
  return fragments_.count(fragment_idx) > 0;
}

bool Coordinator::BackendState::HasFragmentIdx(
    const std::unordered_set<int>& fragment_idxs) const {
  for (int fragment_idx : fragment_idxs) {
    if (HasFragmentIdx(fragment_idx)) return true;
  }
  return false;
}

void Coordinator::BackendState::LogFirstInProgress(
    std::vector<Coordinator::BackendState*> backend_states) {
  for (Coordinator::BackendState* backend_state : backend_states) {
    if (!backend_state->IsDone()) {
      VLOG_QUERY << "query_id=" << PrintId(backend_state->query_id_)
                 << ": first in-progress backend: " << backend_state->impalad_address();
      break;
    }
  }
}

bool Coordinator::BackendState::IsDone() {
  unique_lock<mutex> lock(lock_);
  return IsDoneLocked(lock);
}

inline bool Coordinator::BackendState::IsDoneLocked(
    const unique_lock<std::mutex>& lock) const {
  DCHECK(lock.owns_lock() && lock.mutex() == &lock_);
  return num_remaining_instances_ == 0 || !status_.ok();
}

bool Coordinator::BackendState::ApplyExecStatusReport(
    const ReportExecStatusRequestPB& backend_exec_status,
    const TRuntimeProfileForest& thrift_profiles, ExecSummary* exec_summary,
    ProgressUpdater* scan_range_progress, DmlExecState* dml_exec_state,
    vector<AuxErrorInfoPB>* aux_error_info) {
  DCHECK(!IsEmptyBackend());
  // Hold the exec_summary's lock to avoid exposing it half-way through
  // the update loop below.
  lock_guard<SpinLock> l1(exec_summary->lock);
  unique_lock<mutex> lock(lock_);
  last_report_time_ms_ = GenerateReportTimestamp();

  // If this backend completed previously, don't apply the update. This ensures that
  // the profile doesn't change during or after Coordinator::ComputeQuerySummary(), but
  // can mean that we lose profile information for failed queries, since the Coordinator
  // will call Cancel() on all BackendStates and we may stop accepting reports before some
  // backends send their final report.
  // TODO: revisit ComputeQuerySummary()
  if (IsDoneLocked(lock)) return false;

  // Use empty profile in case profile serialization/deserialization failed.
  // 'thrift_profiles' and 'instance_exec_status' vectors have one-to-one correspondance.
  vector<TRuntimeProfileTree> empty_profiles;
  vector<TRuntimeProfileTree>::const_iterator profile_iter;
  if (UNLIKELY(thrift_profiles.profile_trees.size() == 0)) {
    empty_profiles.resize(backend_exec_status.instance_exec_status().size());
    profile_iter = empty_profiles.begin();
  } else {
    DCHECK_EQ(thrift_profiles.profile_trees.size(),
        backend_exec_status.instance_exec_status().size());
    profile_iter = thrift_profiles.profile_trees.begin();
  }

  for (auto status_iter = backend_exec_status.instance_exec_status().begin();
       status_iter != backend_exec_status.instance_exec_status().end();
       ++status_iter, ++profile_iter) {
    const FragmentInstanceExecStatusPB& instance_exec_status = *status_iter;
    int64_t report_seq_no = instance_exec_status.report_seq_no();
    int instance_idx = GetInstanceIdx(instance_exec_status.fragment_instance_id());
    DCHECK_EQ(instance_stats_map_.count(instance_idx), 1);
    InstanceStats* instance_stats = instance_stats_map_[instance_idx];
    int64_t last_report_seq_no = instance_stats->last_report_seq_no_;
    DCHECK(instance_stats->exec_params_.instance_id ==
        ProtoToQueryId(instance_exec_status.fragment_instance_id()));
    // Ignore duplicate or out-of-order messages.
    if (report_seq_no <= last_report_seq_no) {
      VLOG_QUERY << Substitute("Ignoring stale update for query instance $0 with "
          "seq no $1", PrintId(instance_stats->exec_params_.instance_id), report_seq_no);
      continue;
    }

    DCHECK(!instance_stats->done_);
    instance_stats->Update(instance_exec_status, *profile_iter, exec_summary,
        scan_range_progress);

    // Update DML stats
    if (instance_exec_status.has_dml_exec_status()) {
      dml_exec_state->Update(instance_exec_status.dml_exec_status());
    }

    // Handle the non-idempotent parts of the report for any sequence numbers that we
    // haven't seen yet.
    if (instance_exec_status.stateful_report_size() > 0) {
      for (const auto& stateful_report : instance_exec_status.stateful_report()) {
        DCHECK_LE(stateful_report.report_seq_no(), report_seq_no);
        if (last_report_seq_no < stateful_report.report_seq_no()) {
          // Append the log messages from each update with the global state of the query
          // execution
          MergeErrorMaps(stateful_report.error_log(), &error_log_);
          VLOG_FILE << "host=" << host_
                    << " error log: " << PrintErrorMapToString(error_log_);

          if (stateful_report.has_aux_error_info()) {
            aux_error_info->push_back(stateful_report.aux_error_info());
          }
        }
      }
    }

    DCHECK_GT(num_remaining_instances_, 0);
    if (instance_exec_status.done()) {
      DCHECK(!instance_stats->done_);
      instance_stats->done_ = true;
      --num_remaining_instances_;
    }
  }

  // status_ has incorporated the status from all fragment instances. If the overall
  // backend status is not OK, but no specific fragment instance reported an error, then
  // this is a general backend error. Incorporate the general error into status_.
  Status overall_status(backend_exec_status.overall_status());
  if (!overall_status.ok() && (status_.ok() || status_.IsCancelled())) {
    status_ = overall_status;
    if (backend_exec_status.has_fragment_instance_id()) {
      failed_instance_id_ = ProtoToQueryId(backend_exec_status.fragment_instance_id());
      is_fragment_failure_ = true;
    }
  }

  // TODO: keep backend-wide stopwatch?
  return IsDoneLocked(lock);
}

void Coordinator::BackendState::UpdateHostProfile(
    const TRuntimeProfileTree& thrift_profile) {
  // We do not take 'lock_' here because RuntimeProfile::Update() is thread-safe.
  DCHECK(!IsEmptyBackend());
  host_profile_->Update(thrift_profile);
}

void Coordinator::BackendState::UpdateExecStats(
    const vector<FragmentStats*>& fragment_stats) {
  lock_guard<mutex> l(lock_);
  DCHECK(exec_done_) << "May only be called after WaitOnExecRpc() completes.";
  for (const auto& entry: instance_stats_map_) {
    const InstanceStats& instance_stats = *entry.second;
    int fragment_idx = instance_stats.exec_params_.fragment().idx;
    DCHECK_LT(fragment_idx, fragment_stats.size());
    FragmentStats* f = fragment_stats[fragment_idx];
    int64_t completion_time = instance_stats.stopwatch_.ElapsedTime();
    f->completion_times_(completion_time);
    if (completion_time > 0) {
      f->rates_(instance_stats.total_split_size_
        / (completion_time / 1000.0 / 1000.0 / 1000.0));
    }
    f->avg_profile_->UpdateAverage(instance_stats.profile_);
  }
}

bool Coordinator::BackendState::Cancel() {
  unique_lock<mutex> l(lock_);

  // Nothing to cancel if the exec rpc was not sent.
  if (!exec_rpc_sent_) {
    if (status_.ok()) status_ = Status::CANCELLED;
    VLogForBackend("Not sending Cancel() rpc because nothing was started.");
    exec_done_ = true;
    // Notify after releasing 'lock_' so that we don't wake up a thread just to have it
    // immediately block again.
    l.unlock();
    exec_done_cv_.NotifyAll();
    return false;
  }

  // If the exec rpc was sent but the callback hasn't been executed, try to cancel the rpc
  // and then wait for it to be done.
  if (!exec_done_) {
    VLogForBackend("Attempting to cancel Exec() rpc");
    exec_rpc_controller_.Cancel();
    WaitOnExecLocked(&l);
  }

  // Don't cancel if we're done. Note that its possible the backend is still running, eg.
  // if the rpc layer reported that the Exec() rpc failed but it actually reached the
  // backend. In that case, the backend will cancel itself the first time it tries to send
  // a status report and the coordinator responds with an error.
  if (IsDoneLocked(l)) {
    VLogForBackend(Substitute(
        "Not cancelling because the backend is already done: $0", status_.GetDetail()));
    return false;
  }

  // Set an error status to make sure we only cancel this once.
  if (status_.ok()) status_ = Status::CANCELLED;

  VLogForBackend("Sending CancelQueryFInstances rpc");

  std::unique_ptr<ControlServiceProxy> proxy;
  Status get_proxy_status = ControlService::GetProxy(
      FromNetworkAddressPB(krpc_host_), host_.hostname(), &proxy);
  if (!get_proxy_status.ok()) {
    status_.MergeStatus(get_proxy_status);
    VLogForBackend(Substitute("Could not get proxy: $0", get_proxy_status.msg().msg()));
    return true;
  }

  CancelQueryFInstancesRequestPB request;
  TUniqueIdToUniqueIdPB(query_id_, request.mutable_query_id());
  CancelQueryFInstancesResponsePB response;

  const int num_retries = 3;
  const int64_t timeout_ms = 10 * MILLIS_PER_SEC;
  const int64_t backoff_time_ms = 3 * MILLIS_PER_SEC;
  Status rpc_status =
      RpcMgr::DoRpcWithRetry(proxy, &ControlServiceProxy::CancelQueryFInstances, request,
          &response, query_ctx_, "Cancel() RPC failed", num_retries, timeout_ms,
          backoff_time_ms, "COORD_CANCEL_QUERY_FINSTANCES_RPC");

  if (!rpc_status.ok()) {
    status_.MergeStatus(rpc_status);
    VLogForBackend(
        Substitute("CancelQueryFInstances rpc failed: $0", rpc_status.msg().msg()));
    return true;
  }
  Status cancel_status = Status(response.status());
  if (!cancel_status.ok()) {
    status_.MergeStatus(cancel_status);
    VLogForBackend(
        Substitute("CancelQueryFInstances failed: $0", cancel_status.msg().msg()));
    return true;
  }
  return true;
}

void Coordinator::BackendState::PublishFilter(FilterState* state,
    MemTracker* mem_tracker, const PublishFilterParamsPB& rpc_params,
    RpcController& controller, PublishFilterResultPB& res) {
  DCHECK_EQ(ProtoToQueryId(rpc_params.dst_query_id()), query_id_);
  // If the backend is already done, it's not waiting for this filter, so we skip
  // sending it in this case.
  if (IsDone()) return;
  VLOG(2) << "PublishFilter filter_id=" << rpc_params.filter_id() << " backend=" << host_;
  Status status;

  std::unique_ptr<DataStreamServiceProxy> proxy;
  Status get_proxy_status = DataStreamService::GetProxy(
      FromNetworkAddressPB(krpc_host_), host_.hostname(), &proxy);
  if (!get_proxy_status.ok()) {
    // Failing to send a filter is not a query-wide error - the remote fragment will
    // continue regardless.
    LOG(ERROR) << "Couldn't get proxy: " << get_proxy_status.msg().msg();
    return;
  }

  state->IncrementNumInflightRpcs(1);

  proxy->PublishFilterAsync(rpc_params, &res, &controller,
      boost::bind(&Coordinator::BackendState::PublishFilterCompleteCb, this, &controller,
                                state, mem_tracker));
}

void Coordinator::BackendState::PublishFilterCompleteCb(
    const kudu::rpc::RpcController* rpc_controller, FilterState* state,
    MemTracker* mem_tracker) {
  const kudu::Status controller_status = rpc_controller->status();

  // In the case of an unsuccessful KRPC call, we only log this event w/o retrying.
  // Failing to send a filter is not a query-wide error - the remote fragment will
  // continue regardless.
  if (!controller_status.ok()) {
    LOG(ERROR) << "PublishFilter() failed: " << controller_status.message().ToString();
  }

  {
    lock_guard<SpinLock> l(state->lock());

    state->IncrementNumInflightRpcs(-1);

    if (state->num_inflight_rpcs() == 0) {
      // Since we disabled the filter once complete and held FilterState::lock_ while
      // issuing all PublishFilter() rpcs, at this point there can't be any more
      // PublishFilter() rpcs issued.
      DCHECK(state->disabled());
      if (state->is_bloom_filter() && state->bloom_filter_directory().size() > 0) {
        mem_tracker->Release(state->bloom_filter_directory().size());
        state->bloom_filter_directory().clear();
        state->bloom_filter_directory().shrink_to_fit();
      }
      state->get_publish_filter_done_cv().notify_one();
    }
  }
}

Coordinator::BackendState::InstanceStats::InstanceStats(
    const FInstanceExecParams& exec_params, FragmentStats* fragment_stats,
    ObjectPool* obj_pool)
  : exec_params_(exec_params),
    profile_(nullptr) {
  const string& profile_name = Substitute("Instance $0 (host=$1)",
      PrintId(exec_params.instance_id), TNetworkAddressToString(exec_params.host));
  profile_ = RuntimeProfile::Create(obj_pool, profile_name);
  profile_->AddInfoString(LAST_REPORT_TIME_DESC, ToStringFromUnixMillis(UnixMillis()));
  fragment_stats->root_profile()->AddChild(profile_);

  // add total split size to fragment_stats->bytes_assigned()
  for (const PerNodeScanRanges::value_type& entry: exec_params_.per_node_scan_ranges) {
    for (const TScanRangeParams& scan_range_params: entry.second) {
      if (!scan_range_params.scan_range.__isset.hdfs_file_split) continue;
      total_split_size_ += scan_range_params.scan_range.hdfs_file_split.length;
    }
  }
  (*fragment_stats->bytes_assigned())(total_split_size_);
}

void Coordinator::BackendState::InstanceStats::InitCounters() {
  vector<RuntimeProfile*> children;
  profile_->GetAllChildren(&children);
  for (RuntimeProfile* p : children) {
    RuntimeProfile::Counter* c = p->GetCounter(PROFILE_ScanRangesComplete.name());
    if (c != nullptr) scan_ranges_complete_counters_.push_back(c);

    RuntimeProfile::Counter* bytes_read = p->GetCounter(PROFILE_BytesRead.name());
    if (bytes_read != nullptr) bytes_read_counters_.push_back(bytes_read);

    RuntimeProfile::Counter* bytes_sent =
        p->GetCounter(KrpcDataStreamSender::TOTAL_BYTES_SENT_COUNTER);
    if (bytes_sent != nullptr) bytes_sent_counters_.push_back(bytes_sent);
  }
}

void Coordinator::BackendState::InstanceStats::Update(
    const FragmentInstanceExecStatusPB& exec_status,
    const TRuntimeProfileTree& thrift_profile, ExecSummary* exec_summary,
    ProgressUpdater* scan_range_progress) {
  last_report_time_ms_ = UnixMillis();
  DCHECK_GT(exec_status.report_seq_no(), last_report_seq_no_);
  last_report_seq_no_ = exec_status.report_seq_no();
  if (exec_status.done()) stopwatch_.Stop();
  profile_->UpdateInfoString(LAST_REPORT_TIME_DESC,
      ToStringFromUnixMillis(last_report_time_ms_));
  profile_->Update(thrift_profile);
  if (!profile_created_) {
    profile_created_ = true;
    InitCounters();
  }
  profile_->ComputeTimeInProfile();

  // update exec_summary
  // TODO: why do this every time we get an updated instance profile?
  vector<RuntimeProfile*> children;
  profile_->GetAllChildren(&children);
  TExecSummary& thrift_exec_summary = exec_summary->thrift_exec_summary;
  for (RuntimeProfile* child : children) {
    bool is_plan_node = child->metadata().__isset.plan_node_id;
    bool is_data_sink = child->metadata().__isset.data_sink_id;
    // Plan Nodes and data sinks get an entry in the summary.
    if (!is_plan_node && !is_data_sink) continue;

    int exec_summary_idx;
    if (is_plan_node) {
      exec_summary_idx = exec_summary->node_id_to_idx_map[child->metadata().plan_node_id];
    } else {
      exec_summary_idx =
          exec_summary->data_sink_id_to_idx_map[child->metadata().data_sink_id];
    }
    TPlanNodeExecSummary& node_exec_summary = thrift_exec_summary.nodes[exec_summary_idx];
    DCHECK_EQ(node_exec_summary.fragment_idx, exec_params_.fragment().idx);
    int per_fragment_instance_idx = exec_params_.per_fragment_instance_idx;
    DCHECK_LT(per_fragment_instance_idx, node_exec_summary.exec_stats.size())
        << " name=" << child->name()
        << " instance_id=" << PrintId(exec_params_.instance_id)
        << " fragment_idx=" << exec_params_.fragment().idx;
    TExecStats& instance_stats = node_exec_summary.exec_stats[per_fragment_instance_idx];

    RuntimeProfile::Counter* rows_counter = child->GetCounter("RowsReturned");
    RuntimeProfile::Counter* mem_counter = child->GetCounter("PeakMemoryUsage");
    if (rows_counter != nullptr) instance_stats.__set_cardinality(rows_counter->value());
    if (mem_counter != nullptr) instance_stats.__set_memory_used(mem_counter->value());
    instance_stats.__set_latency_ns(child->local_time());
    // TODO: track interesting per-node metrics
    node_exec_summary.__isset.exec_stats = true;
  }

  // determine newly-completed scan ranges and update scan_range_progress
  int64_t total = 0;
  for (RuntimeProfile::Counter* c: scan_ranges_complete_counters_) total += c->value();
  int64_t delta = total - total_ranges_complete_;
  total_ranges_complete_ = total;
  scan_range_progress->Update(delta);

  // extract the current execution state of this instance
  current_state_ = exec_status.current_state();
}

void Coordinator::BackendState::InstanceStats::ToJson(Value* value, Document* document) {
  Value instance_id_val(PrintId(exec_params_.instance_id).c_str(),
      document->GetAllocator());
  value->AddMember("instance_id", instance_id_val, document->GetAllocator());

  // We send 'done' explicitly so we don't have to infer it by comparison with a string
  // constant in the debug page JS code.
  value->AddMember("done", done_, document->GetAllocator());

  Value state_val(FragmentInstanceState::ExecStateToString(current_state_).c_str(),
      document->GetAllocator());
  value->AddMember("current_state", state_val, document->GetAllocator());

  Value fragment_name_val(exec_params_.fragment().display_name.c_str(),
      document->GetAllocator());
  value->AddMember("fragment_name", fragment_name_val, document->GetAllocator());

  value->AddMember("first_status_update_received", last_report_time_ms_ > 0,
      document->GetAllocator());
  int64_t elapsed_time_ms =
      std::max(static_cast<int64_t>(0), UnixMillis() - last_report_time_ms_);
  value->AddMember("time_since_last_heard_from", elapsed_time_ms,
      document->GetAllocator());
}

Coordinator::FragmentStats::FragmentStats(const string& avg_profile_name,
    const string& root_profile_name, int num_instances, ObjectPool* obj_pool)
  : avg_profile_(RuntimeProfile::Create(obj_pool, avg_profile_name, true)),
    root_profile_(RuntimeProfile::Create(obj_pool, root_profile_name)),
    num_instances_(num_instances) {
}

void Coordinator::FragmentStats::AddSplitStats() {
  double min = accumulators::min(bytes_assigned_);
  double max = accumulators::max(bytes_assigned_);
  double mean = accumulators::mean(bytes_assigned_);
  double stddev = sqrt(accumulators::variance(bytes_assigned_));
  stringstream ss;
  ss << " min: " << PrettyPrinter::Print(min, TUnit::BYTES)
    << ", max: " << PrettyPrinter::Print(max, TUnit::BYTES)
    << ", avg: " << PrettyPrinter::Print(mean, TUnit::BYTES)
    << ", stddev: " << PrettyPrinter::Print(stddev, TUnit::BYTES);
  avg_profile_->AddInfoString("split sizes", ss.str());
}

void Coordinator::FragmentStats::AddExecStats() {
  root_profile_->SortChildrenByTotalTime();
  stringstream times_label;
  times_label
    << "min:" << PrettyPrinter::Print(
        accumulators::min(completion_times_), TUnit::TIME_NS)
    << "  max:" << PrettyPrinter::Print(
        accumulators::max(completion_times_), TUnit::TIME_NS)
    << "  mean: " << PrettyPrinter::Print(
        accumulators::mean(completion_times_), TUnit::TIME_NS)
    << "  stddev:" << PrettyPrinter::Print(
        sqrt(accumulators::variance(completion_times_)), TUnit::TIME_NS);

  stringstream rates_label;
  rates_label
    << "min:" << PrettyPrinter::Print(
        accumulators::min(rates_), TUnit::BYTES_PER_SECOND)
    << "  max:" << PrettyPrinter::Print(
        accumulators::max(rates_), TUnit::BYTES_PER_SECOND)
    << "  mean:" << PrettyPrinter::Print(
        accumulators::mean(rates_), TUnit::BYTES_PER_SECOND)
    << "  stddev:" << PrettyPrinter::Print(
        sqrt(accumulators::variance(rates_)), TUnit::BYTES_PER_SECOND);

  // why plural?
  avg_profile_->AddInfoString("completion times", times_label.str());
  // why plural?
  avg_profile_->AddInfoString("execution rates", rates_label.str());
  avg_profile_->AddInfoString("num instances", lexical_cast<string>(num_instances_));
}

void Coordinator::BackendState::ToJson(Value* value, Document* document) {
  unique_lock<mutex> l(lock_);
  DCHECK(exec_done_) << "May only be called after WaitOnExecRpc() completes.";
  ResourceUtilization resource_utilization = ComputeResourceUtilizationLocked();
  value->AddMember("num_instances", fragments_.size(), document->GetAllocator());
  value->AddMember("done", IsDoneLocked(l), document->GetAllocator());
  value->AddMember("peak_per_host_mem_consumption",
      resource_utilization.peak_per_host_mem_consumption, document->GetAllocator());
  value->AddMember("bytes_read", resource_utilization.bytes_read,
      document->GetAllocator());
  value->AddMember("cpu_user_s", resource_utilization.cpu_user_ns / 1e9,
      document->GetAllocator());
  value->AddMember("cpu_sys_s", resource_utilization.cpu_sys_ns / 1e9,
      document->GetAllocator());

  string host = NetworkAddressPBToString(impalad_address());
  Value val(host.c_str(), document->GetAllocator());
  value->AddMember("host", val, document->GetAllocator());

  value->AddMember("rpc_latency", rpc_latency(), document->GetAllocator());
  value->AddMember("time_since_last_heard_from", MonotonicMillis() - last_report_time_ms_,
      document->GetAllocator());

  string status_str = status_.ok() ? "OK" : status_.GetDetail();
  Value status_val(status_str.c_str(), document->GetAllocator());
  value->AddMember("status", status_val, document->GetAllocator());

  value->AddMember(
      "num_remaining_instances", num_remaining_instances_, document->GetAllocator());
}

void Coordinator::BackendState::InstanceStatsToJson(Value* value, Document* document) {
  Value instance_stats(kArrayType);
  {
    lock_guard<mutex> l(lock_);
    DCHECK(exec_done_) << "May only be called after WaitOnExecRpc() completes.";
    for (const auto& elem : instance_stats_map_) {
      Value val(kObjectType);
      elem.second->ToJson(&val, document);
      instance_stats.PushBack(val, document->GetAllocator());
    }
  }
  value->AddMember("instance_stats", instance_stats, document->GetAllocator());

  // impalad_address is not protected by lock_. The lifetime of the backend state is
  // protected by Coordinator::lock_.
  Value val(
      NetworkAddressPBToString(impalad_address()).c_str(), document->GetAllocator());
  value->AddMember("host", val, document->GetAllocator());
}

void Coordinator::BackendState::VLogForBackend(const string& msg) {
  VLOG_QUERY << "query_id=" << PrintId(query_id_) << " target backend=" << krpc_host_
             << ": " << msg;
}

} // namespace impala
