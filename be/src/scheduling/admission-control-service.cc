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

#include "scheduling/admission-control-service.h"

#include "common/constant-strings.h"
#include "gen-cpp/admission_control_service.pb.h"
#include "gutil/strings/substitute.h"
#include "kudu/rpc/rpc_context.h"
#include "rpc/rpc-mgr.h"
#include "rpc/rpc-mgr.inline.h"
#include "rpc/sidecar-util.h"
#include "rpc/thrift-util.h"
#include "runtime/exec-env.h"
#include "runtime/mem-tracker.h"
#include "scheduling/admission-controller.h"
#include "scheduling/admissiond-env.h"
#include "util/cpu-info.h"
#include "util/kudu-status-util.h"
#include "util/memory-metrics.h"
#include "util/parse-util.h"
#include "util/promise.h"

#include "common/names.h"

using kudu::rpc::RpcContext;

static const string QUEUE_LIMIT_MSG = "(Advanced) Limit on RPC payloads consumption for "
                                      "AdmissionControlService. "
    + Substitute(MEM_UNITS_HELP_MSG, "the process memory limit");
DEFINE_string(admission_control_service_queue_mem_limit, "50MB", QUEUE_LIMIT_MSG.c_str());
DEFINE_int32(admission_control_service_num_svc_threads, 0,
    "Number of threads for processing admission control service's RPCs. if left at "
    "default value 0, it will be set to number of CPU cores. Set it to a positive value "
    "to change from the default.");
DEFINE_int32(admission_thread_pool_size, 5,
    "(Advanced) Size of the thread-pool processing AdmitQuery requests.");
DEFINE_int32(max_admission_queue_size, 50,
    "(Advanced) Max size of the queue for the AdmitQuery thread pool.");

DEFINE_string(admission_service_host, "",
    "If provided, queries submitted to this impalad will be scheduled and admitted by "
    "contacting the admission control service at the specified address and "
    "--admission_service_port.");
DEFINE_int32(admission_status_wait_time_ms, 100,
    "(Advanced) The number of milliseconds the GetQueryStatus() rpc in the admission "
    "control service will wait for admission to complete before returning.");

namespace impala {

#define RESPOND_IF_ERROR(stmt)                          \
  do {                                                  \
    const Status& _status = (stmt);                     \
    if (UNLIKELY(!_status.ok())) {                      \
      RespondAndReleaseRpc(_status, resp, rpc_context); \
      return;                                           \
    }                                                   \
  } while (false)

AdmissionControlService::AdmissionControlService(MetricGroup* metric_group)
  : AdmissionControlServiceIf(AdmissiondEnv::GetInstance()->rpc_mgr()->metric_entity(),
        AdmissiondEnv::GetInstance()->rpc_mgr()->result_tracker()) {
  MemTracker* process_mem_tracker = AdmissiondEnv::GetInstance()->process_mem_tracker();
  bool is_percent; // not used
  int64_t bytes_limit =
      ParseUtil::ParseMemSpec(FLAGS_admission_control_service_queue_mem_limit,
          &is_percent, process_mem_tracker->limit());
  if (bytes_limit <= 0) {
    CLEAN_EXIT_WITH_ERROR(
        Substitute("Invalid mem limit for admission control service queue: "
                   "'$0'.",
            FLAGS_admission_control_service_queue_mem_limit));
  }
  mem_tracker_.reset(new MemTracker(
      bytes_limit, "Admission Control Service Queue", process_mem_tracker));
  MemTrackerMetric::CreateMetrics(
      metric_group, mem_tracker_.get(), "AdmissionControlService");
}

Status AdmissionControlService::Init() {
  int num_svc_threads = FLAGS_admission_control_service_num_svc_threads > 0 ?
      FLAGS_admission_control_service_num_svc_threads :
      CpuInfo::num_cores();
  // The maximum queue length is set to maximum 32-bit value. Its actual capacity is
  // bound by memory consumption against 'mem_tracker_'.
  RETURN_IF_ERROR(AdmissiondEnv::GetInstance()->rpc_mgr()->RegisterService(
      num_svc_threads, std::numeric_limits<int32_t>::max(), this, mem_tracker_.get(),
      AdmissiondEnv::GetInstance()->rpc_metrics()));

  admission_thread_pool_.reset(
      new ThreadPool<UniqueIdPB>("admission-control-service", "admission-worker",
          FLAGS_admission_thread_pool_size, FLAGS_max_admission_queue_size,
          bind<void>(&AdmissionControlService::AdmitFromThreadPool, this, _2)));
  ABORT_IF_ERROR(admission_thread_pool_->Init());

  return Status::OK();
}

void AdmissionControlService::Join() {
  admission_thread_pool_->Join();
}

Status AdmissionControlService::GetProxy(
    unique_ptr<AdmissionControlServiceProxy>* proxy) {
  NetworkAddressPB admission_service_address;
  RETURN_IF_ERROR(ExecEnv::GetInstance()->GetAdmissionServiceAddress(
      admission_service_address));
  // Create a AdmissionControlService proxy to the destination.
  RETURN_IF_ERROR(ExecEnv::GetInstance()->rpc_mgr()->GetProxy(
      admission_service_address, FLAGS_admission_service_host,
      proxy));
  return Status::OK();
}

void AdmissionControlService::AdmitQuery(
    const AdmitQueryRequestPB* req, AdmitQueryResponsePB* resp, RpcContext* rpc_context) {
  VLOG(1) << "AdmitQuery: query_id=" << req->query_id()
          << " coordinator=" << req->coord_id();

  shared_ptr<AdmissionState> admission_state;
  admission_state = make_shared<AdmissionState>(req->query_id(), req->coord_id());

  admission_state->summary_profile =
      RuntimeProfile::Create(&admission_state->profile_pool, "Summary");

  RESPOND_IF_ERROR(GetSidecar(req->query_exec_request_sidecar_idx(), rpc_context,
      &admission_state->query_exec_request));

  for (const NetworkAddressPB& address : req->blacklisted_executor_addresses()) {
    admission_state->blacklisted_executor_addresses.emplace(address);
  }

  Status add_status = admission_state_map_.Add(req->query_id(), admission_state);
  if (add_status.ok()) {
    admission_thread_pool_->Offer(req->query_id());
  } else {
    LOG(INFO) << "Query " << req->query_id()
              << " was already submitted for admission, ignoring.";
  }
  RespondAndReleaseRpc(Status::OK(), resp, rpc_context);
}

void AdmissionControlService::GetQueryStatus(const GetQueryStatusRequestPB* req,
    GetQueryStatusResponsePB* resp, kudu::rpc::RpcContext* rpc_context) {
  VLOG(2) << "GetQueryStatus " << req->query_id();

  shared_ptr<AdmissionState> admission_state;
  RESPOND_IF_ERROR(admission_state_map_.Get(req->query_id(), &admission_state));

  Status status = Status::OK();
  {
    lock_guard<mutex> l(admission_state->lock);
    if (admission_state->submitted) {
      if (!admission_state->admission_done) {
        bool timed_out;
        int64_t wait_start_time_ms, wait_end_time_ms;
        admission_state->admit_status =
            AdmissiondEnv::GetInstance()->admission_controller()->WaitOnQueued(
                req->query_id(), &admission_state->schedule,
                FLAGS_admission_status_wait_time_ms, &timed_out,
                &wait_start_time_ms, &wait_end_time_ms);
        resp->set_wait_start_time_ms(wait_start_time_ms);
        resp->set_wait_end_time_ms(wait_end_time_ms);
        if (!timed_out) {
          admission_state->admission_done = true;
          if (admission_state->admit_status.ok()) {
            for (const auto& entry : admission_state->schedule->backend_exec_params()) {
              admission_state->unreleased_backends.emplace(entry.address());
            }
          }
        } else {
          DCHECK(admission_state->admit_status.ok());
        }
      }

      if (admission_state->admission_done) {
        if (admission_state->admit_status.ok()) {
          *resp->mutable_query_schedule() = *admission_state->schedule.get();
        } else {
          status = admission_state->admit_status;
        }
      }

      // Always send the profile even if admission isn't done yet.
      TRuntimeProfileTree tree;
      admission_state->summary_profile->ToThrift(&tree);
      int sidecar_idx;
      Status sidecar_status = SetFaststringSidecar(tree, rpc_context, &sidecar_idx);
      if (!sidecar_status.ok()) {
        // We don't need to fail the query just because we can't return the profile, so
        // just log the error.
        LOG(WARNING) << "Failed to set profile sidecar in GetQueryStatus: "
                     << sidecar_status;
      } else {
        resp->set_summary_profile_sidecar_idx(sidecar_idx);
      }
    }
  }

  RespondAndReleaseRpc(status, resp, rpc_context);
}

void AdmissionControlService::ReleaseQuery(const ReleaseQueryRequestPB* req,
    ReleaseQueryResponsePB* resp, RpcContext* rpc_context) {
  VLOG(1) << "ReleaseQuery: query_id=" << req->query_id();
  shared_ptr<AdmissionState> admission_state;
  RESPOND_IF_ERROR(admission_state_map_.Get(req->query_id(), &admission_state));

  {
    lock_guard<mutex> l(admission_state->lock);
    if (!admission_state->released) {
      AdmissiondEnv::GetInstance()->admission_controller()->ReleaseQuery(req->query_id(),
          admission_state->coord_id, req->peak_mem_consumption(),
          /* release_remaining_backends */ true);
      admission_state->released = true;
    } else {
      LOG(WARNING) << "Query " << req->query_id() << " was already released.";
    }
  }

  RESPOND_IF_ERROR(admission_state_map_.Delete(req->query_id()));
  RespondAndReleaseRpc(Status::OK(), resp, rpc_context);
}

void AdmissionControlService::ReleaseQueryBackends(
    const ReleaseQueryBackendsRequestPB* req, ReleaseQueryBackendsResponsePB* resp,
    RpcContext* rpc_context) {
  VLOG(2) << "ReleaseQueryBackends: query_id=" << req->query_id();
  shared_ptr<AdmissionState> admission_state;
  RESPOND_IF_ERROR(admission_state_map_.Get(req->query_id(), &admission_state));

  {
    lock_guard<mutex> l(admission_state->lock);
    vector<NetworkAddressPB> host_addrs;
    for (const NetworkAddressPB& host_addr : req->host_addr()) {
      auto it = admission_state->unreleased_backends.find(host_addr);
      if (it == admission_state->unreleased_backends.end()) {
        string err = Substitute("Backend $0 was already released for $1",
            NetworkAddressPBToString(host_addr), PrintId(req->query_id()));
        LOG(WARNING) << err;
        RespondAndReleaseRpc(Status(err), resp, rpc_context);
        return;
      }
      host_addrs.push_back(host_addr);
      admission_state->unreleased_backends.erase(it);
    }

    AdmissiondEnv::GetInstance()->admission_controller()->ReleaseQueryBackends(
        req->query_id(), admission_state->coord_id, host_addrs);
  }

  RespondAndReleaseRpc(Status::OK(), resp, rpc_context);
}

void AdmissionControlService::CancelAdmission(const CancelAdmissionRequestPB* req,
    CancelAdmissionResponsePB* resp, kudu::rpc::RpcContext* rpc_context) {
  VLOG(1) << "CancelAdmission: query_id=" << req->query_id();
  shared_ptr<AdmissionState> admission_state;
  RESPOND_IF_ERROR(admission_state_map_.Get(req->query_id(), &admission_state));
  admission_state->admit_outcome.Set(AdmissionOutcome::CANCELLED);
  RespondAndReleaseRpc(Status::OK(), resp, rpc_context);
}

void AdmissionControlService::AdmissionHeartbeat(const AdmissionHeartbeatRequestPB* req,
    AdmissionHeartbeatResponsePB* resp, kudu::rpc::RpcContext* rpc_context) {
  VLOG(2) << "AdmissionHeartbeat: host_id=" << req->host_id();

  if(!CheckAndUpdateHeartbeat(req->host_id(), req->version())) {
    VLOG(1) << "Stale heartbeat received for coord_id: "<< req->host_id();
    RespondAndReleaseRpc(Status::OK(), resp, rpc_context);
    return;
  }
  std::unordered_set<UniqueIdPB> query_ids;
  for (const UniqueIdPB& query_id : req->query_ids()) {
    query_ids.insert(query_id);
  }
  vector<UniqueIdPB> cleaned_up =
      AdmissiondEnv::GetInstance()->admission_controller()->CleanupQueriesForHost(
          req->host_id(), query_ids);

  for (const UniqueIdPB& query_id : cleaned_up) {
    // ShardedQueryMap::Delete will log an error already if anything goes wrong, so just
    // ignore the return value.
    discard_result(admission_state_map_.Delete(query_id));
  }

  RespondAndReleaseRpc(Status::OK(), resp, rpc_context);
}

void AdmissionControlService::CancelQueriesOnFailedCoordinators(
    std::unordered_set<UniqueIdPB> current_backends) {
  std::unordered_map<UniqueIdPB, vector<UniqueIdPB>> cleaned_up =
      AdmissiondEnv::GetInstance()
          ->admission_controller()
          ->CancelQueriesOnFailedCoordinators(current_backends);

  for (const auto& entry : cleaned_up) {
    for (const UniqueIdPB& query_id : entry.second) {
      // ShardedQueryMap::Delete will log an error already if anything goes wrong, so just
      // ignore the return value.
      discard_result(admission_state_map_.Delete(query_id));
    }
  }
}

void AdmissionControlService::AdmitFromThreadPool(UniqueIdPB query_id) {
  shared_ptr<AdmissionState> admission_state;
  Status s = admission_state_map_.Get(query_id, &admission_state);
  if (!s.ok()) {
    LOG(ERROR) << s;
    return;
  }

  {
    lock_guard<mutex> l(admission_state->lock);
    bool queued;
    AdmissionController::AdmissionRequest request = {admission_state->query_id,
        admission_state->coord_id, admission_state->query_exec_request,
        admission_state->query_exec_request.query_ctx.client_request.query_options,
        admission_state->summary_profile,
        admission_state->blacklisted_executor_addresses};
    admission_state->admit_status =
        AdmissiondEnv::GetInstance()->admission_controller()->SubmitForAdmission(request,
            &admission_state->admit_outcome, &admission_state->schedule, queued,
            &admission_state->request_pool);
    admission_state->submitted = true;
    if (!queued) {
      admission_state->admission_done = true;
      if (admission_state->admit_status.ok()) {
        for (const auto& entry : admission_state->schedule->backend_exec_params()) {
          admission_state->unreleased_backends.emplace(entry.address());
        }
      }
    } else {
      DCHECK(admission_state->admit_status.ok());
    }
  }
}

template <typename ResponsePBType>
void AdmissionControlService::RespondAndReleaseRpc(
    const Status& status, ResponsePBType* response, RpcContext* rpc_context) {
  status.ToProto(response->mutable_status());
  // Release the memory against the control service's memory tracker.
  mem_tracker_->Release(rpc_context->GetTransferSize());
  rpc_context->RespondSuccess();
}

bool AdmissionControlService::CheckAndUpdateHeartbeat(
    const UniqueIdPB& coord_id, int64_t update_version) {
  lock_guard<mutex> l(heartbeat_lock_);
  auto& curr_version = coord_id_to_heartbeat_[coord_id];
  if(curr_version < update_version){
    curr_version = update_version;
    return true;
  }
  return false;
}

} // namespace impala
