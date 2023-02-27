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

#include "scheduling/remote-admission-control-client.h"

#include "gen-cpp/admission_control_service.pb.h"
#include "gen-cpp/admission_control_service.proxy.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_sidecar.h"
#include "rpc/rpc-mgr.inline.h"
#include "rpc/sidecar-util.h"
#include "runtime/exec-env.h"
#include "scheduling/admission-control-service.h"
#include "util/debug-util.h"
#include "util/kudu-status-util.h"
#include "util/runtime-profile-counters.h"
#include "util/time.h"
#include "util/uid-util.h"

#include "common/names.h"


DEFINE_int32(admission_status_retry_time_ms, 10,
    "(Advanced) The number of milliseconds coordinators will wait before retrying the "
    "GetQueryStatus rpc.");
DEFINE_int32(admission_max_retry_time_s, 60,
    "(Advanced) The amount of time in seconds the coordinator will spend attempting to "
    "retry admission if the admissiond is unreachable.");

using namespace strings;
using namespace kudu::rpc;

namespace impala {

RemoteAdmissionControlClient::RemoteAdmissionControlClient(const TQueryCtx& query_ctx)
  : query_ctx_(query_ctx) {
  TUniqueIdToUniqueIdPB(query_ctx.query_id, &query_id_);
}

Status RemoteAdmissionControlClient::TryAdmitQuery(AdmissionControlServiceProxy* proxy,
    const TQueryExecRequest& request, AdmitQueryRequestPB* req,
    kudu::Status* rpc_status) {
  AdmitQueryResponsePB resp;
  RpcController rpc_controller;

  KrpcSerializer serializer;
  int sidecar_idx;
  RETURN_IF_ERROR(serializer.SerializeToSidecar(&request, &rpc_controller, &sidecar_idx));
  req->set_query_exec_request_sidecar_idx(sidecar_idx);

  Status admit_status = Status::OK();
  {
    /// We hold 'lock_' for the duration of AdmitQuery to coordinate with CancelAdmission
    /// and avoid the scenario where the CancelAdmission rpc is sent first, the admission
    /// controller doesn't find the query because it wasn't submitted yet so nothing is
    /// cancelled, then the AdmitQuery rpc is sent and the query is scheduled despite
    /// already having been cancelled.
    lock_guard<mutex> l(lock_);
    if (cancelled_) {
      return Status("Query already cancelled.");
    }

    *rpc_status = proxy->AdmitQuery(*req, &resp, &rpc_controller);
    if (!rpc_status->ok()) {
      return Status::OK();
    }

    Status debug_status = DebugAction(
        request.query_ctx.client_request.query_options, "ADMIT_QUERY_NETWORK_ERROR");
    if (UNLIKELY(!debug_status.ok())) {
      *rpc_status = kudu::Status::NetworkError("Hit debug action error.");
      return Status::OK();
    }

    admit_status = Status(resp.status());
    if (admit_status.ok()) {
      pending_admit_ = true;
    }
  }
  return admit_status;
}

Status RemoteAdmissionControlClient::SubmitForAdmission(
    const AdmissionController::AdmissionRequest& request,
    RuntimeProfile::EventSequence* query_events,
    std::unique_ptr<QuerySchedulePB>* schedule_result,
    int64_t* wait_start_time_ms, int64_t* wait_end_time_ms) {
  ScopedEvent completedEvent(
      query_events, AdmissionControlClient::QUERY_EVENT_COMPLETED_ADMISSION);

  std::unique_ptr<AdmissionControlServiceProxy> proxy;
  RETURN_IF_ERROR(AdmissionControlService::GetProxy(&proxy));
  AdmitQueryRequestPB req;

  *req.mutable_query_id() = request.query_id;
  *req.mutable_coord_id() = ExecEnv::GetInstance()->backend_id();

  for (const NetworkAddressPB& address : request.blacklisted_executor_addresses) {
    *req.add_blacklisted_executor_addresses() = address;
  }

  query_events->MarkEvent(QUERY_EVENT_SUBMIT_FOR_ADMISSION);

  int64_t admission_start = MonotonicMillis();
  kudu::Status admit_rpc_status = kudu::Status::OK();
  Status admit_status =
      TryAdmitQuery(proxy.get(), request.request, &req, &admit_rpc_status);
  int32_t num_retries = 0;
  // Only retry AdmitQuery if the rpc layer reported a network error, indicating that the
  // admissiond was unreachable.
  while (admit_rpc_status.IsNetworkError()) {
    int64_t elapsed_s = (MonotonicMillis() - admission_start) / MILLIS_PER_SEC;
    if (elapsed_s > FLAGS_admission_max_retry_time_s) {
      return Status(
          Substitute("Failed to admit query after waiting $0s and retrying $1 times.",
              elapsed_s, num_retries));
    }

    ++num_retries;
    // Generate a random number between 0 and 1 - we'll retry sometime evenly distributed
    // between 'retry_time' and 'retry_time * (num_retries + 1)', so we won't hit the
    // "thundering herd" problem.
    float jitter = static_cast<float>(rand()) / static_cast<float>(RAND_MAX);
    SleepForMs(FLAGS_admission_status_retry_time_ms * (num_retries * jitter + 1));

    VLOG(3) << "Retrying AdmitQuery rpc for " << request.query_id
            << ". Previous rpc failed with status: " << admit_rpc_status.ToString();
    admit_status = TryAdmitQuery(proxy.get(), request.request, &req, &admit_rpc_status);
  }

  KUDU_RETURN_IF_ERROR(admit_rpc_status, "AdmitQuery rpc failed");
  RETURN_IF_ERROR(admit_status);

  bool is_query_queued = false;
  while (true) {
    RpcController rpc_controller2;
    GetQueryStatusRequestPB get_status_req;
    GetQueryStatusResponsePB get_status_resp;
    *get_status_req.mutable_query_id() = request.query_id;
    KUDU_RETURN_IF_ERROR(
        proxy->GetQueryStatus(get_status_req, &get_status_resp, &rpc_controller2),
        "GetQueryStatus rpc failed");

    if (get_status_resp.has_summary_profile_sidecar_idx()) {
      TRuntimeProfileTree tree;
      RETURN_IF_ERROR(GetSidecar(
          get_status_resp.summary_profile_sidecar_idx(), &rpc_controller2, &tree));
      request.summary_profile->Update(tree);
    }

    if (wait_start_time_ms != nullptr && get_status_resp.has_wait_start_time_ms()) {
      *wait_start_time_ms = get_status_resp.wait_start_time_ms();
    }
    if (wait_end_time_ms != nullptr && get_status_resp.has_wait_end_time_ms()) {
      *wait_end_time_ms = get_status_resp.wait_end_time_ms();
    }

    if (get_status_resp.has_query_schedule()) {
      schedule_result->reset(new QuerySchedulePB());
      schedule_result->get()->Swap(get_status_resp.mutable_query_schedule());
      break;
    }
    admit_status = Status(get_status_resp.status());
    if (!admit_status.ok()) {
      break;
    }

    if (!is_query_queued) {
      query_events->MarkEvent(QUERY_EVENT_QUEUED);
      is_query_queued = true;
    }

    SleepForMs(FLAGS_admission_status_retry_time_ms);
  }

  {
    lock_guard<mutex> l(lock_);
    pending_admit_ = false;
  }

  return admit_status;
}

void RemoteAdmissionControlClient::ReleaseQuery(int64_t peak_mem_consumption) {
  std::unique_ptr<AdmissionControlServiceProxy> proxy;
  Status get_proxy_status = AdmissionControlService::GetProxy(&proxy);
  if (!get_proxy_status.ok()) {
    LOG(ERROR) << "ReleaseQuery for " << query_id_
               << " failed to get proxy: " << get_proxy_status;
    return;
  }

  ReleaseQueryRequestPB req;
  ReleaseQueryResponsePB resp;
  *req.mutable_query_id() = query_id_;
  req.set_peak_mem_consumption(peak_mem_consumption);
  Status rpc_status =
      RpcMgr::DoRpcWithRetry(proxy, &AdmissionControlServiceProxy::ReleaseQuery, req,
          &resp, query_ctx_, "ReleaseQuery() RPC failed", RPC_NUM_RETRIES, RPC_TIMEOUT_MS,
          RPC_BACKOFF_TIME_MS, "REMOTE_AC_RELEASE_QUERY");

  // Failure of this rpc is not considered a query failure, so we just log it.
  // TODO: we need to be sure that the resources do in fact get cleaned up in situation
  // like these (IMPALA-9976).
  if (!rpc_status.ok()) {
    LOG(WARNING) << "ReleaseQuery rpc failed for " << query_id_ << ": " << rpc_status;
  }
  Status resp_status(resp.status());
  if (!resp_status.ok()) {
    LOG(WARNING) << "ReleaseQuery failed for " << query_id_ << ": " << resp_status;
  }
}

void RemoteAdmissionControlClient::ReleaseQueryBackends(
    const vector<NetworkAddressPB>& host_addrs) {
  std::unique_ptr<AdmissionControlServiceProxy> proxy;
  Status get_proxy_status = AdmissionControlService::GetProxy(&proxy);
  if (!get_proxy_status.ok()) {
    LOG(ERROR) << "ReleaseQueryBackends for " << query_id_
               << " failed to get proxy: " << get_proxy_status;
    return;
  }

  ReleaseQueryBackendsRequestPB req;
  ReleaseQueryBackendsResponsePB resp;
  *req.mutable_query_id() = query_id_;
  for (const NetworkAddressPB& addr : host_addrs) {
    *req.add_host_addr() = addr;
  }
  Status rpc_status =
      RpcMgr::DoRpcWithRetry(proxy, &AdmissionControlServiceProxy::ReleaseQueryBackends,
          req, &resp, query_ctx_, "ReleaseQueryBackends() RPC failed", RPC_NUM_RETRIES,
          RPC_TIMEOUT_MS, RPC_BACKOFF_TIME_MS, "REMOTE_AC_RELEASE_BACKENDS");

  // Failure of this rpc is not considered a query failure, so we just log it.
  // TODO: we need to be sure that the resources do in fact get cleaned up in situation
  // like these (IMPALA-9976).
  if (!rpc_status.ok()) {
    LOG(WARNING) << "ReleaseQueryBackends rpc failed for " << query_id_ << ": "
                 << rpc_status;
  }
  Status resp_status(resp.status());
  if (!resp_status.ok()) {
    LOG(WARNING) << "ReleaseQueryBackends failed for " << query_id_ << ": "
                 << resp_status;
  }
}

void RemoteAdmissionControlClient::CancelAdmission() {
  {
    lock_guard<mutex> l(lock_);
    cancelled_ = true;
    if (!pending_admit_) {
      // Nothing to cancel.
      return;
    }
  }

  std::unique_ptr<AdmissionControlServiceProxy> proxy;
  Status get_proxy_status = AdmissionControlService::GetProxy(&proxy);
  if (!get_proxy_status.ok()) {
    LOG(WARNING) << "CancelAdmission for " << query_id_
                 << " failed to get proxy: " << get_proxy_status;
  }

  CancelAdmissionRequestPB req;
  CancelAdmissionResponsePB resp;
  *req.mutable_query_id() = query_id_;
  Status rpc_status =
      RpcMgr::DoRpcWithRetry(proxy, &AdmissionControlServiceProxy::CancelAdmission, req,
          &resp, query_ctx_, "CancelAdmission() RPC failed", RPC_NUM_RETRIES,
          RPC_TIMEOUT_MS, RPC_BACKOFF_TIME_MS, "REMOTE_AC_CANCEL_ADMISSION");

  // Failure of this rpc is not considered a query failure, so we just log it.
  if (!rpc_status.ok()) {
    LOG(WARNING) << "CancelAdmission rpc failed for " << query_id_ << ": " << rpc_status;
  }
  Status resp_status(resp.status());
  if (!resp_status.ok()) {
    LOG(WARNING) << "CancelAdmission failed for " << query_id_ << ": " << resp_status;
  }
}

} // namespace impala
