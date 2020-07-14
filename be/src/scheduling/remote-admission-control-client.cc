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

DECLARE_string(admission_control_service_addr);

DEFINE_int32(admission_status_retry_time_ms, 10,
    "(Advanced) The number of milliseconds coordinators will wait before retrying the "
    "GetQueryStatus rpc.");

using namespace strings;
using namespace kudu::rpc;

namespace impala {

RemoteAdmissionControlClient::RemoteAdmissionControlClient(const TQueryCtx& query_ctx)
  : query_ctx_(query_ctx),
    address_(MakeNetworkAddress(FLAGS_admission_control_service_addr)) {
  TUniqueIdToUniqueIdPB(query_ctx.query_id, &query_id_);
}

Status RemoteAdmissionControlClient::SubmitForAdmission(
    const AdmissionController::AdmissionRequest& request,
    RuntimeProfile::EventSequence* query_events,
    std::unique_ptr<QuerySchedulePB>* schedule_result) {
  ScopedEvent completedEvent(
      query_events, AdmissionControlClient::QUERY_EVENT_COMPLETED_ADMISSION);

  std::unique_ptr<AdmissionControlServiceProxy> proxy;
  RETURN_IF_ERROR(AdmissionControlService::GetProxy(address_, address_.hostname, &proxy));
  AdmitQueryRequestPB req;
  AdmitQueryResponsePB resp;
  RpcController rpc_controller;

  *req.mutable_query_id() = request.query_id;
  *req.mutable_coord_id() = ExecEnv::GetInstance()->backend_id();

  KrpcSerializer serializer;
  int sidecar_idx1;
  RETURN_IF_ERROR(
      serializer.SerializeToSidecar(&request.request, &rpc_controller, &sidecar_idx1));
  req.set_query_exec_request_sidecar_idx(sidecar_idx1);

  for (const NetworkAddressPB& address : request.blacklisted_executor_addresses) {
    *req.add_blacklisted_executor_addresses() = address;
  }

  query_events->MarkEvent(QUERY_EVENT_SUBMIT_FOR_ADMISSION);
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

    KUDU_RETURN_IF_ERROR(
        proxy->AdmitQuery(req, &resp, &rpc_controller), "AdmitQuery rpc failed");
    Status admit_status(resp.status());
    RETURN_IF_ERROR(admit_status);

    pending_admit_ = true;
  }

  Status admit_status = Status::OK();
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

    if (get_status_resp.has_query_schedule()) {
      schedule_result->reset(new QuerySchedulePB());
      schedule_result->get()->Swap(get_status_resp.mutable_query_schedule());
      break;
    }
    admit_status = Status(get_status_resp.status());
    if (!admit_status.ok()) {
      break;
    }
    query_events->MarkEvent(QUERY_EVENT_QUEUED);

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
  Status get_proxy_status =
      AdmissionControlService::GetProxy(address_, address_.hostname, &proxy);
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
  Status get_proxy_status =
      AdmissionControlService::GetProxy(address_, address_.hostname, &proxy);
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
  Status get_proxy_status =
      AdmissionControlService::GetProxy(address_, address_.hostname, &proxy);
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
