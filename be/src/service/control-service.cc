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

#include "service/control-service.h"

#include "common/constant-strings.h"
#include "exec/kudu-util.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/rpc/rpc_controller.h"
#include "rpc/rpc-mgr.h"
#include "rpc/rpc-mgr.inline.h"
#include "runtime/coordinator.h"
#include "runtime/exec-env.h"
#include "runtime/mem-tracker.h"
#include "runtime/query-state.h"
#include "service/client-request-state.h"
#include "service/impala-server.h"
#include "testutil/fault-injection-util.h"
#include "util/debug-util.h"
#include "util/memory-metrics.h"
#include "util/parse-util.h"
#include "util/uid-util.h"

#include "gen-cpp/control_service.pb.h"
#include "gen-cpp/control_service.proxy.h"
#include "gen-cpp/RuntimeProfile_types.h"

#include "common/names.h"

using kudu::rpc::RpcContext;

static const string QUEUE_LIMIT_MSG = "(Advanced) Limit on RPC payloads consumption for "
    "ControlService. " + Substitute(MEM_UNITS_HELP_MSG, "the process memory limit");
DEFINE_string(control_service_queue_mem_limit, "50MB", QUEUE_LIMIT_MSG.c_str());
DEFINE_int32(control_service_num_svc_threads, 0, "Number of threads for processing "
    "control service's RPCs. if left at default value 0, it will be set to number of "
    "CPU cores. Set it to a positive value to change from the default.");
DECLARE_string(debug_actions);

namespace impala {

ControlService::ControlService(MetricGroup* metric_group)
  : ControlServiceIf(ExecEnv::GetInstance()->rpc_mgr()->metric_entity(),
        ExecEnv::GetInstance()->rpc_mgr()->result_tracker()) {
  MemTracker* process_mem_tracker = ExecEnv::GetInstance()->process_mem_tracker();
  bool is_percent; // not used
  int64_t bytes_limit = ParseUtil::ParseMemSpec(FLAGS_control_service_queue_mem_limit,
      &is_percent, process_mem_tracker->limit());
  if (bytes_limit <= 0) {
    CLEAN_EXIT_WITH_ERROR(Substitute("Invalid mem limit for control service queue: "
        "'$0'.", FLAGS_control_service_queue_mem_limit));
  }
  mem_tracker_.reset(new MemTracker(
      bytes_limit, "Control Service Queue", process_mem_tracker));
  MemTrackerMetric::CreateMetrics(metric_group, mem_tracker_.get(), "ControlService");
}

Status ControlService::Init() {
  int num_svc_threads = FLAGS_control_service_num_svc_threads > 0 ?
      FLAGS_control_service_num_svc_threads : CpuInfo::num_cores();
  // The maximum queue length is set to maximum 32-bit value. Its actual capacity is
  // bound by memory consumption against 'mem_tracker_'.
  RETURN_IF_ERROR(ExecEnv::GetInstance()->rpc_mgr()->RegisterService(num_svc_threads,
      std::numeric_limits<int32_t>::max(), this, mem_tracker_.get()));
  return Status::OK();
}

Status ControlService::GetProxy(const TNetworkAddress& address, const string& hostname,
    unique_ptr<ControlServiceProxy>* proxy) {
  // Create a ControlService proxy to the destination.
  RETURN_IF_ERROR(ExecEnv::GetInstance()->rpc_mgr()->GetProxy(address, hostname, proxy));
  // Use a network plane different from DataStreamService's to avoid being blocked by
  // large payloads in DataStreamService.
  (*proxy)->set_network_plane("control");
  return Status::OK();
}

bool ControlService::Authorize(const google::protobuf::Message* req,
    google::protobuf::Message* resp, RpcContext* context) {
  return ExecEnv::GetInstance()->rpc_mgr()->Authorize("ControlService", context,
      mem_tracker_.get());
}

Status ControlService::GetProfile(const ReportExecStatusRequestPB& request,
    const ClientRequestState& request_state, RpcContext* rpc_context,
    TRuntimeProfileForest* thrift_profiles) {
  // Debug action to simulate deserialization failure.
  RETURN_IF_ERROR(DebugAction(request_state.query_options(),
      "REPORT_EXEC_STATUS_PROFILE"));
  kudu::Slice thrift_profiles_slice;
  KUDU_RETURN_IF_ERROR(rpc_context->GetInboundSidecar(
      request.thrift_profiles_sidecar_idx(), &thrift_profiles_slice),
      "Failed to get thrift profile sidecar");
  uint32_t len = thrift_profiles_slice.size();
  RETURN_IF_ERROR(DeserializeThriftMsg(thrift_profiles_slice.data(),
      &len, true, thrift_profiles));
  return Status::OK();
}

void ControlService::ReportExecStatus(const ReportExecStatusRequestPB* request,
    ReportExecStatusResponsePB* response, RpcContext* rpc_context) {
  const TUniqueId query_id = ProtoToQueryId(request->query_id());
  shared_ptr<ClientRequestState> request_state =
      ExecEnv::GetInstance()->impala_server()->GetClientRequestState(query_id);

  // This failpoint is to allow jitter to be injected.
  DebugActionNoFail(FLAGS_debug_actions, "REPORT_EXEC_STATUS_DELAY");

  if (request_state.get() == nullptr) {
    // This is expected occasionally (since a report RPC might be in flight while
    // cancellation is happening). Return an error to the caller to get it to stop.
    const string& err = Substitute("ReportExecStatus(): Received report for unknown "
                                   "query ID (probably closed or cancelled): $0 "
                                   "remote host=$1",
        PrintId(query_id), rpc_context->remote_address().ToString());
    VLOG(1) << err;
    RespondAndReleaseRpc(Status::Expected(err), response, rpc_context);
    return;
  }

  // The runtime profile is sent as a Thrift serialized buffer via sidecar. Get the
  // sidecar and deserialize the thrift profile if there is any. The sender may have
  // failed to serialize the Thrift profile so an empty thrift profile is valid.
  // TODO: Fix IMPALA-7232 to indicate incomplete profile in this case.
  TRuntimeProfileForest thrift_profiles;
  if (LIKELY(request->has_thrift_profiles_sidecar_idx())) {
    const Status& profile_status =
        GetProfile(*request, *request_state.get(), rpc_context, &thrift_profiles);
    if (UNLIKELY(!profile_status.ok())) {
      LOG(ERROR) << Substitute("ReportExecStatus(): Failed to deserialize profile "
          "for query ID $0: $1", PrintId(request_state->query_id()),
          profile_status.GetDetail());
      // Do not expose a partially deserialized profile.
      TRuntimeProfileForest empty_profiles;
      swap(thrift_profiles, empty_profiles);
    }
  }

  Status resp_status = request_state->UpdateBackendExecStatus(*request, thrift_profiles);
  RespondAndReleaseRpc(resp_status, response, rpc_context);
}

template <typename ResponsePBType>
void ControlService::RespondAndReleaseRpc(
    const Status& status, ResponsePBType* response, RpcContext* rpc_context) {
  status.ToProto(response->mutable_status());
  // Release the memory against the control service's memory tracker.
  mem_tracker_->Release(rpc_context->GetTransferSize());
  rpc_context->RespondSuccess();
}

void ControlService::CancelQueryFInstances(const CancelQueryFInstancesRequestPB* request,
    CancelQueryFInstancesResponsePB* response, RpcContext* rpc_context) {
  DCHECK(request->has_query_id());
  const TUniqueId& query_id = ProtoToQueryId(request->query_id());
  VLOG_QUERY << "CancelQueryFInstances(): query_id=" << PrintId(query_id);
  // This failpoint is to allow jitter to be injected.
  DebugActionNoFail(FLAGS_debug_actions, "CANCEL_QUERY_FINSTANCES_DELAY");
  QueryState::ScopedRef qs(query_id);
  if (qs.get() == nullptr) {
    Status status(ErrorMsg(TErrorCode::INTERNAL_ERROR,
        Substitute("Unknown query id: $0", PrintId(query_id))));
    RespondAndReleaseRpc(status, response, rpc_context);
    return;
  }
  qs->Cancel();
  RespondAndReleaseRpc(Status::OK(), response, rpc_context);
}

void ControlService::RemoteShutdown(const RemoteShutdownParamsPB* req,
    RemoteShutdownResultPB* response, RpcContext* rpc_context) {
  // This failpoint is to allow jitter to be injected.
  DebugActionNoFail(FLAGS_debug_actions, "REMOTE_SHUTDOWN_DELAY");
  Status status = ExecEnv::GetInstance()->impala_server()->StartShutdown(
      req->has_deadline_s() ? req->deadline_s() : -1,
      response->mutable_shutdown_status());

  RespondAndReleaseRpc(status, response, rpc_context);
}
}
