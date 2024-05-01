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

#include "service/data-stream-service.h"

#include <climits>

#include "common/constant-strings.h"
#include "common/status.h"
#include "exec/kudu/kudu-util.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/util/monotime.h"
#include "rpc/rpc-mgr.h"
#include "rpc/rpc-mgr.inline.h"
#include "runtime/exec-env.h"
#include "runtime/krpc-data-stream-mgr.h"
#include "runtime/mem-tracker.h"
#include "runtime/query-state.h"
#include "runtime/row-batch.h"
#include "service/impala-server.h"
#include "util/memory-metrics.h"
#include "util/parse-util.h"
#include "util/uid-util.h"

#include "gen-cpp/data_stream_service.pb.h"
#include "gen-cpp/data_stream_service.proxy.h"

#include "common/names.h"

using kudu::rpc::RpcContext;
using kudu::MonoDelta;
using kudu::MonoTime;

static const string QUEUE_LIMIT_MSG = "(Advanced) Limit on RPC payloads consumption for "
    "DataStreamService. " + Substitute(MEM_UNITS_HELP_MSG, "the process memory limit");
DEFINE_string(datastream_service_queue_mem_limit, "5%", QUEUE_LIMIT_MSG.c_str());
DEFINE_int32(datastream_service_num_svc_threads, 0, "Number of threads for processing "
    "datastream services' RPCs. If left at default value 0, it will be set to number of "
    "CPU cores.  Set it to a positive value to change from the default.");
DEFINE_int32_hidden(update_filter_min_wait_time_ms, 500,
    "Minimum time for UpdateFilterFromRemote RPC to wait until destination QueryState is "
    "ready.");
DECLARE_string(debug_actions);

namespace impala {

DataStreamService::DataStreamService(MetricGroup* metric_group)
  : DataStreamServiceIf(ExecEnv::GetInstance()->rpc_mgr()->metric_entity(),
        ExecEnv::GetInstance()->rpc_mgr()->result_tracker()) {
  MemTracker* process_mem_tracker = ExecEnv::GetInstance()->process_mem_tracker();
  bool is_percent; // not used
  int64_t bytes_limit = ParseUtil::ParseMemSpec(FLAGS_datastream_service_queue_mem_limit,
      &is_percent, process_mem_tracker->limit());
  if (bytes_limit <= 0) {
    CLEAN_EXIT_WITH_ERROR(Substitute("Invalid mem limit for data stream service queue: "
        "'$0'.", FLAGS_datastream_service_queue_mem_limit));
  }
  mem_tracker_.reset(new MemTracker(
      bytes_limit, "Data Stream Service Queue", process_mem_tracker));
  MemTrackerMetric::CreateMetrics(metric_group, mem_tracker_.get(), "DataStreamService");
  DCHECK_EQ(methods_by_name_.count(string(END_DATA_STREAM)), 1);
}

Status DataStreamService::Init() {
  int num_svc_threads = FLAGS_datastream_service_num_svc_threads > 0 ?
      FLAGS_datastream_service_num_svc_threads : CpuInfo::num_cores();
  // The maximum queue length is set to maximum 32-bit value. Its actual capacity is
  // bound by memory consumption against 'mem_tracker_'.
  RETURN_IF_ERROR(ExecEnv::GetInstance()->rpc_mgr()->RegisterService(num_svc_threads,
      std::numeric_limits<int32_t>::max(), this, mem_tracker(),
      ExecEnv::GetInstance()->rpc_metrics()));
  return Status::OK();
}

Status DataStreamService::GetProxy(const NetworkAddressPB& address,
    const string& hostname, unique_ptr<DataStreamServiceProxy>* proxy) {
  // Create a DataStreamService proxy to the destination.
  RETURN_IF_ERROR(ExecEnv::GetInstance()->rpc_mgr()->GetProxy(address, hostname, proxy));
  (*proxy)->set_network_plane("datastream");
  return Status::OK();
}

bool DataStreamService::Authorize(const google::protobuf::Message* req,
    google::protobuf::Message* resp, RpcContext* context) {
  return ExecEnv::GetInstance()->rpc_mgr()->Authorize("DataStreamService", context,
      mem_tracker());
}

void DataStreamService::EndDataStream(const EndDataStreamRequestPB* request,
    EndDataStreamResponsePB* response, RpcContext* rpc_context) {
  DebugActionNoFail(FLAGS_debug_actions, "END_DATA_STREAM_DELAY");
  // CloseSender() is guaranteed to eventually respond to this RPC so we don't do it here.
  ExecEnv::GetInstance()->stream_mgr()->CloseSender(request, response, rpc_context);
}

void DataStreamService::TransmitData(const TransmitDataRequestPB* request,
    TransmitDataResponsePB* response, RpcContext* rpc_context) {
  DebugActionNoFail(FLAGS_debug_actions, "TRANSMIT_DATA_DELAY");
  // AddData() is guaranteed to eventually respond to this RPC so we don't do it here.
  ExecEnv::GetInstance()->stream_mgr()->AddData(request, response, rpc_context);
}

void DataStreamService::UpdateFilter(
    const UpdateFilterParamsPB* req, UpdateFilterResultPB* resp, RpcContext* context) {
  // This failpoint is to allow jitter to be injected.
  DebugActionNoFail(FLAGS_debug_actions, "UPDATE_FILTER_DELAY");
  DCHECK(req->has_filter_id());
  DCHECK(req->has_query_id());
  DCHECK(req->has_bloom_filter() || req->has_min_max_filter()
      || req->has_in_list_filter());
  ExecEnv::GetInstance()->impala_server()->UpdateFilter(resp, *req, context);
  RespondAndReleaseRpc(Status::OK(), resp, context, mem_tracker_.get());
}

void DataStreamService::UpdateFilterFromRemote(
    const UpdateFilterParamsPB* req, UpdateFilterResultPB* resp, RpcContext* context) {
  DCHECK(req->has_filter_id());
  DCHECK(req->has_query_id());
  DCHECK(
      req->has_bloom_filter() || req->has_min_max_filter() || req->has_in_list_filter());
  int64_t arrival_time = MonotonicMillis();

  // Loop until destination QueryState is ready to accept filter update from remote.
  // Sleep for few miliseconds in-between and break after 500ms grace period passed.
  // The grace period is short so that RPC thread is not blocked for too long.
  // This is a much simpler mechanism than KrpcDataStreamMgr::AddData.
  // TODO: Revisit this with more sophisticated deferral mechanism if needed.
  bool query_found = false;
  int64_t total_wait_time = 0;
  int32_t sleep_duration_ms = 2;
  if (req->remaining_filter_wait_time_ms() < FLAGS_update_filter_min_wait_time_ms) {
    LOG(INFO) << "UpdateFilterFromRemote RPC called with remaining wait time "
              << req->remaining_filter_wait_time_ms() << " ms, less than "
              << FLAGS_update_filter_min_wait_time_ms << " ms minimum wait time.";
  }

  do {
    {
      QueryState::ScopedRef qs(ProtoToQueryId(req->query_id()));
      query_found |= (qs.get() != nullptr);
      if (query_found) {
        if (qs.get() == nullptr || qs->IsTerminalState()) {
          // Query was found, but now is either missing or in terminal state.
          // Break the loop and response with an error.
          break;
        } else if (qs->is_initialized()) {
          qs->UpdateFilterFromRemote(*req, context);
          RespondAndReleaseRpc(Status::OK(), resp, context, mem_tracker_.get());
          return;
        }
      }
    }
    usleep(sleep_duration_ms * 1000);
    // double sleep time for next iteration up to 128ms.
    if (2 * sleep_duration_ms <= 128) sleep_duration_ms *= 2;
    total_wait_time = MonotonicMillis() - arrival_time;
  } while (total_wait_time < FLAGS_update_filter_min_wait_time_ms);

  // Query state for requested query_id might have been cancelled, closed, or not ready.
  // i.e., RUNTIME_FILTER_WAIT_TIME_MS has passed and all fragment instances of
  // query_id has complete their execution.
  string err_msg = Substitute("QueryState for query_id=$0 $1 after $2 ms",
      PrintId(ProtoToQueryId(req->query_id())),
      query_found ? "no longer running" : "not found", total_wait_time);
  LOG(INFO) << err_msg;
  RespondAndReleaseRpc(Status(err_msg), resp, context, mem_tracker_.get());
}

void DataStreamService::PublishFilter(
    const PublishFilterParamsPB* req, PublishFilterResultPB* resp, RpcContext* context) {
  // This failpoint is to allow jitter to be injected.
  DebugActionNoFail(FLAGS_debug_actions, "PUBLISH_FILTER_DELAY");
  DCHECK(req->has_filter_id());
  DCHECK(req->has_dst_query_id());
  DCHECK(req->has_bloom_filter() || req->has_min_max_filter()
      || req->has_in_list_filter());
  QueryState::ScopedRef qs(ProtoToQueryId(req->dst_query_id()));

  if (qs.get() != nullptr) {
    qs->PublishFilter(*req, context);
    RespondAndReleaseRpc(Status::OK(), resp, context, mem_tracker_.get());
  } else {
    string err_msg = Substitute("Query State not found for query_id=$0",
        PrintId(ProtoToQueryId(req->dst_query_id())));
    LOG(INFO) << err_msg;
    RespondAndReleaseRpc(Status(err_msg), resp, context, mem_tracker_.get());
  }
}

template<typename ResponsePBType>
void DataStreamService::RespondRpc(const Status& status,
    ResponsePBType* response, kudu::rpc::RpcContext* ctx) {
  MonoDelta duration(MonoTime::Now() - ctx->GetTimeReceived());
  status.ToProto(response->mutable_status());
  response->set_receiver_latency_ns(duration.ToNanoseconds());
  ctx->RespondSuccess();
}

template<typename ResponsePBType>
void DataStreamService::RespondAndReleaseRpc(const Status& status,
    ResponsePBType* response, kudu::rpc::RpcContext* ctx, MemTracker* mem_tracker) {
  mem_tracker->Release(ctx->GetTransferSize());
  RespondRpc(status, response, ctx);
}

template void DataStreamService::RespondRpc(const Status& status,
    TransmitDataResponsePB* response, kudu::rpc::RpcContext* ctx);

template void DataStreamService::RespondRpc(const Status& status,
    EndDataStreamResponsePB* response, kudu::rpc::RpcContext* ctx);

template void DataStreamService::RespondAndReleaseRpc(const Status& status,
    TransmitDataResponsePB* response, kudu::rpc::RpcContext* ctx,
    MemTracker* mem_tracker);

template void DataStreamService::RespondAndReleaseRpc(const Status& status,
    EndDataStreamResponsePB* response, kudu::rpc::RpcContext* ctx,
    MemTracker* mem_tracker);

}
