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
#include "exec/kudu-util.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/util/monotime.h"
#include "rpc/rpc-mgr.h"
#include "rpc/rpc-mgr.inline.h"
#include "runtime/krpc-data-stream-mgr.h"
#include "runtime/exec-env.h"
#include "runtime/mem-tracker.h"
#include "runtime/row-batch.h"
#include "util/memory-metrics.h"
#include "util/parse-util.h"
#include "testutil/fault-injection-util.h"

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
}

Status DataStreamService::Init() {
  int num_svc_threads = FLAGS_datastream_service_num_svc_threads > 0 ?
      FLAGS_datastream_service_num_svc_threads : CpuInfo::num_cores();
  // The maximum queue length is set to maximum 32-bit value. Its actual capacity is
  // bound by memory consumption against 'mem_tracker_'.
  RETURN_IF_ERROR(ExecEnv::GetInstance()->rpc_mgr()->RegisterService(num_svc_threads,
      std::numeric_limits<int32_t>::max(), this, mem_tracker()));
  return Status::OK();
}

Status DataStreamService::GetProxy(const TNetworkAddress& address, const string& hostname,
    unique_ptr<DataStreamServiceProxy>* proxy) {
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

template<typename ResponsePBType>
void DataStreamService::RespondRpc(const Status& status,
    ResponsePBType* response, kudu::rpc::RpcContext* ctx) {
  MonoDelta duration(MonoTime::Now().GetDeltaSince(ctx->GetTimeReceived()));
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
