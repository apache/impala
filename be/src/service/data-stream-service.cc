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
#include "rpc/rpc-mgr.h"
#include "runtime/krpc-data-stream-mgr.h"
#include "runtime/exec-env.h"
#include "runtime/row-batch.h"
#include "util/parse-util.h"
#include "testutil/fault-injection-util.h"

#include "gen-cpp/data_stream_service.pb.h"

#include "common/names.h"

using kudu::rpc::RpcContext;

static const string queue_limit_msg = "(Advanced) Limit on RPC payloads consumption for "
    "DataStreamService. " + Substitute(MEM_UNITS_HELP_MSG, "the process memory limit");
DEFINE_string(datastream_service_queue_mem_limit, "5%", queue_limit_msg.c_str());
DEFINE_int32(datastream_service_num_svc_threads, 0, "Number of threads for processing "
    "datastream services' RPCs. If left at default value 0, it will be set to number of "
    "CPU cores");

namespace impala {

DataStreamService::DataStreamService()
  : DataStreamServiceIf(ExecEnv::GetInstance()->rpc_mgr()->metric_entity(),
        ExecEnv::GetInstance()->rpc_mgr()->result_tracker()) {
  MemTracker* process_mem_tracker = ExecEnv::GetInstance()->process_mem_tracker();
  bool is_percent;
  int64_t bytes_limit = ParseUtil::ParseMemSpec(FLAGS_datastream_service_queue_mem_limit,
      &is_percent, process_mem_tracker->limit());
  mem_tracker_.reset(new MemTracker(
      bytes_limit, "Data Stream Service Queue", process_mem_tracker));
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

void DataStreamService::EndDataStream(const EndDataStreamRequestPB* request,
    EndDataStreamResponsePB* response, RpcContext* rpc_context) {
  // CloseSender() is guaranteed to eventually respond to this RPC so we don't do it here.
  ExecEnv::GetInstance()->KrpcStreamMgr()->CloseSender(request, response, rpc_context);
}

void DataStreamService::TransmitData(const TransmitDataRequestPB* request,
    TransmitDataResponsePB* response, RpcContext* rpc_context) {
  FAULT_INJECTION_RPC_DELAY(RPC_TRANSMITDATA);
  // AddData() is guaranteed to eventually respond to this RPC so we don't do it here.
  ExecEnv::GetInstance()->KrpcStreamMgr()->AddData(request, response, rpc_context);
}

template<typename ResponsePBType>
void DataStreamService::RespondAndReleaseRpc(const Status& status,
    ResponsePBType* response, kudu::rpc::RpcContext* ctx, MemTracker* mem_tracker) {
  mem_tracker->Release(ctx->GetTransferSize());
  status.ToProto(response->mutable_status());
  ctx->RespondSuccess();
}

template void DataStreamService::RespondAndReleaseRpc(const Status& status,
    TransmitDataResponsePB* response, kudu::rpc::RpcContext* ctx,
    MemTracker* mem_tracker);

template void DataStreamService::RespondAndReleaseRpc(const Status& status,
    EndDataStreamResponsePB* response, kudu::rpc::RpcContext* ctx,
    MemTracker* mem_tracker);

}
