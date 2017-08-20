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

#include "common/status.h"
#include "exec/kudu-util.h"
#include "kudu/rpc/rpc_context.h"
#include "rpc/rpc-mgr.h"
#include "runtime/krpc-data-stream-mgr.h"
#include "runtime/exec-env.h"
#include "runtime/row-batch.h"
#include "testutil/fault-injection-util.h"

#include "gen-cpp/data_stream_service.pb.h"

#include "common/names.h"

using kudu::rpc::RpcContext;

namespace impala {

DataStreamService::DataStreamService(RpcMgr* mgr)
  : DataStreamServiceIf(mgr->metric_entity(), mgr->result_tracker()) {}

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

}
