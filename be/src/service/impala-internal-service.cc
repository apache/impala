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

#include "service/impala-internal-service.h"

#include <boost/lexical_cast.hpp>

#include "common/status.h"
#include "gutil/strings/substitute.h"
#include "service/impala-server.h"
#include "runtime/query-exec-mgr.h"
#include "runtime/query-state.h"
#include "runtime/fragment-instance-state.h"
#include "runtime/exec-env.h"
#include "testutil/fault-injection-util.h"

#include "common/names.h"

using namespace impala;

ImpalaInternalService::ImpalaInternalService() {
  impala_server_ = ExecEnv::GetInstance()->impala_server();
  DCHECK(impala_server_ != nullptr);
  query_exec_mgr_ = ExecEnv::GetInstance()->query_exec_mgr();
  DCHECK(query_exec_mgr_ != nullptr);
}

void ImpalaInternalService::ExecQueryFInstances(TExecQueryFInstancesResult& return_val,
    const TExecQueryFInstancesParams& params) {
  VLOG_QUERY << "ExecQueryFInstances():" << " query_id=" << params.query_ctx.query_id;
  FAULT_INJECTION_RPC_DELAY(RPC_EXECQUERYFINSTANCES);
  DCHECK(params.__isset.coord_state_idx);
  DCHECK(params.__isset.query_ctx);
  DCHECK(params.__isset.fragment_ctxs);
  DCHECK(params.__isset.fragment_instance_ctxs);
  query_exec_mgr_->StartQuery(params).SetTStatus(&return_val);
}

template <typename T> void SetUnknownIdError(
    const string& id_type, const TUniqueId& id, T* status_container) {
  Status status(ErrorMsg(TErrorCode::INTERNAL_ERROR,
      Substitute("Unknown $0 id: $1", id_type, lexical_cast<string>(id))));
  status.SetTStatus(status_container);
}

void ImpalaInternalService::CancelQueryFInstances(
    TCancelQueryFInstancesResult& return_val,
    const TCancelQueryFInstancesParams& params) {
  VLOG_QUERY << "CancelQueryFInstances(): query_id=" << params.query_id;
  FAULT_INJECTION_RPC_DELAY(RPC_CANCELQUERYFINSTANCES);
  DCHECK(params.__isset.query_id);
  QueryState::ScopedRef qs(params.query_id);
  if (qs.get() == nullptr) {
    SetUnknownIdError("query", params.query_id, &return_val);
    return;
  }
  qs->Cancel();
}

void ImpalaInternalService::ReportExecStatus(TReportExecStatusResult& return_val,
    const TReportExecStatusParams& params) {
  FAULT_INJECTION_RPC_DELAY(RPC_REPORTEXECSTATUS);
  DCHECK(params.__isset.query_id);
  DCHECK(params.__isset.coord_state_idx);
  impala_server_->ReportExecStatus(return_val, params);
}

void ImpalaInternalService::TransmitData(TTransmitDataResult& return_val,
    const TTransmitDataParams& params) {
  FAULT_INJECTION_RPC_DELAY(RPC_TRANSMITDATA);
  DCHECK(params.__isset.dest_fragment_instance_id);
  DCHECK(params.__isset.sender_id);
  DCHECK(params.__isset.dest_node_id);
  impala_server_->TransmitData(return_val, params);
}

void ImpalaInternalService::UpdateFilter(TUpdateFilterResult& return_val,
    const TUpdateFilterParams& params) {
  FAULT_INJECTION_RPC_DELAY(RPC_UPDATEFILTER);
  DCHECK(params.__isset.filter_id);
  DCHECK(params.__isset.query_id);
  DCHECK(params.__isset.bloom_filter);
  impala_server_->UpdateFilter(return_val, params);
}

void ImpalaInternalService::PublishFilter(TPublishFilterResult& return_val,
    const TPublishFilterParams& params) {
  FAULT_INJECTION_RPC_DELAY(RPC_PUBLISHFILTER);
  DCHECK(params.__isset.filter_id);
  DCHECK(params.__isset.dst_query_id);
  DCHECK(params.__isset.dst_fragment_idx);
  DCHECK(params.__isset.bloom_filter);
  QueryState::ScopedRef qs(params.dst_query_id);
  if (qs.get() == nullptr) return;
  qs->PublishFilter(params.filter_id, params.dst_fragment_idx, params.bloom_filter);
}
