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

void ImpalaInternalService::ExecPlanFragment(TExecPlanFragmentResult& return_val,
    const TExecPlanFragmentParams& params) {
  VLOG_QUERY << "ExecPlanFragment():"
            << " instance_id=" << params.fragment_instance_ctx.fragment_instance_id;
  FAULT_INJECTION_RPC_DELAY(RPC_EXECPLANFRAGMENT);
  query_exec_mgr_->StartFInstance(params).SetTStatus(&return_val);
}

template <typename T> void SetUnknownIdError(
    const string& id_type, const TUniqueId& id, T* status_container) {
  Status status(ErrorMsg(TErrorCode::INTERNAL_ERROR,
      Substitute("Unknown $0 id: $1", id_type, lexical_cast<string>(id))));
  status.SetTStatus(status_container);
}

void ImpalaInternalService::CancelPlanFragment(TCancelPlanFragmentResult& return_val,
    const TCancelPlanFragmentParams& params) {
  VLOG_QUERY << "CancelPlanFragment(): instance_id=" << params.fragment_instance_id;
  FAULT_INJECTION_RPC_DELAY(RPC_CANCELPLANFRAGMENT);
  QueryState::ScopedRef qs(GetQueryId(params.fragment_instance_id));
  if (qs.get() == nullptr) {
    SetUnknownIdError("query", GetQueryId(params.fragment_instance_id), &return_val);
    return;
  }
  FragmentInstanceState* fis = qs->GetFInstanceState(params.fragment_instance_id);
  if (fis == nullptr) {
    SetUnknownIdError("instance", params.fragment_instance_id, &return_val);
    return;
  }
  Status status = fis->Cancel();
  status.SetTStatus(&return_val);
}

void ImpalaInternalService::ReportExecStatus(TReportExecStatusResult& return_val,
    const TReportExecStatusParams& params) {
  VLOG_QUERY << "ReportExecStatus(): instance_id=" << params.fragment_instance_id;
  FAULT_INJECTION_RPC_DELAY(RPC_REPORTEXECSTATUS);
  impala_server_->ReportExecStatus(return_val, params);
}

void ImpalaInternalService::TransmitData(TTransmitDataResult& return_val,
    const TTransmitDataParams& params) {
  FAULT_INJECTION_RPC_DELAY(RPC_TRANSMITDATA);
  impala_server_->TransmitData(return_val, params);
}

void ImpalaInternalService::UpdateFilter(TUpdateFilterResult& return_val,
    const TUpdateFilterParams& params) {
  VLOG_QUERY << "UpdateFilter(): filter=" << params.filter_id
            << " query_id=" << PrintId(params.query_id);
  FAULT_INJECTION_RPC_DELAY(RPC_UPDATEFILTER);
  impala_server_->UpdateFilter(return_val, params);
}

void ImpalaInternalService::PublishFilter(TPublishFilterResult& return_val,
    const TPublishFilterParams& params) {
  VLOG_QUERY << "PublishFilter(): filter=" << params.filter_id
            << " instance_id=" << PrintId(params.dst_instance_id);
  FAULT_INJECTION_RPC_DELAY(RPC_PUBLISHFILTER);
  QueryState::ScopedRef qs(GetQueryId(params.dst_instance_id));
  if (qs.get() == nullptr) return;
  FragmentInstanceState* fis = qs->GetFInstanceState(params.dst_instance_id);
  if (fis == nullptr) return;
  fis->PublishFilter(params.filter_id, params.bloom_filter);
}
