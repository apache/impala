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


#include "runtime/fragment-instance-state.h"

#include <boost/thread/locks.hpp>
#include <boost/thread/lock_guard.hpp>

#include "runtime/exec-env.h"
#include "runtime/backend-client.h"
#include "runtime/runtime-filter-bank.h"
#include "runtime/client-cache.h"
#include "runtime/runtime-state.h"
#include "runtime/query-state.h"
#include "gen-cpp/ImpalaInternalService_types.h"

#include "common/names.h"

using namespace impala;

FragmentInstanceState::FragmentInstanceState(
    QueryState* query_state, const TPlanFragmentCtx& fragment_ctx,
    const TPlanFragmentInstanceCtx& instance_ctx, const TDescriptorTable& desc_tbl)
  : query_state_(query_state),
    fragment_ctx_(fragment_ctx),
    instance_ctx_(instance_ctx),
    desc_tbl_(desc_tbl),
    executor_(
        [this](const Status& status, RuntimeProfile* profile, bool done) {
          ReportStatusCb(status, profile, done);
        }) {
}

Status FragmentInstanceState::UpdateStatus(const Status& status) {
  lock_guard<mutex> l(status_lock_);
  if (!status.ok() && exec_status_.ok()) exec_status_ = status;
  return exec_status_;
}

Status FragmentInstanceState::Cancel() {
  lock_guard<mutex> l(status_lock_);
  RETURN_IF_ERROR(exec_status_);
  executor_.Cancel();
  return Status::OK();
}

void FragmentInstanceState::Exec() {
  Status status =
      executor_.Prepare(query_state_, desc_tbl_, fragment_ctx_, instance_ctx_);
  prepare_promise_.Set(status);
  if (status.ok()) {
    if (executor_.Open().ok()) {
      executor_.Exec();
    }
  }
  executor_.Close();
}

void FragmentInstanceState::ReportStatusCb(
    const Status& status, RuntimeProfile* profile, bool done) {
  DCHECK(status.ok() || done);  // if !status.ok() => done
  Status exec_status = UpdateStatus(status);

  Status coord_status;
  ImpalaBackendConnection coord(
      ExecEnv::GetInstance()->impalad_client_cache(), coord_address(), &coord_status);
  if (!coord_status.ok()) {
    stringstream s;
    s << "Couldn't get a client for " << coord_address() <<"\tReason: "
      << coord_status.GetDetail();
    UpdateStatus(Status(ErrorMsg(TErrorCode::INTERNAL_ERROR, s.str())));
    return;
  }

  TReportExecStatusParams params;
  params.protocol_version = ImpalaInternalServiceVersion::V1;
  params.__set_query_id(query_state_->query_ctx().query_id);
  params.__set_fragment_instance_id(instance_ctx_.fragment_instance_id);
  exec_status.SetTStatus(&params);
  params.__set_done(done);

  if (profile != NULL) {
    profile->ToThrift(&params.profile);
    params.__isset.profile = true;
  }

  RuntimeState* runtime_state = executor_.runtime_state();
  // If executor_ did not successfully prepare, runtime state may not have been set.
  if (runtime_state != NULL) {
    // Only send updates to insert status if fragment is finished, the coordinator
    // waits until query execution is done to use them anyhow.
    if (done) {
      TInsertExecStatus insert_status;

      if (runtime_state->hdfs_files_to_move()->size() > 0) {
        insert_status.__set_files_to_move(*runtime_state->hdfs_files_to_move());
      }
      if (runtime_state->per_partition_status()->size() > 0) {
        insert_status.__set_per_partition_status(*runtime_state->per_partition_status());
      }

      params.__set_insert_exec_status(insert_status);
    }

    // Send new errors to coordinator
    runtime_state->GetUnreportedErrors(&(params.error_log));
  }
  params.__isset.error_log = (params.error_log.size() > 0);

  TReportExecStatusResult res;
  Status rpc_status;
  bool retry_is_safe;
  // Try to send the RPC 3 times before failing.
  for (int i = 0; i < 3; ++i) {
    rpc_status = coord.DoRpc(
        &ImpalaBackendClient::ReportExecStatus, params, &res, &retry_is_safe);
    if (rpc_status.ok()) {
      rpc_status = Status(res.status);
      break;
    }
    if (!retry_is_safe) break;
    if (i < 2) SleepForMs(100);
  }
  if (!rpc_status.ok()) {
    UpdateStatus(rpc_status);
    executor_.Cancel();
  }
}

void FragmentInstanceState::PublishFilter(
    int32_t filter_id, const TBloomFilter& thrift_bloom_filter) {
  VLOG_FILE << "PublishFilter(): instance_id=" << PrintId(instance_id())
            << " filter_id=" << filter_id;
  // Defensively protect against blocking forever in case there's some problem with
  // Prepare().
  static const int WAIT_MS = 30000;
  bool timed_out = false;
  // Wait until Prepare() is done, so we know that the filter bank is set up.
  // TODO: get rid of concurrency in the setup phase as part of the per-query exec rpc
  Status prepare_status = prepare_promise_.Get(WAIT_MS, &timed_out);
  if (timed_out) {
    LOG(ERROR) << "Unexpected timeout in PublishFilter()";
    return;
  }
  if (!prepare_status.ok()) return;
  executor_.runtime_state()->filter_bank()->PublishGlobalFilter(
      filter_id, thrift_bloom_filter);
}

const TQueryCtx& FragmentInstanceState::query_ctx() const {
  return query_state_->query_ctx();
}
