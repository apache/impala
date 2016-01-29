// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "service/fragment-exec-state.h"

#include <sstream>

#include "codegen/llvm-codegen.h"
#include "gen-cpp/ImpalaInternalService.h"
#include "rpc/thrift-util.h"
#include "gutil/strings/substitute.h"
#include "util/bloom-filter.h"

#include "common/names.h"

using namespace apache::thrift;
using namespace strings;
using namespace impala;

Status FragmentMgr::FragmentExecState::UpdateStatus(const Status& status) {
  lock_guard<mutex> l(status_lock_);
  if (!status.ok() && exec_status_.ok()) exec_status_ = status;
  return exec_status_;
}

Status FragmentMgr::FragmentExecState::Cancel() {
  lock_guard<mutex> l(status_lock_);
  RETURN_IF_ERROR(exec_status_);
  executor_.Cancel();
  return Status::OK();
}

Status FragmentMgr::FragmentExecState::Prepare() {
  Status status = executor_.Prepare(exec_params_);
  if (!status.ok()) ReportStatusCb(status, NULL, true);
  prepare_promise_.Set(status);
  return status;
}

void FragmentMgr::FragmentExecState::Exec() {
  // Open() does the full execution, because all plan fragments have sinks
  executor_.Open();
  executor_.Close();
}

// There can only be one of these callbacks in-flight at any moment, because
// it is only invoked from the executor's reporting thread.
// Also, the reported status will always reflect the most recent execution status,
// including the final status when execution finishes.
void FragmentMgr::FragmentExecState::ReportStatusCb(
    const Status& status, RuntimeProfile* profile, bool done) {
  DCHECK(status.ok() || done);  // if !status.ok() => done
  Status exec_status = UpdateStatus(status);

  Status coord_status;
  ImpalaInternalServiceConnection coord(client_cache_, coord_address(), &coord_status);
  if (!coord_status.ok()) {
    stringstream s;
    s << "Couldn't get a client for " << coord_address() <<"\tReason: "
      << coord_status.GetDetail();
    UpdateStatus(Status(ErrorMsg(TErrorCode::INTERNAL_ERROR, s.str())));
    return;
  }

  TReportExecStatusParams params;
  params.protocol_version = ImpalaInternalServiceVersion::V1;
  params.__set_query_id(fragment_instance_ctx_.query_ctx.query_id);
  params.__set_fragment_instance_idx(fragment_instance_ctx_.fragment_instance_idx);
  params.__set_fragment_instance_id(fragment_instance_ctx_.fragment_instance_id);
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
  // Try to send the RPC 3 times before failing.
  for (int i = 0; i < 3; ++i) {
    rpc_status =
        coord.DoRpc(&ImpalaInternalServiceClient::ReportExecStatus, params, &res);
    if (rpc_status.ok()) {
      rpc_status = Status(res.status);
      break;
    }
    if (i < 2) SleepForMs(100);
  }
  if (!rpc_status.ok()) {
    UpdateStatus(rpc_status);
    // TODO: Do we really need to cancel?
    executor_.Cancel();
  }
}

void FragmentMgr::FragmentExecState::PublishFilter(int32_t filter_id,
    const TBloomFilter& thrift_bloom_filter) {
  // Defensively protect against blocking forever in case there's some problem with
  // Prepare().
  static const int WAIT_MS = 30000;
  bool timed_out = false;
  // Wait until Prepare() is done, so we know that the filter bank is set up.
  Status prepare_status = prepare_promise_.Get(WAIT_MS, &timed_out);
  if (timed_out) {
    LOG(ERROR) << "Unexpected timeout in PublishFilter()";
    return;
  }
  if (!prepare_status.ok()) return;
  executor_.runtime_state()->filter_bank()->PublishGlobalFilter(filter_id,
      thrift_bloom_filter);
}
