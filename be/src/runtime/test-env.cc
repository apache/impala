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

#include "runtime/test-env.h"

#include <limits>
#include <memory>

#include "runtime/query-exec-mgr.h"
#include "runtime/tmp-file-mgr.h"
#include "runtime/query-state.h"
#include "util/disk-info.h"
#include "util/impalad-metrics.h"
#include "gutil/strings/substitute.h"
#include "common/names.h"

using boost::scoped_ptr;
using std::numeric_limits;

namespace impala {

scoped_ptr<MetricGroup> TestEnv::static_metrics_;

TestEnv::TestEnv()
  : have_tmp_file_mgr_args_(false),
    buffer_pool_min_buffer_len_(64 * 1024),
    buffer_pool_capacity_(0) {}

Status TestEnv::Init() {
  if (static_metrics_ == NULL) {
    static_metrics_.reset(new MetricGroup("test-env-static-metrics"));
    ImpaladMetrics::CreateMetrics(static_metrics_.get());
  }

  exec_env_.reset(new ExecEnv);
  // Populate the ExecEnv state that the backend tests need.
  exec_env_->mem_tracker_.reset(new MemTracker(-1, "Process"));
  RETURN_IF_ERROR(exec_env_->disk_io_mgr()->Init(exec_env_->process_mem_tracker()));
  exec_env_->metrics_.reset(new MetricGroup("test-env-metrics"));
  exec_env_->tmp_file_mgr_.reset(new TmpFileMgr);
  if (have_tmp_file_mgr_args_) {
    RETURN_IF_ERROR(
        tmp_file_mgr()->InitCustom(tmp_dirs_, one_tmp_dir_per_device_, metrics()));
  } else {
    RETURN_IF_ERROR(tmp_file_mgr()->Init(metrics()));
  }
  exec_env_->InitBufferPool(buffer_pool_min_buffer_len_, buffer_pool_capacity_,
      static_cast<int64_t>(0.1 * buffer_pool_capacity_));
  return Status::OK();
}

void TestEnv::SetTmpFileMgrArgs(
    const std::vector<std::string>& tmp_dirs, bool one_dir_per_device) {
  have_tmp_file_mgr_args_ = true;
  tmp_dirs_ = tmp_dirs;
  one_tmp_dir_per_device_ = one_dir_per_device;
}

void TestEnv::SetBufferPoolArgs(int64_t min_buffer_len, int64_t capacity) {
  buffer_pool_min_buffer_len_ = min_buffer_len;
  buffer_pool_capacity_ = capacity;
}

TestEnv::~TestEnv() {
  // Queries must be torn down first since they are dependent on global state.
  TearDownQueries();
  // tear down exec env state to avoid leaks
  exec_env_.reset();
}

void TestEnv::TearDownQueries() {
  for (RuntimeState* runtime_state : runtime_states_) runtime_state->ReleaseResources();
  runtime_states_.clear();
  for (QueryState* query_state : query_states_) {
    query_state->ReleaseExecResourceRefcount();
    exec_env_->query_exec_mgr()->ReleaseQueryState(query_state);
  }
  query_states_.clear();
}

int64_t TestEnv::CalculateMemLimit(int max_buffers, int block_size) {
  DCHECK_GE(max_buffers, -1);
  if (max_buffers == -1) return -1;
  return max_buffers * static_cast<int64_t>(block_size);
}

int64_t TestEnv::TotalQueryMemoryConsumption() {
  int64_t total = 0;
  for (QueryState* query_state : query_states_) {
    total += query_state->query_mem_tracker()->consumption();
  }
  return total;
}

Status TestEnv::CreateQueryState(
    int64_t query_id, const TQueryOptions* query_options, RuntimeState** runtime_state) {
  TQueryCtx query_ctx;
  if (query_options != nullptr) query_ctx.client_request.query_options = *query_options;
  query_ctx.query_id.hi = 0;
  query_ctx.query_id.lo = query_id;
  query_ctx.request_pool = "test-pool";

  // CreateQueryState() enforces the invariant that 'query_id' must be unique.
  QueryState* qs = exec_env_->query_exec_mgr()->CreateQueryState(query_ctx);
  query_states_.push_back(qs);
  // make sure to initialize data structures unrelated to the TExecQueryFInstancesParams
  // param
  TExecQueryFInstancesParams rpc_params;
  // create dummy -Ctx fields, we need them for FragmentInstance-/RuntimeState
  rpc_params.__set_coord_state_idx(0);
  rpc_params.__set_query_ctx(TQueryCtx());
  rpc_params.__set_fragment_ctxs(vector<TPlanFragmentCtx>({TPlanFragmentCtx()}));
  rpc_params.__set_fragment_instance_ctxs(
      vector<TPlanFragmentInstanceCtx>({TPlanFragmentInstanceCtx()}));
  RETURN_IF_ERROR(qs->Init(rpc_params));
  FragmentInstanceState* fis = qs->obj_pool()->Add(
      new FragmentInstanceState(qs, qs->rpc_params().fragment_ctxs[0], qs->rpc_params().fragment_instance_ctxs[0]));
  RuntimeState* rs = qs->obj_pool()->Add(
      new RuntimeState(qs, fis->fragment_ctx(), fis->instance_ctx(), exec_env_.get()));
  runtime_states_.push_back(rs);

  *runtime_state = rs;
  return Status::OK();
}
}
