// Copyright 2015 Cloudera Inc.
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

#include "runtime/test-env.h"
#include "util/disk-info.h"
#include "util/impalad-metrics.h"

using namespace boost;

namespace impala {

scoped_ptr<MetricGroup> TestEnv::static_metrics_;

TestEnv::TestEnv() {
  if (static_metrics_ == NULL) {
    static_metrics_.reset(new MetricGroup("test-env-static-metrics"));
    ImpaladMetrics::CreateMetrics(static_metrics_.get());
  }
  exec_env_.reset(new ExecEnv);
  exec_env_->InitForFeTests();
  io_mgr_tracker_.reset(new MemTracker(-1));
  block_mgr_parent_tracker_.reset(new MemTracker(-1));
  exec_env_->disk_io_mgr()->Init(io_mgr_tracker_.get());
  InitMetrics();
  tmp_file_mgr_.reset(new TmpFileMgr);
  tmp_file_mgr_->Init(metrics_.get());
}

void TestEnv::InitMetrics() {
  metrics_.reset(new MetricGroup("test-env-metrics"));
}

void TestEnv::InitTmpFileMgr(const std::vector<std::string>& tmp_dirs,
    bool one_dir_per_device) {
  // Need to recreate metrics to avoid error when registering metric twice.
  InitMetrics();
  tmp_file_mgr_.reset(new TmpFileMgr);
  tmp_file_mgr_->InitCustom(tmp_dirs, one_dir_per_device, metrics_.get());
}

TestEnv::~TestEnv() {
  // Queries must be torn down first since they are dependent on global state.
  TearDownQueryStates();
  block_mgr_parent_tracker_.reset();
  exec_env_.reset();
  io_mgr_tracker_.reset();
  tmp_file_mgr_.reset();
  metrics_.reset();
}

RuntimeState* TestEnv::CreateRuntimeState(int64_t query_id) {
  TExecPlanFragmentParams plan_params = TExecPlanFragmentParams();
  plan_params.fragment_instance_ctx.query_ctx.query_id.hi = 0;
  plan_params.fragment_instance_ctx.query_ctx.query_id.lo = query_id;
  return new RuntimeState(plan_params, "", exec_env_.get());
}

Status TestEnv::CreateQueryState(int64_t query_id, int max_buffers, int block_size,
    RuntimeState** runtime_state) {
  *runtime_state = CreateRuntimeState(query_id);
  if (*runtime_state == NULL) {
    return Status("Unexpected error creating RuntimeState");
  }

  shared_ptr<BufferedBlockMgr> mgr;
  RETURN_IF_ERROR(BufferedBlockMgr::Create(*runtime_state,
      block_mgr_parent_tracker_.get(), (*runtime_state)->runtime_profile(),
      tmp_file_mgr_.get(), CalculateMemLimit(max_buffers, block_size), block_size, &mgr));
  (*runtime_state)->set_block_mgr(mgr);

  query_states_.push_back(shared_ptr<RuntimeState>(*runtime_state));
  return Status::OK();
}

Status TestEnv::CreateQueryStates(int64_t start_query_id, int num_mgrs,
    int buffers_per_mgr, int block_size,
    vector<RuntimeState*>* runtime_states) {
  for (int i = 0; i < num_mgrs; ++i) {
    RuntimeState* runtime_state;
    RETURN_IF_ERROR(CreateQueryState(start_query_id + i, buffers_per_mgr, block_size,
        &runtime_state));
    runtime_states->push_back(runtime_state);
  }
  return Status::OK();
}

void TestEnv::TearDownQueryStates() {
  query_states_.clear();
}


int64_t TestEnv::CalculateMemLimit(int max_buffers, int block_size) {
  DCHECK_GE(max_buffers, -1);
  if (max_buffers == -1) return -1;
  return max_buffers * static_cast<int64_t>(block_size);
}

}
