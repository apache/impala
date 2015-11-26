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

#ifndef IMPALA_RUNTIME_TEST_ENV
#define IMPALA_RUNTIME_TEST_ENV

#include "runtime/disk-io-mgr.h"
#include "runtime/buffered-block-mgr.h"
#include "runtime/exec-env.h"
#include "runtime/mem-tracker.h"
#include "runtime/runtime-state.h"

namespace impala {

/// Helper testing class that creates an environment with a buffered-block-mgr similar
/// to the one Impala's runtime is using.
class TestEnv {
 public:
  TestEnv();
  ~TestEnv();

  /// Reinitialize tmp_file_mgr with custom configuration. Only valid to call before
  /// query states have been created.
  void InitTmpFileMgr(const std::vector<std::string>& tmp_dirs, bool one_dir_per_device);

  /// Create a RuntimeState for a query with a new block manager. The RuntimeState is
  /// owned by the TestEnv.
  Status CreateQueryState(int64_t query_id, int max_buffers, int block_size,
      RuntimeState** runtime_state);

  /// Create multiple separate RuntimeStates with associated block managers, e.g. as if
  /// multiple queries were executing. The RuntimeStates are owned by TestEnv.
  Status CreateQueryStates(int64_t start_query_id, int num_mgrs, int buffers_per_mgr,
      int block_size, std::vector<RuntimeState*>* runtime_states);

  /// Destroy all RuntimeStates and block managers created by this TestEnv.
  void TearDownQueryStates();

  /// Calculate memory limit accounting for overflow and negative values.
  /// If max_buffers is -1, no memory limit will apply.
  int64_t CalculateMemLimit(int max_buffers, int block_size);

  ExecEnv* exec_env() { return exec_env_.get(); }
  MemTracker* block_mgr_parent_tracker() { return block_mgr_parent_tracker_.get(); }
  MemTracker* io_mgr_tracker() { return io_mgr_tracker_.get(); }
  MetricGroup* metrics() { return metrics_.get(); }
  TmpFileMgr* tmp_file_mgr() { return tmp_file_mgr_.get(); }

 private:

  /// Recreate global metric groups.
  void InitMetrics();

  /// Create a new RuntimeState sharing global environment.
  RuntimeState* CreateRuntimeState(int64_t query_id);

  /// Global state for test environment.
  static boost::scoped_ptr<MetricGroup> static_metrics_;
  boost::scoped_ptr<ExecEnv> exec_env_;
  boost::scoped_ptr<MemTracker> block_mgr_parent_tracker_;
  boost::scoped_ptr<MemTracker> io_mgr_tracker_;
  boost::scoped_ptr<MetricGroup> metrics_;
  boost::scoped_ptr<TmpFileMgr> tmp_file_mgr_;

  /// Per-query states with associated block managers.
  vector<boost::shared_ptr<RuntimeState> > query_states_;
};

}

#endif
