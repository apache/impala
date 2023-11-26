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

#ifndef IMPALA_RUNTIME_TEST_ENV
#define IMPALA_RUNTIME_TEST_ENV

#include "codegen/llvm-codegen-cache.h"
#include "runtime/exec-env.h"
#include "runtime/io/disk-io-mgr.h"
#include "runtime/runtime-state.h"

namespace impala {

class QueryState;

/// Helper testing class that creates an environment with runtime memory management
/// similar to the one used by the Impala runtime. Only one TestEnv can be active at a
/// time, because it modifies the global ExecEnv singleton.
class TestEnv {
 public:
  TestEnv();
  ~TestEnv();

  /// Set custom configuration for TmpFileMgr. Only has effect if called before Init().
  /// If not called, the default configuration is used.
  void SetTmpFileMgrArgs(const std::vector<std::string>& tmp_dirs,
      bool one_dir_per_device, const std::string& compression, bool punch_holes);

  /// Disables creation of a BufferPool instance as part of this TestEnv in Init().
  void DisableBufferPool() { enable_buffer_pool_ = false; }

  /// Set configuration for BufferPool. Only has effect if called before Init().
  /// If not called, a buffer pool with no capacity is created.
  void SetBufferPoolArgs(int64_t min_buffer_len, int64_t capacity);

  /// Set configuration for the process memory tracker. Only has effect if called before
  /// Init(). If 'use_metrics' is true, the usual process memory tracker that uses
  /// memory consumption metrics is created, otherwise no metrics are used.
  /// If not called, a process memory tracker with no limit is created.
  void SetProcessMemTrackerArgs(int64_t bytes_limit, bool use_metrics);

  /// Set the Default FS of ExecEnv.
  void SetDefaultFS(const string& fs) { exec_env_->default_fs_ = fs; }

  /// Initialize the TestEnv with the specified arguments.
  Status Init();

  /// Create a QueryState and a RuntimeState for a query with the given query options.
  /// The states are owned by the TestEnv. Returns an error if CreateQueryState() has
  /// been called with the same query ID already. 'runtime_state' is set to the newly
  /// created RuntimeState. The QueryState can be obtained via 'runtime_state'.
  Status CreateQueryState(
      int64_t query_id, const TQueryOptions* query_options, RuntimeState** runtime_state);

  /// Destroy all query states and associated RuntimeStates, etc, that were created since
  /// the last TearDownQueries() call.
  void TearDownQueries();

  /// Calculate memory limit accounting for overflow and negative values.
  /// If max_buffers is -1, no memory limit will apply.
  int64_t CalculateMemLimit(int max_buffers, int page_len);

  /// Return total of mem tracker consumption for all queries.
  int64_t TotalQueryMemoryConsumption();

  /// Reset the codegen cache.
  void ResetCodegenCache(MetricGroup* metrics = nullptr) {
    if (metrics == nullptr) {
      exec_env_->codegen_cache_.reset();
      return;
    }
    exec_env_->codegen_cache_.reset(new CodeGenCache(metrics));
  }

  /// Get a full URI for the provided path based on the default filesystem.
  std::string GetDefaultFsPath(std::string path);

  ExecEnv* exec_env() { return exec_env_.get(); }
  MetricGroup* metrics() { return exec_env_->metrics(); }
  TmpFileMgr* tmp_file_mgr() { return exec_env_->tmp_file_mgr(); }
  CodeGenCache* codegen_cache() { return exec_env_->codegen_cache_.get(); }

 private:
  /// Recreate global metric groups.
  void InitMetrics();

  /// Arguments for TmpFileMgr, used in Init().
  bool have_tmp_file_mgr_args_;
  std::vector<std::string> tmp_dirs_;
  bool one_tmp_dir_per_device_;
  std::string tmp_file_mgr_compression_;
  bool tmp_file_mgr_punch_holes_;

  /// Whether a buffer pool should be created in Init().
  bool enable_buffer_pool_ = true;

  /// Arguments for BufferPool, used in Init().
  int64_t buffer_pool_min_buffer_len_;
  int64_t buffer_pool_capacity_;

  /// Arguments for process memory tracker, used in Init().
  /// Default to 8GB, which should be enough for any tests that are not deliberately
  /// allocating large amounts of memory.
  int64_t process_mem_limit_ = 8L * 1024L * 1024L * 1024L;
  bool process_mem_tracker_use_metrics_ = false;

  /// Global state for test environment.
  static boost::scoped_ptr<MetricGroup> static_metrics_;
  boost::scoped_ptr<ExecEnv> exec_env_;

  /// Per-query states. TestEnv holds 1 refcount per QueryState in this map.
  std::vector<QueryState*> query_states_;

  /// One runtime state per query with an associated block manager. Each is owned
  /// by one of the 'query_states_'.
  std::vector<RuntimeState*> runtime_states_;
};
}

#endif
