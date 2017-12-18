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


#ifndef IMPALA_RUNTIME_RUNTIME_STATE_H
#define IMPALA_RUNTIME_RUNTIME_STATE_H

#include <boost/scoped_ptr.hpp>
#include <vector>
#include <string>

// NOTE: try not to add more headers here: runtime-state.h is included in many many files.
#include "common/global-types.h"  // for PlanNodeId
#include "runtime/client-cache-types.h"
#include "runtime/thread-resource-mgr.h"
#include "util/runtime-profile.h"
#include "gen-cpp/ImpalaInternalService_types.h"

namespace impala {

class BufferPool;
class DataStreamRecvr;
class DescriptorTbl;
class Expr;
class LlvmCodeGen;
class MemTracker;
class ObjectPool;
class ReservationTracker;
class RuntimeFilterBank;
class ScalarFnCall;
class Status;
class TimestampValue;
class TUniqueId;
class ExecEnv;
class DataStreamMgrBase;
class HBaseTableFactory;
class TPlanFragmentCtx;
class TPlanFragmentInstanceCtx;
class QueryState;

namespace io {
  class DiskIoMgr;
}

/// TODO: move the typedefs into a separate .h (and fix the includes for that)

/// Counts how many rows an INSERT query has added to a particular partition
/// (partitions are identified by their partition keys: k1=v1/k2=v2
/// etc. Unpartitioned tables have a single 'default' partition which is
/// identified by ROOT_PARTITION_KEY.
typedef std::map<std::string, TInsertPartitionStatus> PartitionStatusMap;

/// Stats per partition for insert queries. They key is the same as for PartitionRowCount
typedef std::map<std::string, TInsertStats> PartitionInsertStats;

/// Tracks files to move from a temporary (key) to a final destination (value) as
/// part of query finalization. If the destination is empty, the file is to be
/// deleted.
typedef std::map<std::string, std::string> FileMoveMap;

/// A collection of items that are part of the global state of a query and shared across
/// all execution nodes of that query. After initialisation, callers must call
/// ReleaseResources() to ensure that all resources are correctly freed before
/// destruction.
class RuntimeState {
 public:
  /// query_state, fragment_ctx, and instance_ctx need to be alive at least as long as
  /// the constructed RuntimeState
  RuntimeState(QueryState* query_state, const TPlanFragmentCtx& fragment_ctx,
      const TPlanFragmentInstanceCtx& instance_ctx, ExecEnv* exec_env);

  /// RuntimeState for test execution and fe-support.cc. Creates its own QueryState and
  /// installs desc_tbl, if set. If query_ctx.request_pool isn't set, sets it to "test-pool".
  RuntimeState(
      const TQueryCtx& query_ctx, ExecEnv* exec_env, DescriptorTbl* desc_tbl = nullptr);

  /// Empty d'tor to avoid issues with scoped_ptr.
  ~RuntimeState();

  /// Initializes the runtime filter bank and claims the initial buffer reservation
  /// for it.
  Status InitFilterBank(long runtime_filters_reservation_bytes);

  QueryState* query_state() const { return query_state_; }
  /// Return the query's ObjectPool
  ObjectPool* obj_pool() const;
  const DescriptorTbl& desc_tbl() const;
  const TQueryOptions& query_options() const;
  int batch_size() const { return query_options().batch_size; }
  bool abort_on_error() const { return query_options().abort_on_error; }
  bool strict_mode() const { return query_options().strict_mode; }
  bool decimal_v2() const { return query_options().decimal_v2; }
  const TQueryCtx& query_ctx() const;
  const TPlanFragmentInstanceCtx& instance_ctx() const { return *instance_ctx_; }
  const TUniqueId& session_id() const { return query_ctx().session.session_id; }
  const std::string& do_as_user() const { return query_ctx().session.delegated_user; }
  const std::string& connected_user() const {
    return query_ctx().session.connected_user;
  }
  const TimestampValue* now() const { return now_.get(); }
  const TimestampValue* utc_timestamp() const { return utc_timestamp_.get(); }
  void set_now(const TimestampValue* now);
  const TUniqueId& query_id() const { return query_ctx().query_id; }
  const TUniqueId& fragment_instance_id() const {
    return instance_ctx_ != nullptr
        ? instance_ctx_->fragment_instance_id
        : no_instance_id_;
  }
  ExecEnv* exec_env() { return exec_env_; }
  DataStreamMgrBase* stream_mgr();
  HBaseTableFactory* htable_factory();
  ImpalaBackendClientCache* impalad_client_cache();
  CatalogServiceClientCache* catalogd_client_cache();
  io::DiskIoMgr* io_mgr();
  MemTracker* instance_mem_tracker() { return instance_mem_tracker_.get(); }
  MemTracker* query_mem_tracker();  // reference to the query_state_'s memtracker
  ReservationTracker* instance_buffer_reservation() {
    return instance_buffer_reservation_.get();
  }
  ThreadResourceMgr::ResourcePool* resource_pool() { return resource_pool_; }

  FileMoveMap* hdfs_files_to_move() { return &hdfs_files_to_move_; }

  void set_fragment_root_id(PlanNodeId id) {
    DCHECK_EQ(root_node_id_, -1) << "Should not set this twice.";
    root_node_id_ = id;
  }

  /// The seed value to use when hashing tuples.
  /// See comment on root_node_id_. We add one to prevent having a hash seed of 0.
  uint32_t fragment_hash_seed() const { return root_node_id_ + 1; }

  RuntimeFilterBank* filter_bank() { return filter_bank_.get(); }

  PartitionStatusMap* per_partition_status() { return &per_partition_status_; }

  /// Returns runtime state profile
  RuntimeProfile* runtime_profile() { return profile_; }

  /// Returns the LlvmCodeGen object for this fragment instance.
  LlvmCodeGen* codegen() { return codegen_.get(); }

  const std::string& GetEffectiveUser() const;

  /// Add ScalarFnCall expression 'udf' to be codegen'd later if it's not disabled by
  /// query option. This is for cases in which the UDF cannot be interpreted or if the
  /// plan fragment doesn't contain any codegen enabled operator.
  void AddScalarFnToCodegen(ScalarFnCall* udf) { scalar_fns_to_codegen_.push_back(udf); }

  /// Returns true if there are ScalarFnCall expressions in the fragments which can't be
  /// interpreted. This should only be used after the Prepare() phase in which all
  /// expressions' Prepare() are invoked.
  bool ScalarFnNeedsCodegen() const { return !scalar_fns_to_codegen_.empty(); }

  /// Returns true if there is a hint to disable codegen. This can be true for single node
  /// optimization or expression evaluation request from FE to BE (see fe-support.cc).
  /// Note that this internal flag is advisory and it may be ignored if the fragment has
  /// any UDF which cannot be interpreted. See ScalarFnCall::Prepare() for details.
  inline bool CodegenHasDisableHint() const {
    return query_ctx().disable_codegen_hint;
  }

  /// Returns true iff there is a hint to disable codegen and all expressions in the
  /// fragment can be interpreted. This should only be used after the Prepare() phase
  /// in which all expressions' Prepare() are invoked.
  inline bool CodegenDisabledByHint() const {
    return CodegenHasDisableHint() && !ScalarFnNeedsCodegen();
  }

  /// Returns true if codegen is disabled by query option.
  inline bool CodegenDisabledByQueryOption() const {
    return query_options().disable_codegen;
  }

  /// Returns true if codegen should be enabled for this fragment. Codegen is enabled
  /// if all the following conditions hold:
  /// 1. it's enabled by query option
  /// 2. it's not disabled by internal hints or there are expressions in the fragment
  ///    which cannot be interpreted.
  inline bool ShouldCodegen() const {
    return !CodegenDisabledByQueryOption() && !CodegenDisabledByHint();
  }

  inline Status GetQueryStatus() {
    // Do a racy check for query_status_ to avoid unnecessary spinlock acquisition.
    if (UNLIKELY(!query_status_.ok())) {
      boost::lock_guard<SpinLock> l(query_status_lock_);
      return query_status_;
    }
    return Status::OK();
  }

  /// Log an error that will be sent back to the coordinator based on an instance of the
  /// ErrorMsg class. The runtime state aggregates log messages based on type with one
  /// exception: messages with the GENERAL type are not aggregated but are kept
  /// individually.
  bool LogError(const ErrorMsg& msg, int vlog_level = 1);

  /// Returns true if the error log has not reached max_errors_.
  bool LogHasSpace() {
    boost::lock_guard<SpinLock> l(error_log_lock_);
    return error_log_.size() < query_options().max_errors;
  }

  /// Returns true if there are entries in the error log.
  bool HasErrors() {
    boost::lock_guard<SpinLock> l(error_log_lock_);
    return !error_log_.empty();
  }

  /// Returns the error log lines as a string joined with '\n'.
  std::string ErrorLog();

  /// Copy error_log_ to *errors
  void GetErrors(ErrorLogMap* errors);

  /// Append all accumulated errors since the last call to this function to new_errors to
  /// be sent back to the coordinator
  void GetUnreportedErrors(ErrorLogMap* new_errors);

  /// Given an error message, determine whether execution should be aborted and, if so,
  /// return the corresponding error status. Otherwise, log the error and return
  /// Status::OK(). Execution is aborted if the ABORT_ON_ERROR query option is set to
  /// true or the error is not recoverable and should be handled upstream.
  Status LogOrReturnError(const ErrorMsg& message);

  bool is_cancelled() const { return is_cancelled_; }
  void set_is_cancelled() { is_cancelled_ = true; }

  RuntimeProfile::Counter* total_storage_wait_timer() {
    return total_storage_wait_timer_;
  }

  RuntimeProfile::Counter* total_network_send_timer() {
    return total_network_send_timer_;
  }

  RuntimeProfile::Counter* total_network_receive_timer() {
    return total_network_receive_timer_;
  }

  RuntimeProfile::ThreadCounters* total_thread_statistics() const {
   return total_thread_statistics_;
  }

  /// Sets query_status_ with err_msg if no error has been set yet.
  void SetQueryStatus(const std::string& err_msg) {
    boost::lock_guard<SpinLock> l(query_status_lock_);
    if (!query_status_.ok()) return;
    query_status_ = Status(err_msg);
  }

  /// Sets query_status_ to MEM_LIMIT_EXCEEDED and logs all the registered trackers.
  /// Subsequent calls to this will be no-ops.
  /// If 'failed_allocation_size' is not 0, then it is the size of the allocation (in
  /// bytes) that would have exceeded the limit allocated for 'tracker'.
  /// This value and tracker are only used for error reporting.
  /// If 'msg' is non-NULL, it will be appended to query_status_ in addition to the
  /// generic "Memory limit exceeded" error.
  /// Note that this interface is deprecated and MemTracker::LimitExceeded() should be
  /// used and the error status should be returned.
  void SetMemLimitExceeded(MemTracker* tracker,
      int64_t failed_allocation_size = 0, const ErrorMsg* msg = NULL);

  /// Returns a non-OK status if query execution should stop (e.g., the query was
  /// cancelled or a mem limit was exceeded). Exec nodes should check this periodically so
  /// execution doesn't continue if the query terminates abnormally. This should not be
  /// called after ReleaseResources().
  Status CheckQueryState();

  /// Create a codegen object accessible via codegen() if it doesn't exist already.
  Status CreateCodegen();

  /// Codegen all ScalarFnCall expressions in 'scalar_fns_to_codegen_'. If codegen fails
  /// for any expressions, return immediately with the error status. Once IMPALA-4233 is
  /// fixed, it's not fatal to fail codegen if the expression can be interpreted.
  /// TODO: Fix IMPALA-4233
  Status CodegenScalarFns();

  /// Helper to call QueryState::StartSpilling().
  Status StartSpilling(MemTracker* mem_tracker);

  /// Release resources and prepare this object for destruction. Can only be called once.
  void ReleaseResources();

  static const char* LLVM_CLASS_NAME;

 private:
  /// Allow TestEnv to use private methods for testing.
  friend class TestEnv;

  /// Set per-fragment state.
  void Init();

  /// Lock protecting error_log_
  SpinLock error_log_lock_;

  /// Logs error messages.
  ErrorLogMap error_log_;

  /// Global QueryState and original thrift descriptors for this fragment instance.
  QueryState* const query_state_;
  const TPlanFragmentCtx* const fragment_ctx_;
  const TPlanFragmentInstanceCtx* const instance_ctx_;

  /// only populated by the (const QueryCtx&, ExecEnv*, DescriptorTbl*) c'tor
  boost::scoped_ptr<QueryState> local_query_state_;

  /// Provides instance id if instance_ctx_ == nullptr
  TUniqueId no_instance_id_;

  /// Query-global timestamps for implementing now() and utc_timestamp(). Both represent
  /// the same point in time but now_ is in local time and utc_timestamp_ is in UTC.
  /// Set from query_globals_. Use pointer to avoid inclusion of timestampvalue.h and
  /// avoid clang issues.
  boost::scoped_ptr<TimestampValue> now_;
  boost::scoped_ptr<TimestampValue> utc_timestamp_;

  /// TODO: get rid of this and use ExecEnv::GetInstance() instead
  ExecEnv* exec_env_;
  boost::scoped_ptr<LlvmCodeGen> codegen_;

  /// Contains all ScalarFnCall expressions which need to be codegen'd.
  vector<ScalarFnCall*> scalar_fns_to_codegen_;

  /// Thread resource management object for this fragment's execution.  The runtime
  /// state is responsible for returning this pool to the thread mgr.
  ThreadResourceMgr::ResourcePool* resource_pool_ = nullptr;

  /// Temporary Hdfs files created, and where they should be moved to ultimately.
  /// Mapping a filename to a blank destination causes it to be deleted.
  FileMoveMap hdfs_files_to_move_;

  /// Records summary statistics for the results of inserts into Hdfs partitions.
  PartitionStatusMap per_partition_status_;

  RuntimeProfile* const profile_;

  /// Total time waiting in storage (across all threads)
  RuntimeProfile::Counter* total_storage_wait_timer_;

  /// Total time spent sending over the network (across all threads)
  RuntimeProfile::Counter* total_network_send_timer_;

  /// Total time spent receiving over the network (across all threads)
  RuntimeProfile::Counter* total_network_receive_timer_;

  /// Total CPU utilization for all threads in this plan fragment.
  RuntimeProfile::ThreadCounters* total_thread_statistics_;

  /// Memory usage of this fragment instance, a child of 'query_mem_tracker_'.
  boost::scoped_ptr<MemTracker> instance_mem_tracker_;

  /// Buffer reservation for this fragment instance - a child of the query buffer
  /// reservation. Non-NULL if 'query_state_' is not NULL.
  boost::scoped_ptr<ReservationTracker> instance_buffer_reservation_;

  /// if true, execution should stop with a CANCELLED status
  bool is_cancelled_ = false;

  /// if true, ReleaseResources() was called.
  bool released_resources_ = false;

  /// Non-OK if an error has occurred and query execution should abort. Used only for
  /// asynchronously reporting such errors (e.g., when a UDF reports an error), so this
  /// will not necessarily be set in all error cases.
  SpinLock query_status_lock_;
  Status query_status_;

  /// This is the node id of the root node for this plan fragment. This is used as the
  /// hash seed and has two useful properties:
  /// 1) It is the same for all exec nodes in a fragment, so the resulting hash values
  /// can be shared.
  /// 2) It is different between different fragments, so we do not run into hash
  /// collisions after data partitioning (across fragments). See IMPALA-219 for more
  /// details.
  PlanNodeId root_node_id_ = -1;

  /// Manages runtime filters that are either produced or consumed (or both!) by plan
  /// nodes that share this runtime state.
  boost::scoped_ptr<RuntimeFilterBank> filter_bank_;

  /// prohibit copies
  RuntimeState(const RuntimeState&);

};

#define RETURN_IF_CANCELLED(state) \
  do { \
    if (UNLIKELY((state)->is_cancelled())) return Status::CANCELLED; \
  } while (false)

}

#endif
