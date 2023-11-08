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

#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include <boost/scoped_ptr.hpp>

// NOTE: try not to add more headers here: runtime-state.h is included in many many files.
#include "common/global-types.h"  // for PlanNodeId
#include "common/atomic.h"
#include "runtime/client-cache-types.h"
#include "runtime/dml-exec-state.h"
#include "util/error-util-internal.h"
#include "util/runtime-profile.h"
#include "gen-cpp/ImpalaInternalService_types.h"

namespace impala {

class BufferPool;
class DataStreamRecvr;
class DescriptorTbl;
class Expr;
class KrpcDataStreamMgr;
class LlvmCodeGen;
class MemTracker;
class ObjectPool;
class ReservationTracker;
class RuntimeFilterBank;
class ScalarExpr;
class Status;
class TimestampValue;
class ThreadResourcePool;
class TUniqueId;
class ExecEnv;
class HBaseTableFactory;
class TPlanFragment;
class TPlanFragmentInstanceCtx;
class QueryState;
class ConditionVariable;
class CyclicBarrier;

namespace io {
  class DiskIoMgr;
}

/// Shared state for Impala's runtime query execution. Used in two contexts:
/// * Within execution of a fragment instance to hold shared state for the fragment
///   instance (i.e. there is a 1:1 relationship with FragmentInstanceState).
/// * A standalone mode for other cases where query execution infrastructure is used
///   outside the context of a fragment instance, e.g. tests, evaluation of constant
///   expressions, etc. In this case the RuntimeState sets up all the required
///   infrastructure.
///
/// RuntimeState is shared between multiple threads and so methods must generally be
/// thread-safe.
///
/// After initialisation, callers must call ReleaseResources() to ensure that all
/// resources are correctly freed before destruction.
class RuntimeState {
 public:
  /// query_state, fragment_ctx, and instance_ctx need to be alive at least as long as
  /// the constructed RuntimeState
  RuntimeState(QueryState* query_state, const TPlanFragment& fragment,
      const TPlanFragmentInstanceCtx& instance_ctx,
      const PlanFragmentCtxPB& fragment_ctx,
      const PlanFragmentInstanceCtxPB& instance_ctx_pb, ExecEnv* exec_env);

  /// RuntimeState for test execution and fe-support.cc. Creates its own QueryState and
  /// installs desc_tbl, if set. If query_ctx.request_pool isn't set, sets it to "test-pool".
  RuntimeState(
      const TQueryCtx& query_ctx, ExecEnv* exec_env, DescriptorTbl* desc_tbl = nullptr);

  /// Empty d'tor to avoid issues with scoped_ptr.
  ~RuntimeState();

  QueryState* query_state() const { return query_state_; }
  /// Return the query's ObjectPool
  ObjectPool* obj_pool() const;
  const DescriptorTbl& desc_tbl() const;
  const TQueryOptions& query_options() const;
  int batch_size() const { return query_options().batch_size; }
  bool abort_on_error() const { return query_options().abort_on_error; }
  bool strict_mode() const { return query_options().strict_mode; }
  bool utf8_mode() const { return query_options().utf8_mode; }
  bool decimal_v2() const { return query_options().decimal_v2; }
  bool abort_java_udf_on_exception() const {
     return query_options().abort_java_udf_on_exception;
  }
  const TQueryCtx& query_ctx() const;
  const TPlanFragment& fragment() const { return *fragment_; }
  const TPlanFragmentInstanceCtx& instance_ctx() const { return *instance_ctx_; }
  const PlanFragmentCtxPB& fragment_ctx() const { return *fragment_ctx_; }
  const PlanFragmentInstanceCtxPB& instance_ctx_pb() const { return *instance_ctx_pb_; }
  const TUniqueId& session_id() const { return query_ctx().session.session_id; }
  const std::string& do_as_user() const { return query_ctx().session.delegated_user; }
  const std::string& connected_user() const {
    return query_ctx().session.connected_user;
  }
  const TimestampValue* now() const { return now_.get(); }
  const TimestampValue* utc_timestamp() const { return utc_timestamp_.get(); }
  void set_now(const TimestampValue* now);
  const Timezone* local_time_zone() const { return local_time_zone_; }
  const Timezone* time_zone_for_unix_time_conversions() const {
    return time_zone_for_unix_time_conversions_;
  }
  const TUniqueId& query_id() const { return query_ctx().query_id; }
  const TUniqueId& fragment_instance_id() const {
    return instance_ctx_ != nullptr
        ? instance_ctx_->fragment_instance_id
        : no_instance_id_;
  }
  MemTracker* instance_mem_tracker() { return instance_mem_tracker_; }
  MemTracker* query_mem_tracker();  // reference to the query_state_'s memtracker
  ReservationTracker* instance_buffer_reservation() {
    return instance_buffer_reservation_;
  }
  ThreadResourcePool* resource_pool() { return resource_pool_.get(); }

  void set_fragment_root_id(PlanNodeId id) {
    DCHECK_EQ(root_node_id_, -1) << "Should not set this twice.";
    root_node_id_ = id;
  }

  /// The seed value to use when hashing tuples.
  /// See comment on root_node_id_. We add one to prevent having a hash seed of 0.
  uint32_t fragment_hash_seed() const { return root_node_id_ + 1; }

  RuntimeFilterBank* filter_bank() const;

  DmlExecState* dml_exec_state() { return &dml_exec_state_; }

  /// Returns runtime state profile
  RuntimeProfile* runtime_profile() { return profile_; }

  const std::string& GetEffectiveUser() const;

  inline Status GetQueryStatus() {
    if (UNLIKELY(!is_query_status_ok_.Load())) {
      std::lock_guard<SpinLock> l(query_status_lock_);
      return query_status_;
    }
    return Status::OK();
  }

  /// Return maximum number of non-fatal error to report to client through coordinator.
  /// max_errors does not indicate how many errors in total have been recorded, but rather
  /// how many are distinct. It is defined as the sum of the number of generic errors and
  /// the number of distinct other errors. Default to 100 if non-positive number is
  /// specified in max_errors query option.
  inline int max_errors() const {
    return query_options().max_errors <= 0 ? 100 : query_options().max_errors;
  }

  /// Log an error that will be sent back to the coordinator based on an instance of the
  /// ErrorMsg class. The runtime state aggregates log messages based on type with one
  /// exception: messages with the GENERAL type are not aggregated but are kept
  /// individually.
  bool LogError(const ErrorMsg& msg, int vlog_level = 1);

  /// Returns true if the error log has not reached max_errors_.
  bool LogHasSpace() {
    std::lock_guard<SpinLock> l(error_log_lock_);
    return error_log_.size() < query_options().max_errors;
  }

  /// Returns true if there are entries in the error log.
  bool HasErrors() {
    std::lock_guard<SpinLock> l(error_log_lock_);
    return !error_log_.empty();
  }

  /// Returns the error log lines as a string joined with '\n'.
  std::string ErrorLog();

  /// Clear 'new_errors' and append all accumulated errors since the last call to this
  /// function to 'new_errors' to be sent back to the coordinator. This has the side
  /// effect of clearing out the internal error log map once this function returns.
  void GetUnreportedErrors(ErrorLogMapPB* new_errors);

  /// Given an error message, determine whether execution should be aborted and, if so,
  /// return the corresponding error status. Otherwise, log the error and return
  /// Status::OK(). Execution is aborted if the ABORT_ON_ERROR query option is set to
  /// true or the error is not recoverable and should be handled upstream.
  Status LogOrReturnError(const ErrorMsg& message);

  bool is_cancelled() const { return is_cancelled_.Load(); }

  /// Cancel this runtime state, signalling all condition variables and cancelling all
  /// barriers added in AddCancellationCV() and AddBarrierToCancel(). This function will
  /// acquire mutexes added in AddCancellationCV(), so the caller must not hold any locks
  /// that must acquire after those mutexes in the lock order.
  void Cancel();

  /// Add a condition variable to be signalled when this RuntimeState is cancelled.
  /// Adding a condition variable multiple times is a no-op. Each distinct 'cv' will be
  /// signalled once with NotifyAll() when is_cancelled() becomes true. 'mutex' will
  /// be acquired by the cancelling thread after is_cancelled() becomes true. The caller
  /// must hold 'mutex' when checking is_cancelled() to avoid a race like IMPALA-9611
  /// where the notification on 'cv' is lost.
  /// The condition variable must have query lifetime.
  void AddCancellationCV(std::mutex* mutex, ConditionVariable* cv);

  /// Add a barrier to be cancelled when this RuntimeState is cancelled. Adding a barrier
  /// multiple times is a no-op. Each distinct 'cb' will be cancelled with status code
  /// CANCELLED_INTERNALLY when is_cancelled() becomes true. 'cb' must have query
  /// lifetime.
  void AddBarrierToCancel(CyclicBarrier* cb);

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

  void AddBytesReadCounter(RuntimeProfile::Counter* counter) {
    bytes_read_counters_.push_back(counter);
  }

  void AddBytesSentCounter(RuntimeProfile::Counter* counter) {
    bytes_sent_counters_.push_back(counter);
  }

  /// Computes the ratio between the bytes sent and the bytes read by this runtime state's
  /// fragment instance. For fragment instances that don't scan data, this returns 0.
  double ComputeExchangeScanRatio() const;

  /// Sets query_status_ with err_msg if no error has been set yet.
  void SetQueryStatus(const std::string& err_msg) {
    std::lock_guard<SpinLock> l(query_status_lock_);
    if (!query_status_.ok()) return;
    query_status_ = Status(err_msg);
    bool set_query_status_ok_ = is_query_status_ok_.CompareAndSwap(true, false);
    DCHECK(set_query_status_ok_);
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

  /// Helper to call QueryState::StartSpilling().
  Status StartSpilling(MemTracker* mem_tracker);

  /// Release resources and prepare this object for destruction. Can only be called once.
  void ReleaseResources();

  /// If the fragment instance associated with this RuntimeState failed due to a RPC
  /// failure, use this method to set the network address of the RPC's target node and
  /// the posix error code of the failed RPC. The target node address and posix error code
  /// will be included in the AuxErrorInfo returned by GetAuxErrorInfo. This method is
  /// idempotent.
  void SetRPCErrorInfo(NetworkAddressPB dest_node, int16_t posix_error_code);

  /// Returns true if this RuntimeState has any auxiliary error information, false
  /// otherwise. Currently, only SetRPCErrorInfo() sets aux error info.
  bool HasAuxErrorInfo() {
    std::lock_guard<SpinLock> l(aux_error_info_lock_);
    return aux_error_info_ != nullptr;
  }

  /// Sets the given AuxErrorInfoPB with all relevant aux error info from the fragment
  /// instance associated with this RuntimeState. If no aux error info for this
  /// RuntimeState has been set, this method does nothing. Currently, only
  /// SetRPCErrorInfo() sets aux error info. This method clears aux_error_info_. Calls to
  /// HasAuxErrorInfo() after this method has been called will return false.
  void GetUnreportedAuxErrorInfo(AuxErrorInfoPB* aux_error_info);

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

  /// Track how many error has been printed to VLOG(1).
  int64_t vlog_1_errors = 0;

  /// Global QueryState and original thrift descriptors for this fragment instance.
  QueryState* const query_state_;
  const TPlanFragment* const fragment_;
  const TPlanFragmentInstanceCtx* const instance_ctx_;
  const PlanFragmentCtxPB* const fragment_ctx_;
  const PlanFragmentInstanceCtxPB* const instance_ctx_pb_;

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

  /// Query-global timezone used as local timezone when executing the query.
  /// Owned by a static storage member of TimezoneDatabase class.
  const Timezone* local_time_zone_;

  /// Query-global timezone used during int<->timestamp conversions. UTC by default, but
  /// if use_local_tz_for_unix_timestamp_conversions=1, then the local_time_zone_ is used
  /// instead.
  const Timezone* time_zone_for_unix_time_conversions_;

  /// Thread resource management object for this fragment's execution.  The runtime
  /// state is responsible for returning this pool to the thread mgr.
  std::unique_ptr<ThreadResourcePool> resource_pool_;

  /// Execution state for DML statements.
  DmlExecState dml_exec_state_;

  RuntimeProfile* const profile_;

  /// Total time waiting in storage (across all threads)
  RuntimeProfile::Counter* total_storage_wait_timer_;

  /// Total time spent waiting for RPCs to complete. This time is a combination of:
  /// - network time of sending the RPC payload to the destination
  /// - processing and queuing time in the destination
  /// - network time of sending the RPC response to the originating node
  /// TODO: rename this counter and account for the 3 components above. IMPALA-6705.
  RuntimeProfile::Counter* total_network_send_timer_;

  /// Total time spent receiving over the network (across all threads)
  RuntimeProfile::Counter* total_network_receive_timer_;

  /// Total CPU utilization for all threads in this plan fragment.
  RuntimeProfile::ThreadCounters* total_thread_statistics_;

  /// BytesRead counters in this instance's tree, not owned.
  std::vector<RuntimeProfile::Counter*> bytes_read_counters_;

  /// Counters for bytes sent over the network in this instance's tree, not owned.
  std::vector<RuntimeProfile::Counter*> bytes_sent_counters_;

  /// Memory usage of this fragment instance, a child of 'query_mem_tracker_'. Owned by
  /// 'query_state_' and destroyed with the rest of the query's MemTracker hierarchy.
  /// See IMPALA-8270 for a reason why having the QueryState own this is important.
  MemTracker* instance_mem_tracker_ = nullptr;

  /// Buffer reservation for this fragment instance - a child of the query buffer
  /// reservation. Non-NULL if this is a finstance's RuntimeState used for query
  /// execution. Owned by 'query_state_'.
  ReservationTracker* const instance_buffer_reservation_;

  /// If true, execution should stop, either because the query was cancelled by the
  /// client, or because execution of the fragment instance is finished. If the main
  /// fragment instance thread is still running, it should terminate with a CANCELLED
  /// status once it notices is_cancelled_ == true.
  AtomicBool is_cancelled_{false};

  /// Condition variables that will be signalled by Cancel(). Protected by
  /// 'cancellation_cvs_lock_'.
  std::vector<std::pair<std::mutex*, ConditionVariable*>> cancellation_cvs_;

  /// Cyclic barriers that will be signalled by Cancel(). Protected by
  /// 'cancellation_cvs_lock_'.
  std::vector<CyclicBarrier*> cancellation_cbs_;

  /// Condition variables that will be signalled by Cancel(). Protected by
  /// 'cancellation_cvs_lock_'.
  SpinLock cancellation_cvs_lock_;

  /// if true, ReleaseResources() was called.
  bool released_resources_ = false;

  /// Non-OK if an error has occurred and query execution should abort. Used only for
  /// asynchronously reporting such errors (e.g., when a UDF reports an error), so this
  /// will not necessarily be set in all error cases.
  SpinLock query_status_lock_;
  Status query_status_;

  /// True if the query_status_ is OK, false otherwise. Used to check if the
  /// query_status_ is OK without incurring the overhead of acquiring the
  /// query_status_lock_.
  AtomicBool is_query_status_ok_{true};

  /// This is the node id of the root node for this plan fragment.
  ///
  /// This is used as the hash seed within the fragment so we do not run into hash
  /// collisions after data partitioning (across fragments). See IMPALA-219 for more
  /// details.
  PlanNodeId root_node_id_ = -1;

  /// Lock protecting aux_error_info_.
  SpinLock aux_error_info_lock_;

  /// Auxiliary error information, only set if the fragment instance failed (e.g.
  /// query_status_ != Status::OK()). Owned by this RuntimeState.
  std::unique_ptr<AuxErrorInfoPB> aux_error_info_;

  /// True if aux_error_info_ has been sent in a status report, false otherwise.
  bool reported_aux_error_info_ = false;

  /// prohibit copies
  RuntimeState(const RuntimeState&);
};

#define RETURN_IF_CANCELLED(state) \
  do { \
    if (UNLIKELY((state)->is_cancelled())) return Status::CANCELLED; \
  } while (false)

}

#endif
