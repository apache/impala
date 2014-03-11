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


#ifndef IMPALA_RUNTIME_RUNTIME_STATE_H
#define IMPALA_RUNTIME_RUNTIME_STATE_H

// needed for scoped_ptr to work on ObjectPool
#include "common/object-pool.h"

#include <boost/scoped_ptr.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>
#include <vector>
#include <string>
// stringstream is a typedef, so can't forward declare it.
#include <sstream>

#include "statestore/query-resource-mgr.h"
#include "runtime/exec-env.h"
#include "runtime/descriptors.h"  // for PlanNodeId
#include "runtime/disk-io-mgr.h"  // for DiskIoMgr::ReaderContext
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/thread-resource-mgr.h"
#include "gen-cpp/PlanNodes_types.h"
#include "gen-cpp/Types_types.h"  // for TUniqueId
#include "gen-cpp/ImpalaInternalService_types.h"  // for TQueryOptions
#include "util/runtime-profile.h"

namespace impala {

class DescriptorTbl;
class ObjectPool;
class Status;
class ExecEnv;
class Expr;
class LlvmCodeGen;
class TimestampValue;
class DataStreamRecvr;

// Counts how many rows an INSERT query has added to a particular partition
// (partitions are identified by their partition keys: k1=v1/k2=v2
// etc. Unpartitioned tables have a single 'default' partition which is
// identified by the empty string.
typedef std::map<std::string, int64_t> PartitionRowCount;

// Stats per partition for insert queries. They key is the same as for PartitionRowCount
typedef std::map<std::string, TInsertStats> PartitionInsertStats;

// Tracks files to move from a temporary (key) to a final destination (value) as
// part of query finalization. If the destination is empty, the file is to be
// deleted.
typedef std::map<std::string, std::string> FileMoveMap;

// A collection of items that are part of the global state of a
// query and shared across all execution nodes of that query.
class RuntimeState {
 public:
  RuntimeState(const TUniqueId& query_id, const TUniqueId& fragment_instance_id,
      const TQueryContext& query_ctxt, const std::string& cgroup, ExecEnv* exec_env);

  // RuntimeState for executing expr in fe-support.
  RuntimeState(const TQueryContext& query_ctxt);

  // Empty d'tor to avoid issues with scoped_ptr.
  ~RuntimeState();

  // Set up five-level hierarchy of mem trackers: process, pool, query, fragment
  // instance. The instance tracker is tied to our profile. Specific parts of the
  // fragment (i.e. exec nodes, sinks, data stream senders, etc) will add a fifth level
  // when they are initialized. This function also initializes a user function mem
  // tracker (in the fifth level). If 'request_pool' is null, no request pool mem
  // tracker is set up, i.e. query pools will have the process mem pool as the parent.
  Status InitMemTrackers(const TUniqueId& query_id, const std::string* request_pool,
      int64_t query_bytes_limit);

  ObjectPool* obj_pool() const { return obj_pool_.get(); }
  const DescriptorTbl& desc_tbl() const { return *desc_tbl_; }
  void set_desc_tbl(DescriptorTbl* desc_tbl) { desc_tbl_ = desc_tbl; }
  const TQueryOptions& query_options() const {
    return query_ctxt_.request.query_options;
  }
  int batch_size() const { return query_ctxt_.request.query_options.batch_size; }
  bool abort_on_error() const {
    return query_ctxt_.request.query_options.abort_on_error;
  }
  bool abort_on_default_limit_exceeded() const {
    return query_ctxt_.request.query_options.abort_on_default_limit_exceeded;
  }
  int max_errors() const { return query_ctxt_.request.query_options.max_errors; }
  const TQueryContext& query_ctxt() const { return query_ctxt_; }
  const std::string& connected_user() const { return query_ctxt_.session.connected_user; }
  const TimestampValue* now() const { return now_.get(); }
  void set_now(const TimestampValue* now);
  const std::vector<std::string>& error_log() const { return error_log_; }
  const std::vector<std::pair<std::string, int> >& file_errors() const {
    return file_errors_;
  }
  const TUniqueId& query_id() const { return query_id_; }
  const TUniqueId& fragment_instance_id() const { return fragment_instance_id_; }
  const std::string& cgroup() const { return cgroup_; }
  ExecEnv* exec_env() { return exec_env_; }
  DataStreamMgr* stream_mgr() { return exec_env_->stream_mgr(); }
  HBaseTableFactory* htable_factory() { return exec_env_->htable_factory(); }
  ImpalaInternalServiceClientCache* impalad_client_cache() {
    return exec_env_->impalad_client_cache();
  }
  CatalogServiceClientCache* catalogd_client_cache() {
    return exec_env_->catalogd_client_cache();
  }
  DiskIoMgr* io_mgr() { return exec_env_->disk_io_mgr(); }
  MemTracker* instance_mem_tracker() { return instance_mem_tracker_.get(); }
  MemTracker* query_mem_tracker() { return query_mem_tracker_.get(); }
  ThreadResourceMgr::ResourcePool* resource_pool() { return resource_pool_; }

  FileMoveMap* hdfs_files_to_move() { return &hdfs_files_to_move_; }
  PartitionRowCount* num_appended_rows() { return &num_appended_rows_; }
  PartitionInsertStats* insert_stats() { return &insert_stats_; }
  std::vector<DiskIoMgr::ReaderContext*>* reader_contexts() { return &reader_contexts_; }

  // Returns runtime state profile
  RuntimeProfile* runtime_profile() { return &profile_; }

  // Returns true if codegen is enabled for this query.
  bool codegen_enabled() const {
    return !query_ctxt_.request.query_options.disable_codegen;
  }

  // Returns CodeGen object.  Returns NULL if the codegen object has not been
  // created. If codegen is enabled for the query, the codegen object will be
  // created as part of the RuntimeState's initialization.
  // Otherwise, it can be created by calling CreateCodegen().
  LlvmCodeGen* codegen() { return codegen_.get(); }

  // Create a codegen object in codegen_. No-op if it has already been called.
  // If codegen is enabled for the query, this is created when the runtime
  // state is created. If codegen is disabled for the query, this is created
  // on first use.
  Status CreateCodegen();

  Status query_status() {
    boost::lock_guard<boost::mutex> l(query_status_lock_);
    return query_status_;
  };

  MemPool* udf_pool() { return udf_pool_.get(); };

  // Create and return a stream receiver for fragment_instance_id_
  // from the data stream manager. The receiver is added to data_stream_recvrs_pool_.
  DataStreamRecvr* CreateRecvr(
      const RowDescriptor& row_desc, PlanNodeId dest_node_id, int num_senders,
      int buffer_size, RuntimeProfile* profile);

  // Appends error to the error_log_ if there is space. Returns true if there was space
  // and the error was logged.
  bool LogError(const std::string& error);

  // If !status.ok(), appends the error to the error_log_
  void LogError(const Status& status);

  // Returns true if the error log has not reached max_errors_.
  bool LogHasSpace() {
    boost::lock_guard<boost::mutex> l(error_log_lock_);
    return error_log_.size() < query_ctxt_.request.query_options.max_errors;
  }

  // Report that num_errors occurred while parsing file_name.
  void ReportFileErrors(const std::string& file_name, int num_errors);

  // Clear the file errors.
  void ClearFileErrors() { file_errors_.clear(); }

  // Return true if error log is empty.
  bool ErrorLogIsEmpty();

  // Returns the error log lines as a string joined with '\n'.
  std::string ErrorLog();

  // Append all error_log_[unreported_error_idx_+] to new_errors and set
  // unreported_error_idx_ to errors_log_.size()
  void GetUnreportedErrors(std::vector<std::string>* new_errors);

  // Returns a string representation of the file_errors_.
  std::string FileErrors() const;

  bool is_cancelled() const { return is_cancelled_; }
  void set_is_cancelled(bool v) { is_cancelled_ = v; }

  RuntimeProfile::Counter* total_cpu_timer() { return total_cpu_timer_; }
  RuntimeProfile::Counter* total_storage_wait_timer() {
    return total_storage_wait_timer_;
  }
  RuntimeProfile::Counter* total_network_send_timer() {
    return total_network_send_timer_;
  }
  RuntimeProfile::Counter* total_network_receive_timer() {
    return total_network_receive_timer_;
  }

  // Sets query_status_ with err_msg if no error has been set yet.
  void set_query_status(const std::string& err_msg) {
    boost::lock_guard<boost::mutex> l(query_status_lock_);
    if (!query_status_.ok()) return;
    query_status_ = Status(err_msg);
  }

  // Sets query_status_ to MEM_LIMIT_EXCEEDED and logs all the registered trackers.
  // Subsequent calls to this will be no-ops.
  // If failed_allocation_size is not 0, then it is the size of the allocation (in
  // bytes) that would have exceeded the limit allocated for 'tracker'.
  // This value and tracker are only used for error reporting.
  Status SetMemLimitExceeded(MemTracker* tracker = NULL,
      int64_t failed_allocation_size = 0);

  // Returns a non-OK status if query execution should stop (e.g., the query was cancelled
  // or a mem limit was exceeded). Exec nodes should check this periodically so execution
  // doesn't continue if the query terminates abnormally.
  Status CheckQueryState();

  QueryResourceMgr* query_resource_mgr() const { return query_resource_mgr_; }
  void SetQueryResourceMgr(QueryResourceMgr* res_mgr) { query_resource_mgr_ = res_mgr; }

 private:
  // Set per-fragment state.
  Status Init(const TUniqueId& fragment_instance_id, ExecEnv* exec_env);

  static const int DEFAULT_BATCH_SIZE = 1024;

  DescriptorTbl* desc_tbl_;
  boost::scoped_ptr<ObjectPool> obj_pool_;

  // Data stream receivers created by a plan fragment are gathered here to make sure
  // they are destroyed before obj_pool_ (class members are destroyed in reverse order).
  // Receivers depend on the descriptor table and we need to guarantee that their control
  // blocks are removed from the data stream manager before the objects in the
  // descriptor table are destroyed.
  boost::scoped_ptr<ObjectPool> data_stream_recvrs_pool_;

  // Lock protecting error_log_ and unreported_error_idx_
  boost::mutex error_log_lock_;

  // Logs error messages.
  std::vector<std::string> error_log_;

  // error_log_[unreported_error_idx_+] has been not reported to the coordinator.
  int unreported_error_idx_;

  // Lock protecting file_errors_
  mutable boost::mutex file_errors_lock_;

  // Stores the number of parse errors per file.
  std::vector<std::pair<std::string, int> > file_errors_;

  // Query-global parameters used for preparing exprs as now(), user(), etc. that should
  // return a consistent result regardless which impalad is evaluating the expr.
  TQueryContext query_ctxt_;

  // Query-global timestamp, e.g., for implementing now(). Set from query_globals_.
  // Use pointer to avoid inclusion of timestampvalue.h and avoid clang issues.
  boost::scoped_ptr<TimestampValue> now_;

  TUniqueId query_id_;
  TUniqueId fragment_instance_id_;

  // The Impala-internal cgroup into which execution threads are assigned.
  // If empty, no RM is enabled.
  std::string cgroup_;
  ExecEnv* exec_env_;
  boost::scoped_ptr<LlvmCodeGen> codegen_;

  // Thread resource management object for this fragment's execution.  The runtime
  // state is responsible for returning this pool to the thread mgr.
  ThreadResourceMgr::ResourcePool* resource_pool_;

  // Temporary Hdfs files created, and where they should be moved to ultimately.
  // Mapping a filename to a blank destination causes it to be deleted.
  FileMoveMap hdfs_files_to_move_;

  // Records the total number of appended rows per created Hdfs partition
  PartitionRowCount num_appended_rows_;

  // Insert stats per partition.
  PartitionInsertStats insert_stats_;

  RuntimeProfile profile_;

  // Total CPU time (across all threads), including all wait times.
  RuntimeProfile::Counter* total_cpu_timer_;

  // Total time waiting in storage (across all threads)
  RuntimeProfile::Counter* total_storage_wait_timer_;

  // Total time spent sending over the network (across all threads)
  RuntimeProfile::Counter* total_network_send_timer_;

  // Total time spent receiving over the network (across all threads)
  RuntimeProfile::Counter* total_network_receive_timer_;

  // MemTracker that is shared by all fragment instances running on this host.
  // The query mem tracker must be released after the instance_mem_tracker_.
  boost::shared_ptr<MemTracker> query_mem_tracker_;

  // Memory usage of this fragment instance
  boost::scoped_ptr<MemTracker> instance_mem_tracker_;

  // if true, execution should stop with a CANCELLED status
  bool is_cancelled_;

  // Non-OK if an error has occurred and query execution should abort. Used only for
  // asynchronously reporting such errors (e.g., when a UDF reports an error), so this
  // will not necessarily be set in all error cases.
  boost::mutex query_status_lock_;
  Status query_status_;

  // Memory tracker and pool for UDFs
  // TODO: this is a stopgap until we implement ExprContext
  boost::scoped_ptr<MemTracker> udf_mem_tracker_;
  boost::scoped_ptr<MemPool> udf_pool_;

  // Query-wide resource manager for resource expansion etc. Not owned by us; owned by the
  // ResourceBroker instead.
  QueryResourceMgr* query_resource_mgr_;

  // Reader contexts that need to be closed when the fragment is closed.
  std::vector<DiskIoMgr::ReaderContext*> reader_contexts_;

  // prohibit copies
  RuntimeState(const RuntimeState&);
};

#define RETURN_IF_CANCELLED(state) \
  do { \
    if (UNLIKELY((state)->is_cancelled())) return Status(TStatusCode::CANCELLED); \
  } while (false)

}

#endif
