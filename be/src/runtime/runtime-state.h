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

#include "runtime/exec-env.h"
#include "gen-cpp/Types_types.h"  // for TUniqueId
#include "gen-cpp/ImpalaInternalService_types.h"  // for TQueryOptions
#include "util/runtime-profile.h"

namespace impala {

class DescriptorTbl;
class DiskIoMgr;
class ObjectPool;
class Status;
class ExecEnv;
class Expr;
class LlvmCodeGen;
class TimestampValue;
class MemLimit;

// Counts how many rows an INSERT query has added to a particular partition
// (partitions are identified by their partition keys: k1=v1/k2=v2
// etc. Unpartitioned tables have a single 'default' partition which is
// identified by the empty string.
typedef std::map<std::string, int64_t> PartitionRowCount;

// Tracks files to move from a temporary (key) to a final destination (value) as
// part of query finalization. If the destination is empty, the file is to be
// deleted.
typedef std::map<std::string, std::string> FileMoveMap;

// A collection of items that are part of the global state of a
// query and shared across all execution nodes of that query.
class RuntimeState {
 public:
  RuntimeState(const TUniqueId& fragment_instance_id,
               const TQueryOptions& query_options,
               const std::string& now, ExecEnv* exec_env);

  // RuntimeState for executing queries w/o a from clause.
  RuntimeState();
  // Empty d'tor to avoid issues with scoped_ptr.
  ~RuntimeState();

  // Set per-query state.
  Status Init(const TUniqueId& fragment_instance_id,
              const TQueryOptions& query_options,
              const std::string& now, ExecEnv* exec_env);

  ObjectPool* obj_pool() const { return obj_pool_.get(); }
  const DescriptorTbl& desc_tbl() const { return *desc_tbl_; }
  void set_desc_tbl(DescriptorTbl* desc_tbl) { desc_tbl_ = desc_tbl; }
  int batch_size() const { return query_options_.batch_size; }
  bool abort_on_error() const { return query_options_.abort_on_error; }
  bool abort_on_default_limit_exceeded() const {
    return query_options_.abort_on_default_limit_exceeded;
  }
  int max_errors() const { return query_options_.max_errors; }
  int max_io_buffers() const { return query_options_.max_io_buffers; }
  int num_scanner_threads() const { return query_options_.num_scanner_threads; }
  const TimestampValue* now() const { return now_.get(); }
  void set_now(const TimestampValue* now);
  const std::vector<std::string>& error_log() const { return error_log_; }
  const std::vector<std::pair<std::string, int> >& file_errors() const {
    return file_errors_;
  }
  const TUniqueId& fragment_instance_id() const { return fragment_instance_id_; }
  ExecEnv* exec_env() { return exec_env_; }
  DataStreamMgr* stream_mgr() { return exec_env_->stream_mgr(); }
  HdfsFsCache* fs_cache() { return exec_env_->fs_cache(); }
  HBaseTableFactory* htable_factory() { return exec_env_->htable_factory(); }
  ImpalaInternalServiceClientCache* client_cache() { return exec_env_->client_cache(); }
  DiskIoMgr* io_mgr() { return exec_env_->disk_io_mgr(); }
  std::vector<MemLimit*>* mem_limits() { return &mem_limits_; }

  FileMoveMap* hdfs_files_to_move() { return &hdfs_files_to_move_; }
  PartitionRowCount* num_appended_rows() { return &num_appended_rows_; }

  // Returns runtime state profile
  RuntimeProfile* runtime_profile() { return &profile_; }

  // Returns CodeGen object.  Returns NULL if codegen is disabled.
  LlvmCodeGen* llvm_codegen() { return codegen_.get(); }

  // Appends error to the error_log_ if there is space
  void LogError(const std::string& error);

  // Returns true if the error log has not reached max_errors_.
  bool LogHasSpace() {
    boost::lock_guard<boost::mutex> l(error_log_lock_);
    return error_log_.size() < query_options_.max_errors;
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

 private:
  static const int DEFAULT_BATCH_SIZE = 1024;

  DescriptorTbl* desc_tbl_;
  boost::scoped_ptr<ObjectPool> obj_pool_;

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

  // Query-global timestamp, e.g., for implementing now().
  // Use pointer to avoid inclusion of timestampvalue.h and avoid clang issues.
  boost::scoped_ptr<TimestampValue> now_;

  TUniqueId fragment_instance_id_;
  TQueryOptions query_options_;
  ExecEnv* exec_env_;
  boost::scoped_ptr<LlvmCodeGen> codegen_;

  // Temporary Hdfs files created, and where they should be moved to ultimately.
  // Mapping a filename to a blank destination causes it to be deleted.
  FileMoveMap hdfs_files_to_move_;

  // Records the total number of appended rows per created Hdfs partition
  PartitionRowCount num_appended_rows_;

  RuntimeProfile profile_;

  // all mem limits that apply to this query
  std::vector<MemLimit*> mem_limits_;

  // if true, execution should stop with a CANCELLED status
  bool is_cancelled_;

  // prohibit copies
  RuntimeState(const RuntimeState&);

  // set codegen_
  Status CreateCodegen();
};

#define RETURN_IF_CANCELLED(state) \
  do { \
    if (UNLIKELY((state)->is_cancelled())) return Status(TStatusCode::CANCELLED); \
  } while (false)

}

#endif
