// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_RUNTIME_RUNTIME_STATE_H
#define IMPALA_RUNTIME_RUNTIME_STATE_H

#include <boost/scoped_ptr.hpp>
#include <vector>
#include <string>
// stringstream is a typedef, so can't forward declare it.
#include <sstream>

#include "gen-cpp/Types_types.h"  // for TUniqueId

namespace impala {

class HdfsFsCache;
class DescriptorTbl;
class ObjectPool;
class Status;
class DataStreamMgr;

// A collection of items that are part of the global state of a 
// query and potentially shared across execution nodes.
class RuntimeState {
 public:
  RuntimeState(const TUniqueId& query_id, bool abort_on_error, int max_errors,
               DataStreamMgr* stream_mgr, HdfsFsCache* fs_cache);

  ObjectPool* obj_pool() const { return obj_pool_.get(); }
  const DescriptorTbl& desc_tbl() const { return *desc_tbl_; }
  void set_desc_tbl(DescriptorTbl* desc_tbl) { desc_tbl_ = desc_tbl; }
  int batch_size() const { return batch_size_; }
  void set_batch_size(int batch_size) { batch_size_ = batch_size; }
  int file_buffer_size() const { return file_buffer_size_; }
  bool abort_on_error() const { return abort_on_error_; }
  int max_errors() const { return max_errors_; }
  const std::vector<std::string>& error_log() const { return error_log_; }
  // Get error stream for appending error message snippets.
  std::stringstream& error_stream() { return error_stream_; }
  const std::vector<std::pair<std::string, int> >& file_errors() const {
    return file_errors_;
  }
  const TUniqueId& query_id() const { return query_id_; }
  static void* hbase_conf() { return hbase_conf_; }
  DataStreamMgr* stream_mgr() { return stream_mgr_; }
  HdfsFsCache* fs_cache() { return fs_cache_; }

  // Creates a global reference to a new HBaseConfiguration object via JniUtil.
  // Cleanup is done in JniUtil::Cleanup().
  // Returns Status::OK if created successfully, non-ok status otherwise.
  static Status InitHBaseConf();

  // Append contents of error_stream_ as one message to error_log_. Clears error_stream_.
  void LogErrorStream();

  // Returns true if the error log has not reached max_errors_.
  bool LogHasSpace() const { return error_log_.size() < max_errors_; }

  // Clears the error log.
  void ClearErrorLog() { error_log_.clear(); }

  // Report that num_errors occurred while parsing file_name.
  void ReportFileErrors(const std::string& file_name, int num_errors);

  // Clear the file errors.
  void ClearFileErrors() { file_errors_.clear(); }

  // Returns the error log lines as a string joined with '\n'.
  std::string ErrorLog() const;

  // Returns a string representation of the file_errors_.
  std::string FileErrors() const;

 private:
  static const int DEFAULT_BATCH_SIZE = 1024;
  static const int DEFAULT_FILE_BUFFER_SIZE = 128 * 1024;

  DescriptorTbl* desc_tbl_;
  boost::scoped_ptr<ObjectPool> obj_pool_;
  int batch_size_;
  int file_buffer_size_;
  // A buffer for error messages.
  // The entire error stream is written to the error_log_ in log_error_stream().
  std::stringstream error_stream_;
  // Logs error messages.
  std::vector<std::string> error_log_;
  // Stores the number of parse errors per file.
  std::vector<std::pair<std::string, int> > file_errors_;
  // Whether to abort if an error is encountered.
  const bool abort_on_error_;
  // Maximum number of errors to log.
  const int max_errors_;
  // HBaseConfiguration jobject. Initialized in InitHBaseConf().
  static void* hbase_conf_;
  TUniqueId query_id_;
  DataStreamMgr* stream_mgr_;
  HdfsFsCache* fs_cache_;

  // prohibit copies
  RuntimeState(const RuntimeState&);
};

}

#endif
