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

#ifndef IMPALA_UTIL_HDFS_BULK_OPS_H
#define IMPALA_UTIL_HDFS_BULK_OPS_H

#include <string>
#include <boost/thread.hpp>

#include "common/hdfs.h"
#include "common/atomic.h"
#include "common/status.h"
#include "util/thread-pool.h"

namespace impala {

enum HdfsOpType {
  DELETE,
  CREATE_DIR,
  RENAME,
  DELETE_THEN_CREATE,
  CHMOD
};

class HdfsOperationSet;

// Container class that encapsulates a single HDFS operation. Used only internally by
// HdfsOperationSet, but visible because it parameterises HdfsOpThreadPool.
class HdfsOp {
 public:
  HdfsOp(HdfsOpType op, const std::string& src, HdfsOperationSet* op_set);

  HdfsOp(HdfsOpType op, const std::string& src, const std::string& dst,
      HdfsOperationSet* op_set);

  HdfsOp(HdfsOpType op, const std::string& src, short permissions,
      HdfsOperationSet* op_set);

  // Required for ThreadPool
  HdfsOp();

  HdfsOpType op() const { return op_; }
  const std::string& src() const { return src_; }
  const std::string& dst() const { return dst_; }

  // Actually performs the corresponding HDFS operation, and signals completion to
  // HdfsOperationSet via MarkOneOpDone.
  void Execute() const;

 private:
  // The kind of operation to execute
  HdfsOpType op_;

  // First operand
  std::string src_;

  // Second string operand, ignored except for RENAME
  std::string dst_;

  // Permission operand, ignored except for CHMOD
  short permissions_;

  // Containing operation set, used to record errors and to signal completion.
  HdfsOperationSet* op_set_;
};

typedef ThreadPool<HdfsOp> HdfsOpThreadPool;

// Creates a new HdfsOp-processing thread pool.
HdfsOpThreadPool* CreateHdfsOpThreadPool(const std::string& name, uint32_t num_threads,
    uint32_t max_queue_length);

// This class contains a set of operations to be executed in parallel on an
// HdfsOpThreadPool. These operations may not be executed in the order that they are
// added.
class HdfsOperationSet {
 public:
  // Constructs a new operation set. The hdfsFS parameter is shared between all
  // operations, and is not owned by this class (and therefore should remain valid until
  // Execute returns).
  HdfsOperationSet(hdfsFS* hdfs_connection);

  // Add an operation that takes only a single 'src' parameter (e.g. DELETE, CREATE_DIR,
  // DELETE_THEN_CREATE)
  void Add(HdfsOpType op, const std::string& src);

  // Add an operation that takes two parameters (e.g. RENAME)
  void Add(HdfsOpType op, const std::string& src, const std::string& dst);

  // Add an operation that takes a permission argument (i.e. CHMOD)
  void Add(HdfsOpType op, const std::string& src, short permissions);

  // Run all operations on the given pool, blocking until all are complete. Returns false
  // if there were any errors, true otherwise.
  // If abort_on_error is true, execution will finish after the first error seen.
  bool Execute(HdfsOpThreadPool* pool, bool abort_on_error);

  typedef std::pair<const HdfsOp*, std::string> Error;
  typedef std::vector<Error> Errors;

  // Returns the (possible zero-length) list of errors during op execution. Not valid
  // until Execute has returned.
  const Errors& errors() { return errors_; }

  hdfsFS* hdfs_connection() const { return hdfs_connection_; }

 private:
  // The set of operations to be submitted to HDFS
  std::vector<HdfsOp> ops_;

  // Used to coordinate between the executing threads and the caller; Execute blocks until
  // this is signalled.
  Promise<bool> promise_;

  // The number of ops remaining to be executed. Used to coordinate between executor
  // threads so that when all ops are finished, promise_ is signalled.
  AtomicInt<int64_t> num_ops_;

  // HDFS connection shared between all operations. Not owned by this class.
  hdfsFS* hdfs_connection_;

  // Protects errors_ and abort_on_error_ during Execute
  boost::mutex errors_lock_;

  // All errors produced during Execute
  Errors errors_;

  // True if a single error should cause any subsequent operations to become no-ops.
  bool abort_on_error_;

  friend class HdfsOp;

  // Called by HdfsOp to signal its completion. When the last op has finished, this method
  // signals Execute() so that it can return.
  void MarkOneOpDone();

  // Called by HdfsOp to record an error
  void AddError(const std::string& err, const HdfsOp* op);

  // Called by HdfsOp at the start of execution to decide whether to continue. Returns
  // true iff abort_on_error_ is true and at least one error has been recorded.
  bool ShouldAbort();
};

}

#endif // IMPALA_UTIL_HDFS_BULK_OPS_H
