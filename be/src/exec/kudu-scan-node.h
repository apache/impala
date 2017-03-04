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

#ifndef IMPALA_EXEC_KUDU_SCAN_NODE_H_
#define IMPALA_EXEC_KUDU_SCAN_NODE_H_

#include <boost/scoped_ptr.hpp>
#include <gtest/gtest.h>
#include <kudu/client/client.h>

#include "exec/kudu-scan-node-base.h"
#include "runtime/thread-resource-mgr.h"
#include "gutil/gscoped_ptr.h"
#include "util/thread.h"

namespace impala {

class KuduScanner;

/// A scan node that scans a Kudu table.
///
/// This takes a set of serialized Kudu scan tokens which encode the information needed
/// for this scan. A Kudu client deserializes the tokens into kudu scanners, and those
/// are used to retrieve the rows for this scan.
class KuduScanNode : public KuduScanNodeBase {
 public:
  KuduScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

  ~KuduScanNode();

  virtual Status Open(RuntimeState* state);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual void Close(RuntimeState* state);

 private:
  friend class KuduScanner;

  // Outgoing row batches queue. Row batches are produced asynchronously by the scanner
  // threads and consumed by the main thread.
  boost::scoped_ptr<RowBatchQueue> materialized_row_batches_;

  /// Protects access to state accessed by scanner threads, such as 'status_' or
  /// 'num_active_scanners_'.
  boost::mutex lock_;

  /// The current status of the scan, set to non-OK if any problems occur, e.g. if an
  /// error occurs in a scanner.
  /// Protected by lock_
  Status status_;

  /// Number of active running scanner threads.
  /// Protected by lock_
  int num_active_scanners_;

  /// Set to true when the scan is complete (either because all scan tokens have been
  /// processed, the limit was reached or some error occurred).
  /// Protected by lock_. It is safe to do optimistic reads without taking lock_ in
  /// certain places, based on the decisions taken after that.
  /// The tradeoff is occasionally doing some extra work versus increasing lock
  /// contention.
  volatile bool done_;

  /// Thread group for all scanner worker threads
  ThreadGroup scanner_threads_;

  /// The id of the callback added to the thread resource manager when a thread
  /// is available. Used to remove the callback before this scan node is destroyed.
  /// -1 if no callback is registered.
  int thread_avail_cb_id_;

  /// Called when scanner threads are available for this scan node. This will
  /// try to spin up as many scanner threads as the quota allows.
  void ThreadAvailableCb(ThreadResourceMgr::ResourcePool* pool);

  /// Main function for scanner thread which executes a KuduScanner. Begins by processing
  /// 'initial_token', and continues processing scan tokens returned by
  /// 'GetNextScanToken()' until there are none left, an error occurs, or the limit is
  /// reached.
  void RunScannerThread(const std::string& name, const std::string* initial_token);

  /// Processes a single scan token. Row batches are fetched using 'scanner' and enqueued
  /// in 'materialized_row_batches_' until the scanner reports eos, an error occurs, or
  /// the limit is reached.
  Status ProcessScanToken(KuduScanner* scanner, const std::string& scan_token);
};

}

#endif
