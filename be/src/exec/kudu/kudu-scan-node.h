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

#pragma once

#include <boost/scoped_ptr.hpp>
#include <gtest/gtest.h>
#include <kudu/client/client.h>

#include "common/atomic.h"
#include "exec/kudu/kudu-scan-node-base.h"
#include "gutil/gscoped_ptr.h"

namespace impala {

class KuduScanner;
class ThreadResourcePool;

/// A scan node that scans a Kudu table.
///
/// This takes a set of serialized Kudu scan tokens which encode the information needed
/// for this scan. A Kudu client deserializes the tokens into kudu scanners, and those
/// are used to retrieve the rows for this scan.
class KuduScanNode : public KuduScanNodeBase {
 public:
  KuduScanNode(ObjectPool* pool, const ScanPlanNode& pnode, const DescriptorTbl& descs);

  ~KuduScanNode();

  virtual Status Prepare(RuntimeState* state) override;
  virtual Status Open(RuntimeState* state) override;
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) override;
  virtual void Close(RuntimeState* state) override;
  virtual ExecutionModel getExecutionModel() const override {
    return NON_TASK_BASED_SYNC;
  }

 private:
  friend class KuduScanner;

  ScannerThreadState thread_state_;

  /// Protects access to state accessed by scanner threads, such as 'status_'.
  std::mutex lock_;

  /// The current status of the scan, set to non-OK if any problems occur, e.g. if an
  /// error occurs in a scanner.
  /// Protected by lock_
  Status status_;

  /// Set to true when the scan is complete (either because all scan tokens have been
  /// processed, the limit was reached or some error occurred).
  AtomicBool done_;

  /// The id of the callback added to the thread resource manager when a thread
  /// is available. Used to remove the callback before this scan node is destroyed.
  /// -1 if no callback is registered.
  int thread_avail_cb_id_;

  /// Compute the estimated memory consumption of each Kudu scanner thread.
  int64_t EstimateScannerThreadMemConsumption();

  /// Called when scanner threads are available for this scan node. This will
  /// try to spin up as many scanner threads as the quota allows.
  void ThreadAvailableCb(ThreadResourcePool* pool);

  /// Main function for scanner thread which executes a KuduScanner. Begins by processing
  /// 'initial_token', and continues processing scan tokens returned by GetNextScanToken()
  /// until there are none left, an error occurs, or the limit is reached. The caller must
  /// have acquired a thread token from the ThreadResourceMgr for this thread. The token
  /// is released before this function returns. 'first_thread' is true if this was the
  /// first scanner thread to start and it acquired a "required" thread token. The first
  /// thread will continue running until 'done_' is true or an error is encountered. Other
  /// threads may terminate early if the optional tokens in
  /// runtime_state_->resource_pool() are exceeded.
  void RunScannerThread(
      bool first_thread, const std::string& name, const std::string* initial_token);

  /// Processes a single scan token. Row batches are fetched using 'scanner' and enqueued
  /// in the row batch queue until the scanner reports eos, an error occurs, or
  /// the limit is reached.
  Status ProcessScanToken(KuduScanner* scanner, const std::string& scan_token);

  /// Sets done_ to true and triggers threads to cleanup. Must be called with lock_
  /// taken. Calling it repeatedly ignores subsequent calls.
  void SetDoneInternal();

  /// Gets lock_ and calls SetDoneInternal()
  void SetDone();
};

}
