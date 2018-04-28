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


#ifndef IMPALA_EXEC_HDFS_SCAN_NODE_H_
#define IMPALA_EXEC_HDFS_SCAN_NODE_H_

#include <map>
#include <memory>
#include <stdint.h>
#include <vector>

#include <boost/scoped_ptr.hpp>
#include <boost/thread/mutex.hpp>

#include "common/atomic.h"
#include "exec/filter-context.h"
#include "exec/hdfs-scan-node-base.h"
#include "util/counting-barrier.h"
#include "util/thread.h"

namespace impala {

class DescriptorTbl;
class ObjectPool;
class RuntimeState;
class RowBatch;
class ThreadResourcePool;
class TPlanNode;

/// Legacy ScanNode implementation used in the non-multi-threaded execution mode
/// that is used for all tables read directly from HDFS-serialised data.
/// A HdfsScanNode spawns multiple scanner threads to process the bytes in
/// parallel.  There is a handshake between the scan node and the scanners
/// to get all the splits queued and bytes processed.
/// 1. The scan node initially calls the Scanner with a list of files and splits
///    for that scanner/file format.
/// 2. The scanner issues the initial byte ranges for each of those files.  For text
///    this is simply the entire range but for rc files, this would just be the header
///    byte range.  The scan node doesn't care either way.
/// 3. The scan node spins up a number of scanner threads. Each of those threads
///    pulls the next scan range to work on from the IoMgr and then processes the
///    range end to end.
/// 4. The scanner processes the buffers, issuing more scan ranges if necessary.
/// 5. The scanner finishes the scan range and informs the scan node so it can track
///    end of stream.
///
/// Buffer management:
/// ------------------
/// The different scanner threads all allocate I/O buffers from the node's Buffer Pool
/// client. The scan node ensures that enough reservation is available to start a
/// scanner thread before launching each one with, after which the scanner thread must
/// stay within the reservation handed off to it. Scanner threads can try to increase
/// their reservation if desired (e.g. for scanning columnar formats like Parquet), but
/// must be able to make progress within the initial reservation handed off from the scan
/// node.
///
/// TODO: Remove this class once the fragment-based multi-threaded execution is
/// fully functional.
class HdfsScanNode : public HdfsScanNodeBase {
 public:
  HdfsScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
  ~HdfsScanNode();

  virtual Status Init(const TPlanNode& tnode, RuntimeState* state) WARN_UNUSED_RESULT;
  virtual Status Prepare(RuntimeState* state) WARN_UNUSED_RESULT;
  virtual Status Open(RuntimeState* state) WARN_UNUSED_RESULT;
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos)
      WARN_UNUSED_RESULT;
  virtual void Close(RuntimeState* state);

  virtual bool HasRowBatchQueue() const { return true; }

  bool done() const { return done_; }

  /// Adds ranges to the io mgr queue and starts up new scanner threads if possible.
  virtual Status AddDiskIoRanges(const std::vector<io::ScanRange*>& ranges,
      int num_files_queued) WARN_UNUSED_RESULT;

  /// Adds a materialized row batch for the scan node.  This is called from scanner
  /// threads.
  /// This function will block if materialized_row_batches_ is full.
  void AddMaterializedRowBatch(std::unique_ptr<RowBatch> row_batch);

  /// Called by scanners when a range is complete. Used to record progress and set done_.
  /// This *must* only be called after a scanner has completely finished its
  /// scan range (i.e. context->Flush()), and has added the final row batch to the row
  /// batch queue. Otherwise, we may lose the last batch due to racing with shutting down
  /// the RowBatch queue.
  virtual void RangeComplete(const THdfsFileFormat::type& file_type,
      const std::vector<THdfsCompression::type>& compression_type, bool skipped = false);

  /// Transfers all memory from 'pool' to 'scan_node_pool_'.
  virtual void TransferToScanNodePool(MemPool* pool);

 private:
  /// Released when initial ranges are issued in the first call to GetNext().
  CountingBarrier ranges_issued_barrier_{1};

  /// Thread group for all scanner worker threads
  ThreadGroup scanner_threads_;

  /// Outgoing row batches queue. Row batches are produced asynchronously by the scanner
  /// threads and consumed by the main thread.
  boost::scoped_ptr<RowBatchQueue> materialized_row_batches_;

  /// Maximum size of materialized_row_batches_.
  int max_materialized_row_batches_;

  /// Lock protects access between scanner thread and main query thread (the one calling
  /// GetNext()) for all fields below.  If this lock and any other locks needs to be taken
  /// together, this lock must be taken first.
  boost::mutex lock_;

  /// Protects file_type_counts_. Cannot be taken together with any other lock
  /// except lock_, and if so, lock_ must be taken first.
  SpinLock file_type_counts_;

  /// Flag signaling that all scanner threads are done.  This could be because they
  /// are finished, an error/cancellation occurred, or the limit was reached.
  /// Setting this to true triggers the scanner threads to clean up.
  /// This should not be explicitly set. Instead, call SetDone().
  bool done_ = false;

  /// Set to true if all ranges have started. Some of the ranges may still be in flight
  /// being processed by scanner threads, but no new ScannerThreads should be started.
  bool all_ranges_started_ = false;

  /// The id of the callback added to the thread resource manager when thread token
  /// is available. Used to remove the callback before this scan node is destroyed.
  /// -1 if no callback is registered.
  int thread_avail_cb_id_ = -1;

  /// Maximum number of scanner threads. Set to 'NUM_SCANNER_THREADS' if that query
  /// option is set. Otherwise, it's set to the number of cpu cores. Scanner threads
  /// are generally cpu bound so there is no benefit in spinning up more threads than
  /// the number of cores.
  int max_num_scanner_threads_;

  // Number of times scanner threads were not created because of reservation increase
  // being denied.
  RuntimeProfile::Counter* scanner_thread_reservations_denied_counter_ = nullptr;

  /// The wait time for fetching a row batch from the row batch queue.
  RuntimeProfile::Counter* row_batches_get_timer_ = nullptr;

  /// The wait time for enqueuing a row batch into the row batch queue.
  RuntimeProfile::Counter* row_batches_put_timer_ = nullptr;

  /// Tries to spin up as many scanner threads as the quota allows. Called explicitly
  /// (e.g., when adding new ranges) or when threads are available for this scan node.
  void ThreadTokenAvailableCb(ThreadResourcePool* pool);

  /// Main function for scanner thread. This thread pulls the next range to be
  /// processed from the IoMgr and then processes the entire range end to end.
  /// This thread terminates when all scan ranges are complete or an error occurred.
  /// 'first_thread' is true if this was the first scanner thread to start and
  /// it acquired a "required" thread token from ThreadResourceMgr. The first thread
  /// will continue running until 'done_' is true or an error is encountered. Other
  /// threads may terminate early if the optional tokens in
  /// runtime_state_->resource_pool() are exceeded.
  /// The caller must have reserved 'scanner_thread_reservation' bytes of memory for
  /// this thread. Before returning, this function releases the reservation with
  /// ReturnReservationFromScannerThread().
  void ScannerThread(bool first_thread, int64_t scanner_thread_reservation);

  /// Process the entire scan range with a new scanner object. Executed in scanner
  /// thread. 'filter_ctxs' is a clone of the class-wide filter_ctxs_, used to filter rows
  /// in this split. 'scanner_thread_reservation' is an in/out argument that tracks the
  /// total reservation from 'buffer_pool_client_' that is allotted for this thread's
  /// use.
  Status ProcessSplit(const std::vector<FilterContext>& filter_ctxs,
      MemPool* expr_results_pool, io::ScanRange* scan_range,
      int64_t* scanner_thread_reservation) WARN_UNUSED_RESULT;

  /// Called by scanner thread to return some or all of its reservation that is not
  /// needed. Always holds onto at least the minimum reservation to avoid violating the
  /// invariants of ExecNode::buffer_pool_client_. 'lock_' must be held via 'lock'.
  void ReturnReservationFromScannerThread(const boost::unique_lock<boost::mutex>& lock,
      int64_t bytes);

  /// Checks for eos conditions and returns batches from materialized_row_batches_.
  Status GetNextInternal(RuntimeState* state, RowBatch* row_batch, bool* eos)
      WARN_UNUSED_RESULT;

  /// Sets done_ to true and triggers threads to cleanup. Must be called with lock_
  /// taken. Calling it repeatedly ignores subsequent calls.
  void SetDoneInternal();

  /// Gets lock_ and calls SetDoneInternal()
  void SetDone();
};

}

#endif
