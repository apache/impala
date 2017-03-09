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

#include "exec/filter-context.h"
#include "exec/hdfs-scan-node-base.h"
#include "runtime/disk-io-mgr.h"
#include "util/counting-barrier.h"
#include "util/thread.h"

namespace impala {

class DescriptorTbl;
class ObjectPool;
class RuntimeState;
class RowBatch;
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
/// TODO: This class allocates a bunch of small utility objects that should be
/// recycled.
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
  virtual Status AddDiskIoRanges(const std::vector<DiskIoMgr::ScanRange*>& ranges,
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
      const std::vector<THdfsCompression::type>& compression_type);

  /// Transfers all memory from 'pool' to 'scan_node_pool_'.
  virtual void TransferToScanNodePool(MemPool* pool);

 private:
  /// Released when initial ranges are issued in the first call to GetNext().
  CountingBarrier ranges_issued_barrier_;

  /// The estimated memory required to start up a new scanner thread. If the memory
  /// left (due to limits) is less than this value, we won't start up optional
  /// scanner threads.
  int64_t scanner_thread_bytes_required_;

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
  bool done_;

  /// Set to true if all ranges have started. Some of the ranges may still be in flight
  /// being processed by scanner threads, but no new ScannerThreads should be started.
  bool all_ranges_started_;

  /// The id of the callback added to the thread resource manager when thread token
  /// is available. Used to remove the callback before this scan node is destroyed.
  /// -1 if no callback is registered.
  int thread_avail_cb_id_;

  /// Maximum number of scanner threads. Set to 'NUM_SCANNER_THREADS' if that query
  /// option is set. Otherwise, it's set to the number of cpu cores. Scanner threads
  /// are generally cpu bound so there is no benefit in spinning up more threads than
  /// the number of cores.
  int max_num_scanner_threads_;

  /// The wait time for fetching a row batch from the row batch queue.
  RuntimeProfile::Counter* row_batches_get_timer_;

  /// The wait time for enqueuing a row batch into the row batch queue.
  RuntimeProfile::Counter* row_batches_put_timer_;

  /// Tries to spin up as many scanner threads as the quota allows. Called explicitly
  /// (e.g., when adding new ranges) or when threads are available for this scan node.
  void ThreadTokenAvailableCb(ThreadResourceMgr::ResourcePool* pool);

  /// Main function for scanner thread. This thread pulls the next range to be
  /// processed from the IoMgr and then processes the entire range end to end.
  /// This thread terminates when all scan ranges are complete or an error occurred.
  void ScannerThread();

  /// Process the entire scan range with a new scanner object. Executed in scanner
  /// thread. 'filter_ctxs' is a clone of the class-wide filter_ctxs_, used to filter rows
  /// in this split.
  Status ProcessSplit(const std::vector<FilterContext>& filter_ctxs,
      DiskIoMgr::ScanRange* scan_range) WARN_UNUSED_RESULT;

  /// Returns true if there is enough memory (against the mem tracker limits) to
  /// have a scanner thread.
  /// If new_thread is true, the calculation is for starting a new scanner thread.
  /// If false, it determines whether there's adequate memory for the existing
  /// set of scanner threads.
  /// lock_ must be taken before calling this.
  bool EnoughMemoryForScannerThread(bool new_thread);

  /// Checks for eos conditions and returns batches from materialized_row_batches_.
  Status GetNextInternal(RuntimeState* state, RowBatch* row_batch, bool* eos)
      WARN_UNUSED_RESULT;

  /// sets done_ to true and triggers threads to cleanup. Cannot be called with
  /// any locks taken. Calling it repeatedly ignores subsequent calls.
  void SetDone();
};

}

#endif
