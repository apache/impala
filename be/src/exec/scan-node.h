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

#include <string>
#include "exec/exec-node.h"
#include "exec/filter-context.h"
#include "util/runtime-profile.h"
#include "util/thread.h"
#include "gen-cpp/ImpalaInternalService_types.h"

namespace impala {

class BlockingRowBatchQueue;
class ScanRangeParamsPB;

class ScanPlanNode : public PlanNode {
 public:
  virtual Status Init(const TPlanNode& tnode, FragmentState* state) override;
  virtual Status CreateExecNode(RuntimeState* state, ExecNode** node) const override;
};

/// Abstract base class of all scan nodes. Subclasses support different storage layers
/// and different threading models.
///
/// Includes ScanNode common counters:
///   BytesRead - total bytes read from disk by this scan node. Provided as a counter
///     as well as a time series that samples the counter. Only implemented for scan node
///     subclasses that expose the bytes read, e.g. HDFS and HBase.
///
///   TotalReadThroughput - BytesRead divided by the total wall clock time that this scan
///     was executing (from Open() to Close()). This gives the aggregate rate that data
///     is read from disks. If this is the only scan executing, ideally this will
///     approach the maximum bandwidth supported by the disks.
///
///   RowsRead - number of top-level rows/tuples read from the storage layer, including
///     those discarded by predicate evaluation. Used for all types of scans.
///
///   CollectionItemsRead - total number of nested collection items read by the scan.
///     Only created for scans (e.g. Parquet) that support nested types.
///
///   ScanRangesComplete - number of scan ranges completed. Initialized for scans that
///     have a concept of "scan range".
///
///   MaterializeTupleTime - wall clock time spent materializing tuples and evaluating
///     predicates.
///
/// The following counters are specific to multithreaded scan node implementations:
///
///   PeakScannerThreadConcurrency - the peak number of scanner threads executing at any
///     one time. Present only for multithreaded scan nodes.
///
///   AverageScannerThreadConcurrency - the average number of scanner threads executing
///     between Open() and the time when the scan completes. Present only for
///     multithreaded scan nodes.
///
///   NumScannerThreadsStarted - the number of scanner threads started for the duration
///     of the ScanNode. This is at most the number of scan ranges but should be much
///     less since a single scanner thread will likely process multiple scan ranges.
///     This is *not* the same as peak scanner thread concurrency because the number of
///     scanner threads can fluctuate during execution of the scan.
///
///   ScannerThreadsTotalWallClockTime - total wall clock time spent in all scanner
///     threads.
///
///   ScannerThreadsUserTime, ScannerThreadsSysTime,
///   ScannerThreadsVoluntaryContextSwitches, ScannerThreadsInvoluntaryContextSwitches -
///     these are aggregated counters across all scanner threads of this scan node. They
///     are taken from getrusage. See RuntimeProfile::ThreadCounters for details.
///
///   RowBatchesEnqueued, RowBatchBytesEnqueued - Number of row batches and bytes enqueued
///     in the scan node's output queue.
///
///   RowBatchQueueGetWaitTime - Wall clock time that the fragment execution thread spent
///     blocked waiting for row batches to be added to the scan node's output queue.
///
///   RowBatchQueuePutWaitTime - Wall clock time that the scanner threads spent blocked
///     waiting for space in the scan node's output queue when it is full.
///
///   RowBatchQueueCapacity - capacity in batches of the scan node's output queue.
///
///   RowBatchQueuePeakMemoryUsage - peak memory consumption of row batches enqueued in
///     the scan node's output queue.
///

class ScanNode : public ExecNode {
 public:
  ScanNode(ObjectPool* pool, const ScanPlanNode& pnode, const DescriptorTbl& descs)
    : ExecNode(pool, pnode, descs),
      scan_range_params_(NULL) {
    filter_exprs_ = pnode.runtime_filter_exprs_;
  }

  virtual Status Prepare(RuntimeState* state) WARN_UNUSED_RESULT;
  virtual Status Open(RuntimeState* state) WARN_UNUSED_RESULT;

  /// Stops all periodic counters and calls ExecNode::Close(). Subclasses of ScanNode can
  /// start periodic counters and rely on this function stopping them.
  virtual void Close(RuntimeState* state);

  /// This should be called before Prepare(), and the argument must be not destroyed until
  /// after Prepare().
  void SetScanRanges(
      const google::protobuf::RepeatedPtrField<ScanRangeParamsPB>& scan_range_params) {
    scan_range_params_ = &scan_range_params;
  }

  virtual bool IsScanNode() const { return true; }

  /// Returns true iff the data cache in IoMgr is disabled by query options.
  bool IsDataCacheDisabled() const;

  RuntimeState* runtime_state() const { return runtime_state_; }
  RuntimeProfile::Counter* bytes_read_counter() const { return bytes_read_counter_; }
  RuntimeProfile::Counter* rows_read_counter() const { return rows_read_counter_; }
  RuntimeProfile::Counter* collection_items_read_counter() const {
    return collection_items_read_counter_;
  }
  RuntimeProfile::Counter* materialize_tuple_timer() const {
    return materialize_tuple_timer_;
  }

  static const std::string SCANNER_THREAD_COUNTERS_PREFIX;

  const std::vector<ScalarExpr*>& filter_exprs() const { return filter_exprs_; }

  const std::vector<FilterContext>& filter_ctxs() const { return filter_ctxs_; }

 protected:
  RuntimeState* runtime_state_ = nullptr;

  /// The scan ranges this scan node is responsible for. Not owned.
  const google::protobuf::RepeatedPtrField<ScanRangeParamsPB>* scan_range_params_;

  /// Total bytes read from the scanner. Initialised in subclasses that track
  /// bytes read, including HDFS and HBase by calling AddBytesReadCounters().
  RuntimeProfile::Counter* bytes_read_counter_ = nullptr;

  /// Time series of 'bytes_read_counter_', initialized at the same time.
  RuntimeProfile::TimeSeriesCounter* bytes_read_timeseries_counter_ = nullptr;

  /// Wall based aggregate read throughput [bytes/sec]. Depends on 'bytes_read_counter_'
  /// and initialized at the same time.
  RuntimeProfile::Counter* total_throughput_counter_ = nullptr;

  /// # top-level rows/tuples read from the scanner, including those discarded by
  /// EvalConjuncts(). Used for all types of scans.
  RuntimeProfile::Counter* rows_read_counter_ = nullptr;

  /// # items the scanner read into CollectionValues. For example, for schema
  /// array<struct<B: INT, array<C: INT>> and tuple
  /// [(2, [(3)]), (4, [])] this counter will be 3: (2, [(3)]), (3) and (4, [])
  /// Initialized by subclasses that support scanning nested types.
  RuntimeProfile::Counter* collection_items_read_counter_ = nullptr;

  /// Total time writing tuple slots. Used for all types of scans.
  RuntimeProfile::Counter* materialize_tuple_timer_ = nullptr;

  /// Total number of scan ranges completed. Initialised in subclasses that have a
  /// concept of "scan range", including HDFS and Kudu.
  RuntimeProfile::Counter* scan_ranges_complete_counter_ = nullptr;

  /// Expressions to evaluate the input rows for filtering against runtime filters.
  std::vector<ScalarExpr*> filter_exprs_;

  /// List of contexts for expected runtime filters for this scan node. These contexts are
  /// cloned by individual scanners to be used in multi-threaded contexts, passed through
  /// the per-scanner ScannerContext. Correspond to exprs in 'filter_exprs_'.
  std::vector<FilterContext> filter_ctxs_;

  /// Initializes 'bytes_read_counter_', 'bytes_read_timeseries_counter_' and
  /// 'total_throughput_counter_'
  void AddBytesReadCounters();

  /// Waits for runtime filters to arrive, checking every 20ms. Max wait time is specified
  /// by the 'runtime_filter_wait_time_ms' flag, which is overridden by the query option
  /// of the same name. The wait starts from when this function is called. Returns
  /// true if all filters arrived within the time limit, false otherwise.
  bool WaitForRuntimeFilters();

  /// Additional state only used by multi-threaded scan node implementations.
  /// The lifecycle is as follows:
  /// 1. Prepare() is called.
  /// 2. Open() is called.
  /// 3. Other methods can be called.
  /// 4. Shutdown() is called to prevent new batches being added to the queue.
  /// 5. Close() is called to release all resources.
  class ScannerThreadState {
   public:
    /// Called from *ScanNode::Prepare() to initialize counters and MemTracker.
    /// 'estimated_per_thread_mem' is the estimated memory consumption of each scanner
    /// thread and must be positive. Prepare() registers the scan with the query-global
    /// ScannerMemLimit and accounts for the first thread's memory consumption.
    void Prepare(ScanNode* parent, int64_t estimated_per_thread_mem);

    /// Called from *ScanNode::Open() to create the row batch queue and start periodic
    /// counters running. 'max_row_batches_override' determines size of the row batch
    /// queue if >= 0. Otherwise the size is automatically determined.
    void Open(ScanNode* parent, int64_t max_row_batches_override);

    /// Called when no more batches need to be enqueued or dequeued. Shuts down the
    /// queue. Thread-safe.
    void Shutdown();

    /// Waits for all scanner threads to finish and cleans up the queue. Called from
    /// *ScanNode::Close(). No other methods can be called after this. Not thread-safe.
    void Close(ScanNode* parent);

    /// Add a new scanner thread to the thread group. Not thread-safe: only one thread
    /// should call AddThread() at a time.
    void AddThread(std::unique_ptr<Thread> thread);

    /// Get the number of active scanner threads. Thread-safe.
    int32_t GetNumActive() const { return num_active_.Load(); }

    /// Get the number of started scanner threads. Thread-safe.
    int32_t GetNumStarted() const { return num_threads_started_->value(); }

    /// Called from a scanner thread that is exiting to decrement the number of active
    /// scanner threads. Returns true if this was the last thread to exit. Thread-safe.
    bool DecrementNumActive();

    /// Adds a materialized row batch for the scan node.  This is called from scanner
    /// threads. This function will block if the row batch queue is full. Thread-safe.
    void EnqueueBatch(std::unique_ptr<RowBatch> row_batch);

    /// Adds a materialized row batch for the scan node. This is called from scanner
    /// threads. This function will block for up to timeout_micros if the row batch
    /// queue is full. Return true and takes ownership of '*row_batch' if the batch
    /// was successfully enqueued. Returns false if the timeout expired or the queue
    /// was shut down and the batch could not be enqueued and does not take ownership
    /// of '*row_batch'. Thread-safe.
    bool EnqueueBatchWithTimeout(std::unique_ptr<RowBatch>* row_batch,
        int64_t timeout_micros);

    BlockingRowBatchQueue* batch_queue() { return batch_queue_.get(); }
    RuntimeProfile::ThreadCounters* thread_counters() const { return thread_counters_; }
    int max_num_scanner_threads() const { return max_num_scanner_threads_; }
    int64_t estimated_per_thread_mem() const { return estimated_per_thread_mem_; }
    RuntimeProfile::Counter* scanner_thread_mem_unavailable_counter() const {
      return scanner_thread_mem_unavailable_counter_;
    }

   private:
    /// Thread group for all scanner threads.
    ThreadGroup scanner_threads_;

    /// Maximum number of scanner threads.
    /// The value is set to either of the following:
    /// - 1, if COMPUTE_PROCESSING_COST=true;
    /// - 'NUM_SCANNER_THREADS', if that query option is set and
    ///   COMPUTE_PROCESSING_COST=false;
    /// - Otherwise, it's set to the number of cpu cores.
    /// Scanner threads are generally cpu bound so there is no benefit in spinning up
    /// more threads than the number of cores. Set in Open().
    int max_num_scanner_threads_ = 0;

    /// Estimated amount of memory that each additional scanner thread will consume. Used
    /// to decide whether enough memory is available to create a new scanner thread. Set
    /// to 0 when the memory for the first thread claimed in Prepare() is released.
    int64_t estimated_per_thread_mem_ = 0;

    // MemTracker for queued row batches. Initialized in Prepare(). Owned by RuntimeState.
    MemTracker* row_batches_mem_tracker_ = nullptr;

    /// Outgoing row batches queue. Row batches are produced asynchronously by the scanner
    /// threads and consumed by the main fragment thread that calls GetNext() on the scan
    /// node.
    boost::scoped_ptr<BlockingRowBatchQueue> batch_queue_;

    /// The number of scanner threads currently running.
    AtomicInt32 num_active_{0};

    /// Aggregated scanner thread CPU time counters.
    RuntimeProfile::ThreadCounters* thread_counters_ = nullptr;

    /// Average number of executing scanner threads
    /// This should be created in Open and stopped when all the scanner threads are done.
    RuntimeProfile::Counter* average_concurrency_ = nullptr;

    /// Peak number of executing scanner threads.
    RuntimeProfile::HighWaterMarkCounter* peak_concurrency_ = nullptr;

    /// Cumulative number of scanner threads created during the scan. Some may be created
    /// and then destroyed, so this can exceed the peak number of threads.
    RuntimeProfile::Counter* num_threads_started_ = nullptr;

    /// The number of row batches enqueued into the row batch queue.
    RuntimeProfile::Counter* row_batches_enqueued_ = nullptr;

    /// The total bytes of row batches enqueued into the row batch queue.
    RuntimeProfile::Counter* row_batch_bytes_enqueued_ = nullptr;

    /// The wait time for fetching a row batch from the row batch queue.
    RuntimeProfile::Counter* row_batches_get_timer_ = nullptr;

    /// The wait time for enqueuing a row batch into the row batch queue.
    RuntimeProfile::Counter* row_batches_put_timer_ = nullptr;

    /// Peak memory consumption of the materialized batch queue. Updated in Close().
    RuntimeProfile::Counter* row_batches_peak_mem_consumption_ = nullptr;

    /// Number of times scanner threads were not created because of memory not available.
    RuntimeProfile::Counter* scanner_thread_mem_unavailable_counter_ = nullptr;
  };
};
}
