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

#include "exec/hdfs-scan-node.h"

#include <chrono>
#include <memory>
#include <sstream>

#include "common/logging.h"
#include "exec/base-sequence-scanner.h"
#include "exec/exec-node-util.h"
#include "exec/hdfs-scanner.h"
#include "exec/scanner-context.h"
#include "exprs/scalar-expr-evaluator.h"
#include "runtime/blocking-row-batch-queue.h"
#include "runtime/descriptors.h"
#include "runtime/fragment-instance-state.h"
#include "runtime/io/request-context.h"
#include "runtime/mem-tracker.h"
#include "runtime/query-state.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-filter.inline.h"
#include "runtime/runtime-state.h"
#include "runtime/scanner-mem-limiter.h"
#include "runtime/thread-resource-mgr.h"
#include "util/debug-util.h"
#include "util/disk-info.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

DEFINE_int32(max_row_batches, 0,
    "the maximum number of batches to queue in multithreaded HDFS scans");

#ifndef NDEBUG
DECLARE_bool(skip_file_runtime_filtering);
#endif

using namespace impala;
using namespace impala::io;

// Amount of memory that we approximate a scanner thread will use not including I/O
// buffers. The memory used does not vary considerably between file formats (just a
// couple of MBs). This value is conservative and taken from running against the tpch
// lineitem table. Note: this is a crude heuristic to help reduce odds of OOM until
// we can remove the multithreaded scanners.
DEFINE_int64_hidden(hdfs_scanner_thread_max_estimated_bytes, 32L * 1024L * 1024L,
    "Estimated bytes of memory consumed by HDFS scanner thread.");

// Estimated upper bound on the compression ratio of compressed text files. Used to
// estimate scanner thread memory usage.
const int COMPRESSED_TEXT_COMPRESSION_RATIO = 11;

// Amount of time to block waiting for GetNext() to release scanner threads between
// checking if a scanner thread should yield itself back to the global thread pool.
const int SCANNER_THREAD_WAIT_TIME_MS = 20;

HdfsScanNode::HdfsScanNode(ObjectPool* pool, const HdfsScanPlanNode& pnode,
                           const DescriptorTbl& descs)
    : HdfsScanNodeBase(pool, pnode, pnode.tnode_->hdfs_scan_node, descs) {
}

HdfsScanNode::~HdfsScanNode() {
}


Status HdfsScanNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedGetNextEventAdder ea(this, eos);

  if (!initial_ranges_issued_.Load()) {
    // We do this in GetNext() to maximise the amount of work we can do while waiting for
    // runtime filters to show up. The scanner threads have already started (in Open()),
    // so we need to tell them there is work to do.
    // TODO: This is probably not worth splitting the organisational cost of splitting
    // initialisation across two places. Move to before the scanner threads start.
    Status status = IssueInitialScanRanges(state);
    if (!status.ok()) {
      // If the status returned is CANCELLED, it could be because the
      // reader_context_ was cancelled by a scanner thread which hit an error. In this
      // case, the scanner thread's error must take precedence. In other cases,
      // the non-ok status represents the error in ValidateScanRange() or describes
      // the unsupported compression formats. For such non-CANCELLED cases, the status
      // returned by IssueInitialScanRanges() takes precedence.
      unique_lock<timed_mutex> l(lock_);
      if (status.IsCancelled() && !status_.ok()) return status_;
      return status;
    }

    // Release the scanner threads
    discard_result(ranges_issued_barrier_.Notify());

    if (shared_state_->progress().done()) SetDone();
  }

  Status status = GetNextInternal(state, row_batch, eos);
  if (!status.ok() || *eos) {
    unique_lock<timed_mutex> l(lock_);
    lock_guard<SpinLock> l2(file_type_counts_lock_);
    StopAndFinalizeCounters();
  }
  return status;
}

Status HdfsScanNode::GetNextInternal(
    RuntimeState* state, RowBatch* row_batch, bool* eos) {
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));

  if (ReachedLimitShared()) {
    // LIMIT 0 case.  Other limit values handled below.
    DCHECK_EQ(limit_, 0);
    *eos = true;
    return Status::OK();
  }
  *eos = false;
  unique_ptr<RowBatch> materialized_batch = thread_state_.batch_queue()->GetBatch();
  if (materialized_batch != NULL) {
    row_batch->AcquireState(materialized_batch.get());
    // Note that the scanner threads may have processed and queued up extra rows before
    // this thread incremented the rows returned.
    if (CheckLimitAndTruncateRowBatchIfNeededShared(row_batch, eos)) SetDone();
    COUNTER_SET(rows_returned_counter_, rows_returned_shared());
    materialized_batch.reset();
    return Status::OK();
  }
  // The RowBatchQueue was shutdown either because all scan ranges are complete or a
  // scanner thread encountered an error.  Check status_ to distinguish those cases.
  *eos = true;
  unique_lock<timed_mutex> l(lock_);
  return status_;
}

Status HdfsScanNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(HdfsScanNodeBase::Prepare(state));
  thread_state_.Prepare(this, EstimateScannerThreadMemConsumption(state));
  scanner_thread_reservations_denied_counter_ =
      ADD_COUNTER(runtime_profile(), "NumScannerThreadReservationsDenied", TUnit::UNIT);
  scanner_thread_workless_loops_counter_ =
      ADD_COUNTER(runtime_profile(), "ScannerThreadWorklessLoops", TUnit::UNIT);
  return Status::OK();
}

// This function registers the ThreadTokenAvailableCb to start up the initial scanner
// threads. Scan ranges are not issued until the first GetNext() call; scanner threads
// will block on ranges_issued_barrier_ until ranges are issued.
Status HdfsScanNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedOpenEventAdder ea(this);
  RETURN_IF_ERROR(HdfsScanNodeBase::Open(state));
  thread_state_.Open(this, FLAGS_max_row_batches);

  thread_avail_cb_id_ = runtime_state_->resource_pool()->AddThreadAvailableCb(
      bind<void>(mem_fn(&HdfsScanNode::ThreadTokenAvailableCb), this, _1));
  return Status::OK();
}

void HdfsScanNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  SetDone();
  if (thread_avail_cb_id_ != -1) {
    state->resource_pool()->RemoveThreadAvailableCb(thread_avail_cb_id_);
  }
  thread_state_.Close(this);
#ifndef NDEBUG
  // At this point, the other threads have been joined, and
  // remaining_scan_range_submissions_ should be 0, if the
  // query started and wasn't cancelled or exited early.
  if (ranges_issued_barrier_.pending() == 0 && initial_ranges_issued_.Load()
      && shared_state_->progress().done()) {
    DCHECK_EQ(shared_state_->RemainingScanRangeSubmissions(), 0);
  }
#endif
  HdfsScanNodeBase::Close(state);
}

void HdfsScanNode::RangeComplete(const THdfsFileFormat::type& file_type,
    const std::vector<THdfsCompression::type>& compression_type, bool skipped) {
  lock_guard<SpinLock> l(file_type_counts_lock_);
  HdfsScanNodeBase::RangeComplete(file_type, compression_type, skipped);
}

void HdfsScanNode::AddMaterializedRowBatch(unique_ptr<RowBatch> row_batch) {
  InitNullCollectionValues(row_batch.get());
  thread_state_.EnqueueBatch(move(row_batch));
}

Status HdfsScanNode::AddDiskIoRanges(const vector<ScanRange*>& ranges,
    EnqueueLocation enqueue_location) {
  DCHECK_GT(shared_state_->RemainingScanRangeSubmissions(), 0);
  RETURN_IF_ERROR(reader_context_->AddScanRanges(ranges, enqueue_location));
  if (!ranges.empty()) ThreadTokenAvailableCb(runtime_state_->resource_pool());
  return Status::OK();
}

int64_t HdfsScanNode::EstimateScannerThreadMemConsumption(RuntimeState* state) const {
  // Start with the minimum I/O buffer requirement.
  int64_t est_total_bytes = resource_profile_.min_reservation;

  // Next add in the other memory that we estimate the scanner thread will use,
  // e.g. decompression buffers, tuple buffers, etc.
  // For compressed text, we estimate this based on the file size (since the whole file
  // will need to be decompressed at once). For all other formats, we use a constant from
  // either of the HDFS_SCANNER_NON_RESERVED_BYTES query option or the
  // hdfs_scanner_thread_max_estimated_bytes flag, with the query option taking
  // precedence over the flag if the query option is set to a positive value.
  // Note: this is crude and we could try to refine it by factoring in the number of
  // columns, etc, but it is unclear how beneficial this would be.
  int64_t est_non_reserved_bytes = FLAGS_hdfs_scanner_thread_max_estimated_bytes;
  if (state->query_options().__isset.hdfs_scanner_non_reserved_bytes
      && state->query_options().hdfs_scanner_non_reserved_bytes > 0) {
    est_non_reserved_bytes = state->query_options().hdfs_scanner_non_reserved_bytes;
  }
  auto it = shared_state_->per_type_files().find(THdfsFileFormat::TEXT);
  if (it != shared_state_->per_type_files().end()) {
    for (HdfsFileDesc* file : it->second) {
      if (file->file_compression != THdfsCompression::NONE) {
        int64_t compressed_text_est_bytes =
            file->file_length * COMPRESSED_TEXT_COMPRESSION_RATIO;
        est_non_reserved_bytes = max(compressed_text_est_bytes, est_non_reserved_bytes);
      }
    }
  }
  est_total_bytes += est_non_reserved_bytes;
  return est_total_bytes;
}

void HdfsScanNode::ReturnReservationFromScannerThread(
    const unique_lock<timed_mutex>& lock, int64_t bytes) {
  DCHECK(lock.mutex() == &lock_ && lock.owns_lock());
  // Release as much memory as possible. Must hold onto the minimum reservation, though.
  Status status = buffer_pool_client()->DecreaseReservationTo(
      bytes, resource_profile_.min_reservation);
  DCHECK(status.ok()) << "Not possible, scans don't unpin pages" << status.GetDetail();
  DCHECK_GE(buffer_pool_client()->GetReservation(), resource_profile_.min_reservation);
}

void HdfsScanNode::ThreadTokenAvailableCb(ThreadResourcePool* pool) {
  // This is called to start up new scanner threads. It's not a big deal if we
  // spin up more than strictly necessary since they will go through and terminate
  // promptly. However, we want to minimize that by checking a conditions.
  //  1. Don't start up if the ScanNode is done
  //  2. Don't start up if all the ranges have been taken by another thread.
  //  3. Don't start up if the number of ranges left is less than the number of
  //     active scanner threads.
  //  4. Don't start up if no initial ranges have been issued (see IMPALA-1722).
  //  5. Don't start up a ScannerThread if the row batch queue is full since
  //     we are not scanner bound.
  //  6. Don't start up a thread if there is not enough memory available for the
  //     estimated memory consumption (include reservation and non-reserved memory).
  //  7. Don't start up a thread if it is an extra thread and we can't reserve another
  //     minimum reservation's worth of memory for the thread.
  //  8. Don't start up more than maximum number of scanner threads configured.
  //  9. Don't start up if there are no thread tokens.

  // Case 4. We have not issued the initial ranges so don't start a scanner thread.
  // Issuing ranges will call this function and we'll start the scanner threads then.
  // TODO: It would be good to have a test case for that.
  if (!initial_ranges_issued_.Load()) return;

  ScannerMemLimiter* scanner_mem_limiter =
      runtime_state_->query_state()->scanner_mem_limiter();
  Status status = Status::OK();
  while (true) {
    // The lock must be given up between loops in order to give writers to done_,
    // all_ranges_started_ etc. a chance to grab the lock.
    // IMPALA-8322: Another thread can hold this lock for significant periods of time
    // if the scan node is being cancelled. Since this function can be called while
    // holding other locks that can block other threads (e.g. ThreadResourceMgr::lock_),
    // avoid blocking unnecessarily. There are two remedies. First, we do a check of
    // done() to try to avoid acquiring the lock_, as there is nothing to do if
    // the scan node is done. Second, this uses a timeout of 10 milliseconds when
    // acquiring the lock_ to allow a periodic check of done(). The 10 millisecond
    // timeout is arbitrary.
    // TODO: This still leans heavily on starvation-free locks, come up with a more
    // correct way to communicate between this method and ScannerThread().
    if (done()) break;
    unique_lock<timed_mutex> lock(lock_, std::chrono::milliseconds(10));
    if (!lock.owns_lock()) {
      continue;
    }

    const int64_t num_active_scanner_threads = thread_state_.GetNumActive();
    const bool first_thread = num_active_scanner_threads == 0;
    const int64_t est_mem = thread_state_.estimated_per_thread_mem();
    const int64_t scanner_thread_reservation = resource_profile_.min_reservation;
    // Cases 1, 2, 3.
    if (done() || all_ranges_started_ ||
        num_active_scanner_threads >= shared_state_->progress().remaining()) {
      break;
    }

    if (!first_thread) {
      // Cases 5, 6 and 7.
      if (thread_state_.batch_queue()->IsFull()) break;
      if (!scanner_mem_limiter->ClaimMemoryForScannerThread(this, est_mem)) {
        COUNTER_ADD(thread_state_.scanner_thread_mem_unavailable_counter(), 1);
        break;
      }

      // The node's min reservation is for the first thread so we don't need to check
      if (!buffer_pool_client()->IncreaseReservation(scanner_thread_reservation)) {
        scanner_mem_limiter->ReleaseMemoryForScannerThread(this, est_mem);
        COUNTER_ADD(scanner_thread_reservations_denied_counter_, 1);
        break;
      }
    }

    // Case 8 and 9.
    if (first_thread) {
      // The first thread is required to make progress on the scan.
      pool->AcquireThreadToken();
    } else if (thread_state_.GetNumActive() >= thread_state_.max_num_scanner_threads()
        || !pool->TryAcquireThreadToken()) {
      scanner_mem_limiter->ReleaseMemoryForScannerThread(this, est_mem);
      ReturnReservationFromScannerThread(lock, scanner_thread_reservation);
      break;
    }

    string name = Substitute("scanner-thread (finst:$0, plan-node-id:$1, thread-idx:$2)",
        PrintId(runtime_state_->fragment_instance_id()), id(),
        thread_state_.GetNumStarted());
    auto fn = [this, first_thread, scanner_thread_reservation]() {
      this->ScannerThread(first_thread, scanner_thread_reservation);
    };
    std::unique_ptr<Thread> t;
    status = Thread::Create(
        FragmentInstanceState::FINST_THREAD_GROUP_NAME, name, fn, &t, true);
    if (!status.ok()) {
      if (!first_thread) {
        scanner_mem_limiter->ReleaseMemoryForScannerThread(this, est_mem);
      }
      ReturnReservationFromScannerThread(lock, scanner_thread_reservation);
      // Release the token and skip running callbacks to find a replacement. Skipping
      // serves two purposes. First, it prevents a mutual recursion between this function
      // and ReleaseThreadToken()->InvokeCallbacks(). Second, Thread::Create() failed and
      // is likely to continue failing for future callbacks.
      pool->ReleaseThreadToken(first_thread, true);

      // Abort the query. This is still holding the lock_, so done_ is known to be
      // false and status_ must be ok.
      discard_result(ExecDebugAction(TExecNodePhase::SCANNER_ERROR, runtime_state_));
      SetDoneInternal(status);
      break;
    }
    // Thread successfully started
    thread_state_.AddThread(move(t));
  }
}

void HdfsScanNode::ScannerThread(bool first_thread, int64_t scanner_thread_reservation) {
  SCOPED_THREAD_COUNTER_MEASUREMENT(thread_state_.thread_counters());
  SCOPED_THREAD_COUNTER_MEASUREMENT(runtime_state_->total_thread_statistics());
  // Make thread-local copy of filter contexts to prune scan ranges, and to pass to the
  // scanner for finer-grained filtering. Use a thread-local MemPool for the filter
  // contexts as the embedded expression evaluators may allocate from it and MemPool
  // is not thread safe.
  MemPool filter_mem_pool(expr_mem_tracker());
  MemPool expr_results_pool(expr_mem_tracker());
  vector<FilterContext> filter_ctxs;
  Status filter_status = Status::OK();
  for (auto& filter_ctx: filter_ctxs_) {
    FilterContext filter;
    filter_status = filter.CloneFrom(filter_ctx, pool_, runtime_state_, &filter_mem_pool,
        &expr_results_pool);
    if (!filter_status.ok()) break;
    filter_ctxs.push_back(filter);
  }

  while (!done()) {
    // Prevent memory accumulating across scan ranges.
    expr_results_pool.Clear();
    // Check if we have enough thread tokens to keep using this optional thread. This
    // check is racy: multiple threads may notice that the optional tokens are exceeded
    // and shut themselves down. If we shut down too many and there are more optional
    // tokens, ThreadAvailableCb() will be invoked again.
    if (!first_thread && runtime_state_->resource_pool()->optional_exceeded()) break;

    bool unused = false;
    // Wake up every SCANNER_THREAD_COUNTERS to yield scanner threads back if unused, or
    // to return if there's an error.
    ranges_issued_barrier_.Wait(SCANNER_THREAD_WAIT_TIME_MS, &unused);

    // Take a snapshot of remaining_scan_range_submissions before calling
    // StartNextScanRange().  We don't want it to go to zero between the return from
    // StartNextScanRange() and the check for when all ranges are complete.
    int remaining_scan_range_submissions = shared_state_->RemainingScanRangeSubmissions();
    ScanRange* scan_range;
    Status status =
        StartNextScanRange(filter_ctxs, &scanner_thread_reservation, &scan_range);
    if (!status.ok()) {
      unique_lock<timed_mutex> l(lock_);
      // If there was already an error, the main thread will do the cleanup
      if (!status_.ok()) break;

      // Invoke SetDoneInternal() with the error status (which shuts down the
      // RowBatchQueue) to ensure that GetNextInternal() notices the error.
      SetDoneInternal(status);
      break;
    }
    if (scan_range != nullptr) {
      discard_result(DebugAction(
          runtime_state_->query_options(), "HDFS_SCANNER_THREAD_OBTAINED_RANGE"));
      // Got a scan range. Process the range end to end (in this thread).
      ProcessSplit(filter_status.ok() ? filter_ctxs : vector<FilterContext>(),
          &expr_results_pool, scan_range, &scanner_thread_reservation);
    }

    // Done with range and it completed successfully
    if (shared_state_->progress().done()) {
      // All ranges are finished.  Indicate we are done.
      SetDone();
      break;
    }

    if (scan_range == nullptr && remaining_scan_range_submissions == 0) {
      unique_lock<timed_mutex> l(lock_);
      // All ranges have been queued and DiskIoMgr has no more new ranges for this scan
      // node to process. This means that every range is either done or being processed by
      // another thread.
      all_ranges_started_ = true;
      break;
    }

    // Stop extra threads if we're over a soft limit in order to free up memory.
    if (!first_thread &&
        (mem_tracker_->AnyLimitExceeded(MemLimit::SOFT) ||
          !DebugAction(runtime_state_->query_options(),
              "HDFS_SCANNER_THREAD_CHECK_SOFT_MEM_LIMIT").ok())) {
      VLOG_QUERY << "Soft memory limit exceeded. Extra scanner thread exiting.";
      break;
    }

    if (scan_range == nullptr) COUNTER_ADD(scanner_thread_workless_loops_counter_, 1);
  }

  {
    unique_lock<timed_mutex> l(lock_);
    ReturnReservationFromScannerThread(l, scanner_thread_reservation);
  }
  for (auto& ctx: filter_ctxs) ctx.expr_eval->Close(runtime_state_);
  filter_mem_pool.FreeAll();
  expr_results_pool.FreeAll();
  runtime_state_->resource_pool()->ReleaseThreadToken(first_thread);
  if (!first_thread) {
    // Memory for the first thread is released in thread_state_.Close().
    runtime_state_->query_state()->scanner_mem_limiter()->ReleaseMemoryForScannerThread(
        this, thread_state_.estimated_per_thread_mem());
  }
  thread_state_.DecrementNumActive();
}

void HdfsScanNode::ProcessSplit(const vector<FilterContext>& filter_ctxs,
    MemPool* expr_results_pool, ScanRange* scan_range,
    int64_t* scanner_thread_reservation) {
  DCHECK(scan_range != nullptr);
  ScanRangeMetadata* metadata = static_cast<ScanRangeMetadata*>(scan_range->meta_data());
  int64_t partition_id = metadata->partition_id;
  HdfsPartitionDescriptor* partition = hdfs_table_->GetPartition(partition_id);
  DCHECK(partition != nullptr) << "table_id=" << hdfs_table_->id()
                               << " partition_id=" << partition_id
                               << "\n" << PrintThrift(runtime_state_->instance_ctx());
  ScannerContext context(runtime_state_, this, buffer_pool_client(),
      *scanner_thread_reservation, partition, filter_ctxs, expr_results_pool);
  context.AddStream(scan_range, *scanner_thread_reservation);
  scoped_ptr<HdfsScanner> scanner;
  Status status = CreateAndOpenScannerHelper(partition, &context, &scanner);
  if (!status.ok()) {
    // If preparation fails, avoid leaking unread buffers in the scan_range.
    scan_range->Cancel(status);

    if (VLOG_QUERY_IS_ON) {
      stringstream ss;
      ss << "Error preparing scanner for scan range " << scan_range->file() <<
          "(" << scan_range->offset() << ":" << scan_range->len() << "). ";
      ss << status.msg().msg() << endl << runtime_state_->ErrorLog();
      VLOG_QUERY << ss.str();
    }

    // Ensure that the error is propagated before marking a range as complete (The
    // scanner->Close() call marks a scan range as complete).
    SetError(status);
    if (scanner != nullptr) scanner->Close();
    return;
  }

  status = scanner->ProcessSplit();
  if (!status.ok()) {
    if (VLOG_QUERY_IS_ON && !status.IsCancelled()) {
      // This thread hit an error, record it and bail
      stringstream ss;
      ss << "Scan node (id=" << id() << ") ran into a parse error for scan range "
         << scan_range->file() << "(" << scan_range->offset() << ":" << scan_range->len()
         << ").";
      // Parquet doesn't read the range end to end so the current offset isn't useful.
      // TODO: make sure the parquet reader is outputting as much diagnostic
      // information as possible.
      if (partition->file_format() != THdfsFileFormat::PARQUET) {
        ScannerContext::Stream* stream = context.GetStream();
        ss << " Processed " << stream->total_bytes_returned() << " bytes.";
      }
      VLOG_QUERY << ss.str();
    }

    // If status is the first non-ok status returned by any scanner thread, update
    // HdfsScanNodeBase::status_ variable and notify other scanner threads. Ensure that
    // status_ is updated before marking a range as complete (The scanner->Close() call
    // marks a scan range as complete).
    SetError(status);
  }
  // Transfer remaining resources to a final batch and add it to the row batch queue and
  // decrement progress() to indicate that the scan range is complete.
  scanner->Close();
  // Reservation may have been increased by the scanner, e.g. Parquet may allocate
  // additional reservation to scan columns.
  *scanner_thread_reservation = context.total_reservation();
}

void HdfsScanNode::SetDoneInternal(const Status& status) {
  // If the scan node is already in the done state, do nothing.
  if (done()) return;
  DCHECK(status_.ok());
  done_.Store(true);
  if (!status.ok()) status_ = status;
  // TODO: Cancelling the RequestContext will wait on in-flight IO. We should
  //       investigate dropping the lock_ or restructuring this cancel.
  if (reader_context_ != nullptr) reader_context_->Cancel();
  thread_state_.Shutdown();
}

void HdfsScanNode::SetDone() {
  unique_lock<timed_mutex> l(lock_);
  SetDoneInternal(status_);
}

void HdfsScanNode::SetError(const Status& status) {
  discard_result(ExecDebugAction(TExecNodePhase::SCANNER_ERROR, runtime_state_));
  unique_lock<timed_mutex> l(lock_);
  SetDoneInternal(status);
}

Status HdfsScanNode::GetNextScanRangeToRead(
    io::ScanRange** scan_range, bool* needs_buffers) {
  return reader_context_->GetNextUnstartedRange(scan_range, needs_buffers);
}
