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

#include <memory>
#include <sstream>

#include "common/logging.h"
#include "exec/base-sequence-scanner.h"
#include "exec/hdfs-scanner.h"
#include "exec/scanner-context.h"
#include "runtime/descriptors.h"
#include "runtime/fragment-instance-state.h"
#include "runtime/io/request-context.h"
#include "runtime/runtime-filter.inline.h"
#include "runtime/runtime-state.h"
#include "runtime/mem-tracker.h"
#include "runtime/row-batch.h"
#include "util/debug-util.h"
#include "util/disk-info.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

DEFINE_int32(max_row_batches, 0, "the maximum size of materialized_row_batches_");

#ifndef NDEBUG
DECLARE_bool(skip_file_runtime_filtering);
#endif

using namespace impala;
using namespace impala::io;

// Amount of time to block waiting for GetNext() to release scanner threads between
// checking if a scanner thread should yield itself back to the global thread pool.
const int SCANNER_THREAD_WAIT_TIME_MS = 20;

HdfsScanNode::HdfsScanNode(ObjectPool* pool, const TPlanNode& tnode,
                           const DescriptorTbl& descs)
    : HdfsScanNodeBase(pool, tnode, descs),
      max_num_scanner_threads_(CpuInfo::num_cores()) {
}

HdfsScanNode::~HdfsScanNode() {
}

Status HdfsScanNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());

  if (!initial_ranges_issued_) {
    // We do this in GetNext() to maximise the amount of work we can do while waiting for
    // runtime filters to show up. The scanner threads have already started (in Open()),
    // so we need to tell them there is work to do.
    // TODO: This is probably not worth splitting the organisational cost of splitting
    // initialisation across two places. Move to before the scanner threads start.
    RETURN_IF_ERROR(IssueInitialScanRanges(state));

    // Release the scanner threads
    ranges_issued_barrier_.Notify();

    if (progress_.done()) SetDone();
  }

  Status status = GetNextInternal(state, row_batch, eos);
  if (!status.ok() || *eos) {
    unique_lock<mutex> l(lock_);
    lock_guard<SpinLock> l2(file_type_counts_);
    StopAndFinalizeCounters();
    row_batches_put_timer_->Set(materialized_row_batches_->total_put_wait_time());
    row_batches_get_timer_->Set(materialized_row_batches_->total_get_wait_time());
  }
  return status;
}

Status HdfsScanNode::GetNextInternal(
    RuntimeState* state, RowBatch* row_batch, bool* eos) {
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));

  if (ReachedLimit()) {
    // LIMIT 0 case.  Other limit values handled below.
    DCHECK_EQ(limit_, 0);
    *eos = true;
    return Status::OK();
  }
  *eos = false;
  unique_ptr<RowBatch> materialized_batch = materialized_row_batches_->GetBatch();
  if (materialized_batch != NULL) {
    row_batch->AcquireState(materialized_batch.get());
    // Update the number of materialized rows now instead of when they are materialized.
    // This means that scanners might process and queue up more rows than are necessary
    // for the limit case but we want to avoid the synchronized writes to
    // num_rows_returned_.
    num_rows_returned_ += row_batch->num_rows();
    COUNTER_SET(rows_returned_counter_, num_rows_returned_);

    if (ReachedLimit()) {
      int num_rows_over = num_rows_returned_ - limit_;
      row_batch->set_num_rows(row_batch->num_rows() - num_rows_over);
      num_rows_returned_ -= num_rows_over;
      COUNTER_SET(rows_returned_counter_, num_rows_returned_);

      *eos = true;
      SetDone();
    }
    materialized_batch.reset();
    return Status::OK();
  }
  // The RowBatchQueue was shutdown either because all scan ranges are complete or a
  // scanner thread encountered an error.  Check status_ to distinguish those cases.
  *eos = true;
  unique_lock<mutex> l(lock_);
  return status_;
}

Status HdfsScanNode::Init(const TPlanNode& tnode, RuntimeState* state) {
  int default_max_row_batches = FLAGS_max_row_batches;
  if (default_max_row_batches <= 0) {
    default_max_row_batches = 10 * (DiskInfo::num_disks() + DiskIoMgr::REMOTE_NUM_DISKS);
  }
  if (state->query_options().__isset.mt_dop && state->query_options().mt_dop > 0) {
    // To avoid a significant memory increase, adjust the number of maximally queued
    // row batches per scan instance based on MT_DOP. The max materialized row batches
    // is at least 2 to allow for some parallelism between the producer/consumer.
    max_materialized_row_batches_ =
        max(2, default_max_row_batches / state->query_options().mt_dop);
  } else {
    max_materialized_row_batches_ = default_max_row_batches;
  }
  VLOG_QUERY << "Max row batch queue size for scan node '" << id_
      << "' in fragment instance '" << state->fragment_instance_id()
      << "': " << max_materialized_row_batches_;
  materialized_row_batches_.reset(new RowBatchQueue(max_materialized_row_batches_));
  return HdfsScanNodeBase::Init(tnode, state);
}

Status HdfsScanNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(HdfsScanNodeBase::Prepare(state));
  row_batches_get_timer_ = ADD_TIMER(runtime_profile(), "RowBatchQueueGetWaitTime");
  row_batches_put_timer_ = ADD_TIMER(runtime_profile(), "RowBatchQueuePutWaitTime");
  return Status::OK();
}

// This function registers the ThreadTokenAvailableCb to start up the initial scanner
// threads. Scan ranges are not issued until the first GetNext() call; scanner threads
// will block on ranges_issued_barrier_ until ranges are issued.
Status HdfsScanNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(HdfsScanNodeBase::Open(state));

  if (file_descs_.empty() || progress_.done()) return Status::OK();

  // We need at least one scanner thread to make progress. We need to make this
  // reservation before any ranges are issued.
  runtime_state_->resource_pool()->ReserveOptionalTokens(1);
  if (runtime_state_->query_options().num_scanner_threads > 0) {
    max_num_scanner_threads_ = runtime_state_->query_options().num_scanner_threads;
  }
  DCHECK_GT(max_num_scanner_threads_, 0);
  spare_reservation_.Store(buffer_pool_client_.GetReservation());
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
  scanner_threads_.JoinAll();
  materialized_row_batches_->Cleanup();
  HdfsScanNodeBase::Close(state);
}

void HdfsScanNode::RangeComplete(const THdfsFileFormat::type& file_type,
    const std::vector<THdfsCompression::type>& compression_type, bool skipped) {
  lock_guard<SpinLock> l(file_type_counts_);
  HdfsScanNodeBase::RangeComplete(file_type, compression_type, skipped);
}

void HdfsScanNode::TransferToScanNodePool(MemPool* pool) {
  unique_lock<mutex> l(lock_);
  HdfsScanNodeBase::TransferToScanNodePool(pool);
}

void HdfsScanNode::AddMaterializedRowBatch(unique_ptr<RowBatch> row_batch) {
  InitNullCollectionValues(row_batch.get());
  materialized_row_batches_->AddBatch(move(row_batch));
}

Status HdfsScanNode::AddDiskIoRanges(const vector<ScanRange*>& ranges,
    int num_files_queued) {
  RETURN_IF_ERROR(
      runtime_state_->io_mgr()->AddScanRanges(reader_context_.get(), ranges));
  num_unqueued_files_.Add(-num_files_queued);
  DCHECK_GE(num_unqueued_files_.Load(), 0);
  if (!ranges.empty()) ThreadTokenAvailableCb(runtime_state_->resource_pool());
  return Status::OK();
}

bool HdfsScanNode::EnoughReservationForExtraThread(const unique_lock<mutex>& lock) {
  DCHECK(lock.mutex() == &lock_ && lock.owns_lock());
  if (spare_reservation_.Load() >= ideal_scan_range_reservation_) return true;
  int64_t increase = ideal_scan_range_reservation_ - spare_reservation_.Load();
  if (!buffer_pool_client_.IncreaseReservation(increase)) return false;
  spare_reservation_.Add(increase);
  return true;
}

int64_t HdfsScanNode::DeductReservationForScannerThread(const unique_lock<mutex>& lock,
    bool first_thread) {
  DCHECK(lock.mutex() == &lock_ && lock.owns_lock());
  int64_t amount;
  if (first_thread) {
    amount = spare_reservation_.Load() >= ideal_scan_range_reservation_ ?
        ideal_scan_range_reservation_ : resource_profile_.min_reservation;
  } else {
    amount = ideal_scan_range_reservation_;
  }
  int64_t remainder = spare_reservation_.Add(-amount);
  DCHECK_GE(remainder, 0);
  return amount;
}

void HdfsScanNode::ThreadTokenAvailableCb(ThreadResourceMgr::ResourcePool* pool) {
  // This is called to start up new scanner threads. It's not a big deal if we
  // spin up more than strictly necessary since they will go through and terminate
  // promptly. However, we want to minimize that by checking a conditions.
  //  1. Don't start up if the ScanNode is done
  //  2. Don't start up if all the ranges have been taken by another thread.
  //  3. Don't start up if the number of ranges left is less than the number of
  //     active scanner threads.
  //  4. Don't start up if no initial ranges have been issued (see IMPALA-1722).
  //  5. Don't start up a ScannerThread if materialized_row_batches_ is full since
  //     we are not scanner bound.
  //  6. Don't start up a thread if there isn't enough memory left to run it.
  //  7. Don't start up more than maximum number of scanner threads configured.
  //  8. Don't start up if there are no thread tokens.

  // Case 4. We have not issued the initial ranges so don't start a scanner thread.
  // Issuing ranges will call this function and we'll start the scanner threads then.
  // TODO: It would be good to have a test case for that.
  if (!initial_ranges_issued_) return;

  Status status = Status::OK();
  while (true) {
    // The lock must be given up between loops in order to give writers to done_,
    // all_ranges_started_ etc. a chance to grab the lock.
    // TODO: This still leans heavily on starvation-free locks, come up with a more
    // correct way to communicate between this method and ScannerThreadHelper
    unique_lock<mutex> lock(lock_);

    const int64_t num_active_scanner_threads = active_scanner_thread_counter_.value();
    const bool first_thread = num_active_scanner_threads == 0;
    // Cases 1, 2, 3.
    if (done_ || all_ranges_started_ ||
        num_active_scanner_threads >= progress_.remaining()) {
      break;
    }

    // Cases 5 and 6.
    if (!first_thread &&
        (materialized_row_batches_->Size() >= max_materialized_row_batches_ ||
         !EnoughReservationForExtraThread(lock))) {
      break;
    }

    // Case 7 and 8.
    if (num_active_scanner_threads >= max_num_scanner_threads_ ||
        !pool->TryAcquireThreadToken()) {
      break;
    }

    // Deduct the reservation. We haven't dropped the lock since the
    // first_thread/EnoughReservationForExtraThread() checks so spare reservation
    // must be available.
    int64_t scanner_thread_reservation =
        DeductReservationForScannerThread(lock, first_thread);
    COUNTER_ADD(&active_scanner_thread_counter_, 1);
    string name = Substitute("scanner-thread (finst:$0, plan-node-id:$1, thread-idx:$2)",
        PrintId(runtime_state_->fragment_instance_id()), id(),
        num_scanner_threads_started_counter_->value());
    auto fn = [this, scanner_thread_reservation]() {
      this->ScannerThread(scanner_thread_reservation);
    };
    std::unique_ptr<Thread> t;
    status =
      Thread::Create(FragmentInstanceState::FINST_THREAD_GROUP_NAME, name, fn, &t, true);
    if (!status.ok()) {
      ReturnReservationFromScannerThread(scanner_thread_reservation);
      COUNTER_ADD(&active_scanner_thread_counter_, -1);
      // Release the token and skip running callbacks to find a replacement. Skipping
      // serves two purposes. First, it prevents a mutual recursion between this function
      // and ReleaseThreadToken()->InvokeCallbacks(). Second, Thread::Create() failed and
      // is likely to continue failing for future callbacks.
      pool->ReleaseThreadToken(false, true);

      // Abort the query. This is still holding the lock_, so done_ is known to be
      // false and status_ must be ok.
      DCHECK(status_.ok());
      status_ = status;
      SetDoneInternal();
      break;
    }
    // Thread successfully started
    COUNTER_ADD(num_scanner_threads_started_counter_, 1);
    scanner_threads_.AddThread(move(t));
  }
}

void HdfsScanNode::ScannerThread(int64_t scanner_thread_reservation) {
  SCOPED_THREAD_COUNTER_MEASUREMENT(scanner_thread_counters());
  SCOPED_THREAD_COUNTER_MEASUREMENT(runtime_state_->total_thread_statistics());
  DiskIoMgr* io_mgr = runtime_state_->io_mgr();

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

  while (!done_) {
    // Prevent memory accumulating across scan ranges.
    expr_results_pool.Clear();
    {
      // Check if we have enough resources (thread token and memory) to keep using
      // this thread.
      unique_lock<mutex> l(lock_);
      if (active_scanner_thread_counter_.value() > 1) {
        if (runtime_state_->resource_pool()->optional_exceeded()) {
          // We can't break here. We need to update the counter with the lock held or else
          // all threads might see active_scanner_thread_counter_.value > 1
          COUNTER_ADD(&active_scanner_thread_counter_, -1);
          // Unlock before releasing the thread token to avoid deadlock in
          // ThreadTokenAvailableCb().
          l.unlock();
          goto exit;
        }
      } else {
        // If this is the only scanner thread, it should keep running regardless
        // of resource constraints.
      }
    }

    bool unused = false;
    // Wake up every SCANNER_THREAD_COUNTERS to yield scanner threads back if unused, or
    // to return if there's an error.
    ranges_issued_barrier_.Wait(SCANNER_THREAD_WAIT_TIME_MS, &unused);

    // Take a snapshot of num_unqueued_files_ before calling GetNextUnstartedRange().
    // We don't want num_unqueued_files_ to go to zero between the return from
    // GetNextUnstartedRange() and the check for when all ranges are complete.
    int num_unqueued_files = num_unqueued_files_.Load();
    // TODO: the Load() acts as an acquire barrier.  Is this needed? (i.e. any earlier
    // stores that need to complete?)
    AtomicUtil::MemoryBarrier();
    ScanRange* scan_range;
    bool needs_buffers;
    Status status =
        io_mgr->GetNextUnstartedRange(reader_context_.get(), &scan_range, &needs_buffers);

    if (status.ok() && scan_range != nullptr) {
      if (needs_buffers) {
        status = io_mgr->AllocateBuffersForRange(
            reader_context_.get(), &buffer_pool_client_, scan_range,
            scanner_thread_reservation);
      }
      if (status.ok()) {
        // Got a scan range. Process the range end to end (in this thread).
        status = ProcessSplit(filter_status.ok() ? filter_ctxs : vector<FilterContext>(),
            &expr_results_pool, scan_range, scanner_thread_reservation);
      }
    }

    if (!status.ok()) {
      unique_lock<mutex> l(lock_);
      // If there was already an error, the main thread will do the cleanup
      if (!status_.ok()) break;

      if (status.IsCancelled() && done_) {
        // Scan node initiated scanner thread cancellation.  No need to do anything.
        break;
      }
      // Set status_ before calling SetDone() (which shuts down the RowBatchQueue),
      // to ensure that GetNextInternal() notices the error status.
      status_ = status;
      SetDoneInternal();
      break;
    }

    // Done with range and it completed successfully
    if (progress_.done()) {
      // All ranges are finished.  Indicate we are done.
      SetDone();
      break;
    }

    if (scan_range == NULL && num_unqueued_files == 0) {
      // TODO: Based on the usage pattern of all_ranges_started_, it looks like it is not
      // needed to acquire the lock in x86.
      unique_lock<mutex> l(lock_);
      // All ranges have been queued and GetNextUnstartedRange() returned NULL. This means
      // that every range is either done or being processed by another thread.
      all_ranges_started_ = true;
      break;
    }
  }
  COUNTER_ADD(&active_scanner_thread_counter_, -1);

exit:
  ReturnReservationFromScannerThread(scanner_thread_reservation);
  runtime_state_->resource_pool()->ReleaseThreadToken(false);
  for (auto& ctx: filter_ctxs) ctx.expr_eval->Close(runtime_state_);
  filter_mem_pool.FreeAll();
  expr_results_pool.FreeAll();
}

Status HdfsScanNode::ProcessSplit(const vector<FilterContext>& filter_ctxs,
    MemPool* expr_results_pool, ScanRange* scan_range,
    int64_t scanner_thread_reservation) {
  DCHECK(scan_range != NULL);

  ScanRangeMetadata* metadata = static_cast<ScanRangeMetadata*>(scan_range->meta_data());
  int64_t partition_id = metadata->partition_id;
  HdfsPartitionDescriptor* partition = hdfs_table_->GetPartition(partition_id);
  DCHECK(partition != NULL) << "table_id=" << hdfs_table_->id()
                            << " partition_id=" << partition_id
                            << "\n" << PrintThrift(runtime_state_->instance_ctx());

  if (!PartitionPassesFilters(partition_id, FilterStats::SPLITS_KEY, filter_ctxs)) {
    // Avoid leaking unread buffers in scan_range.
    scan_range->Cancel(Status::CANCELLED);
    HdfsFileDesc* desc = GetFileDesc(partition_id, *scan_range->file_string());
    if (metadata->is_sequence_header) {
      // File ranges haven't been issued yet, skip entire file
      SkipFile(partition->file_format(), desc);
    } else {
      // Mark this scan range as done.
      HdfsScanNodeBase::RangeComplete(partition->file_format(), desc->file_compression,
          true);
    }
    return Status::OK();
  }

  ScannerContext context(runtime_state_, this, &buffer_pool_client_, partition,
      filter_ctxs, expr_results_pool);
  context.AddStream(scan_range, scanner_thread_reservation);
  scoped_ptr<HdfsScanner> scanner;
  Status status = CreateAndOpenScanner(partition, &context, &scanner);
  if (!status.ok()) {
    // If preparation fails, avoid leaking unread buffers in the scan_range.
    scan_range->Cancel(status);

    if (VLOG_QUERY_IS_ON) {
      stringstream ss;
      ss << "Error preparing scanner for scan range " << scan_range->file() <<
          "(" << scan_range->offset() << ":" << scan_range->len() << ").";
      ss << endl << runtime_state_->ErrorLog();
      VLOG_QUERY << ss.str();
    }
    return status;
  }

  status = scanner->ProcessSplit();
  if (VLOG_QUERY_IS_ON && !status.ok() && !status.IsCancelled()) {
    // This thread hit an error, record it and bail
    stringstream ss;
    ss << "Scan node (id=" << id() << ") ran into a parse error for scan range "
       << scan_range->file() << "(" << scan_range->offset() << ":"
       << scan_range->len() << ").";
    // Parquet doesn't read the range end to end so the current offset isn't useful.
    // TODO: make sure the parquet reader is outputting as much diagnostic
    // information as possible.
    if (partition->file_format() != THdfsFileFormat::PARQUET) {
      ScannerContext::Stream* stream = context.GetStream();
      ss << " Processed " << stream->total_bytes_returned() << " bytes.";
    }
    VLOG_QUERY << ss.str();
  }

  // Transfer remaining resources to a final batch and add it to the row batch queue.
  scanner->Close();
  return status;
}

void HdfsScanNode::SetDoneInternal() {
  if (done_) return;
  done_ = true;
  if (reader_context_ != nullptr) reader_context_->Cancel();
  materialized_row_batches_->Shutdown();
}

void HdfsScanNode::SetDone() {
  unique_lock<mutex> l(lock_);
  SetDoneInternal();
}
