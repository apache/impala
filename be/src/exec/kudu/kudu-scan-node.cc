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

#include "exec/kudu/kudu-scan-node.h"

#include <thrift/protocol/TDebugProtocol.h>

#include "exec/exec-node-util.h"
#include "exec/kudu/kudu-scanner.h"
#include "exec/kudu/kudu-util.h"
#include "exprs/scalar-expr.h"
#include "gutil/gscoped_ptr.h"
#include "runtime/blocking-row-batch-queue.h"
#include "runtime/fragment-instance-state.h"
#include "runtime/mem-pool.h"
#include "runtime/query-state.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/scanner-mem-limiter.h"
#include "runtime/thread-resource-mgr.h"
#include "runtime/tuple-row.h"
#include "util/debug-util.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

DEFINE_int32(kudu_max_row_batches, 0, "The maximum size of the row batch queue, "
    " for Kudu scanners.");

// Empirically derived estimate for the Kudu scan's memory consumption per column of
// data materialized.
DEFINE_int64_hidden(kudu_scanner_thread_estimated_bytes_per_column, 384L * 1024L,
    "Estimated bytes of memory per materialized column consumed by Kudu scanner thread.");

// Empirically derived estimate for the maximum consumption of Kudu scan, based on
// experiments with 250-column table with num_scanner_threads=1, where I wasn't able
// to coax the scan to use more than 25MB of memory.
DEFINE_int64_hidden(kudu_scanner_thread_max_estimated_bytes, 32L * 1024L * 1024L,
    "Estimated maximum bytes of memory consumed by Kudu scanner thread for high column "
    "counts.");

namespace impala {

KuduScanNode::KuduScanNode(ObjectPool* pool, const ScanPlanNode& pnode,
    const DescriptorTbl& descs)
    : KuduScanNodeBase(pool, pnode, descs),
      thread_avail_cb_id_(-1) {
  DCHECK(KuduIsAvailable());
}

KuduScanNode::~KuduScanNode() {
  DCHECK(is_closed());
}

Status KuduScanNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(KuduScanNodeBase::Prepare(state));
  thread_state_.Prepare(this, EstimateScannerThreadMemConsumption());
  return Status::OK();
}

Status KuduScanNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedOpenEventAdder ea(this);
  RETURN_IF_ERROR(KuduScanNodeBase::Open(state));
  thread_state_.Open(this, FLAGS_kudu_max_row_batches);
  thread_avail_cb_id_ = state->resource_pool()->AddThreadAvailableCb(
      bind<void>(mem_fn(&KuduScanNode::ThreadAvailableCb), this, _1));
  ThreadAvailableCb(state->resource_pool());
  return Status::OK();
}

Status KuduScanNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedGetNextEventAdder ea(this, eos);
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));

  // If there are no scan tokens, nothing is ever placed in the materialized
  // row batch, so exit early for this case.
  if (NumScanTokens() == 0 || ReachedLimitShared()) {
    *eos = true;
    return Status::OK();
  }

  *eos = false;
  unique_ptr<RowBatch> materialized_batch = thread_state_.batch_queue()->GetBatch();
  if (materialized_batch != NULL) {
    row_batch->AcquireState(materialized_batch.get());
    if (CheckLimitAndTruncateRowBatchIfNeededShared(row_batch, eos)) {
      SetDone();
    }
    COUNTER_SET(rows_returned_counter_, rows_returned_shared());
    materialized_batch.reset();
  } else {
    *eos = true;
  }

  unique_lock<mutex> l(lock_);
  return status_;
}

void KuduScanNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  if (thread_avail_cb_id_ != -1) {
    state->resource_pool()->RemoveThreadAvailableCb(thread_avail_cb_id_);
  }

  SetDone();

  thread_state_.Close(this);
  KuduScanNodeBase::Close(state);
}

int64_t KuduScanNode::EstimateScannerThreadMemConsumption() {
  int64_t num_cols = max<int64_t>(1, tuple_desc()->slots().size());
  return min(FLAGS_kudu_scanner_thread_max_estimated_bytes,
      FLAGS_kudu_scanner_thread_estimated_bytes_per_column * num_cols);
}

void KuduScanNode::ThreadAvailableCb(ThreadResourcePool* pool) {
  ScannerMemLimiter* mem_limiter = runtime_state_->query_state()->scanner_mem_limiter();
  while (true) {
    unique_lock<mutex> lock(lock_);
    // All done or all tokens are assigned.
    if (done_.Load() || !HasScanToken()) break;
    bool first_thread = thread_state_.GetNumActive() == 0;

    // * Don't start up a ScannerThread if the row batch queue is full since
    //    we are not scanner bound.
    // * Don't start up a thread if there is not enough memory available for the
    //    estimated memory consumption (include reservation and non-reserved memory).
    if (!first_thread) {
      if (thread_state_.batch_queue()->IsFull()) break;
      if (!mem_limiter->ClaimMemoryForScannerThread(
              this, EstimateScannerThreadMemConsumption())) {
        COUNTER_ADD(thread_state_.scanner_thread_mem_unavailable_counter(), 1);
        break;
      }
    }

    // Check if we can get a token. We need at least one thread to run.
    if (first_thread) {
      pool->AcquireThreadToken();
    } else if (thread_state_.GetNumActive() >= thread_state_.max_num_scanner_threads()
        || !pool->TryAcquireThreadToken()) {
      mem_limiter->ReleaseMemoryForScannerThread(
          this, EstimateScannerThreadMemConsumption());
      break;
    }

    string name = Substitute(
        "kudu-scanner-thread (finst:$0, plan-node-id:$1, thread-idx:$2)",
        PrintId(runtime_state_->fragment_instance_id()), id(),
        thread_state_.GetNumStarted());

    // Reserve the first token so no other thread picks it up.
    const string* token = GetNextScanToken();
    auto fn = [this, first_thread, token, name]() {
      this->RunScannerThread(first_thread, name, token);
    };
    std::unique_ptr<Thread> t;
    Status status =
      Thread::Create(FragmentInstanceState::FINST_THREAD_GROUP_NAME, name, fn, &t, true);
    if (!status.ok()) {
      // Release the token and skip running callbacks to find a replacement. Skipping
      // serves two purposes. First, it prevents a mutual recursion between this function
      // and ReleaseThreadToken()->InvokeCallbacks(). Second, Thread::Create() failed and
      // is likely to continue failing for future callbacks.
      pool->ReleaseThreadToken(first_thread, true);
      if (!first_thread) {
        mem_limiter->ReleaseMemoryForScannerThread(
            this, EstimateScannerThreadMemConsumption());
      }

      // Abort the query. This is still holding the lock_, so done_ is known to be
      // false and status_ must be ok.
      DCHECK(status_.ok());
      status_ = status;
      SetDoneInternal();
      break;
    }
    // Thread successfully started
    thread_state_.AddThread(move(t));
  }
}

Status KuduScanNode::ProcessScanToken(KuduScanner* scanner, const string& scan_token) {
  bool eos;
  RETURN_IF_ERROR(scanner->OpenNextScanToken(scan_token, &eos));
  if (eos) return Status::OK();
  while (!eos && !done_.Load()) {
    unique_ptr<RowBatch> row_batch = std::make_unique<RowBatch>(row_desc(),
        runtime_state_->batch_size(), mem_tracker());
    RETURN_IF_ERROR(scanner->GetNext(row_batch.get(), &eos));
    while (!done_.Load()) {
      scanner->KeepKuduScannerAlive();
      if (thread_state_.EnqueueBatchWithTimeout(&row_batch, 1000000)) {
        break;
      }
      // Make sure that we still own the RowBatch if BlockingPutWithTimeout() timed out.
      DCHECK(row_batch != nullptr);
    }
  }
  if (eos) scan_ranges_complete_counter_->Add(1);
  return Status::OK();
}

void KuduScanNode::RunScannerThread(
    bool first_thread, const string& name, const string* initial_token) {
  DCHECK(initial_token != nullptr);
  SCOPED_THREAD_COUNTER_MEASUREMENT(thread_state_.thread_counters());
  SCOPED_THREAD_COUNTER_MEASUREMENT(runtime_state_->total_thread_statistics());
  KuduScanner scanner(this, runtime_state_);

  const string* scan_token = initial_token;
  Status status = scanner.Open();
  if (status.ok()) {
    while (!done_.Load() && scan_token != nullptr) {
      status = ProcessScanToken(&scanner, *scan_token);
      if (!status.ok()) break;

      // Check if we have enough thread tokens to keep using this optional thread. This
      // check is racy: multiple threads may notice that the optional tokens are exceeded
      // and shut themselves down. If we shut down too many and there are more optional
      // tokens, ThreadAvailableCb() will be invoked again.
      if (!first_thread && runtime_state_->resource_pool()->optional_exceeded()) break;

      if (!done_.Load()) {
        unique_lock<mutex> l(lock_);
        scan_token = GetNextScanToken();
      } else {
        scan_token = nullptr;
      }
    }
  }
  scanner.Close();

  {
    unique_lock<mutex> l(lock_);
    if (!status.ok() && status_.ok()) {
      status_ = status;
      SetDoneInternal();
    }
    if (thread_state_.DecrementNumActive()) SetDoneInternal();
  }

  // lock_ is released before calling ThreadResourceMgr::ReleaseThreadToken() which
  // invokes ThreadAvailableCb() which attempts to take the same lock.
  VLOG_RPC << "Thread done: " << name;
  if (!first_thread) {
    ScannerMemLimiter* mem_limiter = runtime_state_->query_state()->scanner_mem_limiter();
    mem_limiter->ReleaseMemoryForScannerThread(
        this, EstimateScannerThreadMemConsumption());
  }
  runtime_state_->resource_pool()->ReleaseThreadToken(first_thread);
}

void KuduScanNode::SetDoneInternal() {
  if (done_.Load()) return;
  done_.Store(true);
  thread_state_.Shutdown();
}

void KuduScanNode::SetDone() {
  unique_lock<mutex> l(lock_);
  SetDoneInternal();
}

}  // namespace impala
