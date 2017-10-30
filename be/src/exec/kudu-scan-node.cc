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

#include "exec/kudu-scan-node.h"

#include <thrift/protocol/TDebugProtocol.h>

#include "exec/kudu-scanner.h"
#include "exec/kudu-util.h"
#include "exprs/scalar-expr.h"
#include "gutil/gscoped_ptr.h"
#include "runtime/fragment-instance-state.h"
#include "runtime/mem-pool.h"
#include "runtime/runtime-state.h"
#include "runtime/row-batch.h"
#include "runtime/tuple-row.h"
#include "util/disk-info.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

DEFINE_int32(kudu_max_row_batches, 0, "The maximum size of the row batch queue, "
    " for Kudu scanners.");

namespace impala {

KuduScanNode::KuduScanNode(ObjectPool* pool, const TPlanNode& tnode,
    const DescriptorTbl& descs)
    : KuduScanNodeBase(pool, tnode, descs),
      num_active_scanners_(0),
      done_(false),
      thread_avail_cb_id_(-1) {
  DCHECK(KuduIsAvailable());

  int max_row_batches = FLAGS_kudu_max_row_batches;
  if (max_row_batches <= 0) {
    // TODO: See comment on hdfs-scan-node.
    // This value is built the same way as it assumes that the scan node runs co-located
    // with a Kudu tablet server and that the tablet server is using disks similarly as
    // a datanode would.
    max_row_batches = 10 * (DiskInfo::num_disks() + DiskIoMgr::REMOTE_NUM_DISKS);
  }
  materialized_row_batches_.reset(new RowBatchQueue(max_row_batches));
}

KuduScanNode::~KuduScanNode() {
  DCHECK(is_closed());
}

Status KuduScanNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(KuduScanNodeBase::Open(state));

  num_scanner_threads_started_counter_ =
      ADD_COUNTER(runtime_profile(), NUM_SCANNER_THREADS_STARTED, TUnit::UNIT);

  // Reserve one thread.
  state->resource_pool()->ReserveOptionalTokens(1);
  if (state->query_options().num_scanner_threads > 0) {
    state->resource_pool()->set_max_quota(
        state->query_options().num_scanner_threads);
  }

  thread_avail_cb_id_ = state->resource_pool()->AddThreadAvailableCb(
      bind<void>(mem_fn(&KuduScanNode::ThreadAvailableCb), this, _1));
  ThreadAvailableCb(state->resource_pool());
  return Status::OK();
}

Status KuduScanNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  DCHECK(row_batch != NULL);
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  SCOPED_TIMER(materialize_tuple_timer());

  // If there are no scan tokens, nothing is ever placed in the materialized
  // row batch, so exit early for this case.
  if (ReachedLimit() || NumScanTokens() == 0) {
    *eos = true;
    return Status::OK();
  }

  *eos = false;
  unique_ptr<RowBatch> materialized_batch = materialized_row_batches_->GetBatch();
  if (materialized_batch != NULL) {
    row_batch->AcquireState(materialized_batch.get());
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

  scanner_threads_.JoinAll();
  DCHECK_EQ(num_active_scanners_, 0);
  materialized_row_batches_->Cleanup();
  KuduScanNodeBase::Close(state);
}

void KuduScanNode::ThreadAvailableCb(ThreadResourceMgr::ResourcePool* pool) {
  while (true) {
    unique_lock<mutex> lock(lock_);
    // All done or all tokens are assigned.
    if (done_ || !HasScanToken()) break;

    // Check if we can get a token.
    if (!pool->TryAcquireThreadToken()) break;

    string name = Substitute(
        "kudu-scanner-thread (finst:$0, plan-node-id:$1, thread-idx:$2)",
        PrintId(runtime_state_->fragment_instance_id()), id(),
        num_scanner_threads_started_counter_->value());

    // Reserve the first token so no other thread picks it up.
    const string* token = GetNextScanToken();
    auto fn = [this, token, name]() { this->RunScannerThread(name, token); };
    std::unique_ptr<Thread> t;
    Status status =
      Thread::Create(FragmentInstanceState::FINST_THREAD_GROUP_NAME, name, fn, &t, true);
    if (!status.ok()) {
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
    ++num_active_scanners_;
    VLOG_RPC << "Thread started: " << name;
    scanner_threads_.AddThread(move(t));
  }
}

Status KuduScanNode::ProcessScanToken(KuduScanner* scanner, const string& scan_token) {
  RETURN_IF_ERROR(scanner->OpenNextScanToken(scan_token));
  bool eos = false;
  while (!eos && !done_) {
    unique_ptr<RowBatch> row_batch = std::make_unique<RowBatch>(row_desc(),
        runtime_state_->batch_size(), mem_tracker());
    RETURN_IF_ERROR(scanner->GetNext(row_batch.get(), &eos));
    while (!done_) {
      scanner->KeepKuduScannerAlive();
      if (materialized_row_batches_->BlockingPutWithTimeout(move(row_batch), 1000000)) {
        break;
      }
      // Make sure that we still own the RowBatch if BlockingPutWithTimeout() timed out.
      DCHECK(row_batch != nullptr);
    }
  }
  if (eos) scan_ranges_complete_counter()->Add(1);
  return Status::OK();
}

void KuduScanNode::RunScannerThread(const string& name, const string* initial_token) {
  DCHECK(initial_token != NULL);
  SCOPED_THREAD_COUNTER_MEASUREMENT(scanner_thread_counters());
  SCOPED_THREAD_COUNTER_MEASUREMENT(runtime_state_->total_thread_statistics());
  // Set to true if this thread observes that the number of optional threads has been
  // exceeded and is exiting early.
  bool optional_thread_exiting = false;
  KuduScanner scanner(this, runtime_state_);

  const string* scan_token = initial_token;
  Status status = scanner.Open();
  if (status.ok()) {
    // Here, even though a read of 'done_' may conflict with a write to it,
    // ProcessScanToken() will return early, as will GetNextScanToken().
    while (!done_ && scan_token != NULL) {
      status = ProcessScanToken(&scanner, *scan_token);
      if (!status.ok()) break;

      // Check if the number of optional threads has been exceeded.
      if (runtime_state_->resource_pool()->optional_exceeded()) {
        unique_lock<mutex> l(lock_);
        // Don't exit if this is the last thread. Otherwise, the scan will indicate it's
        // done before all scan tokens have been processed.
        if (num_active_scanners_ > 1) {
          --num_active_scanners_;
          optional_thread_exiting = true;
          break;
        }
      }
      unique_lock<mutex> l(lock_);
      if (!done_) {
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
    // Decrement num_active_scanners_ unless handling the case of an early exit when
    // optional threads have been exceeded, in which case it already was decremented.
    if (!optional_thread_exiting) --num_active_scanners_;
    if (num_active_scanners_ == 0) {
      SetDoneInternal();
    }
  }

  // lock_ is released before calling ThreadResourceMgr::ReleaseThreadToken() which
  // invokes ThreadAvailableCb() which attempts to take the same lock.
  VLOG_RPC << "Thread done: " << name;
  runtime_state_->resource_pool()->ReleaseThreadToken(false);
}

void KuduScanNode::SetDoneInternal() {
  if (done_) return;
  done_ = true;
  materialized_row_batches_->Shutdown();
}

void KuduScanNode::SetDone() {
  unique_lock<mutex> l(lock_);
  SetDoneInternal();
}

}  // namespace impala
