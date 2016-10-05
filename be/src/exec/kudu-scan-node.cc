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

#include <boost/algorithm/string.hpp>
#include <kudu/client/row_result.h>
#include <kudu/client/schema.h>
#include <kudu/client/value.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <vector>

#include "exec/kudu-scanner.h"
#include "exec/kudu-util.h"
#include "exprs/expr.h"
#include "gutil/gscoped_ptr.h"
#include "gutil/strings/substitute.h"
#include "gutil/stl_util.h"
#include "runtime/mem-pool.h"
#include "runtime/runtime-state.h"
#include "runtime/row-batch.h"
#include "runtime/string-value.h"
#include "runtime/tuple-row.h"
#include "util/disk-info.h"
#include "util/jni-util.h"
#include "util/periodic-counter-updater.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

DEFINE_int32(kudu_max_row_batches, 0, "The maximum size of the row batch queue, "
    " for Kudu scanners.");
DEFINE_int32(kudu_scanner_keep_alive_period_us, 15 * 1000L * 1000L,
    "The period at which Kudu Scanners should send keep-alive requests to the tablet "
    "server to ensure that scanners do not time out.");

using boost::algorithm::to_lower_copy;
using kudu::client::KuduClient;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduPredicate;
using kudu::client::KuduRowResult;
using kudu::client::KuduSchema;
using kudu::client::KuduTable;
using kudu::client::KuduValue;
using kudu::Slice;

namespace impala {

const string KuduScanNode::KUDU_ROUND_TRIPS = "TotalKuduScanRoundTrips";
const string KuduScanNode::KUDU_REMOTE_TOKENS = "KuduRemoteScanTokens";

KuduScanNode::KuduScanNode(ObjectPool* pool, const TPlanNode& tnode,
    const DescriptorTbl& descs)
    : ScanNode(pool, tnode, descs),
      tuple_id_(tnode.kudu_scan_node.tuple_id),
      next_scan_token_idx_(0),
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

Status KuduScanNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ScanNode::Prepare(state));
  runtime_state_ = state;

  scan_ranges_complete_counter_ =
      ADD_COUNTER(runtime_profile(), SCAN_RANGES_COMPLETE_COUNTER, TUnit::UNIT);
  kudu_round_trips_ = ADD_COUNTER(runtime_profile(), KUDU_ROUND_TRIPS, TUnit::UNIT);
  kudu_remote_tokens_ = ADD_COUNTER(runtime_profile(), KUDU_REMOTE_TOKENS, TUnit::UNIT);

  DCHECK(state->desc_tbl().GetTupleDescriptor(tuple_id_) != NULL);

  tuple_desc_ = state->desc_tbl().GetTupleDescriptor(tuple_id_);

  // Initialize the list of scan tokens to process from the TScanRangeParams.
  DCHECK(scan_range_params_ != NULL);
  int num_remote_tokens = 0;
  for (const TScanRangeParams& params: *scan_range_params_) {
    if (params.__isset.is_remote && params.is_remote) ++num_remote_tokens;
    scan_tokens_.push_back(params.scan_range.kudu_scan_token);
  }
  COUNTER_SET(kudu_remote_tokens_, num_remote_tokens);
  return Status::OK();
}

Status KuduScanNode::Open(RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Open(state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));
  SCOPED_TIMER(runtime_profile_->total_time_counter());

  const KuduTableDescriptor* table_desc =
      static_cast<const KuduTableDescriptor*>(tuple_desc_->table_desc());

  kudu::client::KuduClientBuilder b;
  for (const string& address: table_desc->kudu_master_addresses()) {
    b.add_master_server_addr(address);
  }

  KUDU_RETURN_IF_ERROR(b.Build(&client_), "Unable to create Kudu client");

  KUDU_RETURN_IF_ERROR(client_->OpenTable(table_desc->table_name(), &table_),
      "Unable to open Kudu table");

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

  if (ReachedLimit() || scan_tokens_.empty()) {
    *eos = true;
    return Status::OK();
  }

  *eos = false;
  RowBatch* materialized_batch = materialized_row_batches_->GetBatch();
  if (materialized_batch != NULL) {
    row_batch->AcquireState(materialized_batch);
    num_rows_returned_ += row_batch->num_rows();
    COUNTER_SET(rows_returned_counter_, num_rows_returned_);

    if (ReachedLimit()) {
      int num_rows_over = num_rows_returned_ - limit_;
      row_batch->set_num_rows(row_batch->num_rows() - num_rows_over);
      num_rows_returned_ -= num_rows_over;
      COUNTER_SET(rows_returned_counter_, num_rows_returned_);
      *eos = true;

      unique_lock<mutex> l(lock_);
      done_ = true;
      materialized_row_batches_->Shutdown();
    }
    delete materialized_batch;
  } else {
    *eos = true;
  }

  Status status;
  {
    unique_lock<mutex> l(lock_);
    status = status_;
  }
  return status;
}

void KuduScanNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  PeriodicCounterUpdater::StopRateCounter(total_throughput_counter());
  PeriodicCounterUpdater::StopTimeSeriesCounter(bytes_read_timeseries_counter_);
  if (thread_avail_cb_id_ != -1) {
    state->resource_pool()->RemoveThreadAvailableCb(thread_avail_cb_id_);
  }

  if (!done_) {
    unique_lock<mutex> l(lock_);
    done_ = true;
    materialized_row_batches_->Shutdown();
  }

  scanner_threads_.JoinAll();
  DCHECK_EQ(num_active_scanners_, 0);
  materialized_row_batches_->Cleanup();
  ExecNode::Close(state);
}

void KuduScanNode::DebugString(int indentation_level, stringstream* out) const {
  string indent(indentation_level * 2, ' ');
  *out << indent << "KuduScanNode(tupleid=" << tuple_id_ << ")";
}

const string* KuduScanNode::GetNextScanToken() {
  unique_lock<mutex> lock(lock_);
  if (next_scan_token_idx_ >= scan_tokens_.size()) return NULL;
  const string* token = &scan_tokens_[next_scan_token_idx_++];
  return token;
}

Status KuduScanNode::GetConjunctCtxs(vector<ExprContext*>* ctxs) {
  return Expr::CloneIfNotExists(conjunct_ctxs_, runtime_state_, ctxs);
}

void KuduScanNode::ThreadAvailableCb(ThreadResourceMgr::ResourcePool* pool) {
  while (true) {
    unique_lock<mutex> lock(lock_);
    // All done or all tokens are assigned.
    if (done_ || next_scan_token_idx_ >= scan_tokens_.size()) break;

    // Check if we can get a token.
    if (!pool->TryAcquireThreadToken()) break;

    ++num_active_scanners_;
    COUNTER_ADD(num_scanner_threads_started_counter_, 1);

    // Reserve the first token so no other thread picks it up.
    const string* token = &scan_tokens_[next_scan_token_idx_++];
    string name = Substitute("scanner-thread($0)",
        num_scanner_threads_started_counter_->value());

    VLOG_RPC << "Thread started: " << name;
    scanner_threads_.AddThread(new Thread("kudu-scan-node", name,
        &KuduScanNode::RunScannerThread, this, name, token));
  }
}

Status KuduScanNode::ProcessScanToken(KuduScanner* scanner, const string& scan_token) {
  RETURN_IF_ERROR(scanner->OpenNextScanToken(scan_token));
  bool eos = false;
  while (!eos) {
    gscoped_ptr<RowBatch> row_batch(new RowBatch(
        row_desc(), runtime_state_->batch_size(), mem_tracker()));
    RETURN_IF_ERROR(scanner->GetNext(row_batch.get(), &eos));
    while (!done_) {
      scanner->KeepKuduScannerAlive();
      if (materialized_row_batches_->AddBatchWithTimeout(row_batch.get(), 1000000)) {
        ignore_result(row_batch.release());
        break;
      }
    }
  }
  if (eos) scan_ranges_complete_counter()->Add(1);
  return Status::OK();
}

void KuduScanNode::RunScannerThread(const string& name, const string* initial_token) {
  DCHECK(initial_token != NULL);
  SCOPED_THREAD_COUNTER_MEASUREMENT(scanner_thread_counters());
  SCOPED_TIMER(runtime_state_->total_cpu_timer());

  // Set to true if this thread observes that the number of optional threads has been
  // exceeded and is exiting early.
  bool optional_thread_exiting = false;
  KuduScanner scanner(this, runtime_state_);

  const string* scan_token = initial_token;
  Status status = scanner.Open();
  if (status.ok()) {
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
      scan_token = GetNextScanToken();
    }
  }
  scanner.Close();

  {
    unique_lock<mutex> l(lock_);
    if (!status.ok()) {
      if (status_.ok()) {
        status_ = status;
        done_ = true;
      }
    }
    // Decrement num_active_scanners_ unless handling the case of an early exit when
    // optional threads have been exceeded, in which case it already was decremented.
    if (!optional_thread_exiting) --num_active_scanners_;
    if (num_active_scanners_ == 0) {
      done_ = true;
      materialized_row_batches_->Shutdown();
    }
  }

  // lock_ is released before calling ThreadResourceMgr::ReleaseThreadToken() which
  // invokes ThreadAvailableCb() which attempts to take the same lock.
  VLOG_RPC << "Thread done: " << name;
  runtime_state_->resource_pool()->ReleaseThreadToken(false);
}

}  // namespace impala
