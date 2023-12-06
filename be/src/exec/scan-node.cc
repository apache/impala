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

#include "exec/scan-node.h"

#include <boost/algorithm/string/join.hpp>
#include <boost/bind.hpp>

#include "exec/data-source-scan-node.h"
#include "exec/hbase/hbase-scan-node.h"
#include "exec/kudu/kudu-scan-node-mt.h"
#include "exec/kudu/kudu-scan-node.h"
#include "exec/system-table-scan-node.h"
#include "exprs/scalar-expr.h"
#include "exprs/scalar-expr-evaluator.h"
#include "runtime/blocking-row-batch-queue.h"
#include "runtime/fragment-state.h"
#include "runtime/io/disk-io-mgr.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-filter.inline.h"
#include "runtime/runtime-state.h"
#include "runtime/scanner-mem-limiter.h"
#include "util/debug-util.h"
#include "util/disk-info.h"
#include "util/pretty-printer.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

DEFINE_int32(runtime_filter_wait_time_ms, 1000, "(Advanced) the maximum time, in ms, "
    "that a scan node will wait for expected runtime filters to arrive.");

// The maximum capacity of batch_queue_ per scanner thread that can
// be created. This is multiplied by 'max_num_scanner_threads' to get an upper
// bound on the queue size. This reduces the queue size on systems with many disks
// and makes the num_scanner_threads query option more effective at reducing memory
// consumption. For now, make this relatively high. We should consider lowering
// this or using a better heuristic (e.g. based on queued memory).
DEFINE_int32_hidden(max_queued_row_batches_per_scanner_thread, 5,
    "(Advanced) the maximum number of queued row batches per scanner thread.");

DEFINE_int64(max_queued_row_batch_bytes, 16L * 1024 * 1024,
    "(Advanced) the maximum bytes of queued rows per multithreaded scan node.");

using boost::algorithm::join;

namespace impala {

PROFILE_DEFINE_COUNTER(BytesRead, STABLE_HIGH, TUnit::BYTES, "Total bytes read from "
    "disk by a scan node.");
PROFILE_DEFINE_COUNTER(RowsRead, STABLE_HIGH, TUnit::UNIT, "Number of top-level "
    "rows/tuples read from the storage layer, including those discarded by predicate "
    "evaluation. Used for all types of scans.");
PROFILE_DEFINE_RATE_COUNTER(TotalReadThroughput, STABLE_LOW, TUnit::BYTES_PER_SECOND,
    "BytesRead divided by the total wall clock time that this scan "
    "was executing (from Open() to Close()). This gives the aggregate data is scanned.");
PROFILE_DEFINE_TIME_SERIES_COUNTER(BytesReadSeries, UNSTABLE, TUnit::BYTES,
    "Time series of BytesRead that samples the BytesRead counter.");
PROFILE_DEFINE_TIMER(MaterializeTupleTime, UNSTABLE, "Wall clock time spent "
    "materializing tuples and evaluating predicates.");
PROFILE_DEFINE_COUNTER(NumScannerThreadsStarted, DEBUG, TUnit::UNIT,
    "NumScannerThreadsStarted - the number of scanner threads started for the duration "
    "of the ScanNode. A single scanner thread will likely process multiple scan ranges."
    "Meanwhile, more than one scanner thread can be spun up to process data from a "
    "single scan range. This is *not* the same as peak scanner thread concurrency "
    "because the number of scanner threads can fluctuate during execution of the scan.");
PROFILE_DEFINE_COUNTER(RowBatchesEnqueued, STABLE_LOW, TUnit::UNIT, "Number of row "
    "batches enqueued in the scan node's output queue.");
PROFILE_DEFINE_COUNTER(RowBatchBytesEnqueued, STABLE_LOW, TUnit::BYTES,
    "Number of bytes enqueued in the scan node's output queue.");
PROFILE_DEFINE_TIMER(RowBatchQueueGetWaitTime, UNSTABLE, "Wall clock time that the "
    "fragment execution thread spent blocked waiting for row batches to be added to the "
    "scan node's output queue.");
PROFILE_DEFINE_TIMER(RowBatchQueuePutWaitTime, UNSTABLE, "Aggregate wall clock time "
    "across all scanner threads spent blocked waiting for space in the scan node's "
    "output queue when it is full.");
PROFILE_DEFINE_COUNTER(RowBatchQueuePeakMemoryUsage, DEBUG, TUnit::BYTES,
    "Peak memory consumption of row batches enqueued in the scan node's output queue.");
PROFILE_DEFINE_COUNTER(NumScannerThreadMemUnavailable, STABLE_LOW, TUnit::UNIT,
    "Number of times scanner threads were not created because of memory not available.");
PROFILE_DEFINE_SAMPLING_COUNTER(AverageScannerThreadConcurrency, STABLE_LOW,
    "Average number of executing scanner threads.");
PROFILE_DEFINE_HIGH_WATER_MARK_COUNTER(PeakScannerThreadConcurrency, STABLE_LOW,
    TUnit::UNIT, "Peak number of executing scanner threads.");

const string ScanNode::SCANNER_THREAD_COUNTERS_PREFIX = "ScannerThreads";

Status ScanPlanNode::Init(const TPlanNode& tnode, FragmentState* state) {
  RETURN_IF_ERROR(PlanNode::Init(tnode, state));
  const TQueryOptions& query_options = state->query_options();
  for (const TRuntimeFilterDesc& filter_desc : tnode.runtime_filters) {
    auto it = filter_desc.planid_to_target_ndx.find(tnode.node_id);
    DCHECK(it != filter_desc.planid_to_target_ndx.end());
    const TRuntimeFilterTargetDesc& target = filter_desc.targets[it->second];
    DCHECK(state->query_options().runtime_filter_mode == TRuntimeFilterMode::GLOBAL
        || target.is_local_target);
    DCHECK(!query_options.disable_row_runtime_filtering
        || target.is_bound_by_partition_columns);
    ScalarExpr* filter_expr;
    RETURN_IF_ERROR(
        ScalarExpr::Create(target.target_expr, *row_descriptor_, state, &filter_expr));
    runtime_filter_exprs_.push_back(filter_expr);
  }
  return Status::OK();
}

Status ScanPlanNode::CreateExecNode(RuntimeState* state, ExecNode** node) const {
  ObjectPool* pool = state->obj_pool();
  switch (tnode_->node_type) {
    case TPlanNodeType::HBASE_SCAN_NODE:
      *node = pool->Add(new HBaseScanNode(pool, *this, state->desc_tbl()));
      break;
    case TPlanNodeType::DATA_SOURCE_NODE:
      *node = pool->Add(new DataSourceScanNode(pool, *this, state->desc_tbl()));
      break;
    case TPlanNodeType::KUDU_SCAN_NODE:
      if (tnode_->kudu_scan_node.use_mt_scan_node) {
        DCHECK(is_mt_fragment());
        *node = pool->Add(new KuduScanNodeMt(pool, *this, state->desc_tbl()));
      } else {
        DCHECK(!is_mt_fragment() || state->query_options().num_scanner_threads == 1);
        *node = pool->Add(new KuduScanNode(pool, *this, state->desc_tbl()));
      }
      break;
    case TPlanNodeType::SYSTEM_TABLE_SCAN_NODE:
      *node = pool->Add(new SystemTableScanNode(pool, *this, state->desc_tbl()));
      break;
    default:
      DCHECK(false) << "Unexpected scan node type: " << tnode_->node_type;
  }
  return Status::OK();
}

bool ScanNode::IsDataCacheDisabled() const {
  return runtime_state()->query_options().disable_data_cache;
}

Status ScanNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  runtime_state_ = state;
  RETURN_IF_ERROR(ExecNode::Prepare(state));

  for (const TRuntimeFilterDesc& filter_desc : plan_node_.tnode_->runtime_filters) {
    filter_ctxs_.emplace_back();
    FilterContext& filter_ctx = filter_ctxs_.back();
    filter_ctx.filter = state->filter_bank()->RegisterConsumer(filter_desc);
    string filter_profile_title =
        Substitute("$0$1 ($2)", RuntimeProfile::PREFIX_FILTER, filter_desc.filter_id,
            PrettyPrinter::Print(filter_ctx.filter->filter_size(), TUnit::BYTES));
    RuntimeProfile* profile =
        RuntimeProfile::Create(state->obj_pool(), filter_profile_title, false);
    runtime_profile_->AddChild(profile);
    filter_ctx.stats = state->obj_pool()->Add(new FilterStats(profile));
  }

  rows_read_counter_ = PROFILE_RowsRead.Instantiate(runtime_profile());
  materialize_tuple_timer_ = PROFILE_MaterializeTupleTime.Instantiate(runtime_profile());

  DCHECK_EQ(filter_exprs_.size(), filter_ctxs_.size());
  for (int i = 0; i < filter_exprs_.size(); ++i) {
    RETURN_IF_ERROR(ScalarExprEvaluator::Create(*filter_exprs_[i], state, pool_,
        expr_perm_pool(), expr_results_pool(), &filter_ctxs_[i].expr_eval));
  }
  return Status::OK();
}

void ScanNode::AddBytesReadCounters() {
  bytes_read_counter_ = PROFILE_BytesRead.Instantiate(runtime_profile());
  runtime_state()->AddBytesReadCounter(bytes_read_counter_);
  bytes_read_timeseries_counter_ = PROFILE_BytesReadSeries.Instantiate(runtime_profile(),
      bytes_read_counter_);
  total_throughput_counter_ = PROFILE_TotalReadThroughput.Instantiate(runtime_profile(),
      bytes_read_counter_);
}

Status ScanNode::Open(RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Open(state));

  // Open Runtime filter expressions.
  for (FilterContext& ctx : filter_ctxs_) {
    RETURN_IF_ERROR(ctx.expr_eval->Open(state));
  }
  return Status::OK();
}

void ScanNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  // Close filter
  for (auto& filter_ctx : filter_ctxs_) {
    if (filter_ctx.expr_eval != nullptr) filter_ctx.expr_eval->Close(state);
  }
  // ScanNode::Prepare() started periodic counters including 'total_throughput_counter_'
  // and 'bytes_read_timeseries_counter_'. Subclasses may also have started counters.
  runtime_profile_->StopPeriodicCounters();
  ExecNode::Close(state);
}

bool ScanNode::WaitForRuntimeFilters() {
  int32_t wait_time_ms = FLAGS_runtime_filter_wait_time_ms;
  if (runtime_state_->query_options().runtime_filter_wait_time_ms > 0) {
    wait_time_ms = runtime_state_->query_options().runtime_filter_wait_time_ms;
  }
  vector<string> arrived_filter_ids;
  vector<string> missing_filter_ids;
  int32_t max_arrival_delay = 0;
  int64_t start = MonotonicMillis();
  for (auto& ctx: filter_ctxs_) {
    string filter_id = Substitute("$0", ctx.filter->id());
    if (ctx.filter->WaitForArrival(wait_time_ms)) {
      arrived_filter_ids.push_back(filter_id);
    } else {
      missing_filter_ids.push_back(filter_id);
    }
    max_arrival_delay = max(max_arrival_delay, ctx.filter->arrival_delay_ms());
  }
  int64_t end = MonotonicMillis();
  const string& wait_time = PrettyPrinter::Print(end - start, TUnit::TIME_MS);
  const string& arrival_delay = PrettyPrinter::Print(max_arrival_delay, TUnit::TIME_MS);

  if (arrived_filter_ids.size() == filter_ctxs_.size()) {
    runtime_profile()->AddInfoString("Runtime filters",
        Substitute("All filters arrived. Waited $0. Maximum arrival delay: $1.",
                                         wait_time, arrival_delay));
    VLOG(2) << "Filters arrived. Waited " << wait_time
            << ". Current time since reboot(ms) " << end;
    return true;
  }

  const string& filter_str = Substitute("Not all filters arrived (arrived: [$0], missing "
                                        "[$1]), waited for $2. Arrival delay: $3.",
      join(arrived_filter_ids, ", "), join(missing_filter_ids, ", "), wait_time,
      arrival_delay);
  runtime_profile()->AddInfoString("Runtime filters", filter_str);
  VLOG(2) << filter_str;
  return false;
}

void ScanNode::ScannerThreadState::Prepare(
    ScanNode* parent, int64_t estimated_per_thread_mem) {
  DCHECK_GT(estimated_per_thread_mem, 0);
  RuntimeProfile* profile = parent->runtime_profile();
  row_batches_mem_tracker_ = parent->runtime_state_->obj_pool()->Add(
      new MemTracker(-1, "Queued Batches", parent->mem_tracker(), false));

  thread_counters_ = ADD_THREAD_COUNTERS(profile, SCANNER_THREAD_COUNTERS_PREFIX);
  num_threads_started_ = PROFILE_NumScannerThreadsStarted.Instantiate(profile);
  row_batches_enqueued_ = PROFILE_RowBatchesEnqueued.Instantiate(profile);
  row_batch_bytes_enqueued_ = PROFILE_RowBatchBytesEnqueued.Instantiate(profile);
  row_batches_get_timer_ = PROFILE_RowBatchQueueGetWaitTime.Instantiate(profile);
  row_batches_put_timer_ = PROFILE_RowBatchQueuePutWaitTime.Instantiate(profile);
  row_batches_peak_mem_consumption_ =
      PROFILE_RowBatchQueuePeakMemoryUsage.Instantiate(profile);
  scanner_thread_mem_unavailable_counter_ =
      PROFILE_NumScannerThreadMemUnavailable.Instantiate(profile);

  parent->runtime_state()->query_state()->scanner_mem_limiter()->RegisterScan(
      parent, estimated_per_thread_mem);
  estimated_per_thread_mem_ = estimated_per_thread_mem;
}

void ScanNode::ScannerThreadState::Open(
    ScanNode* parent, int64_t max_row_batches_override) {
  RuntimeState* state = parent->runtime_state_;
  max_num_scanner_threads_ = CpuInfo::num_cores();
  int max_row_batches = max_row_batches_override;

  if (state->query_options().compute_processing_cost) {
    // If COMPUTE_PROCESSING_COST=true, we fix the maximum num scanner threads to 1 per
    // instance and maximum row batches to 2.
    max_num_scanner_threads_ = 1;
    max_row_batches = 2;
  } else {
    if (state->query_options().num_scanner_threads > 0) {
      max_num_scanner_threads_ = state->query_options().num_scanner_threads;
    }
    DCHECK_GT(max_num_scanner_threads_, 0);

    if (max_row_batches_override <= 0) {
      // Legacy heuristic to determine the size, based on the idea that more disks means a
      // faster producer. Also used for Kudu under the assumption that the scan runs
      // co-located with a Kudu tablet server and that the tablet server is using disks
      // similarly as a datanode would.
      // TODO: IMPALA-7096: re-evaluate this heuristic to get a tighter bound on memory
      // consumption, we could do something better.
      max_row_batches = max(1,
          min(10 * (DiskInfo::num_disks() + io::DiskIoMgr::REMOTE_NUM_DISKS),
              max_num_scanner_threads_
                  * FLAGS_max_queued_row_batches_per_scanner_thread));
    }
    if (parent->plan_node().is_mt_fragment()) {
      // To avoid a significant memory increase when running the multithreaded scans
      // (if multithreaded fragment is not supported for this scan), then adjust the
      // number of maximally queued row batches per scan instance based on
      // num_instances_per_node(). The max materialized row batches is at least 2 to allow
      // for some parallelism between the producer/consumer.
      max_row_batches =
          max(2, max_row_batches / parent->plan_node().num_instances_per_node());
    }
  }
  VLOG(2) << "Max row batch queue size for scan node '" << parent->id()
          << "' in fragment instance '" << PrintId(state->fragment_instance_id())
          << "': " << max_row_batches;
  batch_queue_.reset(new BlockingRowBatchQueue(max_row_batches,
      FLAGS_max_queued_row_batch_bytes, row_batches_get_timer_, row_batches_put_timer_));

  // Start measuring the scanner thread concurrency only once the node is opened.
  average_concurrency_ = PROFILE_AverageScannerThreadConcurrency.Instantiate(
      parent->runtime_profile(), [&num_active=num_active_] () {
        return num_active.Load();
      });
  peak_concurrency_ =
      PROFILE_PeakScannerThreadConcurrency.Instantiate(parent->runtime_profile());
}

void ScanNode::ScannerThreadState::AddThread(unique_ptr<Thread> thread) {
  VLOG_RPC << "Thread started: " << thread->name();
  COUNTER_ADD(num_threads_started_, 1);
  num_active_.Add(1);
  peak_concurrency_->Add(1);
  scanner_threads_.AddThread(move(thread));
}

bool ScanNode::ScannerThreadState::DecrementNumActive() {
  peak_concurrency_->Add(-1);
  return num_active_.Add(-1) == 0;
}

void ScanNode::ScannerThreadState::EnqueueBatch(
    unique_ptr<RowBatch> row_batch) {
  // Only need to count tuple_data_pool() bytes since after IMPALA-5307, no buffers are
  // returned from the scan node.
  int64_t bytes = row_batch->tuple_data_pool()->total_reserved_bytes();
  row_batch->SetMemTracker(row_batches_mem_tracker_);
  batch_queue_->AddBatch(move(row_batch));
  COUNTER_ADD(row_batches_enqueued_, 1);
  COUNTER_ADD(row_batch_bytes_enqueued_, bytes);
}

bool ScanNode::ScannerThreadState::EnqueueBatchWithTimeout(
    unique_ptr<RowBatch>* row_batch, int64_t timeout_micros) {
  // Only need to count tuple_data_pool() bytes since after IMPALA-5307, no buffers are
  // returned from the scan node.
  int64_t bytes = (*row_batch)->tuple_data_pool()->total_reserved_bytes();
  // Transfer memory ownership before enqueueing. If the caller retries, this transfer
  // is idempotent.
  (*row_batch)->SetMemTracker(row_batches_mem_tracker_);
  if (!batch_queue_->AddBatchWithTimeout(move(*row_batch), timeout_micros)) {
    return false;
  }
  COUNTER_ADD(row_batches_enqueued_, 1);
  COUNTER_ADD(row_batch_bytes_enqueued_, bytes);
  return true;
}

void ScanNode::ScannerThreadState::Shutdown() {
  if (batch_queue_ != nullptr) batch_queue_->Shutdown();
}

void ScanNode::ScannerThreadState::Close(ScanNode* parent) {
  scanner_threads_.JoinAll();
  DCHECK_EQ(num_active_.Load(), 0) << "There should be no active threads";
  if (batch_queue_ != nullptr) {
    row_batches_peak_mem_consumption_->Set(row_batches_mem_tracker_->peak_consumption());
    batch_queue_->Cleanup();
  }
  if (row_batches_mem_tracker_ != nullptr) {
    row_batches_mem_tracker_->Close();
  }
  if (estimated_per_thread_mem_ != 0) {
    parent->runtime_state()->query_state()->scanner_mem_limiter()
        ->ReleaseMemoryForScannerThread(parent, estimated_per_thread_mem_);
    estimated_per_thread_mem_ = 0;
  }
}
}
