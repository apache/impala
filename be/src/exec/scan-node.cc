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

#include <boost/bind.hpp>

#include "exprs/scalar-expr.h"
#include "runtime/runtime-filter.inline.h"
#include "runtime/runtime-state.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

DEFINE_int32(runtime_filter_wait_time_ms, 1000, "(Advanced) the maximum time, in ms, "
    "that a scan node will wait for expected runtime filters to arrive.");

using boost::algorithm::join;

namespace impala {

// Changing these names have compatibility concerns.
const string ScanNode::BYTES_READ_COUNTER = "BytesRead";
const string ScanNode::ROWS_READ_COUNTER = "RowsRead";
const string ScanNode::COLLECTION_ITEMS_READ_COUNTER = "CollectionItemsRead";
const string ScanNode::TOTAL_HDFS_READ_TIMER = "TotalRawHdfsReadTime(*)";
const string ScanNode::TOTAL_HDFS_OPEN_FILE_TIMER = "TotalRawHdfsOpenFileTime(*)";
const string ScanNode::TOTAL_HBASE_READ_TIMER = "TotalRawHBaseReadTime(*)";
const string ScanNode::TOTAL_THROUGHPUT_COUNTER = "TotalReadThroughput";
const string ScanNode::MATERIALIZE_TUPLE_TIMER = "MaterializeTupleTime(*)";
const string ScanNode::PER_READ_THREAD_THROUGHPUT_COUNTER =
    "PerReadThreadRawHdfsThroughput";
const string ScanNode::NUM_DISKS_ACCESSED_COUNTER = "NumDisksAccessed";
const string ScanNode::SCAN_RANGES_COMPLETE_COUNTER = "ScanRangesComplete";
const string ScanNode::SCANNER_THREAD_COUNTERS_PREFIX = "ScannerThreads";
const string ScanNode::SCANNER_THREAD_TOTAL_WALLCLOCK_TIME =
    "ScannerThreadsTotalWallClockTime";
const string ScanNode::AVERAGE_SCANNER_THREAD_CONCURRENCY =
    "AverageScannerThreadConcurrency";
const string ScanNode::AVERAGE_HDFS_READ_THREAD_CONCURRENCY =
    "AverageHdfsReadThreadConcurrency";
const string ScanNode::NUM_SCANNER_THREADS_STARTED =
    "NumScannerThreadsStarted";

Status ScanNode::Init(const TPlanNode& tnode, RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Init(tnode, state));
  const TQueryOptions& query_options = state->query_options();
  for (const TRuntimeFilterDesc& filter_desc : tnode.runtime_filters) {
    auto it = filter_desc.planid_to_target_ndx.find(tnode.node_id);
    DCHECK(it != filter_desc.planid_to_target_ndx.end());
    const TRuntimeFilterTargetDesc& target = filter_desc.targets[it->second];
    DCHECK(state->query_options().runtime_filter_mode == TRuntimeFilterMode::GLOBAL ||
        target.is_local_target);
    DCHECK(!query_options.disable_row_runtime_filtering ||
        target.is_bound_by_partition_columns);
    ScalarExpr* filter_expr;
    RETURN_IF_ERROR(
        ScalarExpr::Create(target.target_expr, *row_desc(), state, &filter_expr));
    filter_exprs_.push_back(filter_expr);

    // TODO: Move this to Prepare()
    filter_ctxs_.emplace_back();
    FilterContext& filter_ctx = filter_ctxs_.back();
    filter_ctx.filter = state->filter_bank()->RegisterFilter(filter_desc, false);
    // TODO: Enable stats for min-max filters when Kudu exposes info about filters
    // (KUDU-2162).
    if (filter_ctx.filter->is_bloom_filter()) {
      string filter_profile_title = Substitute("Filter $0 ($1)", filter_desc.filter_id,
          PrettyPrinter::Print(filter_ctx.filter->filter_size(), TUnit::BYTES));
      RuntimeProfile* profile =
          RuntimeProfile::Create(state->obj_pool(), filter_profile_title);
      runtime_profile_->AddChild(profile);
      filter_ctx.stats = state->obj_pool()->Add(new FilterStats(profile));
    }
  }

  return Status::OK();
}

Status ScanNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  runtime_state_ = state;
  RETURN_IF_ERROR(ExecNode::Prepare(state));

  scanner_thread_counters_ =
      ADD_THREAD_COUNTERS(runtime_profile(), SCANNER_THREAD_COUNTERS_PREFIX);
  bytes_read_counter_ =
      ADD_COUNTER(runtime_profile(), BYTES_READ_COUNTER, TUnit::BYTES);
  bytes_read_timeseries_counter_ = ADD_TIME_SERIES_COUNTER(runtime_profile(),
      BYTES_READ_COUNTER, bytes_read_counter_);
  rows_read_counter_ =
      ADD_COUNTER(runtime_profile(), ROWS_READ_COUNTER, TUnit::UNIT);
  collection_items_read_counter_ =
      ADD_COUNTER(runtime_profile(), COLLECTION_ITEMS_READ_COUNTER, TUnit::UNIT);
  total_throughput_counter_ = runtime_profile()->AddRateCounter(
      TOTAL_THROUGHPUT_COUNTER, bytes_read_counter_);
  materialize_tuple_timer_ = ADD_CHILD_TIMER(runtime_profile(), MATERIALIZE_TUPLE_TIMER,
      SCANNER_THREAD_TOTAL_WALLCLOCK_TIME);

  DCHECK_EQ(filter_exprs_.size(), filter_ctxs_.size());
  for (int i = 0; i < filter_exprs_.size(); ++i) {
    RETURN_IF_ERROR(ScalarExprEvaluator::Create(*filter_exprs_[i], state, pool_,
        expr_perm_pool(), expr_results_pool(), &filter_ctxs_[i].expr_eval));
  }

  return Status::OK();
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
  ScalarExpr::Close(filter_exprs_);
  // ScanNode::Prepare() started periodic counters including 'total_throughput_counter_'
  // and 'bytes_read_timeseries_counter_'. Subclasses may also have started counters.
  runtime_profile_->StopPeriodicCounters();
  ExecNode::Close(state);
}

bool ScanNode::WaitForRuntimeFilters() {
  int32 wait_time_ms = FLAGS_runtime_filter_wait_time_ms;
  if (runtime_state_->query_options().runtime_filter_wait_time_ms > 0) {
    wait_time_ms = runtime_state_->query_options().runtime_filter_wait_time_ms;
  }
  vector<string> arrived_filter_ids;
  vector<string> missing_filter_ids;
  int32_t start = MonotonicMillis();
  for (auto& ctx: filter_ctxs_) {
    string filter_id = Substitute("$0", ctx.filter->id());
    if (ctx.filter->WaitForArrival(wait_time_ms)) {
      arrived_filter_ids.push_back(filter_id);
    } else {
      missing_filter_ids.push_back(filter_id);
    }
  }
  int32_t end = MonotonicMillis();
  const string& wait_time = PrettyPrinter::Print(end - start, TUnit::TIME_MS);

  if (arrived_filter_ids.size() == filter_ctxs_.size()) {
    runtime_profile()->AddInfoString("Runtime filters",
        Substitute("All filters arrived. Waited $0", wait_time));
    VLOG_QUERY << "Filters arrived. Waited " << wait_time;
    return true;
  }

  const string& filter_str = Substitute(
      "Not all filters arrived (arrived: [$0], missing [$1]), waited for $2",
      join(arrived_filter_ids, ", "), join(missing_filter_ids, ", "), wait_time);
  runtime_profile()->AddInfoString("Runtime filters", filter_str);
  VLOG_QUERY << filter_str;
  return false;
}

}
