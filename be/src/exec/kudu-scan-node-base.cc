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

#include "exec/kudu-scan-node-base.h"

#include <boost/algorithm/string.hpp>
#include <boost/foreach.hpp>

#include <kudu/client/row_result.h>
#include <kudu/client/schema.h>
#include <kudu/client/value.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <vector>

#include "exec/kudu-scanner.h"
#include "exec/kudu-util.h"
#include "exprs/expr.h"
#include "exprs/expr-context.h"
#include "runtime/descriptors.h"
#include "runtime/exec-env.h"
#include "runtime/mem-pool.h"
#include "runtime/query-state.h"
#include "runtime/runtime-filter.inline.h"
#include "runtime/runtime-state.h"
#include "runtime/row-batch.h"
#include "runtime/string-value.h"
#include "runtime/tuple-row.h"
#include "util/jni-util.h"
#include "util/periodic-counter-updater.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

using kudu::client::KuduClient;
using kudu::client::KuduTable;
using boost::algorithm::join;

DEFINE_int32(kudu_runtime_filter_wait_time_ms, 1000, "(Advanced) the maximum time, in ms, "
             "that a scan node will wait for expected runtime filters to arrive.");

namespace impala {

const string KuduScanNodeBase::KUDU_ROUND_TRIPS = "TotalKuduScanRoundTrips";
const string KuduScanNodeBase::KUDU_REMOTE_TOKENS = "KuduRemoteScanTokens";

KuduScanNodeBase::KuduScanNodeBase(ObjectPool* pool, const TPlanNode& tnode,
    const DescriptorTbl& descs)
    : ScanNode(pool, tnode, descs),
      tuple_id_(tnode.kudu_scan_node.tuple_id),
      client_(nullptr),
      counters_running_(false),
      next_scan_token_idx_(0) {
  DCHECK(KuduIsAvailable());
}

KuduScanNodeBase::~KuduScanNodeBase() {
  DCHECK(is_closed());
}

Status KuduScanNodeBase::Init(const TPlanNode& tnode, RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Init(tnode, state));

  const TQueryOptions& query_options = state->query_options();
  for (const TRuntimeFilterDesc& filter: tnode.runtime_filters) {
    auto it = filter.planid_to_target_ndx.find(tnode.node_id);
    DCHECK(it != filter.planid_to_target_ndx.end());
    const TRuntimeFilterTargetDesc& target = filter.targets[it->second];
    if (state->query_options().runtime_filter_mode == TRuntimeFilterMode::LOCAL &&
        !target.is_local_target) {
      continue;
    }
    if (query_options.disable_row_runtime_filtering &&
        !target.is_bound_by_partition_columns) {
      continue;
    }

    FilterContext filter_ctx;
    RETURN_IF_ERROR(
        Expr::CreateExprTree(pool_, target.target_expr, &filter_ctx.expr_ctx));
    filter_ctx.filter = state->filter_bank()->RegisterFilter(filter, false);

    string filter_profile_title = Substitute("Filter $0 ($1)", filter.filter_id,
        PrettyPrinter::Print(filter_ctx.filter->filter_size(), TUnit::BYTES));
    RuntimeProfile* profile = state->obj_pool()->Add(
        new RuntimeProfile(state->obj_pool(), filter_profile_title));
    runtime_profile_->AddChild(profile);
    filter_ctx.stats = state->obj_pool()->Add(new FilterStats(profile,
        target.is_bound_by_partition_columns));

    filter_ctxs_.push_back(filter_ctx);
  }

  return Status::OK();
}

Status KuduScanNodeBase::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ScanNode::Prepare(state));
  runtime_state_ = state;

  scan_ranges_complete_counter_ =
      ADD_COUNTER(runtime_profile(), SCAN_RANGES_COMPLETE_COUNTER, TUnit::UNIT);
  kudu_round_trips_ = ADD_COUNTER(runtime_profile(), KUDU_ROUND_TRIPS, TUnit::UNIT);
  kudu_remote_tokens_ = ADD_COUNTER(runtime_profile(), KUDU_REMOTE_TOKENS, TUnit::UNIT);
  counters_running_ = true;

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

Status KuduScanNodeBase::Open(RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Open(state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));
  SCOPED_TIMER(runtime_profile_->total_time_counter());

  const KuduTableDescriptor* table_desc =
      static_cast<const KuduTableDescriptor*>(tuple_desc_->table_desc());

  RETURN_IF_ERROR(runtime_state_->exec_env()->GetKuduClient(
      table_desc->kudu_master_addresses(), &client_));

  uint64_t latest_ts = static_cast<uint64_t>(
      max<int64_t>(0, state->query_ctx().session.kudu_latest_observed_ts));
  VLOG_RPC << "Latest observed Kudu timestamp: " << latest_ts;
  if (latest_ts > 0) client_->SetLatestObservedTimestamp(latest_ts);

  KUDU_RETURN_IF_ERROR(client_->OpenTable(table_desc->table_name(), &table_),
      "Unable to open Kudu table");

  return Status::OK();
}

void KuduScanNodeBase::Close(RuntimeState* state) {
  if (is_closed()) return;
  StopAndFinalizeCounters();
  ExecNode::Close(state);
}

void KuduScanNodeBase::DebugString(int indentation_level, stringstream* out) const {
  string indent(indentation_level * 2, ' ');
  *out << indent << "KuduScanNode(tupleid=" << tuple_id_ << ")";
}

bool KuduScanNodeBase::HasScanToken() {
  return (next_scan_token_idx_ < scan_tokens_.size());
}

bool KuduScanNodeBase::WaitForRuntimeFilters(int32_t time_ms) {
  vector<string> arrived_filter_ids;
  int32_t start = MonotonicMillis();
  for (auto& ctx: filter_ctxs_) {
    if (ctx.filter->WaitForArrival(time_ms)) {
      arrived_filter_ids.push_back(Substitute("$0", ctx.filter->id()));
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

  const string& filter_str = Substitute("Only following filters arrived: $0, waited $1",
      join(arrived_filter_ids, ", "), wait_time);
  runtime_profile()->AddInfoString("Runtime filters", filter_str);
  VLOG_QUERY << filter_str;
  return false;
}

const string* KuduScanNodeBase::GetNextScanToken() {
  if (!HasScanToken()) return nullptr;
  const string* token = &scan_tokens_[next_scan_token_idx_++];
  return token;
}

Status KuduScanNodeBase::IssueRuntimeFilters(RuntimeState* state) {
  DCHECK(!initial_ranges_issued_);
  initial_ranges_issued_ = true;

  int32 wait_time_ms = FLAGS_kudu_runtime_filter_wait_time_ms;
  if (state->query_options().runtime_filter_wait_time_ms > 0) {
      wait_time_ms = state->query_options().runtime_filter_wait_time_ms;
  }
  if (filter_ctxs_.size() > 0) WaitForRuntimeFilters(wait_time_ms);
  return Status::OK();
}

void KuduScanNodeBase::StopAndFinalizeCounters() {
  if (!counters_running_) return;
  counters_running_ = false;

  PeriodicCounterUpdater::StopRateCounter(total_throughput_counter());
  PeriodicCounterUpdater::StopTimeSeriesCounter(bytes_read_timeseries_counter_);
}

Status KuduScanNodeBase::GetConjunctCtxs(vector<ExprContext*>* ctxs) {
  return Expr::CloneIfNotExists(conjunct_ctxs_, runtime_state_, ctxs);
}

}  // namespace impala
