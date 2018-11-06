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
#include <kudu/client/row_result.h>
#include <kudu/client/schema.h>
#include <kudu/client/value.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <vector>

#include "exec/kudu-scanner.h"
#include "exec/kudu-util.h"
#include "exprs/expr.h"
#include "runtime/exec-env.h"
#include "runtime/mem-pool.h"
#include "runtime/query-state.h"
#include "runtime/runtime-state.h"
#include "runtime/row-batch.h"
#include "runtime/string-value.h"
#include "runtime/tuple-row.h"
#include "util/periodic-counter-updater.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

using kudu::client::KuduClient;
using kudu::client::KuduTable;

namespace impala {

const string KuduScanNodeBase::KUDU_ROUND_TRIPS = "TotalKuduScanRoundTrips";
const string KuduScanNodeBase::KUDU_REMOTE_TOKENS = "KuduRemoteScanTokens";
///   KuduClientTime - total amount of time scanner threads spent in the Kudu
///   client, either waiting for data from Kudu or processing data.
const string KuduScanNodeBase::KUDU_CLIENT_TIME = "KuduClientTime";

KuduScanNodeBase::KuduScanNodeBase(
    ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : ScanNode(pool, tnode, descs),
    tuple_id_(tnode.kudu_scan_node.tuple_id) {
  DCHECK(KuduIsAvailable());
}

KuduScanNodeBase::~KuduScanNodeBase() {
  DCHECK(is_closed());
}

Status KuduScanNodeBase::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ScanNode::Prepare(state));

  scan_ranges_complete_counter_ =
      ADD_COUNTER(runtime_profile(), SCAN_RANGES_COMPLETE_COUNTER, TUnit::UNIT);
  kudu_round_trips_ = ADD_COUNTER(runtime_profile(), KUDU_ROUND_TRIPS, TUnit::UNIT);
  kudu_remote_tokens_ = ADD_COUNTER(runtime_profile(), KUDU_REMOTE_TOKENS, TUnit::UNIT);
  kudu_client_time_ = ADD_TIMER(runtime_profile(), KUDU_CLIENT_TIME);

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
  RETURN_IF_ERROR(ScanNode::Open(state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));
  SCOPED_TIMER(runtime_profile_->total_time_counter());

  const KuduTableDescriptor* table_desc =
      static_cast<const KuduTableDescriptor*>(tuple_desc_->table_desc());

  RETURN_IF_ERROR(ExecEnv::GetInstance()->GetKuduClient(
      table_desc->kudu_master_addresses(), &client_));

  uint64_t latest_ts = static_cast<uint64_t>(
      max<int64_t>(0, state->query_ctx().session.kudu_latest_observed_ts));
  VLOG_RPC << "Latest observed Kudu timestamp: " << latest_ts;
  if (latest_ts > 0) client_->SetLatestObservedTimestamp(latest_ts);

  KUDU_RETURN_IF_ERROR(client_->OpenTable(table_desc->table_name(), &table_),
      "Unable to open Kudu table");
  return Status::OK();
}

void KuduScanNodeBase::DebugString(int indentation_level, stringstream* out) const {
  string indent(indentation_level * 2, ' ');
  *out << indent << "KuduScanNode(tupleid=" << tuple_id_ << ")";
}

bool KuduScanNodeBase::HasScanToken() {
  return (next_scan_token_idx_ < scan_tokens_.size());
}

const string* KuduScanNodeBase::GetNextScanToken() {
  if (!HasScanToken()) return nullptr;
  const string* token = &scan_tokens_[next_scan_token_idx_++];
  return token;
}


}  // namespace impala
