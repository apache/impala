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

#include "exec/kudu/kudu-scan-node-base.h"

#include <boost/algorithm/string.hpp>
#include <kudu/client/row_result.h>
#include <kudu/client/schema.h>
#include <kudu/client/value.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <vector>

#include "exec/kudu/kudu-scanner.h"
#include "exec/kudu/kudu-util.h"
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

PROFILE_DECLARE_COUNTER(ScanRangesComplete);
PROFILE_DEFINE_TIMER(KuduScannerTotalDurationTime, STABLE_LOW,
    "Total time taken for all scan rpc requests to complete for Kudu scanners.");
PROFILE_DEFINE_TIMER(KuduScannerQueueDurationTime, STABLE_LOW,
    "Total time taken between scan rpc requests being accepted and when they were "
    "handled by Kudu scanners.");
PROFILE_DEFINE_TIMER(KuduScannerCpuUserTime, STABLE_LOW,
    "Total elapsed CPU user time for all scan rpc requests for Kudu scanners.");
PROFILE_DEFINE_TIMER(KuduScannerCpuSysTime, STABLE_LOW,
    "Total elapsed CPU system time for all scan rpc requests for Kudu scanners.");
PROFILE_DEFINE_COUNTER(KuduScannerCfileCacheHitBytes, STABLE_LOW, TUnit::BYTES,
    "Number of bytes that were read from the block cache because of a hit for Kudu "
    "scanners.");
PROFILE_DEFINE_COUNTER(KuduScannerCfileCacheMissBytes, STABLE_LOW, TUnit::BYTES,
    "Number of bytes that were read because of a block cache miss for Kudu scanners.");

const string KuduScanNodeBase::KUDU_ROUND_TRIPS = "TotalKuduScanRoundTrips";
const string KuduScanNodeBase::KUDU_REMOTE_TOKENS = "KuduRemoteScanTokens";
///   KuduClientTime - total amount of time scanner threads spent in the Kudu
///   client, either waiting for data from Kudu or processing data.
const string KuduScanNodeBase::KUDU_CLIENT_TIME = "KuduClientTime";

KuduScanNodeBase::KuduScanNodeBase(
    ObjectPool* pool, const ScanPlanNode& pnode, const DescriptorTbl& descs)
  : ScanNode(pool, pnode, descs),
    tuple_id_(pnode.tnode_->kudu_scan_node.tuple_id),
    count_star_slot_offset_(
            pnode.tnode_->kudu_scan_node.__isset.count_star_slot_offset ?
            pnode.tnode_->kudu_scan_node.count_star_slot_offset : -1) {
  DCHECK(KuduIsAvailable());
}

KuduScanNodeBase::~KuduScanNodeBase() {
  DCHECK(is_closed());
}

Status KuduScanNodeBase::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ScanNode::Prepare(state));

  AddBytesReadCounters();
  scan_ranges_complete_counter_ =
      PROFILE_ScanRangesComplete.Instantiate(runtime_profile());
  kudu_round_trips_ = ADD_COUNTER(runtime_profile(), KUDU_ROUND_TRIPS, TUnit::UNIT);
  kudu_remote_tokens_ = ADD_COUNTER(runtime_profile(), KUDU_REMOTE_TOKENS, TUnit::UNIT);
  kudu_client_time_ = ADD_TIMER(runtime_profile(), KUDU_CLIENT_TIME);

  kudu_scanner_total_duration_time_ =
      PROFILE_KuduScannerTotalDurationTime.Instantiate(runtime_profile());
  kudu_scanner_queue_duration_time_ =
      PROFILE_KuduScannerQueueDurationTime.Instantiate(runtime_profile());
  kudu_scanner_cpu_user_time_ =
      PROFILE_KuduScannerCpuUserTime.Instantiate(runtime_profile());
  kudu_scanner_cpu_sys_time_ =
      PROFILE_KuduScannerCpuSysTime.Instantiate(runtime_profile());
  kudu_scanner_cfile_cache_hit_bytes_ =
      PROFILE_KuduScannerCfileCacheHitBytes.Instantiate(runtime_profile());
  kudu_scanner_cfile_cache_miss_bytes_ =
      PROFILE_KuduScannerCfileCacheMissBytes.Instantiate(runtime_profile());

  DCHECK(state->desc_tbl().GetTupleDescriptor(tuple_id_) != NULL);
  tuple_desc_ = state->desc_tbl().GetTupleDescriptor(tuple_id_);
  table_desc_ = static_cast<const KuduTableDescriptor*>(tuple_desc_->table_desc());

  // Initialize the list of scan tokens to process from the ScanRangeParamsPB.
  DCHECK(scan_range_params_ != NULL);
  int num_remote_tokens = 0;
  for (const ScanRangeParamsPB& params : *scan_range_params_) {
    if (params.has_is_remote() && params.is_remote()) ++num_remote_tokens;
    DCHECK(params.scan_range().has_kudu_scan_token());
    scan_tokens_.push_back(params.scan_range().kudu_scan_token());
  }
  COUNTER_SET(kudu_remote_tokens_, num_remote_tokens);

  return Status::OK();
}

Status KuduScanNodeBase::Open(RuntimeState* state) {
  RETURN_IF_ERROR(ScanNode::Open(state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));
  SCOPED_TIMER(runtime_profile_->total_time_counter());

  RETURN_IF_ERROR(ExecEnv::GetInstance()->GetKuduClient(
      table_desc_->kudu_master_addresses(), &client_));

  uint64_t latest_ts = static_cast<uint64_t>(
      max<int64_t>(0, state->query_ctx().session.kudu_latest_observed_ts));
  VLOG_RPC << "Latest observed Kudu timestamp: " << latest_ts;
  if (latest_ts > 0) client_->SetLatestObservedTimestamp(latest_ts);

  runtime_profile_->AddInfoString("Table Name", table_desc_->fully_qualified_name());
  if (filter_ctxs_.size() > 0) WaitForRuntimeFilters();
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
