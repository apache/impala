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
#include "scheduling/query-resource-mgr.h"
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
using strings::Substitute;

namespace impala {

const string KuduScanNode::KUDU_READ_TIMER = "TotalKuduReadTime";
const string KuduScanNode::KUDU_ROUND_TRIPS = "TotalKuduScanRoundTrips";

const std::string KuduScanNode::GE_FN = "ge";
const std::string KuduScanNode::LE_FN = "le";
const std::string KuduScanNode::EQ_FN = "eq";

KuduScanNode::KuduScanNode(ObjectPool* pool, const TPlanNode& tnode,
    const DescriptorTbl& descs)
    : ScanNode(pool, tnode, descs),
      tuple_id_(tnode.kudu_scan_node.tuple_id),
      next_scan_range_idx_(0),
      num_active_scanners_(0),
      done_(false),
      pushable_conjuncts_(tnode.kudu_scan_node.kudu_conjuncts),
      thread_avail_cb_id_(-1) {
  DCHECK(KuduIsAvailable());
}

KuduScanNode::~KuduScanNode() {
  STLDeleteElements(&kudu_predicates_);
}

Status KuduScanNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ScanNode::Prepare(state));
  runtime_state_ = state;

  scan_ranges_complete_counter_ =
      ADD_COUNTER(runtime_profile(), SCAN_RANGES_COMPLETE_COUNTER, TUnit::UNIT);
  kudu_read_timer_ = ADD_CHILD_TIMER(runtime_profile(), KUDU_READ_TIMER,
      SCANNER_THREAD_TOTAL_WALLCLOCK_TIME);
  kudu_round_trips_ = ADD_COUNTER(runtime_profile(), KUDU_ROUND_TRIPS, TUnit::UNIT);

  DCHECK(state->desc_tbl().GetTupleDescriptor(tuple_id_) != NULL);

  tuple_desc_ = state->desc_tbl().GetTupleDescriptor(tuple_id_);

  // Convert TScanRangeParams to ScanRanges.
  CHECK(scan_range_params_ != NULL)
      << "Must call SetScanRanges() before calling Prepare()";
  for (const TScanRangeParams& params: *scan_range_params_) {
    const TKuduKeyRange& key_range = params.scan_range.kudu_key_range;
    key_ranges_.push_back(key_range);
  }
  max_materialized_row_batches_ = FLAGS_kudu_max_row_batches;

  if (max_materialized_row_batches_ <= 0) {
    // TODO: See comment on hdfs-scan-node.
    // This value is built the same way as it assumes that the scan node runs co-located
    // with a Kudu tablet server and that the tablet server is using disks similarly as
    // a datanode would.
    max_materialized_row_batches_ =
        10 * (DiskInfo::num_disks() + DiskIoMgr::REMOTE_NUM_DISKS);
  }
  materialized_row_batches_.reset(new RowBatchQueue(max_materialized_row_batches_));
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
  RETURN_IF_ERROR(ProjectedColumnsFromTupleDescriptor(*tuple_desc_, &projected_columns_,
      table_->schema()));
  // Must happen after table_ is opened.
  RETURN_IF_ERROR(TransformPushableConjunctsToRangePredicates());

  num_scanner_threads_started_counter_ =
      ADD_COUNTER(runtime_profile(), NUM_SCANNER_THREADS_STARTED, TUnit::UNIT);

  // Reserve one thread token.
  state->resource_pool()->ReserveOptionalTokens(1);
  if (state->query_options().num_scanner_threads > 0) {
    state->resource_pool()->set_max_quota(
        state->query_options().num_scanner_threads);
  }

  thread_avail_cb_id_ = state->resource_pool()->AddThreadAvailableCb(
      bind<void>(mem_fn(&KuduScanNode::ThreadTokenAvailableCb), this, _1));
  ThreadTokenAvailableCb(state->resource_pool());
  return Status::OK();
}

Status KuduScanNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  SCOPED_TIMER(materialize_tuple_timer());

  if (ReachedLimit() || key_ranges_.empty()) {
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

Status KuduScanNode::TransformPushableConjunctsToRangePredicates() {
  // The only supported pushable predicates are binary operators with a SlotRef and a
  // literal as the left and right operands respectively.
  for (const TExpr& predicate: pushable_conjuncts_) {
    DCHECK_EQ(predicate.nodes.size(), 3);
    const TExprNode& function_call = predicate.nodes[0];

    DCHECK_EQ(function_call.node_type, TExprNodeType::FUNCTION_CALL);

    IdxByLowerCaseColName idx_by_lc_name;
    RETURN_IF_ERROR(MapLowercaseKuduColumnNamesToIndexes(table_->schema(),
        &idx_by_lc_name));

    string impala_col_name;
    GetSlotRefColumnName(predicate.nodes[1], &impala_col_name);

    IdxByLowerCaseColName::const_iterator iter;
    if ((iter = idx_by_lc_name.find(impala_col_name)) ==
        idx_by_lc_name.end()) {
      return Status(Substitute("Could not find col: '$0' in the table schema",
                               impala_col_name));
    }

    KuduColumnSchema column = table_->schema().Column(iter->second);

    KuduValue* bound;
    RETURN_IF_ERROR(GetExprLiteralBound(predicate.nodes[2], column.type(), &bound));
    DCHECK(bound != NULL);

    const string& function_name = function_call.fn.name.function_name;
    if (function_name == GE_FN) {
      kudu_predicates_.push_back(table_->NewComparisonPredicate(column.name(),
          KuduPredicate::GREATER_EQUAL, bound));
    } else if (function_name == LE_FN) {
      kudu_predicates_.push_back(table_->NewComparisonPredicate(column.name(),
          KuduPredicate::LESS_EQUAL, bound));
    } else if (function_name == EQ_FN) {
      kudu_predicates_.push_back(table_->NewComparisonPredicate(column.name(),
          KuduPredicate::EQUAL, bound));
    } else {
      DCHECK(false) << "Received unpushable operator to push down: " << function_name;
    }
  }
  return Status::OK();
}

void KuduScanNode::GetSlotRefColumnName(const TExprNode& node, string* col_name) {
  const KuduTableDescriptor* table_desc =
      static_cast<const KuduTableDescriptor*>(tuple_desc_->table_desc());
  TSlotId slot_id = node.slot_ref.slot_id;
  for (SlotDescriptor* slot: tuple_desc_->slots()) {
    if (slot->id() == slot_id) {
      int col_idx = slot->col_pos();
      *col_name = table_desc->col_descs()[col_idx].name();
      return;
    }
  }

  DCHECK(false) << "Could not find a slot with slot id: " << slot_id;
}

namespace {

typedef std::map<int, const char*> TypeNamesMap;

// Gets the name of an Expr node type.
string NodeTypeToString(TExprNodeType::type type) {
  const TypeNamesMap& type_names_map =
      impala::_TExprNodeType_VALUES_TO_NAMES;
  TypeNamesMap::const_iterator iter = type_names_map.find(type);

  if (iter == type_names_map.end()) {
    return Substitute("Unknown type: $0", type);
  }

  return (*iter).second;
}

} // anonymous namespace

Status KuduScanNode::GetExprLiteralBound(const TExprNode& node,
    KuduColumnSchema::DataType type, KuduValue** value) {

  // Build the Kudu values based on the type of the Kudu column.
  // We're restrictive regarding which types we accept as the planner does all casting
  // in the frontend and predicates only get pushed down if the types match.
  switch (type) {
    // For types BOOL and STRING we expect the expression literal to match perfectly.
    case kudu::client::KuduColumnSchema::BOOL: {
      if (node.node_type != TExprNodeType::BOOL_LITERAL) {
        return Status(Substitute("Cannot create predicate over column of type BOOL with "
            "value of type: $0", NodeTypeToString(node.node_type)));
      }
      *value = KuduValue::FromBool(node.bool_literal.value);
      return Status::OK();
    }
    case kudu::client::KuduColumnSchema::STRING: {
      if (node.node_type != TExprNodeType::STRING_LITERAL) {
        return Status(Substitute("Cannot create predicate over column of type STRING"
            " with value of type: $0", NodeTypeToString(node.node_type)));
      }
      *value = KuduValue::CopyString(node.string_literal.value);
      return Status::OK();
    }
    case kudu::client::KuduColumnSchema::INT8:
    case kudu::client::KuduColumnSchema::INT16:
    case kudu::client::KuduColumnSchema::INT32:
    case kudu::client::KuduColumnSchema::INT64: {
      if (node.node_type != TExprNodeType::INT_LITERAL) {
        return Status(Substitute("Cannot create predicate over column of type INT with "
            "value of type: $0", NodeTypeToString(node.node_type)));
      }
      *value = KuduValue::FromInt(node.int_literal.value);
      return Status::OK();
    }
    case kudu::client::KuduColumnSchema::FLOAT: {
      if (node.node_type != TExprNodeType::FLOAT_LITERAL) {
        return Status(Substitute("Cannot create predicate over column of type FLOAT with "
            "value of type: $0", NodeTypeToString(node.node_type)));
      }
      *value = KuduValue::FromFloat(node.float_literal.value);
      return Status::OK();
    }
    case kudu::client::KuduColumnSchema::DOUBLE: {
      if (node.node_type != TExprNodeType::FLOAT_LITERAL) {
        return Status(Substitute("Cannot create predicate over column of type DOUBLE with "
            "value of type: $0", NodeTypeToString(node.node_type)));
      }
      *value = KuduValue::FromDouble(node.float_literal.value);
      return Status::OK();
    }
    default:
      // Should be unreachable.
      LOG(DFATAL) << "Unsupported node type: " << node.node_type;
  }
  // Avoid compiler warning.
  return Status("Unreachable");
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

TKuduKeyRange* KuduScanNode::GetNextKeyRange() {
  unique_lock<mutex> lock(lock_);
  if (next_scan_range_idx_ >= key_ranges_.size()) return NULL;
  TKuduKeyRange* range = &key_ranges_[next_scan_range_idx_++];
  return range;
}

Status KuduScanNode::GetConjunctCtxs(vector<ExprContext*>* ctxs) {
  return Expr::CloneIfNotExists(conjunct_ctxs_, runtime_state_, ctxs);
}

void KuduScanNode::ClonePredicates(vector<KuduPredicate*>* predicates) {
  unique_lock<mutex> l(lock_);
  for (KuduPredicate* predicate: kudu_predicates_) {
    predicates->push_back(predicate->Clone());
  }
}

void KuduScanNode::ThreadTokenAvailableCb(
    ThreadResourceMgr::ResourcePool* pool) {
  while (true) {
    unique_lock<mutex> lock(lock_);
    // All done or all ranges are assigned.
    if (done_ || next_scan_range_idx_ >= key_ranges_.size()) break;

    // Check if we can get a token.
    if (!pool->TryAcquireThreadToken()) break;

    ++num_active_scanners_;
    COUNTER_ADD(num_scanner_threads_started_counter_, 1);

    // Reserve the first range so no other thread picks it up.
    TKuduKeyRange* range = &key_ranges_[next_scan_range_idx_++];
    string name = Substitute("scanner-thread($0)",
        num_scanner_threads_started_counter_->value());

    VLOG(1) << "Thread started: " << name;
    scanner_threads_.AddThread(new Thread("kudu-scan-node", name,
        &KuduScanNode::ScannerThread, this, name, range));

    if (runtime_state_->query_resource_mgr() != NULL) {
      runtime_state_->query_resource_mgr()->NotifyThreadUsageChange(1);
    }
  }
}

Status KuduScanNode::ProcessRange(KuduScanner* scanner, const TKuduKeyRange* key_range) {
  RETURN_IF_ERROR(scanner->OpenNextRange(*key_range));
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
  // Mark the current scan range as complete.
  if (eos) scan_ranges_complete_counter()->Add(1);
  return Status::OK();
}

void KuduScanNode::ScannerThread(const string& name, const TKuduKeyRange* initial_range) {
  DCHECK(initial_range != NULL);
  SCOPED_THREAD_COUNTER_MEASUREMENT(scanner_thread_counters());
  SCOPED_TIMER(runtime_state_->total_cpu_timer());

  // Set to true if this thread observes that the number of optional threads has been
  // exceeded and is exiting early.
  bool optional_thread_exiting = false;
  KuduScanner scanner(this, runtime_state_);
  Status status = scanner.Open();

  if (status.ok()) {
    const TKuduKeyRange* key_range = initial_range;
    while (!done_ && key_range != NULL) {
      status = ProcessRange(&scanner, key_range);
      if (!status.ok()) break;

      // Check if the number of optional threads has been exceeded.
      if (runtime_state_->resource_pool()->optional_exceeded()) {
        unique_lock<mutex> l(lock_);
        // Don't exit if this is the last thread. Otherwise, the scan will indicate it's
        // done before all ranges have been processed.
        if (num_active_scanners_ > 1) {
          --num_active_scanners_;
          optional_thread_exiting = true;
          break;
        }
      }
      key_range = GetNextKeyRange();
    }
    scanner.Close();
  }

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
  // invokes ThreadTokenAvailableCb() which attempts to take the same lock.
  VLOG(1) << "Thread done: " << name;
  runtime_state_->resource_pool()->ReleaseThreadToken(false);
}

}  // namespace impala
