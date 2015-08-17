// Copyright 2015 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exec/kudu-scan-node.h"

#include <boost/foreach.hpp>
#include <kudu/client/row_result.h>
#include <kudu/client/schema.h>
#include <kudu/client/value.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <vector>

#include "exec/kudu-scanner.h"
#include "exec/kudu-util.h"
#include "exprs/expr.h"
#include "runtime/mem-pool.h"
#include "runtime/runtime-state.h"
#include "runtime/row-batch.h"
#include "runtime/string-value.h"
#include "runtime/tuple-row.h"
#include "gutil/gscoped_ptr.h"
#include "gutil/strings/substitute.h"
#include "gutil/stl_util.h"
#include "util/jni-util.h"
#include "util/periodic-counter-updater.h"
#include "util/runtime-profile.h"

#include "common/names.h"

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

namespace {

// Returns a vector with the slots that are materialized in the TupleDescriptor.
void GetMaterializedSlots(const TupleDescriptor& tuple_desc,
    vector<SlotDescriptor*>* materialized_slots) {
  DCHECK(materialized_slots->empty());

  const vector<SlotDescriptor*>& slots = tuple_desc.slots();
  BOOST_FOREACH(SlotDescriptor* slot, slots) {
    if (slot->is_materialized()) materialized_slots->push_back(slot);
  }
}

} // anonymous namespace

KuduScanNode::KuduScanNode(ObjectPool* pool, const TPlanNode& tnode,
    const DescriptorTbl& descs)
    : ScanNode(pool, tnode, descs),
      tuple_id_(tnode.kudu_scan_node.tuple_id),
      pushable_conjuncts_(tnode.kudu_scan_node.pushable_conjuncts) {
}

KuduScanNode::~KuduScanNode() {
  STLDeleteElements(&kudu_predicates_);
}

Status KuduScanNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ScanNode::Prepare(state));

  kudu_read_timer_ = ADD_CHILD_TIMER(runtime_profile(), KUDU_READ_TIMER,
      SCANNER_THREAD_TOTAL_WALLCLOCK_TIME);
  kudu_round_trips_ = ADD_COUNTER(runtime_profile(), KUDU_ROUND_TRIPS, TUnit::UNIT);

  DCHECK(state->desc_tbl().GetTupleDescriptor(tuple_id_) != NULL);

  tuple_desc_ = state->desc_tbl().GetTupleDescriptor(tuple_id_);
  GetMaterializedSlots(*tuple_desc_, &materialized_slots_);
  RETURN_IF_ERROR(ProjectedColumnsFromTupleDescriptor(*tuple_desc_, &projected_columns_));

  // Convert TScanRangeParams to ScanRanges.
  CHECK(scan_range_params_ != NULL)
      << "Must call SetScanRanges() before calling Prepare()";
  BOOST_FOREACH(const TScanRangeParams& params, *scan_range_params_) {
    const TKuduKeyRange& key_range = params.scan_range.kudu_key_range;
    scan_ranges_.push_back(key_range);
  }

  scanner_.reset(new KuduScanner(this, state, scan_ranges_));
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
  BOOST_FOREACH(const string& address, table_desc->kudu_master_addresses()) {
    b.add_master_server_addr(address);
  }

  KUDU_RETURN_IF_ERROR(b.Build(&client_), "Unable to create Kudu client");

  KUDU_RETURN_IF_ERROR(client_->OpenTable(table_desc->table_name(), &table_),
      "Unable to open Kudu table");

  // Must happen after table_ is opened.
  RETURN_IF_ERROR(TransformPushableConjunctsToRangePredicates());

  RETURN_IF_ERROR(scanner_->Open(client_, table_));

  return Status::OK();
}

Status KuduScanNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  SCOPED_TIMER(materialize_tuple_timer());
  *eos = false;

  if (ReachedLimit()) {
    // LIMIT 0 case.  Other limit values handled below.
    DCHECK_EQ(limit_, 0);
    *eos = true;
    return Status::OK();
  }

  // TODO a multi-threaded implementation will have more than one scanner, likely each
  // one managing their own row_batch and appending the batches to a blocking queue.
  RETURN_IF_ERROR(scanner_->GetNext(row_batch, eos));
  num_rows_returned_ += row_batch->num_rows();
  COUNTER_SET(rows_returned_counter_, num_rows_returned_);

  if (ReachedLimit()) {
    int num_rows_over = num_rows_returned_ - limit_;
    row_batch->set_num_rows(row_batch->num_rows() - num_rows_over);
    num_rows_returned_ -= num_rows_over;
    COUNTER_SET(rows_returned_counter_, num_rows_returned_);

    *eos = true;
  }

  return Status::OK();
}

Status KuduScanNode::TransformPushableConjunctsToRangePredicates() {
  // The only supported pushable predicates are binary operators with a SlotRef and a
  // literal as the left and right operands respectively.
  BOOST_FOREACH(const TExpr& predicate, pushable_conjuncts_) {
    DCHECK_EQ(predicate.nodes.size(), 3);
    const TExprNode& function_call = predicate.nodes[0];

    DCHECK_EQ(function_call.node_type, TExprNodeType::FUNCTION_CALL);

    Slice col_name;
    KuduValue* bound;
    GetSlotRefColumnName(predicate.nodes[1], &col_name);
    GetExprLiteralBound(predicate.nodes[2], &bound);
    DCHECK(bound != NULL);

    const string& function_name = function_call.fn.name.function_name;
    if (function_name == GE_FN) {
      kudu_predicates_.push_back(table_->NewComparisonPredicate(col_name,
          KuduPredicate::GREATER_EQUAL, bound));
    } else if (function_name == LE_FN) {
      kudu_predicates_.push_back(table_->NewComparisonPredicate(col_name,
          KuduPredicate::LESS_EQUAL, bound));
    } else if (function_name == EQ_FN) {
      kudu_predicates_.push_back(table_->NewComparisonPredicate(col_name,
          KuduPredicate::EQUAL, bound));
    } else {
      DCHECK(false) << "Received unpushable operator to push down: " << function_name;
    }
  }
  return Status::OK();
}

void KuduScanNode::GetSlotRefColumnName(const TExprNode& node, Slice* col_name) {
  const KuduTableDescriptor* table_desc =
      static_cast<const KuduTableDescriptor*>(tuple_desc_->table_desc());
  TSlotId slot_id = node.slot_ref.slot_id;
  BOOST_FOREACH(SlotDescriptor* slot, tuple_desc_->slots()) {
    if (slot->id() == slot_id) {
      int col_idx = slot->col_pos();
      *col_name = table_desc->col_names()[col_idx];
      return;
    }
  }
  DCHECK(false) << "Could not find a slot with slot id: " << slot_id;
}

void KuduScanNode::GetExprLiteralBound(const TExprNode& node, KuduValue** value) {
  switch (node.node_type) {
    case TExprNodeType::BOOL_LITERAL:
      *value = KuduValue::FromBool(node.bool_literal.value);
      return;
    case TExprNodeType::FLOAT_LITERAL:
      *value = KuduValue::FromFloat(node.float_literal.value);
      return;
    case TExprNodeType::INT_LITERAL:
      *value = KuduValue::FromInt(node.int_literal.value);
      return;
    case TExprNodeType::STRING_LITERAL:
      *value = KuduValue::CopyString(node.string_literal.value);
      return;
    default:
      // Should be unreachable.
      LOG(DFATAL) << "Unsupported node type: " << node.node_type;
  }
}

void KuduScanNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  PeriodicCounterUpdater::StopRateCounter(total_throughput_counter());
  PeriodicCounterUpdater::StopTimeSeriesCounter(bytes_read_timeseries_counter_);
  ExecNode::Close(state);
}

void KuduScanNode::DebugString(int indentation_level, stringstream* out) const {
  string indent(indentation_level * 2, ' ');
  *out << indent << "KuduScanNode(tupleid=" << tuple_id_ << ")";
}

}  // namespace impala
