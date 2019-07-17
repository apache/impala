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

#include "exec/plan-root-sink.h"

#include "exprs/scalar-expr.h"
#include "exprs/scalar-expr-evaluator.h"
#include "runtime/row-batch.h"
#include "runtime/tuple-row.h"
#include "service/query-result-set.h"
#include "util/pretty-printer.h"

#include <memory>
#include <boost/thread/mutex.hpp>

using namespace std;
using boost::unique_lock;
using boost::mutex;

namespace impala {

PlanRootSink::PlanRootSink(
    TDataSinkId sink_id, const RowDescriptor* row_desc, RuntimeState* state)
  : DataSink(sink_id, row_desc, "PLAN_ROOT_SINK", state),
    num_rows_produced_limit_(state->query_options().num_rows_produced_limit) {}

PlanRootSink::~PlanRootSink() {}

void PlanRootSink::ValidateCollectionSlots(
    const RowDescriptor& row_desc, RowBatch* batch) {
#ifndef NDEBUG
  if (!row_desc.HasVarlenSlots()) return;
  for (int i = 0; i < batch->num_rows(); ++i) {
    TupleRow* row = batch->GetRow(i);
    for (int j = 0; j < row_desc.tuple_descriptors().size(); ++j) {
      const TupleDescriptor* tuple_desc = row_desc.tuple_descriptors()[j];
      if (tuple_desc->collection_slots().empty()) continue;
      for (int k = 0; k < tuple_desc->collection_slots().size(); ++k) {
        const SlotDescriptor* slot_desc = tuple_desc->collection_slots()[k];
        int tuple_idx = row_desc.GetTupleIdx(slot_desc->parent()->id());
        const Tuple* tuple = row->GetTuple(tuple_idx);
        if (tuple == NULL) continue;
        DCHECK(tuple->IsNull(slot_desc->null_indicator_offset()));
      }
    }
  }
#endif
}

Status PlanRootSink::UpdateAndCheckRowsProducedLimit(
    RuntimeState* state, RowBatch* batch) {
  // Since the PlanRootSink has a single producer, the
  // num_rows_returned_ value can be verified without acquiring any locks.
  num_rows_produced_ += batch->num_rows();
  if (num_rows_produced_limit_ > 0 && num_rows_produced_ > num_rows_produced_limit_) {
    Status err = Status::Expected(TErrorCode::ROWS_PRODUCED_LIMIT_EXCEEDED,
        PrintId(state->query_id()),
        PrettyPrinter::Print(num_rows_produced_limit_, TUnit::NONE));
    VLOG_QUERY << err.msg().msg();
    return err;
  }
  return Status::OK();
}
}
