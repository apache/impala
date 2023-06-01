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

#include "exec/buffered-plan-root-sink.h"
#include "exec/blocking-plan-root-sink.h"
#include "exprs/scalar-expr-evaluator.h"
#include "exprs/scalar-expr.h"
#include "runtime/row-batch.h"
#include "runtime/tuple-row.h"
#include "service/query-result-set.h"
#include "util/debug-util.h"
#include "util/pretty-printer.h"
#include "util/runtime-profile-counters.h"

#include <memory>
#include <mutex>

using namespace std;
using std::mutex;
using std::unique_lock;

namespace impala {

DataSink* PlanRootSinkConfig::CreateSink(RuntimeState* state) const {
  TDataSinkId sink_id = state->fragment().idx;
  ObjectPool* pool = state->obj_pool();
  if (state->query_options().spool_query_results) {
    return pool->Add(new BufferedPlanRootSink(
        sink_id, *this, state, state->instance_ctx().debug_options));
  } else {
    return pool->Add(new BlockingPlanRootSink(sink_id, *this, state));
  }
}

PlanRootSink::PlanRootSink(
    TDataSinkId sink_id, const DataSinkConfig& sink_config, RuntimeState* state)
  : DataSink(sink_id, sink_config, "PLAN_ROOT_SINK", state),
    num_rows_produced_limit_(state->query_options().num_rows_produced_limit) {}

PlanRootSink::~PlanRootSink() {}

Status PlanRootSink::Prepare(RuntimeState* state, MemTracker* parent_mem_tracker) {
  RETURN_IF_ERROR(DataSink::Prepare(state, parent_mem_tracker));
  rows_sent_counter_ = ADD_COUNTER(profile_, "RowsSent", TUnit::UNIT);
  rows_sent_rate_ = profile_->AddDerivedCounter("RowsSentRate", TUnit::UNIT_PER_SECOND,
      bind<int64_t>(&RuntimeProfile::UnitsPerSecond, rows_sent_counter_,
                                                    profile_->total_time_counter()));
  create_result_set_timer_ = ADD_TIMER(profile(), "CreateResultSetTime");
  return Status::OK();
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
