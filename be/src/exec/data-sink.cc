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

#include "exec/data-sink.h"

#include <string>
#include <map>

#include "common/logging.h"
#include "exec/exec-node.h"
#include "exec/hbase-table-sink.h"
#include "exec/hdfs-table-sink.h"
#include "exec/kudu-table-sink.h"
#include "exec/kudu-util.h"
#include "exec/plan-root-sink.h"
#include "exprs/scalar-expr.h"
#include "gen-cpp/ImpalaInternalService_constants.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "gutil/strings/substitute.h"
#include "runtime/krpc-data-stream-sender.h"
#include "runtime/mem-tracker.h"
#include "util/container-util.h"

#include "common/names.h"

DEFINE_int64(data_stream_sender_buffer_size, 16 * 1024,
    "(Advanced) Max size in bytes which a row batch in a data stream sender's channel "
    "can accumulate before the row batch is sent over the wire.");

using strings::Substitute;

namespace impala {

// Empty string
const char* const DataSink::ROOT_PARTITION_KEY = "";

DataSink::DataSink(TDataSinkId sink_id, const RowDescriptor* row_desc, const string& name,
    RuntimeState* state)
  : closed_(false), row_desc_(row_desc), name_(name) {
  profile_ = RuntimeProfile::Create(state->obj_pool(), name);
  if (sink_id != -1) {
    // There is one sink per fragment so we can use the fragment index as a unique
    // identifier.
    profile_->SetDataSinkId(sink_id);
  }
}

DataSink::~DataSink() {
  DCHECK(closed_);
}

Status DataSink::Create(const TPlanFragmentCtx& fragment_ctx,
    const TPlanFragmentInstanceCtx& fragment_instance_ctx, const RowDescriptor* row_desc,
    RuntimeState* state, DataSink** sink) {
  const TDataSink& thrift_sink = fragment_ctx.fragment.output_sink;
  const vector<TExpr>& thrift_output_exprs = fragment_ctx.fragment.output_exprs;
  ObjectPool* pool = state->obj_pool();
  // We have one fragment per sink, so we can use the fragment index as the sink ID.
  TDataSinkId sink_id = fragment_ctx.fragment.idx;
  switch (thrift_sink.type) {
    case TDataSinkType::DATA_STREAM_SINK:
      if (!thrift_sink.__isset.stream_sink) return Status("Missing data stream sink.");
      // TODO: figure out good buffer size based on size of output row
      *sink = pool->Add(new KrpcDataStreamSender(sink_id,
          fragment_instance_ctx.sender_id, row_desc, thrift_sink.stream_sink,
          fragment_ctx.destinations, FLAGS_data_stream_sender_buffer_size, state));
      break;
    case TDataSinkType::TABLE_SINK:
      if (!thrift_sink.__isset.table_sink) return Status("Missing table sink.");
      switch (thrift_sink.table_sink.type) {
        case TTableSinkType::HDFS:
          *sink =
              pool->Add(new HdfsTableSink(sink_id, row_desc, thrift_sink, state));
          break;
        case TTableSinkType::HBASE:
          *sink =
              pool->Add(new HBaseTableSink(sink_id, row_desc, thrift_sink, state));
          break;
        case TTableSinkType::KUDU:
          RETURN_IF_ERROR(CheckKuduAvailability());
          *sink =
              pool->Add(new KuduTableSink(sink_id, row_desc, thrift_sink, state));
          break;
        default:
          stringstream error_msg;
          map<int, const char*>::const_iterator i =
              _TTableSinkType_VALUES_TO_NAMES.find(thrift_sink.table_sink.type);
          const char* str = i != _TTableSinkType_VALUES_TO_NAMES.end() ?
              i->second : "Unknown table sink";
          error_msg << str << " not implemented.";
          return Status(error_msg.str());
      }
      break;
    case TDataSinkType::PLAN_ROOT_SINK:
      *sink = pool->Add(new PlanRootSink(sink_id, row_desc, state));
      break;
    case TDataSinkType::JOIN_BUILD_SINK:
      // IMPALA-4224 - join build sink not supported in backend execution.
    default:
      stringstream error_msg;
      map<int, const char*>::const_iterator i =
          _TDataSinkType_VALUES_TO_NAMES.find(thrift_sink.type);
      const char* str = i != _TDataSinkType_VALUES_TO_NAMES.end() ?
          i->second :  "Unknown data sink type ";
      error_msg << str << " not implemented.";
      return Status(error_msg.str());
  }
  RETURN_IF_ERROR((*sink)->Init(thrift_output_exprs, thrift_sink, state));
  return Status::OK();
}

Status DataSink::Init(const vector<TExpr>& thrift_output_exprs,
    const TDataSink& tsink, RuntimeState* state) {
  return ScalarExpr::Create(thrift_output_exprs, *row_desc_, state, &output_exprs_);
}

Status DataSink::Prepare(RuntimeState* state, MemTracker* parent_mem_tracker) {
  DCHECK(parent_mem_tracker != nullptr);
  DCHECK(profile_ != nullptr);
  mem_tracker_.reset(new MemTracker(profile_, -1, name_, parent_mem_tracker));
  expr_mem_tracker_.reset(
      new MemTracker(-1, Substitute("$0 Exprs", name_), mem_tracker_.get(), false));
  expr_perm_pool_.reset(new MemPool(expr_mem_tracker_.get()));
  expr_results_pool_.reset(new MemPool(expr_mem_tracker_.get()));
  RETURN_IF_ERROR(ScalarExprEvaluator::Create(output_exprs_, state, state->obj_pool(),
      expr_perm_pool_.get(), expr_results_pool_.get(), &output_expr_evals_));
  return Status::OK();
}

void DataSink::Codegen(LlvmCodeGen* codegen) {
  return;
}

Status DataSink::Open(RuntimeState* state) {
  DCHECK_EQ(output_exprs_.size(), output_expr_evals_.size());
  return ScalarExprEvaluator::Open(output_expr_evals_, state);
}

void DataSink::Close(RuntimeState* state) {
  if (closed_) return;
  ScalarExprEvaluator::Close(output_expr_evals_, state);
  ScalarExpr::Close(output_exprs_);
  if (expr_perm_pool_ != nullptr) expr_perm_pool_->FreeAll();
  if (expr_results_pool_.get() != nullptr) expr_results_pool_->FreeAll();
  if (expr_mem_tracker_ != nullptr) expr_mem_tracker_->Close();
  if (mem_tracker_ != nullptr) mem_tracker_->Close();
  closed_ = true;
}

}  // namespace impala
