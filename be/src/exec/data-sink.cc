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
#include "exec/blocking-plan-root-sink.h"
#include "exec/buffered-plan-root-sink.h"
#include "exec/exec-node.h"
#include "exec/hbase/hbase-table-sink.h"
#include "exec/hdfs-table-sink.h"
#include "exec/iceberg-delete-builder.h"
#include "exec/iceberg-delete-sink-config.h"
#include "exec/multi-table-sink.h"
#include "exec/kudu/kudu-table-sink.h"
#include "exec/kudu/kudu-util.h"
#include "exec/nested-loop-join-builder.h"
#include "exec/partitioned-hash-join-builder.h"
#include "exec/plan-root-sink.h"
#include "exprs/scalar-expr-evaluator.h"
#include "exprs/scalar-expr.h"
#include "gen-cpp/ImpalaInternalService_constants.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "gutil/strings/substitute.h"
#include "runtime/fragment-state.h"
#include "runtime/krpc-data-stream-sender.h"
#include "runtime/mem-tracker.h"
#include "util/container-util.h"

#include "common/names.h"

using strings::Substitute;

namespace impala {

void DataSinkConfig::Codegen(FragmentState* state) {
  return;
}

void DataSinkConfig::Close() {
  ScalarExpr::Close(output_exprs_);
}

Status DataSinkConfig::Init(
    const TDataSink& tsink, const RowDescriptor* input_row_desc, FragmentState* state) {
  tsink_ = &tsink;
  input_row_desc_ = input_row_desc;
  return ScalarExpr::Create(tsink.output_exprs, *input_row_desc_, state, &output_exprs_);
}

void DataSinkConfig::AddCodegenStatus(
    const Status& codegen_status, const std::string& extra_label) {
  codegen_status_msgs_.emplace_back(FragmentState::GenerateCodegenMsg(
      codegen_status.ok(), codegen_status, extra_label));
}

Status DataSinkConfig::CreateConfig(const TDataSink& thrift_sink,
    const RowDescriptor* row_desc, FragmentState* state,
    DataSinkConfig** data_sink) {
  ObjectPool* pool = state->obj_pool();
  *data_sink = nullptr;
  switch (thrift_sink.type) {
    case TDataSinkType::DATA_STREAM_SINK:
      if (!thrift_sink.__isset.stream_sink) return Status("Missing data stream sink.");
      // TODO: figure out good buffer size based on size of output row
      *data_sink = pool->Add(new KrpcDataStreamSenderConfig());
      break;
    case TDataSinkType::TABLE_SINK:
      if (!thrift_sink.__isset.table_sink) return Status("Missing table sink.");
      switch (thrift_sink.table_sink.type) {
        case TTableSinkType::HDFS:
          if (thrift_sink.table_sink.action == TSinkAction::INSERT) {
            *data_sink = pool->Add(new HdfsTableSinkConfig());
          } else if (thrift_sink.table_sink.action == TSinkAction::DELETE) {
            // Currently only Iceberg tables support DELETE operations for FS tables.
            *data_sink = pool->Add(new IcebergDeleteSinkConfig());
          }
          break;
        case TTableSinkType::KUDU:
          RETURN_IF_ERROR(CheckKuduAvailability());
          *data_sink = pool->Add(new KuduTableSinkConfig());
          break;
        case TTableSinkType::HBASE:
          *data_sink = pool->Add(new HBaseTableSinkConfig());
          break;
        default:
          stringstream error_msg;
          map<int, const char*>::const_iterator i =
              _TTableSinkType_VALUES_TO_NAMES.find(thrift_sink.table_sink.type);
          const char* str = i != _TTableSinkType_VALUES_TO_NAMES.end() ?
              i->second :
              "Unknown table sink";
          error_msg << str << " not implemented.";
          return Status(error_msg.str());
      }
      break;
    case TDataSinkType::PLAN_ROOT_SINK:
      *data_sink = pool->Add(new PlanRootSinkConfig());
      break;
    case TDataSinkType::HASH_JOIN_BUILDER: {
      *data_sink = pool->Add(new PhjBuilderConfig());
      break;
    }
    case TDataSinkType::NESTED_LOOP_JOIN_BUILDER: {
      *data_sink = pool->Add(new NljBuilderConfig());
      break;
    }
    case TDataSinkType::ICEBERG_DELETE_BUILDER: {
      *data_sink = pool->Add(new IcebergDeleteBuilderConfig());
      break;
    }
    case TDataSinkType::MULTI_DATA_SINK: {
      *data_sink = pool->Add(new MultiTableSinkConfig());
      break;
    }
    default:
      stringstream error_msg;
      map<int, const char*>::const_iterator i =
          _TDataSinkType_VALUES_TO_NAMES.find(thrift_sink.type);
      const char* str = i != _TDataSinkType_VALUES_TO_NAMES.end() ?
          i->second :
          "Unknown data sink type ";
      error_msg << str << " not implemented.";
      return Status(error_msg.str());
  }
  RETURN_IF_ERROR((*data_sink)->Init(thrift_sink, row_desc, state));
  return Status::OK();
}

// Empty string
const char* const DataSink::ROOT_PARTITION_KEY = "";

DataSink::DataSink(TDataSinkId sink_id, const DataSinkConfig& sink_config,
    const string& name, RuntimeState* state)
  : sink_config_(sink_config),
    closed_(false),
    row_desc_(sink_config.input_row_desc_),
    name_(name),
    output_exprs_(sink_config.output_exprs_) {
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

Status DataSink::Open(RuntimeState* state) {
  DCHECK_EQ(output_exprs_.size(), output_expr_evals_.size());
  return ScalarExprEvaluator::Open(output_expr_evals_, state);
}

void DataSink::Close(RuntimeState* state) {
  if (closed_) return;
  ScalarExprEvaluator::Close(output_expr_evals_, state);
  if (expr_perm_pool_ != nullptr) expr_perm_pool_->FreeAll();
  if (expr_results_pool_.get() != nullptr) expr_results_pool_->FreeAll();
  if (expr_mem_tracker_ != nullptr) expr_mem_tracker_->Close();
  if (mem_tracker_ != nullptr) mem_tracker_->Close();
  for (const string& codegen_msg : sink_config_.codegen_status_msgs_) {
    profile_->AppendExecOption(codegen_msg);
  }
  closed_ = true;
}

}  // namespace impala
