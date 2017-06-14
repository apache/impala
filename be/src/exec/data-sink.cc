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
#include "runtime/data-stream-sender.h"
#include "runtime/mem-tracker.h"
#include "util/container-util.h"

#include "common/names.h"

using strings::Substitute;

namespace impala {

DataSink::DataSink(const RowDescriptor* row_desc)
  : closed_(false), row_desc_(row_desc), mem_tracker_(NULL) {}

DataSink::~DataSink() {
  DCHECK(closed_);
}

Status DataSink::Create(const TPlanFragmentCtx& fragment_ctx,
    const TPlanFragmentInstanceCtx& fragment_instance_ctx, const RowDescriptor* row_desc,
    RuntimeState* state, DataSink** sink) {
  const TDataSink& thrift_sink = fragment_ctx.fragment.output_sink;
  const vector<TExpr>& thrift_output_exprs = fragment_ctx.fragment.output_exprs;
  ObjectPool* pool = state->obj_pool();
  switch (thrift_sink.type) {
    case TDataSinkType::DATA_STREAM_SINK:
      if (!thrift_sink.__isset.stream_sink) return Status("Missing data stream sink.");

      // TODO: figure out good buffer size based on size of output row
      *sink = pool->Add(new DataStreamSender(fragment_instance_ctx.sender_id, row_desc,
          thrift_sink.stream_sink, fragment_ctx.destinations, 16 * 1024));
      break;
    case TDataSinkType::TABLE_SINK:
      if (!thrift_sink.__isset.table_sink) return Status("Missing table sink.");
      switch (thrift_sink.table_sink.type) {
        case TTableSinkType::HDFS:
          *sink = pool->Add(new HdfsTableSink(row_desc, thrift_sink));
          break;
        case TTableSinkType::HBASE:
          *sink = pool->Add(new HBaseTableSink(row_desc, thrift_sink));
          break;
        case TTableSinkType::KUDU:
          RETURN_IF_ERROR(CheckKuduAvailability());
          *sink = pool->Add(new KuduTableSink(row_desc, thrift_sink));
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
      *sink = pool->Add(new PlanRootSink(row_desc));
      break;
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

void DataSink::MergeDmlStats(const TInsertStats& src_stats,
    TInsertStats* dst_stats) {
  dst_stats->bytes_written += src_stats.bytes_written;
  if (src_stats.__isset.kudu_stats) {
    if (!dst_stats->__isset.kudu_stats) dst_stats->__set_kudu_stats(TKuduDmlStats());
    if (!dst_stats->kudu_stats.__isset.num_row_errors) {
      dst_stats->kudu_stats.__set_num_row_errors(0);
    }
    dst_stats->kudu_stats.__set_num_row_errors(
        dst_stats->kudu_stats.num_row_errors + src_stats.kudu_stats.num_row_errors);
  }
  if (src_stats.__isset.parquet_stats) {
    if (dst_stats->__isset.parquet_stats) {
      MergeMapValues<string, int64_t>(src_stats.parquet_stats.per_column_size,
          &dst_stats->parquet_stats.per_column_size);
    } else {
      dst_stats->__set_parquet_stats(src_stats.parquet_stats);
    }
  }
}

string DataSink::OutputDmlStats(const PartitionStatusMap& stats,
    const string& prefix) {
  const char* indent = "  ";
  stringstream ss;
  ss << prefix;
  bool first = true;
  for (const PartitionStatusMap::value_type& val: stats) {
    if (!first) ss << endl;
    first = false;
    ss << "Partition: ";

    const string& partition_key = val.first;
    if (partition_key == g_ImpalaInternalService_constants.ROOT_PARTITION_KEY) {
      ss << "Default" << endl;
    } else {
      ss << partition_key << endl;
    }
    if (val.second.__isset.num_modified_rows) {
      ss << "NumModifiedRows: " << val.second.num_modified_rows << endl;
    }

    if (!val.second.__isset.stats) continue;
    const TInsertStats& stats = val.second.stats;
    if (stats.__isset.kudu_stats) {
      ss << "NumRowErrors: " << stats.kudu_stats.num_row_errors << endl;
    }

    ss << indent << "BytesWritten: "
       << PrettyPrinter::Print(stats.bytes_written, TUnit::BYTES);
    if (stats.__isset.parquet_stats) {
      const TParquetInsertStats& parquet_stats = stats.parquet_stats;
      ss << endl << indent << "Per Column Sizes:";
      for (map<string, int64_t>::const_iterator i = parquet_stats.per_column_size.begin();
           i != parquet_stats.per_column_size.end(); ++i) {
        ss << endl << indent << indent << i->first << ": "
           << PrettyPrinter::Print(i->second, TUnit::BYTES);
      }
    }
  }
  return ss.str();
}

Status DataSink::Prepare(RuntimeState* state, MemTracker* parent_mem_tracker) {
  DCHECK(parent_mem_tracker != NULL);
  profile_ = state->obj_pool()->Add(new RuntimeProfile(state->obj_pool(), GetName()));
  const string& name = GetName();
  mem_tracker_.reset(new MemTracker(profile_, -1, name, parent_mem_tracker));
  expr_mem_tracker_.reset(
      new MemTracker(-1, Substitute("$0 Exprs", name), mem_tracker_.get(), false));
  expr_mem_pool_.reset(new MemPool(expr_mem_tracker_.get()));
  RETURN_IF_ERROR(ScalarExprEvaluator::Create(output_exprs_, state, state->obj_pool(),
      expr_mem_pool(), &output_expr_evals_));
  return Status::OK();
}

Status DataSink::Open(RuntimeState* state) {
  DCHECK_EQ(output_exprs_.size(), output_expr_evals_.size());
  return ScalarExprEvaluator::Open(output_expr_evals_, state);
}

void DataSink::Close(RuntimeState* state) {
  if (closed_) return;
  ScalarExprEvaluator::Close(output_expr_evals_, state);
  ScalarExpr::Close(output_exprs_);
  if (expr_mem_pool() != nullptr) expr_mem_pool_->FreeAll();
  if (expr_mem_tracker_ != NULL) {
    expr_mem_tracker_->UnregisterFromParent();
    expr_mem_tracker_.reset();
  }
  if (mem_tracker_ != NULL) {
    mem_tracker_->UnregisterFromParent();
    mem_tracker_.reset();
  }
  closed_ = true;
}

}  // namespace impala
