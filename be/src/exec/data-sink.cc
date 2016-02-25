// Copyright 2012 Cloudera Inc.
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

#include "exec/data-sink.h"

#include <string>
#include <map>

#include "common/logging.h"
#include "exec/hdfs-table-sink.h"
#include "exec/hbase-table-sink.h"
#include "exec/exec-node.h"
#include "exprs/expr.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "gen-cpp/ImpalaInternalService_constants.h"
#include "runtime/data-stream-sender.h"
#include "util/container-util.h"

#include "common/names.h"

namespace impala {

Status DataSink::CreateDataSink(ObjectPool* pool,
    const TDataSink& thrift_sink, const vector<TExpr>& output_exprs,
    const TPlanFragmentExecParams& params,
    const RowDescriptor& row_desc, scoped_ptr<DataSink>* sink) {
  DataSink* tmp_sink = NULL;
  switch (thrift_sink.type) {
    case TDataSinkType::DATA_STREAM_SINK:
      if (!thrift_sink.__isset.stream_sink) {
        return Status("Missing data stream sink.");
      }

      // TODO: figure out good buffer size based on size of output row
      tmp_sink = new DataStreamSender(pool,
          params.sender_id, row_desc, thrift_sink.stream_sink,
          params.destinations, 16 * 1024);
      sink->reset(tmp_sink);
      break;

    case TDataSinkType::TABLE_SINK:
      if (!thrift_sink.__isset.table_sink) return Status("Missing table sink.");
      switch (thrift_sink.table_sink.type) {
        case TTableSinkType::HDFS:
          tmp_sink = new HdfsTableSink(row_desc, output_exprs, thrift_sink);
          sink->reset(tmp_sink);
          break;
        case TTableSinkType::HBASE:
          tmp_sink = new HBaseTableSink(row_desc, output_exprs, thrift_sink);
          sink->reset(tmp_sink);
          break;
        default:
          stringstream error_msg;
          const char* str = "Unknown table sink";
          map<int, const char*>::const_iterator i =
              _TTableSinkType_VALUES_TO_NAMES.find(thrift_sink.table_sink.type);
          if (i != _TTableSinkType_VALUES_TO_NAMES.end()) {
            str = i->second;
          }
          error_msg << str << " not implemented.";
          return Status(error_msg.str());
      }

      break;
    default:
      stringstream error_msg;
      map<int, const char*>::const_iterator i =
          _TDataSinkType_VALUES_TO_NAMES.find(thrift_sink.type);
      const char* str = "Unknown data sink type ";
      if (i != _TDataSinkType_VALUES_TO_NAMES.end()) {
        str = i->second;
      }
      error_msg << str << " not implemented.";
      return Status(error_msg.str());
  }
  return Status::OK();
}

void DataSink::MergeInsertStats(const TInsertStats& src_stats,
    TInsertStats* dst_stats) {
  dst_stats->bytes_written += src_stats.bytes_written;
  if (src_stats.__isset.parquet_stats) {
    if (dst_stats->__isset.parquet_stats) {
      MergeMapValues<string, int64_t>(src_stats.parquet_stats.per_column_size,
          &dst_stats->parquet_stats.per_column_size);
    } else {
      dst_stats->__set_parquet_stats(src_stats.parquet_stats);
    }
  }
}

string DataSink::OutputInsertStats(const PartitionStatusMap& stats,
    const string& prefix) {
  const char* indent = "  ";
  stringstream ss;
  ss << prefix;
  bool first = true;
  BOOST_FOREACH(const PartitionStatusMap::value_type& val, stats) {
    if (!first) ss << endl;
    first = false;
    ss << "Partition: ";

    const string& partition_key = val.first;
    if (partition_key == g_ImpalaInternalService_constants.ROOT_PARTITION_KEY) {
      ss << "Default" << endl;
    } else {
      ss << partition_key << endl;
    }
    const TInsertStats& stats = val.second.stats;
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

Status DataSink::Prepare(RuntimeState* state) {
  expr_mem_tracker_.reset(
      new MemTracker(-1, -1, "Data sink expr", state->instance_mem_tracker(), false));
  return Status::OK();
}

void DataSink::Close(RuntimeState* state) {
  if (expr_mem_tracker_.get() != NULL) {
    expr_mem_tracker_->UnregisterFromParent();
    expr_mem_tracker_.reset();
  }
}

}  // namespace impala
