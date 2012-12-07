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
#include "exec/hdfs-table-sink.h"
#include "exec/exec-node.h"
#include "exprs/expr.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "runtime/data-stream-sender.h"

#include <string>

using namespace std;
using namespace boost;

namespace impala {

Status DataSink::CreateDataSink(ObjectPool* pool,
    const TDataSink& thrift_sink, const vector<TExpr>& output_exprs,
    const TPlanFragmentExecParams& params,
    const RowDescriptor& row_desc, scoped_ptr<DataSink>* sink) {
  DataSink* tmp_sink = NULL;
  switch (thrift_sink.type) {
    case TDataSinkType::DATA_STREAM_SINK:
      if (!thrift_sink.__isset.stream_sink) return Status("Missing data stream sink.");
      // TODO: figure out good buffer size based on size of output row
      tmp_sink = new DataStreamSender(pool,
          row_desc, thrift_sink.stream_sink, params.destinations, 16 * 1024);
      sink->reset(tmp_sink);
      break;

    case TDataSinkType::TABLE_SINK:
      if (!thrift_sink.__isset.table_sink) return Status("Missing table sink.");
      tmp_sink = new HdfsTableSink(
          row_desc, params.fragment_instance_id, output_exprs, thrift_sink);
      sink->reset(tmp_sink);
      break;

    default:
      std::stringstream error_msg;
      map<int, const char*>::const_iterator i =
          _TDataSinkType_VALUES_TO_NAMES.find(thrift_sink.type);
      const char* str = "Unknown data sink type ";
      if (i != _TDataSinkType_VALUES_TO_NAMES.end()) {
        str = i->second;
      }
      error_msg << str << " not implemented.";
      return Status(error_msg.str());
  }
  return Status::OK;
}

}
