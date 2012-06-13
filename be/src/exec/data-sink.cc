// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

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

Status DataSink::CreateDataSink(
    const TPlanExecRequest& request, const TPlanExecParams& params,
    const RowDescriptor& row_desc, scoped_ptr<DataSink>* sink) {
  DataSink* tmp_sink = NULL;
  switch (request.data_sink.dataSinkType) {
    case TDataSinkType::DATA_STREAM_SINK:
      if (!request.data_sink.__isset.dataStreamSink) {
        return Status("Missing data stream sink.");
      }
      // TODO: figure out good buffer size based on size of output row
      tmp_sink = new DataStreamSender(row_desc, params.dest_fragment_id,
                                      request.data_sink.dataStreamSink,
                                      params.destinations, 16 * 1024);
      sink->reset(tmp_sink);
      break;

    case TDataSinkType::TABLE_SINK:
      if (!request.data_sink.__isset.tableSink) {
        return Status("Missing table sink.");
      }
      tmp_sink = new HdfsTableSink(row_desc,
          request.fragment_id, request.output_exprs, request.data_sink);
      sink->reset(tmp_sink);
      break;

    default:
      std::stringstream error_msg;
      map<int, const char*>::const_iterator i =
          _TDataSinkType_VALUES_TO_NAMES.find(request.data_sink.dataSinkType);
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
