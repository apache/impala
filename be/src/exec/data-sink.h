// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_DATA_SINK_H
#define IMPALA_EXEC_DATA_SINK_H

#include "common/status.h"

#include <boost/scoped_ptr.hpp>
#include "gen-cpp/DataSinks_types.h"

namespace impala {

class RowBatch;
class RuntimeState;
class TPlanExecRequest;
class TPlanExecParams;
class RowDescriptor;

// Superclass of all data sinks.
class DataSink {
 public:
  virtual ~DataSink() {}

  // Setup. Call before Send() or Close().
  virtual Status Init(RuntimeState* state) = 0;

  // Send a row batch into this sink.
  virtual Status Send(RuntimeState* state, RowBatch* batch) = 0;

  // Releases all resources that were allocated in Init()/Send().
  // Further Send() calls are illegal after calling Close().
  virtual Status Close(RuntimeState* state) = 0;

  // Creates a new data sink from the specification of request.dataSink. A pointer to the
  // new sink is written to *sink, and is owned by the caller.
  static Status CreateDataSink(
    const TPlanExecRequest& request, const TPlanExecParams& params,
    const RowDescriptor& row_desc, boost::scoped_ptr<DataSink>* sink);
};

}
#endif
