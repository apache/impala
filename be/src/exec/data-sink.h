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


#ifndef IMPALA_EXEC_DATA_SINK_H
#define IMPALA_EXEC_DATA_SINK_H

#include <boost/scoped_ptr.hpp>
#include <vector>

#include "common/status.h"
#include "gen-cpp/DataSinks_types.h"
#include "gen-cpp/Exprs_types.h"

namespace impala {

class ObjectPool;
class RowBatch;
class RuntimeProfile;
class RuntimeState;
class TPlanExecRequest;
class TPlanExecParams;
class TPlanFragmentExecParams;
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

  // Creates a new data sink from thrift_sink. A pointer to the
  // new sink is written to *sink, and is owned by the caller.
  static Status CreateDataSink(ObjectPool* pool,
    const TDataSink& thrift_sink, const std::vector<TExpr>& output_exprs,
    const TPlanFragmentExecParams& params,
    const RowDescriptor& row_desc, boost::scoped_ptr<DataSink>* sink);

  // Returns the runtime profile for the sink.  
  virtual RuntimeProfile* profile() = 0;
};

}  // namespace impala
#endif
