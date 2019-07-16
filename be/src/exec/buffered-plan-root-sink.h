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

#pragma once

#include "exec/plan-root-sink.h"

namespace impala {

/// PlanRootSink that buffers RowBatches from the 'sender' (fragment) thread. RowBatches
/// are buffered in memory until a memory limit is hit. Any subsequent calls to Send will
/// block until the 'consumer' (coordinator) thread has read enough RowBatches to free up
/// sufficient memory.
class BufferedPlanRootSink : public PlanRootSink {
 public:
  BufferedPlanRootSink(
      TDataSinkId sink_id, const RowDescriptor* row_desc, RuntimeState* state);

  virtual Status Send(RuntimeState* state, RowBatch* batch) override;

  virtual Status FlushFinal(RuntimeState* state) override;

  virtual void Close(RuntimeState* state) override;

  virtual Status GetNext(
      RuntimeState* state, QueryResultSet* result_set, int num_rows, bool* eos) override;

  virtual void Cancel(RuntimeState* state) override;
};
}
