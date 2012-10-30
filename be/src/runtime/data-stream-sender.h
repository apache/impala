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


#ifndef IMPALA_RUNTIME_DATA_STREAM_SENDER_H
#define IMPALA_RUNTIME_DATA_STREAM_SENDER_H

#include <vector>
#include <string>

#include "exec/data-sink.h"
#include "common/object-pool.h"
#include "common/status.h"
#include "gen-cpp/Data_types.h"  // for TRowBatch

namespace impala {

class Expr;
class RowBatch;
class RowDescriptor;
class TDataStreamSink;
class THostPort;
class TPlanFragmentDestination;

// Single sender of an m:n data stream.
// Row batch data is routed to destinations based on the provided
// partitioning specification.
// *Not* thread-safe.
class DataStreamSender : public DataSink {
 public:
  // Construct a sender according to the output specification (sink),
  // sending to the given destinations.
  // Per_channel_buffer_size is the buffer size allocated to each channel
  // and is specified in bytes.
  // NOTE: This can only do broadcasting right now
  // (sink.output_partition.type == UNPARTITIONED)
  DataStreamSender(
    const RowDescriptor& row_desc, const TDataStreamSink& sink,
    const std::vector<TPlanFragmentDestination>& destinations,
    int per_channel_buffer_size);
  virtual ~DataStreamSender();

  // Setup. Call before Send() or Close().
  virtual Status Init(RuntimeState* state);

  // Send data in 'batch' to destination nodes according to partitioning
  // specification provided in c'tor.
  // Blocks until all rows in batch are placed in their appropriate outgoing
  // buffers (ie, blocks if there are still in-flight rpcs from the last
  // Send() call).
  // TODO: do we need reuse_batch?
  virtual Status Send(RuntimeState* state, RowBatch* batch);

  // Flush all buffered data and close all existing channels to destination
  // hosts. Further Send() calls are illegal after calling Close().
  virtual Status Close(RuntimeState* state);

  // Return total number of bytes sent in TRowBatch.data. If batches are
  // broadcast to multiple receivers, they are counted once per receiver.
  int64_t GetNumDataBytesSent() const;

 private:
  class Channel;

  bool broadcast_;  // if true, send all rows on all channels

  // serialized batches for broadcasting; we need two so we can write
  // one while the other one is still being sent
  TRowBatch thrift_batch1_;
  TRowBatch thrift_batch2_;
  TRowBatch* current_thrift_batch_;  // the next one to fill in Send()

  ObjectPool pool_;  // TODO: reuse RuntimeState's pool
  Expr* partition_expr_;  // computes per-row partitioning value
  std::vector<Channel*> channels_;
};

}

#endif
