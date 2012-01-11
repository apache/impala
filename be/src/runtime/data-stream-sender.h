// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_RUNTIME_DATA_STREAM_SENDER_H
#define IMPALA_RUNTIME_DATA_STREAM_SENDER_H

#include <vector>
#include <string>

#include "common/object-pool.h"
#include "common/status.h"
#include "gen-cpp/Data_types.h"  // for TRowBatch

namespace impala {

class Expr;
class RowBatch;
class RowDescriptor;
class TDataStreamSink;
class THostPort;

// Single sender of an m:n data stream.
// Row batch data is routed to destinations based on the provided
// partitioning specification.
// *Not* thread-safe.
class DataStreamSender {
 public:
  // Construct a sender according to the output specification (sink),
  // sending to the given hosts. 
  // Per_channel_buffer_size is the buffer size allocated to each channel
  // and is specified in bytes.
  DataStreamSender(
    const RowDescriptor& row_desc, const TUniqueId& query_id,
    const TDataStreamSink& sink, const std::vector<THostPort>& destinations,
    int per_channel_buffer_size);
  ~DataStreamSender();

  // Setup. Call before Send() or Close().
  Status Init();

  // Send data in 'batch' to destination nodes according to partitioning
  // specification provided in c'tor.
  // Blocks until all rows in batch are placed in their appropriate outgoing
  // buffers (ie, blocks if there are still in-flight rpcs from the last
  // Send() call).
  // TODO: do we need reuse_batch?
  Status Send(RowBatch* batch);

  // Flush all buffered data and close all existing channels to destination
  // hosts. Further Send() calls are illegal after calling Close().
  Status Close();

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
