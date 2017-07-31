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

#ifndef IMPALA_RUNTIME_DATA_STREAM_RECVR_BASE_H
#define IMPALA_RUNTIME_DATA_STREAM_RECVR_BASE_H

#include "common/status.h"

namespace impala {

class RowBatch;
class TupleRowComparator;

/// Interface for a single receiver of a m:n data stream.
/// DataStreamRecvrBase implementations should maintain one or more queues of row batches
/// received by a DataStreamMgrBase implementation from one or more sender fragment
/// instances.
/// TODO: This is a temporary pure virtual base class that defines the basic interface for
/// 2 parallel implementations of the DataStreamRecvrBase, one each for Thrift and KRPC.
/// Remove this in favor of the KRPC implementation when possible.
class DataStreamRecvrBase {
 public:
  DataStreamRecvrBase() { }
  virtual ~DataStreamRecvrBase() { }

  /// Returns next row batch in data stream.
  virtual Status GetBatch(RowBatch** next_batch) = 0;

  virtual void Close() = 0;

  /// Create a SortedRunMerger instance to merge rows from multiple senders according to
  /// the specified row comparator.
  virtual Status CreateMerger(const TupleRowComparator& less_than) = 0;

  /// Fill output_batch with the next batch of rows.
  virtual Status GetNext(RowBatch* output_batch, bool* eos) = 0;

  /// Transfer all resources from the current batches being processed from each sender
  /// queue to the specified batch.
  virtual void TransferAllResources(RowBatch* transfer_batch) = 0;

};

}

#endif /* IMPALA_RUNTIME_DATA_STREAM_RECVR_BASE_H */
