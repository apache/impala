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

#ifndef IMPALA_RUNTIME_KRPC_DATA_STREAM_RECVR_H
#define IMPALA_RUNTIME_KRPC_DATA_STREAM_RECVR_H

#include "data-stream-recvr-base.h"

#include "common/status.h"

namespace impala {

class RowBatch;
class TupleRowComparator;

/// TODO: Stub for the KRPC version of the DataStreamRecvr. Fill with actual
/// implementation.
class KrpcDataStreamRecvr : public DataStreamRecvrBase {
 public:
  [[noreturn]] KrpcDataStreamRecvr();
  virtual ~KrpcDataStreamRecvr() override;

  [[noreturn]] Status GetBatch(RowBatch** next_batch) override;
  [[noreturn]] void Close() override;
  [[noreturn]] Status CreateMerger(const TupleRowComparator& less_than) override;
  [[noreturn]] Status GetNext(RowBatch* output_batch, bool* eos) override;
  [[noreturn]] void TransferAllResources(RowBatch* transfer_batch) override;

};

} // namespace impala

#endif /* IMPALA_RUNTIME_KRPC_DATA_STREAM_RECVR_H */
