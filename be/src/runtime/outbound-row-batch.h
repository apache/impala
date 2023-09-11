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

#include <cstring>
#include <vector>

#include "gen-cpp/row_batch.pb.h"
#include "kudu/util/slice.h"
#include "runtime/mem-tracker.h"

namespace impala {

template <typename K, typename V> class FixedSizeHashTable;
class MemTracker;
class RowBatchSerializeTest;
class RuntimeState;

/// A KRPC outbound row batch which contains the serialized row batch header and buffers
/// for holding the tuple offsets and tuple data.
class OutboundRowBatch {
 public:

  OutboundRowBatch(std::shared_ptr<CharMemTrackerAllocator> allocator)
    : tuple_data_(*allocator.get()) {}

  const RowBatchHeaderPB* header() const { return &header_; }

  /// Returns the serialized tuple offsets' vector as a kudu::Slice.
  /// The tuple offsets vector is sent as KRPC sidecar.
  kudu::Slice TupleOffsetsAsSlice() const {
    return kudu::Slice(
        const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(tuple_offsets_.data())),
        tuple_offsets_.size() * sizeof(tuple_offsets_[0]));
  }

  /// Returns the serialized tuple data's buffer as a kudu::Slice.
  /// The tuple data is sent as KRPC sidecar.
  kudu::Slice TupleDataAsSlice() const {
    return kudu::Slice(
        const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(tuple_data_.data())),
        tuple_data_.length());
  }

  /// Returns true if the header has been initialized and ready to be sent.
  /// This entails setting some fields initialized in RowBatch::Serialize().
  bool IsInitialized() const {
     return header_.has_num_rows() && header_.has_uncompressed_size() &&
         header_.has_compression_type();
  }

 private:
  friend class RowBatch;
  friend class RowBatchSerializeBaseline;

  /// The serialized header which contains the meta-data of the row batch such as the
  /// number of rows and compression scheme used etc.
  RowBatchHeaderPB header_;

  /// Contains offsets into 'tuple_data_' of all tuples in a row batch. -1 refers to
  /// a NULL tuple.
  vector<int32_t> tuple_offsets_;

  /// Contains the actual data of all the tuples. The data could be compressed.
  TrackedString tuple_data_;
};

}
