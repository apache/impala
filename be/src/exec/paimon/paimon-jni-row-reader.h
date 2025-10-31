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

#include "common/global-types.h"
#include "common/status.h"
#include "runtime/types.h"

#include <arrow/record_batch.h>

namespace impala {

class MemPool;
class RuntimeState;
class ScanNode;
class Status;
class SlotDescriptor;
class Tuple;
class TupleDescriptor;

/// Row reader for Paimon table scans, it translates a row of arrow RecordBatch to
/// Impala row batch tuples.
class PaimonJniRowReader {
 public:
  PaimonJniRowReader();

  /// Materialize the Arrow batch into Impala rows.
  Status MaterializeTuple(const arrow::RecordBatch& recordBatch, const int row_idx,
      const TupleDescriptor* tuple_desc, Tuple* tuple, MemPool* tuple_data_pool,
      RuntimeState* state);

 private:
  // Writes a row denoted by 'row_idx' from an arrow batch into the target tuple.
  Status WriteSlot(const arrow::Array* array, const int row_idx,
      const SlotDescriptor* slot_desc, Tuple* tuple, MemPool* tuple_data_pool,
      RuntimeState* state) WARN_UNUSED_RESULT;

  /// Template method that writes a value from 'arrow_array' to 'slot'.
  /// 'T' is the type of the slot,
  /// 'AT' is the proper subtype of the arrow::Array.
  template <typename T, typename AT>
  Status WriteSlot(const AT* arrow_array, int row_idx, void* slot) WARN_UNUSED_RESULT;

  template <typename T, typename AT>
  Status CastAndWriteSlot(
      const arrow::Array* arrow_array, const int row_idx, void* slot) WARN_UNUSED_RESULT;

  Status WriteDateSlot(
      const arrow::Array* array, const int row_idx, void* slot) WARN_UNUSED_RESULT;

  Status WriteDecimalSlot(const arrow::Array* array, const int row_idx,
      const ColumnType& type, void* slot) WARN_UNUSED_RESULT;

  /// Paimon TimeStamp is parsed into TimestampValue.
  Status WriteTimeStampSlot(const arrow::Array* array, const Timezone* timezone,
      const int row_idx, void* slot) WARN_UNUSED_RESULT;

  template <bool IS_BINARY>
  Status WriteStringOrBinarySlot(const arrow::Array* array, const int row_idx, void* slot,
      MemPool* tuple_data_pool) WARN_UNUSED_RESULT;

  template <bool IS_CHAR>
  Status WriteVarCharOrCharSlot(const arrow::Array* array, const int row_idx, int max_len,
      void* slot, MemPool* tuple_data_pool) WARN_UNUSED_RESULT;
};

} // namespace impala
