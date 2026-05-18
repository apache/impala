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

#include <memory>
#include <vector>

#include "common/status.h"
#include "runtime/types.h"

#include <arrow/record_batch.h>

namespace impala {

class MemPool;
class RuntimeState;
class SlotWriter;
class TupleDescriptor;

/// Static functions for converting data between Arrow and Impala.
class ArrowConverter {
public:
  /// Convert arrow::Array into an array of raw values.
  /// Null values will be converted to nullptr, other values will point into value_buffer.
  /// Caller has to ensure raw_values and value_buffer contain enough memory allocated.
  /// Var len string data will be copied into memory allocated in mem_pool.
  static Status ConvertArrowArrayToRawValues(const arrow::Array* array,
      void** raw_values, uint8_t* value_buffer, const ColumnType& type, MemPool* mem_pool,
      RuntimeState* state);
};

/// Converts arrow::RecordBatches into batches of Impala tuples of 'tuple_desc'.
/// Column i of the record batches is written to slot i of 'tuple_desc'. The slot
/// writers are created once in Init() and reused for every record batch.
class ArrowToTupleConverter {
public:
  explicit ArrowToTupleConverter(const TupleDescriptor* tuple_desc);
  ~ArrowToTupleConverter();

  /// Creates the slot writers. Returns an error if a slot type is not supported.
  Status Init(RuntimeState* state);

  /// Converts rows [start_row_idx, end_row_idx) of 'record_batch'. The tuple of
  /// 'start_row_idx' is written to the beginning of tuple_mem.
  /// Caller has to ensure that there is enough memory allocated for the tuples.
  /// Var len string data will be copied into memory allocated in mem_pool.
  Status Convert(const arrow::RecordBatch& record_batch, int start_row_idx,
      int end_row_idx, uint8_t* tuple_mem, MemPool* mem_pool);

  /// Same as above, but converts all rows of 'record_batch'.
  Status Convert(const arrow::RecordBatch& record_batch, uint8_t* tuple_mem,
      MemPool* mem_pool);

private:
  const TupleDescriptor* tuple_desc_;
  RuntimeState* state_ = nullptr;
  /// One writer per slot of 'tuple_desc_', in slot order.
  std::vector<std::unique_ptr<SlotWriter>> slot_writers_;
};

} // namespace impala