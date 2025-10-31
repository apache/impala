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

#include "exec/paimon/paimon-jni-row-reader.h"

#include <jni.h>
#include <cstdint>
#include <memory>
#include <string_view>
#include <type_traits>
#include <arrow/array/array_base.h>
#include <arrow/array/array_binary.h>
#include <arrow/type_fwd.h>
#include <glog/logging.h>

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include "common/global-types.h"
#include "exec/exec-node.inline.h"
#include "exec/parquet/parquet-common.h"
#include "exec/read-write-util.h"
#include "gutil/walltime.h"
#include "runtime/collection-value-builder.h"
#include "runtime/decimal-value.h"
#include "runtime/runtime-state.h"
#include "runtime/timestamp-value.h"
#include "runtime/timestamp-value.inline.h"
#include "runtime/tuple-row.h"
#include "util/jni-util.h"

namespace impala {

PaimonJniRowReader::PaimonJniRowReader() {}

Status PaimonJniRowReader::MaterializeTuple(const arrow::RecordBatch& recordBatch,
    const int row_index, const TupleDescriptor* tuple_desc, Tuple* tuple,
    MemPool* tuple_data_pool, RuntimeState* state) {
  DCHECK(tuple != nullptr);
  DCHECK(tuple_data_pool != nullptr);
  DCHECK(tuple_desc != nullptr);
  int col = 0;
  DCHECK(recordBatch.num_columns() == tuple_desc->slots().size());
  for (const SlotDescriptor* slot_desc : tuple_desc->slots()) {
    std::shared_ptr<arrow::Array> arr = recordBatch.column(col);
    DCHECK(arr != nullptr);
    RETURN_IF_ERROR(
        WriteSlot(arr.get(), row_index, slot_desc, tuple, tuple_data_pool, state));
    col++;
  }
  return Status::OK();
}

template <typename T, typename AT>
Status PaimonJniRowReader::WriteSlot(const AT* arrow_array, int row_idx, void* slot) {
  T value = arrow_array->Value(row_idx);
  *reinterpret_cast<T*>(slot) = value;
  return Status::OK();
}

template <typename T, typename AT>
Status PaimonJniRowReader::CastAndWriteSlot(
    const arrow::Array* arrow_array, const int row_idx, void* slot) {
  auto derived_array = static_cast<const AT*>(arrow_array);
  return WriteSlot<T, AT>(derived_array, row_idx, slot);
}

Status PaimonJniRowReader::WriteSlot(const arrow::Array* array, int row_index,
    const SlotDescriptor* slot_desc, Tuple* tuple, MemPool* tuple_data_pool,
    RuntimeState* state) {
  if (array->IsNull(row_index)) {
    tuple->SetNull(slot_desc->null_indicator_offset());
    return Status::OK();
  }
  void* slot = tuple->GetSlot(slot_desc->tuple_offset());
  const ColumnType& type = slot_desc->type();
  switch (type.type) {
    case TYPE_CHAR: {
      RETURN_IF_ERROR(WriteVarCharOrCharSlot</* IS_CHAR */ true>(
          array, row_index, slot_desc->type().len, slot, tuple_data_pool));
      break;
    }
    case TYPE_STRING:
    case TYPE_VARCHAR: {
      if (type.IsBinaryType()) { // byte[]
        RETURN_IF_ERROR(WriteStringOrBinarySlot</* IS_BINARY */ true>(
            array, row_index, slot, tuple_data_pool));
      } else {
        RETURN_IF_ERROR(WriteStringOrBinarySlot</* IS_BINARY */ false>(
            array, row_index, slot, tuple_data_pool));
      }
      break;
    }
    case TYPE_BOOLEAN: {
      RETURN_IF_ERROR(
          (CastAndWriteSlot<bool, arrow::BooleanArray>(array, row_index, slot)));
      break;
    }
    case TYPE_DATE: {
      RETURN_IF_ERROR(WriteDateSlot(array, row_index, slot));
      break;
    }
    case TYPE_TINYINT: {
      RETURN_IF_ERROR(
          (CastAndWriteSlot<int8_t, arrow::Int8Array>(array, row_index, slot)));
      break;
    }
    case TYPE_SMALLINT: {
      RETURN_IF_ERROR(
          (CastAndWriteSlot<int16_t, arrow::Int16Array>(array, row_index, slot)));
      break;
    }
    case TYPE_INT: {
      RETURN_IF_ERROR(
          (CastAndWriteSlot<int32_t, arrow::Int32Array>(array, row_index, slot)));
      break;
    }
    case TYPE_BIGINT: {
      RETURN_IF_ERROR(
          (CastAndWriteSlot<int64_t, arrow::Int64Array>(array, row_index, slot)));
      break;
    }
    case TYPE_FLOAT: {
      RETURN_IF_ERROR(
          (CastAndWriteSlot<float, arrow::FloatArray>(array, row_index, slot)));
      break;
    }
    case TYPE_DOUBLE: {
      RETURN_IF_ERROR(
          (CastAndWriteSlot<double, arrow::DoubleArray>(array, row_index, slot)));
      break;
    }
    case TYPE_DECIMAL: {
      RETURN_IF_ERROR(WriteDecimalSlot(array, row_index, type, slot));
      break;
    }
    case TYPE_TIMESTAMP: {
      RETURN_IF_ERROR(
          WriteTimeStampSlot(array, state->local_time_zone(), row_index, slot));
      break;
    }
    case TYPE_STRUCT: {
      // TODO: implement struct type support later
      tuple->SetNull(slot_desc->null_indicator_offset());
      break;
    }
    case TYPE_ARRAY: {
      // TODO: implement array type support later
      tuple->SetNull(slot_desc->null_indicator_offset());
      break;
    }
    case TYPE_MAP: {
      // TODO: implement map type support later
      tuple->SetNull(slot_desc->null_indicator_offset());
      break;
    }
    default:
      DCHECK(false) << "Unsupported column type: " << slot_desc->type().type;
      tuple->SetNull(slot_desc->null_indicator_offset());
  }
  return Status::OK();
}

Status PaimonJniRowReader::WriteDateSlot(
    const arrow::Array* array, const int row_idx, void* slot) {
  const arrow::Date32Array* date_array = static_cast<const arrow::Date32Array*>(array);
  int32_t days_since_epoch = date_array->Value(row_idx);

  // This will set the value to DateValue::INVALID_DAYS_SINCE_EPOCH if it is out of
  // range.
  DateValue result(days_since_epoch);
  *reinterpret_cast<int32_t*>(slot) = result.Value();
  return Status::OK();
}

// Sets the decimal value in the slot. Inline method to avoid nested switch statements.
static const std::string ERROR_INVALID_DECIMAL = "Invalid Decimal Format";
inline Status SetDecimalVal(
    const ColumnType& type, const uint8_t* buffer, int len, void* slot) {
  switch (type.GetByteSize()) {
    case 4: {
      Decimal4Value* val = reinterpret_cast<Decimal4Value*>(slot);
      if (UNLIKELY(
              (ParquetPlainEncoder::Decode<Decimal4Value,
                  parquet::Type::FIXED_LEN_BYTE_ARRAY>(buffer, buffer + len, len, val))
              < 0)) {
        return Status(ERROR_INVALID_DECIMAL);
      }
      break;
    }
    case 8: {
      Decimal8Value* val = reinterpret_cast<Decimal8Value*>(slot);
      if (UNLIKELY(
              (ParquetPlainEncoder::Decode<Decimal8Value,
                  parquet::Type::FIXED_LEN_BYTE_ARRAY>(buffer, buffer + len, len, val))
              < 0)) {
        return Status(ERROR_INVALID_DECIMAL);
      }
      break;
    }
    case 16: {
      Decimal16Value* val = reinterpret_cast<Decimal16Value*>(slot);
      if (UNLIKELY(
              (ParquetPlainEncoder::Decode<Decimal16Value,
                  parquet::Type::FIXED_LEN_BYTE_ARRAY>(buffer, buffer + len, len, val))
              < 0)) {
        return Status(ERROR_INVALID_DECIMAL);
      }
      break;
    }
    default:
      DCHECK(false);
  }
  return Status::OK();
}

Status PaimonJniRowReader::WriteDecimalSlot(
    const arrow::Array* array, const int row_idx, const ColumnType& type, void* slot) {
  const arrow::BinaryArray* binary_array = static_cast<const arrow::BinaryArray*>(array);
  int byte_length = 0;
  const uint8_t* data = binary_array->GetValue(row_idx, &byte_length);
  DCHECK(byte_length > 0 && byte_length <= 16);
  return SetDecimalVal(type, data, byte_length, slot);
}

Status PaimonJniRowReader::WriteTimeStampSlot(
    const arrow::Array* array, const Timezone* timezone, const int row_idx, void* slot) {
  const arrow::TimestampArray* date_array = (const arrow::TimestampArray*)array;
  int64_t value = date_array->Value(row_idx);
  const auto& type = static_cast<const arrow::TimestampType&>(*date_array->type());
  const std::string& tz_name = type.timezone();
  const Timezone* tz = tz_name.empty() ? UTCPTR : timezone;
  switch (type.unit()) {
    case ::arrow::TimeUnit::NANO:
      *reinterpret_cast<TimestampValue*>(slot) = TimestampValue::FromUnixTimeNanos(
          value / NANOS_PER_SEC, value % NANOS_PER_SEC, tz);
      break;
    case ::arrow::TimeUnit::MICRO:
      *reinterpret_cast<TimestampValue*>(slot) =
          TimestampValue::FromUnixTimeMicros(value, tz);
      break;
    case ::arrow::TimeUnit::MILLI:
      *reinterpret_cast<TimestampValue*>(slot) =
          TimestampValue::FromUnixTimeMicros(value * 1000L, tz);
      break;
    case ::arrow::TimeUnit::SECOND:
      *reinterpret_cast<TimestampValue*>(slot) = TimestampValue::FromUnixTime(value, tz);
      break;
  }

  return Status::OK();
}

template <bool IS_CHAR>
Status PaimonJniRowReader::WriteVarCharOrCharSlot(const arrow::Array* array,
    const int row_idx, int dst_len, void* slot, MemPool* tuple_data_pool) {
  const arrow::StringArray* nchar_array = static_cast<const arrow::StringArray*>(array);
  std::string_view v = nchar_array->Value(row_idx);

  int src_len = v.size();
  int unpadded_len = std::min<int>(dst_len, src_len);
  // Allocate memory and copy the bytes from the JVM to the RowBatch.
  char* dst_char = reinterpret_cast<char*>(slot);
  memcpy(dst_char, v.data(), unpadded_len);
  StringValue::PadWithSpaces(dst_char, dst_len, unpadded_len);
  return Status::OK();
}

/// Obtain bytes from arrow string/binary batch first, Then the data has to be copied
/// to the tuple_data_pool, because the Fe PaimonJniScanner releases the JVM offheap
/// memory later.
template <bool IS_BINARY>
Status PaimonJniRowReader::WriteStringOrBinarySlot(
    const arrow::Array* array, const int row_idx, void* slot, MemPool* tuple_data_pool) {
  std::string_view v;
  uint32_t jbuffer_size = 0;
  if constexpr (IS_BINARY) {
    const arrow::BinaryArray* binary_array =
        static_cast<const arrow::BinaryArray*>(array);
    v = binary_array->Value(row_idx);
  } else {
    const arrow::StringArray* string_array =
        static_cast<const arrow::StringArray*>(array);
    v = string_array->Value(row_idx);
  }

  jbuffer_size = v.size();
  // Allocate memory and copy the bytes from the JVM to the RowBatch.
  char* buffer =
      reinterpret_cast<char*>(tuple_data_pool->TryAllocateUnaligned(jbuffer_size));
  if (UNLIKELY(buffer == nullptr)) {
    string details = strings::Substitute("Failed to allocate $0 bytes for $1.",
        jbuffer_size, IS_BINARY ? "binary" : "string");
    return tuple_data_pool->mem_tracker()->MemLimitExceeded(
        nullptr, details, jbuffer_size);
  }

  memcpy(buffer, v.data(), jbuffer_size);
  reinterpret_cast<StringValue*>(slot)->Assign(buffer, jbuffer_size);
  return Status::OK();
}
} // namespace impala
