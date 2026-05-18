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

#include "exec/arrow-converter.h"

#include <arrow/api.h>
#include <arrow/array/array_base.h>
#include <arrow/array/array_binary.h>
#include <arrow/c/bridge.h>
#include <arrow/type_fwd.h>
#include <arrow/util/bitmap_ops.h>

#include "exec/parquet/parquet-common.h"
#include "gutil/strings/substitute.h"
#include "runtime/date-value.h"
#include "runtime/descriptors.h"
#include "runtime/mem-tracker.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/string-value.inline.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"
#include "util/bit-util.h"
#include "util/error-util.h"

using strings::Substitute;

namespace impala {
static const std::string ERROR_ARROW_IMPALA_TYPE_MISMATCH =
    "Mismatching types between Arrow and Impala.";
static const std::string ERROR_INVALID_DECIMAL = "Invalid Decimal Format";

// Class that converts arrow arrays of a single column into the slots of a batch of
// tuples, or into a list of raw value pointers. A writer can be reused for multiple
// arrays, see SetArray().
// It is assumed that memory for the tuples are contiguously allocated at tuple_mem
// with tuple_size offset.
class SlotWriter {
public:
  virtual ~SlotWriter() = default;

  // Must be called before writing the values of 'array'. Returns an error if 'array'
  // does not match the Impala type.
  virtual Status SetArray(const arrow::Array* array) = 0;

  // Write values into the slots of a batch of tuples.
  virtual Status WriteSlotBatch(uint8_t* tuple_mem, int tuple_size, int start_row_idx,
      int end_row_idx, MemPool* mem_pool, RuntimeState* state) = 0;

  // Write values into an array of raw value pointers. The value of 'start_row_idx' is
  // written to the beginning of 'raw_values' and 'value_buffer'.
  virtual Status WriteRawValueBatch(void** raw_values, uint8_t* value_buffer,
      int byte_size, int start_row_idx, int end_row_idx, MemPool* mem_pool,
      RuntimeState* state) = 0;
};

template<typename FinalType>
class SlotWriterImpl : public SlotWriter {
public:
  explicit SlotWriterImpl(const SlotDescriptor* slot_desc) : slot_desc_(slot_desc) {}

  Status SetArray(const arrow::Array* array) override {
    arrow_array_ = array;
    return static_cast<FinalType*>(this)->InitArray();
  }

  template<bool NULLABLE_SLOT>
  Status WriteSlotBatchImpl(uint8_t* tuple_mem, int tuple_size, int start_row_idx,
      int end_row_idx, MemPool* mem_pool, RuntimeState* state) {
    NullIndicatorOffset null_indicator_offset = slot_desc_->null_indicator_offset();
    int tuple_offset = slot_desc_->tuple_offset();

    for (int row_idx = start_row_idx; row_idx < end_row_idx; ++row_idx) {
      Tuple* tuple = reinterpret_cast<Tuple*>(
          tuple_mem + (row_idx - start_row_idx) * tuple_size);
      if constexpr (NULLABLE_SLOT) {
        // Set null flag if value is null.
        // Note that if value is not null, flag doesn't have to be cleared, since all
        // tuple slot null flags are cleared at the start of the conversion.
        if (arrow_array_->IsNull(row_idx)) {
          tuple->SetNull(null_indicator_offset);
          continue;
        }
      } else {
        DCHECK(!arrow_array_->IsNull(row_idx));
      }

      void* slot = tuple->GetSlot(tuple_offset);
      bool is_valid = true;
      RETURN_IF_ERROR(static_cast<FinalType*>(this)->WriteSlotInternal(
          row_idx, slot, mem_pool, &is_valid));
      if (UNLIKELY(!is_valid)) {
        RETURN_IF_ERROR(HandleInvalidValue(row_idx, state));
        tuple->SetNull(null_indicator_offset);
      }
    }
    return Status::OK();
  }

  virtual Status WriteSlotBatch(uint8_t* tuple_mem, int tuple_size, int start_row_idx,
      int end_row_idx, MemPool* mem_pool, RuntimeState* state) override {
    DCHECK(slot_desc_ != nullptr);
    DCHECK(arrow_array_ != nullptr);
    if (NullCountInRange(start_row_idx, end_row_idx) == 0) {
      return WriteSlotBatchImpl<false>(
          tuple_mem, tuple_size, start_row_idx, end_row_idx, mem_pool, state);
    }
    if (UNLIKELY(!slot_desc_->is_nullable())) {
      return Status("Arrow array contains null values for a non-nullable slot.");
    }
    return WriteSlotBatchImpl<true>(
        tuple_mem, tuple_size, start_row_idx, end_row_idx, mem_pool, state);
  }

  virtual Status WriteRawValueBatch(void** raw_values, uint8_t* value_buffer,
      int byte_size, int start_row_idx, int end_row_idx, MemPool* mem_pool,
      RuntimeState* state) override {
    DCHECK(arrow_array_ != nullptr);
    for (int row_idx = start_row_idx; row_idx < end_row_idx; ++row_idx) {
      int out_idx = row_idx - start_row_idx;
      if (arrow_array_->IsNull(row_idx)) {
        raw_values[out_idx] = nullptr;
        continue;
      }

      void* slot = value_buffer + out_idx * byte_size;
      raw_values[out_idx] = slot;
      bool is_valid = true;
      RETURN_IF_ERROR(static_cast<FinalType*>(this)->WriteSlotInternal(
          row_idx, slot, mem_pool, &is_valid));
      if (UNLIKELY(!is_valid)) {
        RETURN_IF_ERROR(HandleInvalidValue(row_idx, state));
        raw_values[out_idx] = nullptr;
      }
    }

    return Status::OK();
  }

  // Overridden by writers that can encounter values Impala cannot represent.
  std::string InvalidValueMessage(int row_idx) const {
    DCHECK(false);
    return Substitute("Arrow array contains an invalid value in row $0.", row_idx);
  }

protected:
  // Casts 'arrow_array_' to the array type expected by the writer.
  template<typename ArrowArrayType>
  Status CastArray(const ArrowArrayType** result) const {
    *result = dynamic_cast<const ArrowArrayType*>(arrow_array_);
    if (UNLIKELY(*result == nullptr)) return Status(ERROR_ARROW_IMPALA_TYPE_MISMATCH);
    return Status::OK();
  }

private:
  int64_t NullCountInRange(int start_row_idx, int end_row_idx) const {
    if (arrow_array_->null_count() == 0) return 0;
    const uint8_t* null_bitmap = arrow_array_->null_bitmap_data();
    // Arrays without a validity bitmap but with nulls (NullArray) are all null.
    if (null_bitmap == nullptr) return arrow_array_->null_count();
    int64_t num_rows = end_row_idx - start_row_idx;
    return num_rows - arrow::internal::CountSetBits(
        null_bitmap, arrow_array_->offset() + start_row_idx, num_rows);
  }

  // Called for values that are valid in Arrow but out of Impala's range, e.g. timestamps
  // before 1400. Like the Parquet scanner, these are converted to NULL with a warning,
  // or fail the query if abort_on_error is set. Unlike the Parquet scanner, which keeps
  // the invalid value in non-nullable slots, this returns an error for them.
  Status HandleInvalidValue(int row_idx, RuntimeState* state) {
    std::string msg = static_cast<FinalType*>(this)->InvalidValueMessage(row_idx);
    if (slot_desc_ != nullptr && !slot_desc_->is_nullable()) {
      return Status(Substitute("$0 It cannot be converted to NULL, because the slot is "
          "not nullable.", msg));
    }
    return state->LogOrReturnError(ErrorMsg(TErrorCode::GENERAL, msg));
  }

  const arrow::Array* arrow_array_ = nullptr;
  // Only used when converting to tuples, nullptr when converting to raw values.
  const SlotDescriptor* slot_desc_;
};

template<class Type, class ArrowArrayType>
class BaseSlotWriter : public SlotWriterImpl<BaseSlotWriter<Type, ArrowArrayType>> {
public:
  explicit BaseSlotWriter(const SlotDescriptor* slot_desc) :
      SlotWriterImpl<BaseSlotWriter<Type, ArrowArrayType>>(slot_desc) {
  }

  Status InitArray() { return this->CastArray(&derived_array_); }

  Status WriteSlotInternal(int row_idx, void* slot, MemPool* mem_pool, bool* is_valid) {
    Type value = derived_array_->Value(row_idx);
    *reinterpret_cast<Type*>(slot) = value;
    return Status::OK();
  }
private:
  const ArrowArrayType* derived_array_ = nullptr;
};

class DateSlotWriter : public SlotWriterImpl<DateSlotWriter> {
public:
  explicit DateSlotWriter(const SlotDescriptor* slot_desc) :
      SlotWriterImpl<DateSlotWriter>(slot_desc) {
  }

  Status InitArray() { return CastArray(&date_array_); }

  Status WriteSlotInternal(int row_idx, void* slot, MemPool* mem_pool, bool* is_valid) {
    DateValue result(date_array_->Value(row_idx));
    if (UNLIKELY(!result.IsValid())) {
      *is_valid = false;
      return Status::OK();
    }
    *reinterpret_cast<DateValue*>(slot) = result;
    return Status::OK();
  }

  std::string InvalidValueMessage(int row_idx) const {
    return Substitute("Arrow array contains an out of range date in row $0. The valid "
        "date range is 0001-01-01..9999-12-31.", row_idx);
  }

private:
  const arrow::Date32Array* date_array_ = nullptr;
};

template<typename Type>
class DecimalSlotWriter : public SlotWriterImpl<DecimalSlotWriter<Type>> {
public:
  explicit DecimalSlotWriter(const SlotDescriptor* slot_desc) :
      SlotWriterImpl<DecimalSlotWriter<Type>>(slot_desc) {
  }

  Status InitArray() {
    RETURN_IF_ERROR(this->CastArray(&binary_array_));
    // StringArray derives from BinaryArray, so the cast alone does not reject it.
    if (UNLIKELY(binary_array_->type_id() != arrow::Type::BINARY)) {
      return Status(ERROR_ARROW_IMPALA_TYPE_MISMATCH);
    }
    return Status::OK();
  }

  Status WriteSlotInternal(int row_idx, void* slot, MemPool* mem_pool, bool* is_valid) {
    int byte_length = 0;
    const uint8_t* data = binary_array_->GetValue(row_idx, &byte_length);
    if (UNLIKELY(byte_length <= 0
        || byte_length > static_cast<int>(sizeof(typename Type::StorageType)))) {
      return Status(ERROR_INVALID_DECIMAL);
    }
    SetDecimalVal(data, byte_length, slot);
    return Status::OK();
  }

  void SetDecimalVal(const uint8_t* buffer, int len, void* slot) {
    Type* val = reinterpret_cast<Type*>(slot);
    int decoded_len = ParquetPlainEncoder::Decode<Type,
        parquet::Type::FIXED_LEN_BYTE_ARRAY>(buffer, buffer + len, len, val);
    DCHECK_EQ(decoded_len, len);
  }

private:
  const arrow::BinaryArray* binary_array_ = nullptr;
};

class TimestampSlotWriter : public SlotWriterImpl<TimestampSlotWriter> {
public:
  TimestampSlotWriter(const SlotDescriptor* slot_desc, const Timezone* local_timezone) :
      SlotWriterImpl<TimestampSlotWriter>(slot_desc),
      local_timezone_(local_timezone) {
  }

  // The time unit and time zone are read from each array, as they may differ.
  Status InitArray() {
    RETURN_IF_ERROR(CastArray(&timestamp_array_));
    const auto& type =
        static_cast<const arrow::TimestampType&>(*timestamp_array_->type());
    time_unit_ = type.unit();
    timezone_ = type.timezone().empty() ? UTCPTR : local_timezone_;
    return Status::OK();
  }

  Status WriteSlotInternal(int row_idx, void* slot, MemPool* mem_pool, bool* is_valid) {
    int64_t value = timestamp_array_->Value(row_idx);
    TimestampValue timestamp_value;
    switch (time_unit_) {
      case ::arrow::TimeUnit::NANO:
        timestamp_value = TimestampValue::FromUnixTimeNanos(value / NANOS_PER_SEC,
            value % NANOS_PER_SEC, timezone_);
        break;
      case ::arrow::TimeUnit::MICRO:
        timestamp_value = TimestampValue::FromUnixTimeMicros(value, timezone_);
        break;
      case ::arrow::TimeUnit::MILLI:
        timestamp_value = TimestampValue::FromUnixTimeNanos(value / MILLIS_PER_SEC,
            (value % MILLIS_PER_SEC) * (NANOS_PER_SEC / MILLIS_PER_SEC), timezone_);
        break;
      case ::arrow::TimeUnit::SECOND:
        timestamp_value = TimestampValue::FromUnixTime(value, timezone_);
        break;
      default:
        DCHECK(false) << "Invalid arrow time unit: " << (int)time_unit_;
    }

    if (UNLIKELY(!TimestampValue::IsValidDate(timestamp_value.date())
        || !TimestampValue::IsValidTime(timestamp_value.time()))) {
      *is_valid = false;
      return Status::OK();
    }
    *reinterpret_cast<TimestampValue*>(slot) = timestamp_value;

    return Status::OK();
  }

  std::string InvalidValueMessage(int row_idx) const {
    return Substitute("Arrow array contains an out of range timestamp in row $0. The "
        "valid date range is 1400-01-01..9999-12-31.", row_idx);
  }

private:
  const Timezone* local_timezone_;
  const arrow::TimestampArray* timestamp_array_ = nullptr;
  // Arrow timestamps without a time zone are not converted to local time.
  const Timezone* timezone_ = nullptr;
  arrow::TimeUnit::type time_unit_ = arrow::TimeUnit::SECOND;
};

template<typename ArrowArrayType>
class StringSlotWriter :
    public SlotWriterImpl<StringSlotWriter<ArrowArrayType>> {
public:
  // 'max_len' is the VARCHAR length, or -1 if values are not truncated.
  StringSlotWriter(const SlotDescriptor* slot_desc, int max_len, RuntimeState* state) :
      SlotWriterImpl<StringSlotWriter<ArrowArrayType>>(slot_desc),
      max_len_(max_len),
      state_(state) {
  }

  Status InitArray() { return this->CastArray(&string_array_); }

  Status WriteSlotInternal(int row_idx, void* slot, MemPool* mem_pool, bool* is_valid) {
    std::string_view v = string_array_->Value(row_idx);
    // TODO: This truncates in bytes, not in UTF-8 characters. See IMPALA-5675.
    if (max_len_ >= 0) v = v.substr(0, max_len_);
    StringValue* string_value = reinterpret_cast<StringValue*>(slot);
    string_value->Assign((char*)v.data(), v.size());
    if (string_value->Smallify()) {
      return Status::OK();
    }

    uint32_t buffer_size = v.size();
    char* buffer = reinterpret_cast<char*>(mem_pool->TryAllocateUnaligned(buffer_size));
    if (UNLIKELY(buffer == nullptr)) {
      string details = strings::Substitute(
          "Failed to allocate $0 bytes for StringSlotWriter.", buffer_size);
      return mem_pool->mem_tracker()->MemLimitExceeded(
          state_, details, buffer_size);
    }

    memcpy(buffer, v.data(), buffer_size);
    string_value->Assign(buffer, buffer_size);

    return Status::OK();
  }

private:
  const ArrowArrayType* string_array_ = nullptr;
  const int max_len_;
  RuntimeState* state_;
};

class CharSlotWriter : public SlotWriterImpl<CharSlotWriter> {
public:
  CharSlotWriter(const SlotDescriptor* slot_desc, int len) :
      SlotWriterImpl<CharSlotWriter>(slot_desc), dst_len_(len) {
  }

  Status InitArray() { return CastArray(&string_array_); }

  Status WriteSlotInternal(int row_idx, void* slot, MemPool* mem_pool, bool* is_valid) {
    std::string_view v = string_array_->Value(row_idx);

    int src_len = v.size();
    int unpadded_len = std::min<int>(dst_len_, src_len);
    // Copy the bytes to the RowBatch.
    char* dst_char = reinterpret_cast<char*>(slot);
    memcpy(dst_char, v.data(), unpadded_len);
    StringValue::PadWithSpaces(dst_char, dst_len_, unpadded_len);
    return Status::OK();
  }

private:
  const arrow::StringArray* string_array_ = nullptr;
  int dst_len_;
};

// Creates the writer for 'type' in 'writer'. SetArray() must be called before use.
Status CreateSlotWriter(const SlotDescriptor* slot_desc, const ColumnType& type,
    RuntimeState* state, std::unique_ptr<SlotWriter>* writer) {
  switch (type.type) {
    case TYPE_CHAR: {
      writer->reset(new CharSlotWriter(slot_desc, type.len));
      break;
    }
    case TYPE_STRING:
    case TYPE_VARCHAR: {
      int max_len = type.type == TYPE_VARCHAR ? type.len : -1;
      if (type.IsBinaryType()) { // byte[]
        writer->reset(
            new StringSlotWriter<arrow::BinaryArray>(slot_desc, max_len, state));
      } else {
        writer->reset(
            new StringSlotWriter<arrow::StringArray>(slot_desc, max_len, state));
      }
      break;
    }
    case TYPE_BOOLEAN: {
      writer->reset(new BaseSlotWriter<bool, arrow::BooleanArray>(slot_desc));
      break;
    }
    case TYPE_DATE: {
      writer->reset(new DateSlotWriter(slot_desc));
      break;
    }
    case TYPE_TINYINT: {
      writer->reset(new BaseSlotWriter<int8_t, arrow::Int8Array>(slot_desc));
      break;
    }
    case TYPE_SMALLINT: {
      writer->reset(new BaseSlotWriter<int16_t, arrow::Int16Array>(slot_desc));
      break;
    }
    case TYPE_INT: {
      writer->reset(new BaseSlotWriter<int32_t, arrow::Int32Array>(slot_desc));
      break;
    }
    case TYPE_BIGINT: {
      writer->reset(new BaseSlotWriter<int64_t, arrow::Int64Array>(slot_desc));
      break;
    }
    case TYPE_FLOAT: {
      writer->reset(new BaseSlotWriter<float, arrow::FloatArray>(slot_desc));
      break;
    }
    case TYPE_DOUBLE: {
      writer->reset(new BaseSlotWriter<double, arrow::DoubleArray>(slot_desc));
      break;
    }
    case TYPE_DECIMAL: {
      switch (type.GetByteSize()) {
        case 4: {
          writer->reset(new DecimalSlotWriter<Decimal4Value>(slot_desc));
          break;
        }
        case 8: {
          writer->reset(new DecimalSlotWriter<Decimal8Value>(slot_desc));
          break;
        }
        case 16: {
          writer->reset(new DecimalSlotWriter<Decimal16Value>(slot_desc));
          break;
        }
        default: {
          DCHECK(false) << "Invalid decimal size: " << type.GetByteSize();
          return Status(Substitute("Invalid decimal size: $0", type.GetByteSize()));
        }
      }
      break;
    }
    case TYPE_TIMESTAMP: {
      writer->reset(new TimestampSlotWriter(slot_desc, state->local_time_zone()));
      break;
    }
    case TYPE_STRUCT:
    case TYPE_ARRAY:
    case TYPE_MAP: {
      // TODO: implement complex type support later
      return Status("Arrow conversion not supported for complex types.");
    }
    default: {
      DCHECK(false) << "Unsupported column type: " << type.type;
      return Status(Substitute(
          "Arrow conversion not supported for type $0.", type.DebugString()));
    }
  }
  return Status::OK();
}

Status ArrowConverter::ConvertArrowArrayToRawValues(const arrow::Array* array,
      void** raw_values, uint8_t* value_buffer, const ColumnType& type, MemPool* mem_pool,
      RuntimeState* state) {
  int num_rows = array->length();
  int value_size = type.GetSlotSize();

  std::unique_ptr<SlotWriter> writer;
  RETURN_IF_ERROR(CreateSlotWriter(nullptr, type, state, &writer));
  RETURN_IF_ERROR(writer->SetArray(array));

  RETURN_IF_ERROR(writer->WriteRawValueBatch(raw_values, value_buffer, value_size,
      0, num_rows, mem_pool, state));

  return Status::OK();
}

ArrowToTupleConverter::ArrowToTupleConverter(const TupleDescriptor* tuple_desc) :
    tuple_desc_(tuple_desc) {
}

ArrowToTupleConverter::~ArrowToTupleConverter() = default;

Status ArrowToTupleConverter::Init(RuntimeState* state) {
  DCHECK(slot_writers_.empty());
  state_ = state;
  slot_writers_.reserve(tuple_desc_->slots().size());
  for (const SlotDescriptor* slot_desc : tuple_desc_->slots()) {
    std::unique_ptr<SlotWriter> writer;
    RETURN_IF_ERROR(CreateSlotWriter(slot_desc, slot_desc->type(), state, &writer));
    slot_writers_.push_back(std::move(writer));
  }
  return Status::OK();
}

Status ArrowToTupleConverter::Convert(const arrow::RecordBatch& record_batch,
    uint8_t* tuple_mem, MemPool* mem_pool) {
  return Convert(record_batch, 0, record_batch.num_rows(), tuple_mem, mem_pool);
}

Status ArrowToTupleConverter::Convert(const arrow::RecordBatch& record_batch,
    int start_row_idx, int end_row_idx, uint8_t* tuple_mem, MemPool* mem_pool) {
  DCHECK_EQ(slot_writers_.size(), tuple_desc_->slots().size()) << "Init() not called";
  DCHECK_LE(0, start_row_idx);
  DCHECK_LE(start_row_idx, end_row_idx);
  DCHECK_LE(end_row_idx, record_batch.num_rows());
  int num_slots = slot_writers_.size();
  if (UNLIKELY(record_batch.num_columns() != num_slots)) {
    return Status(Substitute("Arrow record batch has $0 columns, expected $1.",
        record_batch.num_columns(), num_slots));
  }
  int tuple_size = tuple_desc_->byte_size();
  int num_rows = end_row_idx - start_row_idx;
  // Clear tuple memory to null. This unsets all null flags, when encountering a non-null
  // value in a slot, the flag doesn't have to be cleared.
  memset(tuple_mem, 0, num_rows * tuple_size);
  for (int col = 0; col < num_slots; ++col) {
    std::shared_ptr<arrow::Array> arr = record_batch.column(col);
    SlotWriter* writer = slot_writers_[col].get();
    RETURN_IF_ERROR(writer->SetArray(arr.get()));
    RETURN_IF_ERROR(writer->WriteSlotBatch(
        tuple_mem, tuple_size, start_row_idx, end_row_idx, mem_pool, state_));
  }

  return Status::OK();
}

}
