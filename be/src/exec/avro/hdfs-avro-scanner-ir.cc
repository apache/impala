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

#include <algorithm>
#include <limits>

#include "exec/exec-node.inline.h"
#include "exec/avro/hdfs-avro-scanner.h"
#include "exec/read-write-util.h"
#include "runtime/date-value.h"
#include "runtime/mem-tracker.h"
#include "runtime/runtime-state.h"
#include "runtime/string-value.inline.h"

using namespace impala;
using namespace strings;
using std::numeric_limits;

// Functions in this file are cross-compiled to IR with clang.

static const int AVRO_FLOAT_SIZE = 4;
static const int AVRO_DOUBLE_SIZE = 8;

int HdfsAvroScanner::DecodeAvroData(int max_tuples, MemPool* pool, uint8_t** data,
    uint8_t* data_end, Tuple* tuple, TupleRow* tuple_row) {
  // If the file is uncompressed, StringValues will have pointers into the I/O buffers.
  // We don't attach I/O buffers to output batches so need to copy out data referenced
  // by tuples that survive conjunct evaluation.
  const bool copy_strings = !header_->is_compressed && !string_slot_offsets_.empty();
  int num_to_commit = 0;
  for (int i = 0; i < max_tuples; ++i) {
    InitTuple(template_tuple_, tuple);
    if (UNLIKELY(!MaterializeTuple(*avro_header_->schema.get(), pool, data, data_end,
        tuple))) {
      return 0;
    }
    tuple_row->SetTuple(0, tuple);
    if (EvalConjuncts(tuple_row)) {
      if (copy_strings) {
        if (UNLIKELY(!tuple->CopyStrings("HdfsAvroScanner::DecodeAvroData()",
              state_, string_slot_offsets_.data(), string_slot_offsets_.size(), pool,
              &parse_status_))) {
          return 0;
        }
      }
      ++num_to_commit;
      tuple_row = next_row(tuple_row);
      tuple = next_tuple(tuple_byte_size(), tuple);
    }
  }
  return num_to_commit;
}

bool HdfsAvroScanner::ReadUnionType(int null_union_position, uint8_t** data,
    uint8_t* data_end, bool* is_null) {
  DCHECK(null_union_position == 0 || null_union_position == 1);
  if (UNLIKELY(*data == data_end)) {
    SetStatusCorruptData(TErrorCode::AVRO_TRUNCATED_BLOCK);
    return false;
  }
  int8_t union_position = **data;
  // Union position is varlen zig-zag encoded
  if (UNLIKELY(union_position != 0 && union_position != 2)) {
    SetStatusInvalidValue(TErrorCode::AVRO_INVALID_UNION, union_position);
    return false;
  }
  // "Decode" zig-zag encoding
  if (union_position == 2) union_position = 1;
  *data += 1;
  *is_null = union_position == null_union_position;
  return true;
}

bool HdfsAvroScanner::ReadAvroBoolean(PrimitiveType type, uint8_t** data,
    uint8_t* data_end, bool write_slot, void* slot, MemPool* pool) {
  if (UNLIKELY(*data == data_end)) {
    SetStatusCorruptData(TErrorCode::AVRO_TRUNCATED_BLOCK);
    return false;
  }
  if (write_slot) {
    DCHECK_EQ(type, TYPE_BOOLEAN);
    if (UNLIKELY(**data != 0 && **data != 1)) {
      SetStatusInvalidValue(TErrorCode::AVRO_INVALID_BOOLEAN, **data);
      return false;
    }
    *reinterpret_cast<bool*>(slot) = *reinterpret_cast<bool*>(*data);
  }
  *data += 1;
  return true;
}

bool HdfsAvroScanner::ReadAvroDate(PrimitiveType type, uint8_t** data, uint8_t* data_end,
    bool write_slot, void* slot, MemPool* pool) {
  ReadWriteUtil::ZIntResult r = ReadWriteUtil::ReadZInt(data, data_end);
  if (UNLIKELY(!r.ok)) {
    SetStatusCorruptData(TErrorCode::SCANNER_INVALID_INT);
    return false;
  }
  if (write_slot) {
    DCHECK_EQ(type, TYPE_DATE);
    DateValue dv(r.val);
    if (UNLIKELY(!dv.IsValid())) {
      SetStatusInvalidValue(TErrorCode::AVRO_INVALID_DATE, r.val);
      return false;
    }
    *reinterpret_cast<DateValue*>(slot) = dv;
  }
  return true;
}

bool HdfsAvroScanner::ReadAvroInt32(PrimitiveType type, uint8_t** data, uint8_t* data_end,
    bool write_slot, void* slot, MemPool* pool) {
  ReadWriteUtil::ZIntResult r = ReadWriteUtil::ReadZInt(data, data_end);
  if (UNLIKELY(!r.ok)) {
    SetStatusCorruptData(TErrorCode::SCANNER_INVALID_INT);
    return false;
  }
  if (write_slot) {
    if (type == TYPE_INT) {
      *reinterpret_cast<int32_t*>(slot) = r.val;
    } else if (type == TYPE_BIGINT) {
      *reinterpret_cast<int64_t*>(slot) = r.val;
    } else if (type == TYPE_FLOAT) {
      *reinterpret_cast<float*>(slot) = r.val;
    } else {
      DCHECK_EQ(type, TYPE_DOUBLE);
      *reinterpret_cast<double*>(slot) = r.val;
    }
  }
  return true;
}

bool HdfsAvroScanner::ReadAvroInt64(PrimitiveType type, uint8_t** data, uint8_t* data_end,
    bool write_slot, void* slot, MemPool* pool) {
  ReadWriteUtil::ZLongResult r = ReadWriteUtil::ReadZLong(data, data_end);
  if (UNLIKELY(!r.ok)) {
    SetStatusCorruptData(TErrorCode::SCANNER_INVALID_INT);
    return false;
  }
  if (write_slot) {
    if (type == TYPE_BIGINT) {
      *reinterpret_cast<int64_t*>(slot) = r.val;
    } else if (type == TYPE_FLOAT) {
      *reinterpret_cast<float*>(slot) = r.val;
    } else {
      DCHECK_EQ(type, TYPE_DOUBLE);
      *reinterpret_cast<double*>(slot) = r.val;
    }
  }
  return true;
}

bool HdfsAvroScanner::ReadAvroFloat(PrimitiveType type, uint8_t** data, uint8_t* data_end,
    bool write_slot, void* slot, MemPool* pool) {
  if (UNLIKELY(data_end - *data < AVRO_FLOAT_SIZE)) {
    SetStatusCorruptData(TErrorCode::AVRO_TRUNCATED_BLOCK);
    return false;
  }
  if (write_slot) {
    float val = *reinterpret_cast<float*>(*data);
    if (type == TYPE_FLOAT) {
      *reinterpret_cast<float*>(slot) = val;
    } else {
      DCHECK_EQ(type, TYPE_DOUBLE);
      *reinterpret_cast<double*>(slot) = val;
    }
  }
  *data += AVRO_FLOAT_SIZE;
  return true;
}

bool HdfsAvroScanner::ReadAvroDouble(PrimitiveType type, uint8_t** data, uint8_t* data_end,
    bool write_slot, void* slot, MemPool* pool) {
  if (UNLIKELY(data_end - *data < AVRO_DOUBLE_SIZE)) {
    SetStatusCorruptData(TErrorCode::AVRO_TRUNCATED_BLOCK);
    return false;
  }
  if (write_slot) {
    DCHECK_EQ(type, TYPE_DOUBLE);
    *reinterpret_cast<double*>(slot) = *reinterpret_cast<double*>(*data);
  }
  *data += AVRO_DOUBLE_SIZE;
  return true;
}

ReadWriteUtil::ZLongResult HdfsAvroScanner::ReadFieldLen(uint8_t** data, uint8_t* data_end) {
  ReadWriteUtil::ZLongResult r = ReadWriteUtil::ReadZLong(data, data_end);
  if (UNLIKELY(!r.ok)) {
    SetStatusCorruptData(TErrorCode::SCANNER_INVALID_INT);
    return ReadWriteUtil::ZLongResult::error();
  }
  if (UNLIKELY(r.val < 0)) {
    SetStatusInvalidValue(TErrorCode::AVRO_INVALID_LENGTH, r.val);
    return ReadWriteUtil::ZLongResult::error();
  }
  if (UNLIKELY(data_end - *data < r.val)) {
    SetStatusCorruptData(TErrorCode::AVRO_TRUNCATED_BLOCK);
    return ReadWriteUtil::ZLongResult::error();
  }
  return r;
}

bool HdfsAvroScanner::ReadAvroVarchar(PrimitiveType type, int max_len, uint8_t** data,
    uint8_t* data_end, bool write_slot, void* slot, MemPool* pool) {
  ReadWriteUtil::ZLongResult len = ReadFieldLen(data, data_end);
  if (UNLIKELY(!len.ok)) return false;
  if (write_slot) {
    DCHECK(type == TYPE_VARCHAR);
    StringValue* sv = reinterpret_cast<StringValue*>(slot);
    // 'max_len' is an int, so the result of min() should always be in [0, INT_MAX].
    // We need to be careful not to truncate the length before evaluating min().
    int str_len = static_cast<int>(std::min<int64_t>(len.val, max_len));
    DCHECK_GE(str_len, 0);
    sv->Assign(reinterpret_cast<char*>(*data), str_len);
  }
  *data += len.val;
  return true;
}

bool HdfsAvroScanner::ReadAvroChar(PrimitiveType type, int max_len, uint8_t** data,
    uint8_t* data_end, bool write_slot, void* slot, MemPool* pool) {
  ReadWriteUtil::ZLongResult len = ReadFieldLen(data, data_end);
  if (UNLIKELY(!len.ok)) return false;
  if (write_slot) {
    DCHECK(type == TYPE_CHAR);
    ColumnType ctype = ColumnType::CreateCharType(max_len);
    // 'max_len' is an int, so the result of min() should always be in [0, INT_MAX].
    // We need to be careful not to truncate the length before evaluating min().
    int str_len = static_cast<int>(std::min<int64_t>(len.val, max_len));
    DCHECK_GE(str_len, 0);
    memcpy(slot, *data, str_len);
    StringValue::PadWithSpaces(reinterpret_cast<char*>(slot), max_len, str_len);
  }
  *data += len.val;
  return true;
}

bool HdfsAvroScanner::ReadAvroString(PrimitiveType type, uint8_t** data,
    uint8_t* data_end, bool write_slot, void* slot, MemPool* pool) {
  ReadWriteUtil::ZLongResult len = ReadFieldLen(data, data_end);
  if (UNLIKELY(!len.ok)) return false;
  if (write_slot) {
    DCHECK(type == TYPE_STRING);
    if (UNLIKELY(len.val > numeric_limits<int>::max())) {
      SetStatusValueOverflow(TErrorCode::SCANNER_STRING_LENGTH_OVERFLOW, len.val,
          numeric_limits<int>::max());
      return false;
    }
    StringValue* sv = reinterpret_cast<StringValue*>(slot);
    sv->Assign(reinterpret_cast<char*>(*data), len.val);
  }
  *data += len.val;
  return true;
}

bool HdfsAvroScanner::ReadAvroDecimal(int slot_byte_size, uint8_t** data,
    uint8_t* data_end, bool write_slot, void* slot, MemPool* pool) {
  ReadWriteUtil::ZLongResult len = ReadFieldLen(data, data_end);
  if (UNLIKELY(!len.ok)) return false;
  if (write_slot) {
    DCHECK_GE(len.val, 0);
    if (UNLIKELY(len.val > slot_byte_size)) {
      SetStatusInvalidValue(TErrorCode::AVRO_INVALID_LENGTH, len.val);
      return false;
    }
    // The len.val == 0 case is special due to undefined behavior of shifting and memcpy,
    // so we handle it separately.
    if (UNLIKELY(len.val == 0)) {
      if(LIKELY(slot_byte_size == 4 || slot_byte_size == 8 || slot_byte_size == 16)) {
        memset(slot, 0, slot_byte_size);
      } else {
        DCHECK(false) << "Decimal slots can't be this size: " << slot_byte_size;
      }
      return true;
    }
    // Decimals are encoded as big-endian integers. Copy the decimal into the most
    // significant bytes and then shift down to the correct position to sign-extend the
    // decimal.
    int bytes_to_fill = slot_byte_size - len.val;
#if __BYTE_ORDER == __LITTLE_ENDIAN
    BitUtil::ByteSwap(reinterpret_cast<uint8_t*>(slot) + bytes_to_fill, *data, len.val);
#else
    memcpy(slot, *data, len.val);
#endif
    switch (slot_byte_size) {
      case 4: {
        int32_t* decimal = reinterpret_cast<int32_t*>(slot);
        *decimal >>= bytes_to_fill * 8;
        break;
      }
      case 8: {
        int64_t* decimal = reinterpret_cast<int64_t*>(slot);
        *decimal >>= bytes_to_fill * 8;
        break;
      }
      case 16: {
        __int128_t* decimal = reinterpret_cast<__int128_t*>(slot);
        *decimal >>= bytes_to_fill * 8;
        break;
      }
      default:
        DCHECK(false) << "Decimal slots can't be this size: " << slot_byte_size;
    }
  }
  *data += len.val;
  return true;
}
