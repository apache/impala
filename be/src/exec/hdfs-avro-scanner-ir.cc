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

#include "exec/hdfs-avro-scanner.h"
#include "exec/read-write-util.h"
#include <algorithm>

using namespace impala;

// Functions in this file are cross-compiled to IR with clang.

int HdfsAvroScanner::DecodeAvroData(int max_tuples, MemPool* pool, uint8_t** data,
                                    Tuple* tuple, TupleRow* tuple_row) {
  int num_to_commit = 0;
  for (int i = 0; i < max_tuples; ++i) {
    InitTuple(template_tuple_, tuple);
    MaterializeTuple(*avro_header_->schema.get(), pool, data, tuple);
    tuple_row->SetTuple(scan_node_->tuple_idx(), tuple);
    if (EvalConjuncts(tuple_row)) {
      ++num_to_commit;
      tuple_row = next_row(tuple_row);
      tuple = next_tuple(tuple);
    }
  }
  return num_to_commit;
}

bool HdfsAvroScanner::ReadUnionType(int null_union_position, uint8_t** data) {
  DCHECK(null_union_position == 0 || null_union_position == 1);
  int8_t union_position = **data;
  // Union position is varlen zig-zag encoded
  DCHECK(union_position == 0 || union_position == 2);
  // "Decode" zig-zag encoding
  if (union_position == 2) union_position = 1;
  *data += 1;
  return union_position != null_union_position;
}

void HdfsAvroScanner::ReadAvroBoolean(PrimitiveType type, uint8_t** data, bool write_slot,
                                      void* slot, MemPool* pool) {
  if (write_slot) {
    DCHECK_EQ(type, TYPE_BOOLEAN);
    *reinterpret_cast<bool*>(slot) = *reinterpret_cast<bool*>(*data);
  }
  *data += 1;
}

void HdfsAvroScanner::ReadAvroInt32(PrimitiveType type, uint8_t** data, bool write_slot,
                                    void* slot, MemPool* pool) {
  int32_t val = ReadWriteUtil::ReadZInt(data);
  if (write_slot) {
    if (type == TYPE_INT) {
      *reinterpret_cast<int32_t*>(slot) = val;
    } else if (type == TYPE_BIGINT) {
      *reinterpret_cast<int64_t*>(slot) = val;
    } else if (type == TYPE_FLOAT) {
      *reinterpret_cast<float*>(slot) = val;
    } else if (type == TYPE_DOUBLE) {
      *reinterpret_cast<double*>(slot) = val;
    } else {
      DCHECK(false);
    }
  }
}

void HdfsAvroScanner::ReadAvroInt64(PrimitiveType type, uint8_t** data, bool write_slot,
                                    void* slot, MemPool* pool) {
  int64_t val = ReadWriteUtil::ReadZLong(data);
  if (write_slot) {
    if (type == TYPE_BIGINT) {
      *reinterpret_cast<int64_t*>(slot) = val;
    } else if (type == TYPE_FLOAT) {
      *reinterpret_cast<float*>(slot) = val;
    } else if (type == TYPE_DOUBLE) {
      *reinterpret_cast<double*>(slot) = val;
    } else {
      DCHECK(false);
    }
  }
}

void HdfsAvroScanner::ReadAvroFloat(PrimitiveType type, uint8_t** data, bool write_slot,
                                    void* slot, MemPool* pool) {
  if (write_slot) {
    float val = *reinterpret_cast<float*>(*data);
    if (type == TYPE_FLOAT) {
      *reinterpret_cast<float*>(slot) = val;
    } else if (type == TYPE_DOUBLE) {
      *reinterpret_cast<double*>(slot) = val;
    } else {
      DCHECK(false);
    }
  }
  *data += 4;
}

void HdfsAvroScanner::ReadAvroDouble(PrimitiveType type, uint8_t** data, bool write_slot,
                                     void* slot, MemPool* pool) {
  if (write_slot) {
    DCHECK_EQ(type, TYPE_DOUBLE);
    *reinterpret_cast<double*>(slot) = *reinterpret_cast<double*>(*data);
  }
  *data += 8;
}

void HdfsAvroScanner::ReadAvroVarchar(PrimitiveType type, int max_len, uint8_t** data,
                                     bool write_slot, void* slot, MemPool* pool) {
  int64_t len = ReadWriteUtil::ReadZLong(data);
  if (write_slot) {
    DCHECK(type == TYPE_VARCHAR);
    StringValue* sv = reinterpret_cast<StringValue*>(slot);
    int str_len = std::min(static_cast<int>(len), max_len);
    DCHECK(str_len >= 0);
    sv->len = str_len;
    sv->ptr = reinterpret_cast<char*>(*data);
  }
  *data += len;
}

void HdfsAvroScanner::ReadAvroChar(PrimitiveType type, int max_len, uint8_t** data,
                                   bool write_slot, void* slot, MemPool* pool) {
  int64_t len = ReadWriteUtil::ReadZLong(data);
  if (write_slot) {
    DCHECK(type == TYPE_CHAR);
    ColumnType ctype = ColumnType::CreateCharType(max_len);
    int str_len = std::min(static_cast<int>(len), max_len);
    if (ctype.IsVarLenStringType()) {
      StringValue* sv = reinterpret_cast<StringValue*>(slot);
      sv->ptr = reinterpret_cast<char*>(pool->Allocate(max_len));
      sv->len = max_len;
      memcpy(sv->ptr, *data, str_len);
      StringValue::PadWithSpaces(sv->ptr, max_len, str_len);
    } else {
      memcpy(slot, *data, str_len);
      StringValue::PadWithSpaces(reinterpret_cast<char*>(slot), max_len, str_len);
    }
  }
  *data += len;
}

void HdfsAvroScanner::ReadAvroString(PrimitiveType type, uint8_t** data,
                                     bool write_slot, void* slot, MemPool* pool) {
  int64_t len = ReadWriteUtil::ReadZLong(data);
  if (write_slot) {
    DCHECK(type == TYPE_STRING);
    StringValue* sv = reinterpret_cast<StringValue*>(slot);
    sv->len = len;
    sv->ptr = reinterpret_cast<char*>(*data);
  }
  *data += len;
}

void HdfsAvroScanner::ReadAvroDecimal(int slot_byte_size, uint8_t** data,
                                      bool write_slot, void* slot, MemPool* pool) {
  int64_t len = ReadWriteUtil::ReadZLong(data);
  if (write_slot) {
    // Decimals are encoded as big-endian integers. Copy the decimal into the most
    // significant bytes and then shift down to the correct position to sign-extend the
    // decimal.
    DCHECK_LE(len, slot_byte_size);
    int bytes_to_fill = slot_byte_size - len;
#if __BYTE_ORDER == __LITTLE_ENDIAN
    BitUtil::ByteSwap(reinterpret_cast<uint8_t*>(slot) + bytes_to_fill, *data, len);
#else
    memcpy(slot, *data, len);
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
        int128_t* decimal = reinterpret_cast<int128_t*>(slot);
        *decimal >>= bytes_to_fill * 8;
        break;
      }
      default:
        DCHECK(false) << "Decimal slots can't be this size: " << slot_byte_size;
    }
  }
  *data += len;
}
