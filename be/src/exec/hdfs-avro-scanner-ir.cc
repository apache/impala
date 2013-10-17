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

using namespace impala;

// Functions in this file are cross-compiled to IR with clang.

int HdfsAvroScanner::DecodeAvroData(int max_tuples, MemPool* pool, uint8_t** data,
                                    Tuple* tuple, TupleRow* tuple_row) {
  int num_to_commit = 0;
  for (int i = 0; i < max_tuples; ++i) {
    InitTuple(template_tuple_, tuple);
    MaterializeTuple(pool, data, tuple);
    tuple_row->SetTuple(scan_node_->tuple_idx(), tuple);
    if (ExecNode::EvalConjuncts(&(*conjuncts_)[0], num_conjuncts_, tuple_row)) {
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

void HdfsAvroScanner::ReadAvroString(PrimitiveType type, uint8_t** data, bool write_slot,
                                     void* slot, MemPool* pool) {
  int64_t len = ReadWriteUtil::ReadZLong(data);
  if (write_slot) {
    DCHECK_EQ(type, TYPE_STRING);
    StringValue* sv = reinterpret_cast<StringValue*>(slot);
    sv->len = len;
    if (stream_->compact_data()) {
      sv->ptr = reinterpret_cast<char*>(pool->Allocate(len));
      memcpy(sv->ptr, *data, len);
    } else {
      sv->ptr = reinterpret_cast<char*>(*data);
    }
  }
  *data += len;
}
