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


#ifndef IMPALA_EXEC_PARQUET_COMMON_H
#define IMPALA_EXEC_PARQUET_COMMON_H

#include "gen-cpp/Descriptors_types.h"
#include "gen-cpp/parquet_types.h"
#include "runtime/string-value.h"

// This file contains common elements between the parquet Writer and Scanner.
namespace impala {

class TimestampValue;

const uint8_t PARQUET_VERSION_NUMBER[4] = {'P', 'A', 'R', '1'};
const uint32_t PARQUET_CURRENT_VERSION = 1;

// Mapping of impala types to parquet storage types.  This is indexed by
// PrimitiveType enum
const parquet::Type::type IMPALA_TO_PARQUET_TYPES[] = {
  parquet::Type::BOOLEAN,     // Invalid
  parquet::Type::BOOLEAN,     // NULL type
  parquet::Type::BOOLEAN,
  parquet::Type::INT32,
  parquet::Type::INT32,
  parquet::Type::INT32,
  parquet::Type::INT64,
  parquet::Type::FLOAT,
  parquet::Type::DOUBLE,
  parquet::Type::INT96,       // Timestamp
  parquet::Type::BYTE_ARRAY,
};

// Mapping of Parquet codec enums to Impala enums
const THdfsCompression::type PARQUET_TO_IMPALA_CODEC[] = {
  THdfsCompression::NONE,
  THdfsCompression::SNAPPY,
  THdfsCompression::GZIP,
  THdfsCompression::LZO
};

// Mapping of Impala codec enums to Parquet enums
const parquet::CompressionCodec::type IMPALA_TO_PARQUET_CODEC[] = {
  parquet::CompressionCodec::UNCOMPRESSED,
  parquet::CompressionCodec::SNAPPY,  // DEFAULT
  parquet::CompressionCodec::GZIP,    // GZIP
  parquet::CompressionCodec::GZIP,    // DEFLATE
  parquet::CompressionCodec::SNAPPY,
  parquet::CompressionCodec::SNAPPY,  // SNAPPY_BLOCKED
  parquet::CompressionCodec::LZO,
};

// The plain encoding does not maintain any state so all these functions
// are static helpers.
class ParquetPlainEncoder {
 public:
  template<typename T>
  static int ByteSize(const T& v) { return sizeof(T); }

  // Encodes t into buffer. Returns the number of bytes added.  buffer must
  // be preallocated and big enough.  Buffer need not be aligned.
  template<typename T>
  static int Encode(uint8_t* buffer, const T& t) {
    memcpy(buffer, &t, ByteSize(t));
    return ByteSize(t);
  }

  // Decodes t from buffer. Returns the number of bytes read.  Buffer need
  // not be aligned.
  template<typename T>
  static int Decode(uint8_t* buffer, T* v) {
    memcpy(v, buffer, ByteSize(*v));
    return ByteSize(*v);
  }
};

// Disable for bools. Plain encoding is not used for booleans.
template<> int ParquetPlainEncoder::ByteSize(const bool& b);
template<> int ParquetPlainEncoder::Encode(uint8_t*, const bool&);
template<> int ParquetPlainEncoder::Decode(uint8_t*, bool* v);

// Parquet doesn't have 8-bit or 16-bit ints. They are converted to 32-bit.
template<>
inline int ParquetPlainEncoder::ByteSize(const int8_t& v) { return sizeof(int32_t); }
template<>
inline int ParquetPlainEncoder::ByteSize(const int16_t& v) { return sizeof(int32_t); }

template<>
inline int ParquetPlainEncoder::Decode(uint8_t* buffer, int8_t* v) {
  *v = *buffer;
  return ByteSize(*v);
}
template<>
inline int ParquetPlainEncoder::Decode(uint8_t* buffer, int16_t* v) {
  memcpy(v, buffer, sizeof(int16_t));
  return ByteSize(*v);
}

template<>
inline int ParquetPlainEncoder::Encode(uint8_t* buffer, const int8_t& v) {
  int32_t val = v;
  memcpy(buffer, &val, sizeof(int32_t));
  return ByteSize(v);
}

template<>
inline int ParquetPlainEncoder::Encode(uint8_t* buffer, const int16_t& v) {
  int32_t val = v;
  memcpy(buffer, &val, sizeof(int32_t));
  return ByteSize(v);
}

template<>
inline int ParquetPlainEncoder::ByteSize(const StringValue& v) {
  return sizeof(int32_t) + v.len;
}

template<>
inline int ParquetPlainEncoder::ByteSize(const TimestampValue& v) {
  return 12;
}

template<>
inline int ParquetPlainEncoder::Encode(uint8_t* buffer, const StringValue& v) {
  memcpy(buffer, &v.len, sizeof(int32_t));
  memcpy(buffer + sizeof(int32_t), v.ptr, v.len);
  return ByteSize(v);
}

template<>
inline int ParquetPlainEncoder::Decode(uint8_t* buffer, StringValue* v) {
  memcpy(&v->len, buffer, sizeof(int32_t));
  v->ptr = reinterpret_cast<char*>(buffer) + sizeof(int32_t);
  return ByteSize(*v);
}

}

#endif
