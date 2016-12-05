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


#ifndef IMPALA_EXEC_PARQUET_COMMON_H
#define IMPALA_EXEC_PARQUET_COMMON_H

#include "common/compiler-util.h"
#include "gen-cpp/Descriptors_types.h"
#include "gen-cpp/parquet_types.h"
#include "runtime/decimal-value.h"
#include "runtime/string-value.h"
#include "util/bit-util.h"
#include "util/decimal-util.h"

/// This file contains common elements between the parquet Writer and Scanner.
namespace impala {

class TimestampValue;

const uint8_t PARQUET_VERSION_NUMBER[4] = {'P', 'A', 'R', '1'};
const uint32_t PARQUET_CURRENT_VERSION = 1;

/// Mapping of impala types to parquet storage types.  This is indexed by
/// PrimitiveType enum
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
  parquet::Type::BYTE_ARRAY,  // String
  parquet::Type::BYTE_ARRAY,  // Date, NYI
  parquet::Type::BYTE_ARRAY,  // DateTime, NYI
  parquet::Type::BYTE_ARRAY,  // Binary NYI
  parquet::Type::FIXED_LEN_BYTE_ARRAY, // Decimal
  parquet::Type::BYTE_ARRAY,  // VARCHAR(N)
  parquet::Type::BYTE_ARRAY,  // CHAR(N)
};

/// Mapping of Parquet codec enums to Impala enums
const THdfsCompression::type PARQUET_TO_IMPALA_CODEC[] = {
  THdfsCompression::NONE,
  THdfsCompression::SNAPPY,
  THdfsCompression::GZIP,
  THdfsCompression::LZO
};

/// Mapping of Impala codec enums to Parquet enums
const parquet::CompressionCodec::type IMPALA_TO_PARQUET_CODEC[] = {
  parquet::CompressionCodec::UNCOMPRESSED,
  parquet::CompressionCodec::SNAPPY,  // DEFAULT
  parquet::CompressionCodec::GZIP,    // GZIP
  parquet::CompressionCodec::GZIP,    // DEFLATE
  parquet::CompressionCodec::SNAPPY,
  parquet::CompressionCodec::SNAPPY,  // SNAPPY_BLOCKED
  parquet::CompressionCodec::LZO,
};

/// The plain encoding does not maintain any state so all these functions
/// are static helpers.
/// TODO: we are using templates to provide a generic interface (over the
/// types) to avoid performance penalties. This makes the code more complex
/// and should be removed when we have codegen support to inline virtual
/// calls.
class ParquetPlainEncoder {
 public:
  /// Returns the byte size of 'v'.
  template<typename T>
  static int ByteSize(const T& v) { return sizeof(T); }

  /// Returns the encoded size of values of type t. Returns -1 if it is variable
  /// length. This can be different than the slot size of the types.
  static int EncodedByteSize(const ColumnType& t) {
    switch (t.type) {
      case TYPE_STRING:
      case TYPE_VARCHAR:
      case TYPE_CHAR:
        // CHAR is varlen here because we don't write the padding to the file
        return -1;
      case TYPE_TINYINT:
      case TYPE_SMALLINT:
      case TYPE_INT:
      case TYPE_FLOAT:
        return 4;
      case TYPE_BIGINT:
      case TYPE_DOUBLE:
        return 8;
      case TYPE_TIMESTAMP:
        return 12;
      case TYPE_DECIMAL:
        return DecimalSize(t);
      case TYPE_NULL:
      case TYPE_BOOLEAN: // These types are not plain encoded.
      default:
        DCHECK(false);
        return -1;
    }
  }

  /// The minimum byte size to store decimals of with precision t.precision.
  static int DecimalSize(const ColumnType& t) {
    DCHECK(t.type == TYPE_DECIMAL);
    // Numbers in the comment is the max positive value that can be represented
    // with those number of bits (max negative is -(X + 1)).
    // TODO: use closed form for this?
    switch (t.precision) {
      case 1: case 2:
        return 1; // 127
      case 3: case 4:
        return 2; // 32,767
      case 5: case 6:
        return 3; // 8,388,607
      case 7: case 8: case 9:
        return 4; // 2,147,483,427
      case 10: case 11:
        return 5; // 549,755,813,887
      case 12: case 13: case 14:
        return 6; // 140,737,488,355,327
      case 15: case 16:
        return 7; // 36,028,797,018,963,967
      case 17: case 18:
        return 8; // 9,223,372,036,854,775,807
      case 19: case 20: case 21:
        return 9; // 2,361,183,241,434,822,606,847
      case 22: case 23:
        return 10; // 604,462,909,807,314,587,353,087
      case 24: case 25: case 26:
        return 11; // 154,742,504,910,672,534,362,390,527
      case 27: case 28:
        return 12; // 39,614,081,257,132,168,796,771,975,167
      case 29: case 30: case 31:
        return 13; // 10,141,204,801,825,835,211,973,625,643,007
      case 32: case 33:
        return 14; // 2,596,148,429,267,413,814,265,248,164,610,047
      case 34: case 35:
        return 15; // 664,613,997,892,457,936,451,903,530,140,172,287
      case 36: case 37: case 38:
        return 16; // 170,141,183,460,469,231,731,687,303,715,884,105,727
      default:
        DCHECK(false);
        break;
    }
    return -1;
  }

  /// Encodes t into buffer. Returns the number of bytes added.  buffer must
  /// be preallocated and big enough.  Buffer need not be aligned.
  /// 'fixed_len_size' is only applicable for data encoded using FIXED_LEN_BYTE_ARRAY and
  /// is the number of bytes the plain encoder should use.
  template<typename T>
  static int Encode(uint8_t* buffer, int fixed_len_size, const T& t) {
    memcpy(buffer, &t, ByteSize(t));
    return ByteSize(t);
  }

  /// Decodes t from 'buffer', reading up to the byte before 'buffer_end'. 'buffer'
  /// need not be aligned. For types that are stored as FIXED_LEN_BYTE_ARRAY,
  /// 'fixed_len_size' is the size of the object. Otherwise, it is unused.
  /// Returns the number of bytes read or -1 if the value was not decoded successfully.
  template<typename T>
  static int Decode(uint8_t* buffer, const uint8_t* buffer_end, int fixed_len_size,
      T* v) {
    int byte_size = ByteSize(*v);
    if (UNLIKELY(buffer_end - buffer < byte_size)) return -1;
    memcpy(v, buffer, byte_size);
    return byte_size;
  }
};

/// Calling this with arguments of type ColumnType is certainly a programmer error, so we
/// disallow it.
template <>
int ParquetPlainEncoder::ByteSize(const ColumnType& t);

/// Disable for bools. Plain encoding is not used for booleans.
template<> int ParquetPlainEncoder::ByteSize(const bool& b);
template<> int ParquetPlainEncoder::Encode(uint8_t*, int fixed_len_size, const bool&);
template<> int ParquetPlainEncoder::Decode(uint8_t*, const uint8_t*, int fixed_len_size,
    bool* v);

/// Not used for decimals since the plain encoding encodes them using
/// FIXED_LEN_BYTE_ARRAY.
template<> inline int ParquetPlainEncoder::ByteSize(const Decimal4Value&) {
  DCHECK(false);
  return -1;
}
template<> inline int ParquetPlainEncoder::ByteSize(const Decimal8Value&) {
  DCHECK(false);
  return -1;
}
template<> inline int ParquetPlainEncoder::ByteSize(const Decimal16Value&) {
  DCHECK(false);
  return -1;
}

/// Parquet doesn't have 8-bit or 16-bit ints. They are converted to 32-bit.
template<>
inline int ParquetPlainEncoder::ByteSize(const int8_t& v) { return sizeof(int32_t); }
template<>
inline int ParquetPlainEncoder::ByteSize(const int16_t& v) { return sizeof(int32_t); }

template<>
inline int ParquetPlainEncoder::ByteSize(const StringValue& v) {
  return sizeof(int32_t) + v.len;
}

template<>
inline int ParquetPlainEncoder::ByteSize(const TimestampValue& v) {
  return 12;
}

template<>
inline int ParquetPlainEncoder::Decode(uint8_t* buffer, const uint8_t* buffer_end,
    int fixed_len_size, int8_t* v) {
  int byte_size = ByteSize(*v);
  if (UNLIKELY(buffer_end - buffer < byte_size)) return -1;
  *v = *buffer;
  return byte_size;
}
template<>
inline int ParquetPlainEncoder::Decode(uint8_t* buffer, const uint8_t* buffer_end,
    int fixed_len_size, int16_t* v) {
  int byte_size = ByteSize(*v);
  if (UNLIKELY(buffer_end - buffer < byte_size)) return -1;
  memcpy(v, buffer, sizeof(int16_t));
  return byte_size;
}

template<>
inline int ParquetPlainEncoder::Encode(
    uint8_t* buffer, int fixed_len_size, const int8_t& v) {
  int32_t val = v;
  memcpy(buffer, &val, sizeof(int32_t));
  return ByteSize(v);
}

template<>
inline int ParquetPlainEncoder::Encode(
    uint8_t* buffer, int fixed_len_size, const int16_t& v) {
  int32_t val = v;
  memcpy(buffer, &val, sizeof(int32_t));
  return ByteSize(v);
}

template<>
inline int ParquetPlainEncoder::Encode(
    uint8_t* buffer, int fixed_len_size, const StringValue& v) {
  memcpy(buffer, &v.len, sizeof(int32_t));
  memcpy(buffer + sizeof(int32_t), v.ptr, v.len);
  return ByteSize(v);
}

template<>
inline int ParquetPlainEncoder::Decode(
    uint8_t* buffer, const uint8_t* buffer_end, int fixed_len_size, StringValue* v) {
  if (UNLIKELY(buffer_end - buffer < sizeof(int32_t))) return -1;
  memcpy(&v->len, buffer, sizeof(int32_t));
  int byte_size = ByteSize(*v);
  if (UNLIKELY(v->len < 0 || buffer_end - buffer < byte_size)) return -1;
  v->ptr = reinterpret_cast<char*>(buffer) + sizeof(int32_t);
  if (fixed_len_size > 0) v->len = std::min(v->len, fixed_len_size);
  // we still read byte_size bytes, even if we truncate
  return byte_size;
}

/// Write decimals as big endian (byte comparable) to benefit from common prefixes.
/// fixed_len_size can be less than sizeof(Decimal*Value) for space savings. This means
/// that the value in the in-memory format has leading zeros or negative 1's.
/// For example, precision 2 fits in 1 byte. All decimals stored as Decimal4Value
/// will have 3 bytes of leading zeros, we will only store the interesting byte.
template<>
inline int ParquetPlainEncoder::Encode(
    uint8_t* buffer, int fixed_len_size, const Decimal4Value& v) {
  DecimalUtil::EncodeToFixedLenByteArray(buffer, fixed_len_size, v);
  return fixed_len_size;
}

template<>
inline int ParquetPlainEncoder::Encode(
    uint8_t* buffer, int fixed_len_size, const Decimal8Value& v) {
  DecimalUtil::EncodeToFixedLenByteArray(buffer, fixed_len_size, v);
  return fixed_len_size;
}

template<>
inline int ParquetPlainEncoder::Encode(
    uint8_t* buffer, int fixed_len_size, const Decimal16Value& v) {
  DecimalUtil::EncodeToFixedLenByteArray(buffer, fixed_len_size, v);
  return fixed_len_size;
}

template<>
inline int ParquetPlainEncoder::Decode(uint8_t* buffer, const uint8_t* buffer_end,
    int fixed_len_size, Decimal4Value* v) {
  if (UNLIKELY(buffer_end - buffer < fixed_len_size)) return -1;
  DecimalUtil::DecodeFromFixedLenByteArray(buffer, fixed_len_size, v);
  return fixed_len_size;
}

template<>
inline int ParquetPlainEncoder::Decode(uint8_t* buffer, const uint8_t* buffer_end,
    int fixed_len_size, Decimal8Value* v) {
  if (UNLIKELY(buffer_end - buffer < fixed_len_size)) return -1;
  DecimalUtil::DecodeFromFixedLenByteArray(buffer, fixed_len_size, v);
  return fixed_len_size;
}

template<>
inline int ParquetPlainEncoder::Decode(uint8_t* buffer, const uint8_t* buffer_end,
    int fixed_len_size, Decimal16Value* v) {
  if (UNLIKELY(buffer_end - buffer < fixed_len_size)) return -1;
  DecimalUtil::DecodeFromFixedLenByteArray(buffer, fixed_len_size, v);
  return fixed_len_size;
}

}

#endif
