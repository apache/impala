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

/// Return the Parquet type corresponding to Impala's internal type. The caller must
/// validate that the type is valid, otherwise this will DCHECK.
parquet::Type::type ConvertInternalToParquetType(PrimitiveType type);

/// Return the Impala compression type for the given Parquet codec. The caller must
/// validate that the codec is a supported one, otherwise this will DCHECK.
THdfsCompression::type ConvertParquetToImpalaCodec(parquet::CompressionCodec::type codec);

/// Return the Parquet code for the given Impala compression type. The caller must
/// validate that the codec is a supported one, otherwise this will DCHECK.
parquet::CompressionCodec::type ConvertImpalaToParquetCodec(
    THdfsCompression::type codec);

/// The plain encoding does not maintain any state so all these functions
/// are static helpers.
/// TODO: we are using templates to provide a generic interface (over the
/// types) to avoid performance penalties. This makes the code more complex
/// and should be removed when we have codegen support to inline virtual
/// calls.
class ParquetPlainEncoder {
 public:
  /// Returns the byte size of 'v' where InternalType is the datatype that Impala uses
  /// internally to store tuple data.
  template <typename InternalType>
  static int ByteSize(const InternalType& v) { return sizeof(InternalType); }

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
  template <typename InternalType>
  static int Encode(const InternalType& t, int fixed_len_size, uint8_t* buffer) {
    memcpy(buffer, &t, ByteSize(t));
    return ByteSize(t);
  }

  template <typename InternalType>
  static int DecodeByParquetType(const uint8_t* buffer, const uint8_t* buffer_end,
      int fixed_len_size, InternalType* v, parquet::Type::type parquet_type) {
    switch (parquet_type) {
      case parquet::Type::BOOLEAN:
        return ParquetPlainEncoder::Decode<InternalType, parquet::Type::BOOLEAN>(buffer,
            buffer_end, fixed_len_size, v);
      case parquet::Type::INT32:
        return ParquetPlainEncoder::Decode<InternalType, parquet::Type::INT32>(buffer,
            buffer_end, fixed_len_size, v);
      case parquet::Type::INT64:
        return ParquetPlainEncoder::Decode<InternalType, parquet::Type::INT64>(buffer,
            buffer_end, fixed_len_size, v);
      case parquet::Type::INT96:
        return ParquetPlainEncoder::Decode<InternalType, parquet::Type::INT96>(buffer,
            buffer_end, fixed_len_size, v);
      case parquet::Type::FLOAT:
        return ParquetPlainEncoder::Decode<InternalType, parquet::Type::FLOAT>(buffer,
            buffer_end, fixed_len_size, v);
      case parquet::Type::DOUBLE:
        return ParquetPlainEncoder::Decode<InternalType, parquet::Type::DOUBLE>(buffer,
            buffer_end, fixed_len_size, v);
      case parquet::Type::BYTE_ARRAY:
        return ParquetPlainEncoder::Decode<InternalType,
            parquet::Type::BYTE_ARRAY>(buffer, buffer_end, fixed_len_size, v);
      case parquet::Type::FIXED_LEN_BYTE_ARRAY:
        return ParquetPlainEncoder::Decode<InternalType,
            parquet::Type::FIXED_LEN_BYTE_ARRAY>(buffer, buffer_end, fixed_len_size, v);
      default:
        DCHECK(false) << "Unexpected physical type";
    }
  }

  /// Decodes t from 'buffer', reading up to the byte before 'buffer_end'. 'buffer'
  /// need not be aligned. If PARQUET_TYPE is FIXED_LEN_BYTE_ARRAY then 'fixed_len_size'
  /// is the size of the object. Otherwise, it is unused.
  /// Returns the number of bytes read or -1 if the value was not decoded successfully.
  template <typename InternalType, parquet::Type::type PARQUET_TYPE>
  static int Decode(const uint8_t* buffer, const uint8_t* buffer_end, int fixed_len_size,
      InternalType* v) {
    int byte_size = ByteSize(*v);
    if (UNLIKELY(buffer_end - buffer < byte_size)) return -1;
    memcpy(v, buffer, byte_size);
    return byte_size;
  }
};

/// Calling this with arguments of type ColumnType is certainly a programmer error, so we
/// disallow it.
template <> int ParquetPlainEncoder::ByteSize(const ColumnType& t);

/// Disable for bools. Plain encoding is not used for booleans.
template <> int ParquetPlainEncoder::ByteSize(const bool& b);
template <> int ParquetPlainEncoder::Encode(const bool&, int fixed_len_size, uint8_t*);
template <> int ParquetPlainEncoder::Decode<bool, parquet::Type::BOOLEAN>(const uint8_t*,
    const uint8_t*, int fixed_len_size, bool* v);

/// Not used for decimals since the plain encoding encodes them using
/// FIXED_LEN_BYTE_ARRAY.
inline int DecimalByteSize() {
  DCHECK(false);
  return -1;
}

template <>
inline int ParquetPlainEncoder::ByteSize(const Decimal4Value&) {
  return DecimalByteSize();
}
template <>
inline int ParquetPlainEncoder::ByteSize(const Decimal8Value&) {
  return DecimalByteSize();
}
template <>
inline int ParquetPlainEncoder::ByteSize(const Decimal16Value&) {
  return DecimalByteSize();
}

/// Parquet doesn't have 8-bit or 16-bit ints. They are converted to 32-bit.
template <>
inline int ParquetPlainEncoder::ByteSize(const int8_t& v) { return sizeof(int32_t); }
template <>
inline int ParquetPlainEncoder::ByteSize(const int16_t& v) { return sizeof(int32_t); }

template <>
inline int ParquetPlainEncoder::ByteSize(const StringValue& v) {
  return sizeof(int32_t) + v.len;
}

template <>
inline int ParquetPlainEncoder::ByteSize(const TimestampValue& v) {
  return 12;
}

template <>
inline int ParquetPlainEncoder::Decode<int8_t, parquet::Type::INT32>(
    const uint8_t* buffer, const uint8_t* buffer_end, int fixed_len_size, int8_t* v) {
  int byte_size = ByteSize(*v);
  if (UNLIKELY(buffer_end - buffer < byte_size)) return -1;
  *v = *buffer;
  return byte_size;
}
template <>
inline int ParquetPlainEncoder::Decode<int16_t, parquet::Type::INT32>(
    const uint8_t* buffer, const uint8_t* buffer_end, int fixed_len_size, int16_t* v) {
  int byte_size = ByteSize(*v);
  if (UNLIKELY(buffer_end - buffer < byte_size)) return -1;
  memcpy(v, buffer, sizeof(int16_t));
  return byte_size;
}

template<typename T>
inline int EncodeToInt32(const T& v, int fixed_len_size, uint8_t* buffer) {
  int32_t val = v;
  memcpy(buffer, &val, sizeof(int32_t));
  return ParquetPlainEncoder::ByteSize(v);
}

template <>
inline int ParquetPlainEncoder::Encode(
    const int8_t& v, int fixed_len_size, uint8_t* buffer) {
  return EncodeToInt32(v, fixed_len_size, buffer);
}

template <>
inline int ParquetPlainEncoder::Encode(
    const int16_t& v, int fixed_len_size, uint8_t* buffer) {
  return EncodeToInt32(v, fixed_len_size, buffer);
}

template <>
inline int ParquetPlainEncoder::Encode(
    const StringValue& v, int fixed_len_size, uint8_t* buffer) {
  memcpy(buffer, &v.len, sizeof(int32_t));
  memcpy(buffer + sizeof(int32_t), v.ptr, v.len);
  return ByteSize(v);
}

template <>
inline int ParquetPlainEncoder::Decode<StringValue, parquet::Type::BYTE_ARRAY>(
    const uint8_t* buffer, const uint8_t* buffer_end, int fixed_len_size,
    StringValue* v) {
  if (UNLIKELY(buffer_end - buffer < sizeof(int32_t))) return -1;
  memcpy(&v->len, buffer, sizeof(int32_t));
  int byte_size = ByteSize(*v);
  if (UNLIKELY(v->len < 0 || buffer_end - buffer < byte_size)) return -1;
  v->ptr = reinterpret_cast<char*>(const_cast<uint8_t*>(buffer)) + sizeof(int32_t);
  if (fixed_len_size > 0) v->len = std::min(v->len, fixed_len_size);
  // we still read byte_size bytes, even if we truncate
  return byte_size;
}

/// Write decimals as big endian (byte comparable) to benefit from common prefixes.
/// fixed_len_size can be less than sizeof(Decimal*Value) for space savings. This means
/// that the value in the in-memory format has leading zeros or negative 1's.
/// For example, precision 2 fits in 1 byte. All decimals stored as Decimal4Value
/// will have 3 bytes of leading zeros, we will only store the interesting byte.
template<typename T>
inline int EncodeDecimal(const T& v, int fixed_len_size, uint8_t* buffer) {
  DecimalUtil::EncodeToFixedLenByteArray(buffer, fixed_len_size, v);
  return fixed_len_size;
}

template <>
inline int ParquetPlainEncoder::Encode(
    const Decimal4Value& v, int fixed_len_size, uint8_t* buffer) {
  return EncodeDecimal(v, fixed_len_size, buffer);
}

template <>
inline int ParquetPlainEncoder::Encode(
    const Decimal8Value& v, int fixed_len_size, uint8_t* buffer) {
  return EncodeDecimal(v, fixed_len_size, buffer);
}

template <>
inline int ParquetPlainEncoder::Encode(
    const Decimal16Value& v, int fixed_len_size, uint8_t* buffer) {
  return EncodeDecimal(v, fixed_len_size, buffer);
}

template<typename T>
inline int DecodeDecimalFixedLen(const uint8_t* buffer, const uint8_t* buffer_end,
    int fixed_len_size, T* v) {
  if (UNLIKELY(buffer_end - buffer < fixed_len_size)) return -1;
  DecimalUtil::DecodeFromFixedLenByteArray(buffer, fixed_len_size, v);
  return fixed_len_size;
}

template <>
inline int ParquetPlainEncoder::
Decode<Decimal4Value, parquet::Type::FIXED_LEN_BYTE_ARRAY>(const uint8_t* buffer,
    const uint8_t* buffer_end, int fixed_len_size, Decimal4Value* v) {
  return DecodeDecimalFixedLen(buffer, buffer_end, fixed_len_size, v);
}

template <>
inline int ParquetPlainEncoder::
Decode<Decimal8Value, parquet::Type::FIXED_LEN_BYTE_ARRAY>(const uint8_t* buffer,
    const uint8_t* buffer_end, int fixed_len_size, Decimal8Value* v) {
  return DecodeDecimalFixedLen(buffer, buffer_end, fixed_len_size, v);
}

template <>
inline int ParquetPlainEncoder::
Decode<Decimal16Value, parquet::Type::FIXED_LEN_BYTE_ARRAY>(const uint8_t* buffer,
    const uint8_t* buffer_end, int fixed_len_size, Decimal16Value* v) {
  return DecodeDecimalFixedLen(buffer, buffer_end, fixed_len_size, v);
}

/// Helper method to decode Decimal type stored as variable length byte array.
template<typename T>
inline int DecodeDecimalByteArray(const uint8_t* buffer, const uint8_t* buffer_end,
    int fixed_len_size, T* v) {
  if (UNLIKELY(buffer_end - buffer < sizeof(int32_t))) return -1;
  int encoded_byte_size;
  memcpy(&encoded_byte_size, buffer, sizeof(int32_t));
  int byte_size = sizeof(int32_t) + encoded_byte_size;
  if (UNLIKELY(encoded_byte_size < 0 || buffer_end - buffer < byte_size)) return -1;
  uint8_t* val_ptr = const_cast<uint8_t*>(buffer) + sizeof(int32_t);
  DecimalUtil::DecodeFromFixedLenByteArray(val_ptr, encoded_byte_size, v);
  return byte_size;
}

template <>
inline int ParquetPlainEncoder::Decode<Decimal4Value, parquet::Type::BYTE_ARRAY>(
    const uint8_t* buffer, const uint8_t* buffer_end, int fixed_len_size,
    Decimal4Value* v) {
  return DecodeDecimalByteArray(buffer, buffer_end, fixed_len_size, v);
}

template <>
inline int ParquetPlainEncoder::Decode<Decimal8Value, parquet::Type::BYTE_ARRAY>(
    const uint8_t* buffer, const uint8_t* buffer_end, int fixed_len_size,
    Decimal8Value* v) {
  return DecodeDecimalByteArray(buffer, buffer_end, fixed_len_size, v);
}

template <>
inline int ParquetPlainEncoder::Decode<Decimal16Value, parquet::Type::BYTE_ARRAY>(
    const uint8_t* buffer, const uint8_t* buffer_end, int fixed_len_size,
    Decimal16Value* v) {
  return DecodeDecimalByteArray(buffer, buffer_end, fixed_len_size, v);
}

}
#endif
