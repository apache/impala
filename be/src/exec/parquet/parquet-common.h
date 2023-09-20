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

#include <boost/preprocessor/repetition/repeat_from_to.hpp>
#include <sstream>

#include "common/compiler-util.h"
#include "gen-cpp/Descriptors_types.h"
#include "gen-cpp/parquet_types.h"
#include "runtime/date-value.h"
#include "runtime/decimal-value.h"
#include "runtime/string-value.h"
#include "runtime/timestamp-value.inline.h"
#include "util/bit-util.h"
#include "util/decimal-util.h"
#include "util/mem-util.h"

/// This file contains common elements between the parquet Writer and Scanner.
namespace impala {

const uint8_t PARQUET_VERSION_NUMBER[4] = {'P', 'A', 'R', '1'};
const uint32_t PARQUET_MAX_SUPPORTED_VERSION = 2;
const uint32_t PARQUET_WRITER_VERSION = 1;

/// Struct that specifies an inclusive range of rows.
struct RowRange {
  int64_t first;
  int64_t last;
};

inline bool operator==(const RowRange& lhs, const RowRange& rhs) {
  return lhs.first == rhs.first && lhs.last == rhs.last;
}

inline bool operator<(const RowRange& lhs, const RowRange& rhs) {
  return std::tie(lhs.first, lhs.last) < std::tie(rhs.first, rhs.last);
}

// Return true if this is an encoding enum value that indicates that a data page is
// dictionary encoded.
inline bool IsDictionaryEncoding(parquet::Encoding::type encoding) {
  return encoding == parquet::Encoding::PLAIN_DICTIONARY
      || encoding == parquet::Encoding::RLE_DICTIONARY;
}

/// Return the Impala compression type for the given Parquet codec. The caller must
/// validate that the codec is a supported one, otherwise this will DCHECK.
THdfsCompression::type ConvertParquetToImpalaCodec(parquet::CompressionCodec::type codec);

/// Return the Parquet code for the given Impala compression type. The caller must
/// validate that the codec is a supported one, otherwise this will DCHECK.
parquet::CompressionCodec::type ConvertImpalaToParquetCodec(
    THdfsCompression::type codec);

/// Returns the row range for the given page idx using information from the row group and
/// offset index.
void GetRowRangeForPage(const parquet::RowGroup& row_group,
    const parquet::OffsetIndex& offset_index, int page_idx, RowRange* row_range);

/// Struct that specifies an inclusive range of pages.
struct PageRange {
  int64_t first;
  int64_t last;

  PageRange(int64_t u, int64_t v) : first(u), last(v) {}
  bool overlap(const PageRange& other) const {
    return !(last < other.first || other.last < first);
  }
  bool operator==(const PageRange& other) const {
    return (first == other.first && last == other.last);
  }

  std::string toString() const {
    std::stringstream ss;
    ss << "[" << first << ", " << last << "]";
    return ss.str();
  }

  // Populate the bit vector 'pageIndices' by setting bits corresponding to the pages
  // in range [first, last] to on.
  void convertToIndices(std::vector<bool>* pageIndices) const {
    DCHECK(pageIndices);
    for (int i = first; i <= last; i++) {
      (*pageIndices)[i] = true;
    }
  }
};

inline bool IsValidPageLocation(const parquet::PageLocation& page_loc,
    const int64_t num_rows) {
  return page_loc.offset >= 0 &&
         page_loc.first_row_index >= 0 &&
         page_loc.first_row_index < num_rows;
}

/// Returns the row range for a given page range using information from the row group
/// and offset index.
void GetRowRangeForPageRange(const parquet::RowGroup& row_group,
    const parquet::OffsetIndex& offset_index, const PageRange& page_range,
    RowRange* row_range);

/// Given a column chunk containing rows in the range [0, 'num_rows'), 'skip_ranges'
/// contains the row ranges we are not interested in. 'skip_ranges' can be redundant and
/// can potentially contain ranges that intersect with each other. As a side-effect, this
/// function sorts 'skip_ranges'.
/// 'candidate_ranges' will contain the set of row ranges that we want to scan.
/// Returns false if the input data is invalid.
bool ComputeCandidateRanges(const int64_t num_rows, std::vector<RowRange>* skip_ranges,
    std::vector<RowRange>* candidate_ranges);

/// This function computes the pages that intersect with 'candidate_ranges'. I.e. it
/// determines the pages that we actually need to read from a given column chunk.
/// 'candidate_pages' will hold the indexes of such pages.
/// Returns true on success, false otherwise.
bool ComputeCandidatePages(
    const std::vector<parquet::PageLocation>& page_locations,
    const std::vector<RowRange>& candidate_ranges,
    const int64_t num_rows, std::vector<int>* candidate_pages);

/// The plain encoding does not maintain any state so all these functions
/// are static helpers.
/// TODO: we are using templates to provide a generic interface (over the
/// types) to avoid performance penalties. This makes the code more complex
/// and should be removed when we have codegen support to inline virtual
/// calls.
class ParquetPlainEncoder {
 public:
  /// Returns the byte size of 'v' where InternalType is the datatype that Impala uses
  /// internally to store tuple data. Used in some template function implementations to
  /// determine the encoded byte size for fixed-length types.
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
      case TYPE_DATE:
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

  /// Returns the byte size of a value of Parqet type `type`. If `type` is
  /// FIXED_LEN_BYTE_ARRAY, the argument `fixed_len_size` is returned.
  /// The type must be one of INT32, INT64, INT96, FLOAT, DOUBLE or FIXED_LEN_BYTE_ARRAY.
  /// BOOLEANs are not plain encoded and BYTE_ARRAYs do not have a fixed size, therefore
  /// they are not supported.
  static ALWAYS_INLINE int EncodedByteSize(const parquet::Type::type type,
      const int fixed_len_size) {
    switch (type) {
      case parquet::Type::type::INT32: return sizeof(int32_t);
      case parquet::Type::type::INT64: return sizeof(int64_t);
      case parquet::Type::type::INT96: return 12;
      case parquet::Type::type::FLOAT: return sizeof(float);
      case parquet::Type::type::DOUBLE: return sizeof(double);
      case parquet::Type::type::FIXED_LEN_BYTE_ARRAY: return fixed_len_size;
      default: DCHECK(false);
    };

    return -1;
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
        return -1;
    }
  }

  /// Decodes t from 'buffer', reading up to the byte before 'buffer_end'. 'buffer' need
  /// not be aligned. If PARQUET_TYPE is FIXED_LEN_BYTE_ARRAY then 'fixed_len_size' is the
  /// size of the object. Otherwise, it is unused. Returns the number of bytes read or -1
  /// if the value was not decoded successfully.
  /// This generic template function is used when PARQUET_TYPE is one of INT32, INT64,
  /// INT96, FLOAT, DOUBLE or FIXED_LEN_BYTE_ARRAY.
  template <typename InternalType, parquet::Type::type PARQUET_TYPE>
  static int Decode(const uint8_t* buffer, const uint8_t* buffer_end, int fixed_len_size,
      InternalType* v) {
    /// We cannot make it a static assert because the DecodeByParquetType template
    /// potentially calls this function with every combination of internal and parquet
    /// types, not only the valid ones. The invalid combinations do not occur at runtime,
    /// but they cannot be ruled out at compile time.
    DCHECK(PARQUET_TYPE == parquet::Type::type::INT32
        || PARQUET_TYPE == parquet::Type::type::INT64
        || PARQUET_TYPE == parquet::Type::type::INT96
        || PARQUET_TYPE == parquet::Type::type::FLOAT
        || PARQUET_TYPE == parquet::Type::type::DOUBLE
        || PARQUET_TYPE == parquet::Type::type::FIXED_LEN_BYTE_ARRAY);

    int byte_size = EncodedByteSize(PARQUET_TYPE, fixed_len_size);

    if (UNLIKELY(buffer_end - buffer < byte_size)) return -1;
    DecodeNoBoundsCheck<InternalType, PARQUET_TYPE>(buffer, buffer_end,
        fixed_len_size, v);
    return byte_size;
  }

  /// Returns the byte size of the encoded data when PLAIN encoding is used.
  /// Returns -1 if the encoded data passes the end of the buffer.
  template <parquet::Type::type PARQUET_TYPE>
  static int64_t EncodedLen(const uint8_t* buffer, const uint8_t* buffer_end,
      int fixed_len_size, int32_t num_values) {
    using parquet::Type;
    int byte_size = 0;
    switch (PARQUET_TYPE) {
      case Type::INT32: byte_size = 4; break;
      case Type::INT64: byte_size = 8; break;
      case Type::INT96: byte_size = 12; break;
      case Type::FLOAT: byte_size = 4; break;
      case Type::DOUBLE: byte_size = 8; break;
      case Type::FIXED_LEN_BYTE_ARRAY: byte_size = fixed_len_size; break;
      default:
        DCHECK(false);
        return -1;
    }
    int64_t encoded_len = byte_size * num_values;
    return encoded_len > buffer_end - buffer ? -1 : encoded_len;
  }

  /// Batched version of Decode() that tries to decode 'num_values' values from the memory
  /// range [buffer, buffer_end) and writes them to 'v' with a stride of 'stride' bytes.
  /// Returns the number of bytes read from 'buffer' or -1 if there was an error
  /// decoding, e.g. invalid data or running out of input data before reading
  /// 'num_values'.
  template <typename InternalType, parquet::Type::type PARQUET_TYPE>
  static int64_t DecodeBatch(const uint8_t* buffer, const uint8_t* buffer_end,
      int fixed_len_size, int64_t num_values, int64_t stride, InternalType* v);

  /// Decode 'source' by memcpy() sizeof(InternalType) bytes from 'source' to 'v'.
  template <typename InternalType>
  static ALWAYS_INLINE inline void DecodeNoBoundsCheck(
      const std::string& source, InternalType* v);

  /// Answer the question whether an Impala internal type 'type' has the same
  /// storage type as Parquet.
  static bool IsIdenticalToParquetStorageType(const ColumnType& type) {
    return (type.type == TYPE_INT || type.type == TYPE_BIGINT || type.type == TYPE_FLOAT
        || type.type == TYPE_DOUBLE);
  }

 private:
  /// Decode values without bounds checking. `buffer_end` is only used for DCHECKs in
  /// DEBUG mode, it is unused in RELEASE mode.
  template <typename InternalType, parquet::Type::type PARQUET_TYPE>
  static ALWAYS_INLINE inline void DecodeNoBoundsCheck(const uint8_t* buffer,
      const uint8_t* buffer_end, int fixed_len_size, InternalType* v);

  template <typename InternalType, parquet::Type::type PARQUET_TYPE>
  static int64_t DecodeBatchAlwaysBoundsCheck(const uint8_t* buffer,
      const uint8_t* buffer_end, int fixed_len_size, int64_t num_values, int64_t stride,
      InternalType* v);

  template <typename InternalType, parquet::Type::type PARQUET_TYPE>
  static int64_t DecodeBatchOneBoundsCheck(const uint8_t* buffer,
      const uint8_t* buffer_end, int fixed_len_size, int64_t num_values, int64_t stride,
      InternalType* v);
};

/// Calling this with arguments of type ColumnType is certainly a programmer error, so we
/// disallow it.
template <> int ParquetPlainEncoder::ByteSize(const ColumnType& t);

/// Disable for bools. Plain encoding is not used for booleans.
template <> int ParquetPlainEncoder::ByteSize(const bool& b);
template <> int ParquetPlainEncoder::Encode(const bool&, int fixed_len_size, uint8_t*);
template <> int ParquetPlainEncoder::Decode<bool, parquet::Type::BOOLEAN>(const uint8_t*,
    const uint8_t*, int fixed_len_size, bool* v);

template <>
inline int ParquetPlainEncoder::ByteSize(const Decimal4Value&) {
  // Only used when the decimal is stored as INT32.
  return sizeof(Decimal4Value::StorageType);
}
template <>
inline int ParquetPlainEncoder::ByteSize(const Decimal8Value&) {
  // Only used when the decimal is stored as INT64.
  return sizeof(Decimal8Value::StorageType);
}
template <>
inline int ParquetPlainEncoder::ByteSize(const Decimal16Value&) {
  // Not used, since such big decimals can only be stored as BYTE_ARRAY or
  // FIXED_LEN_BYTE_ARRAY.
  DCHECK(false);
  return -1;
}

/// Parquet doesn't have 8-bit or 16-bit ints. They are converted to 32-bit.
template <>
inline int ParquetPlainEncoder::ByteSize(const int8_t& v) { return sizeof(int32_t); }
template <>
inline int ParquetPlainEncoder::ByteSize(const int16_t& v) { return sizeof(int32_t); }

template <>
inline int ParquetPlainEncoder::ByteSize(const StringValue& v) {
  return sizeof(int32_t) + v.Len();
}

template <>
inline int ParquetPlainEncoder::ByteSize(const TimestampValue& v) {
  return 12;
}

/// Returns the byte size of the encoded data when PLAIN encoding is used.
/// Returns -1 if the encoded data passes the end of the buffer.
template <>
inline int64_t ParquetPlainEncoder::EncodedLen<parquet::Type::BYTE_ARRAY>(
    const uint8_t* buffer, const uint8_t* buffer_end, int fixed_len_size,
    int32_t num_values) {
  const uint8_t* orig_buffer = buffer;
  int64_t values_remaining = num_values;
  while (values_remaining > 0) {
    if (UNLIKELY(buffer_end - buffer < sizeof(int32_t))) return -1;
    int32_t str_len;
    memcpy(&str_len, buffer, sizeof(int32_t));
    str_len += sizeof(int32_t);
    if (UNLIKELY(str_len < sizeof(int32_t) || buffer_end - buffer < str_len)) return -1;
    buffer += str_len;
    --values_remaining;
  }
  return buffer - orig_buffer;
}

template <typename From, typename To>
inline int DecodeWithConversion(const uint8_t* buffer, const uint8_t* buffer_end, To* v) {
  int byte_size = sizeof(From);
  DCHECK_GE(buffer_end - buffer, byte_size);
  From dest;
  memcpy(&dest, buffer, byte_size);
  *v = dest;
  return byte_size;
}

/// Decodes a value without bounds checking.
/// This generic template function is used with the following types:
/// =============================
/// InternalType   | PARQUET_TYPE
/// =============================
/// int8_t         | INT32
/// int16_t        | INT32
/// int32_t        | INT32
/// int64_t        | INT64
/// float          | FLOAT
/// double         | DOUBLE
/// Decimal4Value  | INT32
/// Decimal8Value  | INT64
template <typename InternalType, parquet::Type::type PARQUET_TYPE>
void ParquetPlainEncoder::DecodeNoBoundsCheck(const uint8_t* buffer,
    const uint8_t* buffer_end, int fixed_len_size, InternalType* v) {
  int byte_size = EncodedByteSize(PARQUET_TYPE, -1);
  DCHECK_GE(buffer_end - buffer, byte_size);

  /// This generic template is only used when either no conversion is needed or with
  /// narrowing integer conversions (e.g. int32_t to int16_t or int8_t) where copying the
  /// lower bytes is the correct conversion.
  memcpy(v, buffer, sizeof(InternalType));
}

template <>
inline void ParquetPlainEncoder::
DecodeNoBoundsCheck<TimestampValue, parquet::Type::INT96>(const uint8_t* buffer,
    const uint8_t* buffer_end, int fixed_len_size, TimestampValue* v) {
  int byte_size = EncodedByteSize(parquet::Type::INT96, -1);
  DCHECK_GE(buffer_end - buffer, byte_size);

  /// We copy only 12 bytes from the input buffer but the destination is 16 bytes long
  /// because of padding. The most significant 4 bits remain uninitialized as they are not
  /// used.
  memcpy(v, buffer, byte_size);
}

template <>
inline void ParquetPlainEncoder::DecodeNoBoundsCheck<int64_t, parquet::Type::INT32>(
    const uint8_t* buffer, const uint8_t* buffer_end, int fixed_len_size, int64_t* v) {
  DecodeWithConversion<int32_t, int64_t>(buffer, buffer_end, v);
}

template <>
inline void ParquetPlainEncoder::DecodeNoBoundsCheck<double, parquet::Type::INT32>(
    const uint8_t* buffer, const uint8_t* buffer_end, int fixed_len_size, double* v) {
  DecodeWithConversion<int32_t, double>(buffer, buffer_end, v);
}

template <>
inline void ParquetPlainEncoder::DecodeNoBoundsCheck<double, parquet::Type::FLOAT>(
    const uint8_t* buffer, const uint8_t* buffer_end, int fixed_len_size, double* v) {
  DecodeWithConversion<float, double>(buffer, buffer_end, v);
}

/// The string source version of DecodeNoBoundsCheck() which works with the following
/// combination of internal and Parquet types requiring no conversions and validation.
/// =============================
/// InternalType   | PARQUET_TYPE
/// =============================
/// int32_t        | INT32
/// int64_t        | INT64
/// float          | FLOAT
/// double         | DOUBLE
template <typename InternalType>
inline void ParquetPlainEncoder::DecodeNoBoundsCheck(
    const string& source, InternalType* v) {
  DCHECK_GE(source.size(), sizeof(InternalType));
  memcpy(v, source.data(), sizeof(InternalType));
}

template <>
inline int ParquetPlainEncoder::Decode<DateValue, parquet::Type::type::INT32>(
    const uint8_t* buffer, const uint8_t* buffer_end, int fixed_len_size, DateValue* v) {
  int32_t val;
  if (UNLIKELY(buffer_end - buffer < sizeof(val))) return -1;
  memcpy(&val, buffer, sizeof(val));
  // DateValue constructor validates int32_t date value
  *v = DateValue(val);
  return sizeof(val);
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
  StringValue::SimpleString s = v.ToSimpleString();
  memcpy(buffer, &s.len, sizeof(int32_t));
  memcpy(buffer + sizeof(int32_t), s.ptr, s.len);
  return ByteSize(v);
}

template <>
inline int ParquetPlainEncoder::Decode<StringValue, parquet::Type::BYTE_ARRAY>(
    const uint8_t* buffer, const uint8_t* buffer_end, int fixed_len_size,
    StringValue* v) {
  if (UNLIKELY(buffer_end - buffer < sizeof(int32_t))) return -1;
  int str_len;
  memcpy(&str_len, buffer, sizeof(int32_t));
  v->Assign(reinterpret_cast<char*>(const_cast<uint8_t*>(buffer)) + sizeof(int32_t),
      str_len);
  int byte_size = ByteSize(*v);
  if (UNLIKELY(str_len < 0 || buffer_end - buffer < byte_size)) return -1;
  if (fixed_len_size > 0 && fixed_len_size < str_len) v->SetLen(fixed_len_size);
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

template <typename InternalType, parquet::Type::type PARQUET_TYPE>
inline int64_t ParquetPlainEncoder::DecodeBatchAlwaysBoundsCheck(const uint8_t* buffer,
    const uint8_t* buffer_end, int fixed_len_size, int64_t num_values, int64_t stride,
    InternalType* v) {
  const uint8_t* buffer_pos = buffer;
  StrideWriter<InternalType> out(v, stride);

  for (int64_t i = 0; i < num_values; ++i) {
    int encoded_len = Decode<InternalType, PARQUET_TYPE>(
        buffer_pos, buffer_end, fixed_len_size, out.Advance());
    if (UNLIKELY(encoded_len < 0)) return -1;
    buffer_pos += encoded_len;
  }

  return buffer_pos - buffer;
}

template <typename InternalType, parquet::Type::type PARQUET_TYPE>
inline int64_t ParquetPlainEncoder::DecodeBatchOneBoundsCheck(const uint8_t* buffer,
    const uint8_t* buffer_end, int fixed_len_size, int64_t num_values, int64_t stride,
    InternalType* v) {
  const uint8_t* buffer_pos = buffer;
  uint8_t* output = reinterpret_cast<uint8_t*>(v);
  const int byte_size_of_element = EncodedByteSize(PARQUET_TYPE, fixed_len_size);

  if (UNLIKELY(buffer_end - buffer < num_values * byte_size_of_element)) return -1;

  /// We unroll the loop manually in batches of 8.
  constexpr int batch = 8;
  const int full_batches = num_values / batch;
  const int remainder = num_values % batch;

  for (int b = 0; b < full_batches; b++) {
#pragma push_macro("DECODE_NO_CHECK_UNROLL")
#define DECODE_NO_CHECK_UNROLL(ignore1, i, ignore2) \
    DecodeNoBoundsCheck<InternalType, PARQUET_TYPE>( \
        buffer_pos + i * byte_size_of_element, buffer_end, fixed_len_size, \
        reinterpret_cast<InternalType*>(output + i * stride));

    BOOST_PP_REPEAT_FROM_TO(0, 8 /* The value of `batch` */,
        DECODE_NO_CHECK_UNROLL, ignore);
#pragma pop_macro("DECODE_NO_CHECK_UNROLL")

    output += batch * stride;
    buffer_pos += batch * byte_size_of_element;
  }

  StrideWriter<InternalType> out(reinterpret_cast<InternalType*>(output), stride);
  for (int i = 0; i < remainder; i++) {
    DecodeNoBoundsCheck<InternalType, PARQUET_TYPE>(
        buffer_pos, buffer_end, fixed_len_size, out.Advance());
    buffer_pos += byte_size_of_element;
  }

  DCHECK_EQ(buffer_pos - buffer, num_values * byte_size_of_element);
  return buffer_pos - buffer;
}

template <typename InternalType, parquet::Type::type PARQUET_TYPE>
inline int64_t ParquetPlainEncoder::DecodeBatch(const uint8_t* buffer,
    const uint8_t* buffer_end, int fixed_len_size, int64_t num_values, int64_t stride,
    InternalType* v) {
  /// Whether bounds checking needs to be done for every element or we can check the whole
  /// batch at the same time.
  constexpr bool has_variable_length =
      PARQUET_TYPE == parquet::Type::type::BYTE_ARRAY;
  if (has_variable_length) {
    return DecodeBatchAlwaysBoundsCheck<InternalType, PARQUET_TYPE>(buffer, buffer_end,
        fixed_len_size, num_values, stride, v);
  } else {
    return DecodeBatchOneBoundsCheck<InternalType, PARQUET_TYPE>(buffer, buffer_end,
        fixed_len_size, num_values, stride, v);
  }
}

template <typename T>
inline void DecodeDecimalFixedLen(
    const uint8_t* buffer, const uint8_t* buffer_end, int fixed_len_size, T* v) {
  DCHECK_GE(buffer_end - buffer, fixed_len_size);
  DecimalUtil::DecodeFromFixedLenByteArray(buffer, fixed_len_size, v);
}

template <>
inline int ParquetPlainEncoder::
Decode<bool, parquet::Type::BOOLEAN>(const uint8_t* buffer,
    const uint8_t* buffer_end, int fixed_len_size, bool* v) {
  DCHECK(false) << "Use ParquetBoolDecoder for decoding bools";
  return -1;
}

template <>
inline void ParquetPlainEncoder::
DecodeNoBoundsCheck<Decimal4Value, parquet::Type::FIXED_LEN_BYTE_ARRAY>(
    const uint8_t* buffer, const uint8_t* buffer_end, int fixed_len_size,
    Decimal4Value* v) {
  DecodeDecimalFixedLen(buffer, buffer_end, fixed_len_size, v);
}

template <>
inline void ParquetPlainEncoder::
DecodeNoBoundsCheck<Decimal8Value, parquet::Type::FIXED_LEN_BYTE_ARRAY>(
    const uint8_t* buffer, const uint8_t* buffer_end, int fixed_len_size,
    Decimal8Value* v) {
  DecodeDecimalFixedLen(buffer, buffer_end, fixed_len_size, v);
}

template <>
inline void ParquetPlainEncoder::
DecodeNoBoundsCheck<Decimal16Value, parquet::Type::FIXED_LEN_BYTE_ARRAY>(
    const uint8_t* buffer, const uint8_t* buffer_end, int fixed_len_size,
    Decimal16Value* v) {
  DecodeDecimalFixedLen(buffer, buffer_end, fixed_len_size, v);
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

/// Helper class that contains the parameters needed for Timestamp decoding.
/// Can be safely passed by value.
class ParquetTimestampDecoder {
public:
  ParquetTimestampDecoder() {}

  ParquetTimestampDecoder( const parquet::SchemaElement& e, const Timezone* timezone,
      bool convert_int96_timestamps);

  bool NeedsConversion() const { return timezone_ != nullptr; }

  /// Decodes next PARQUET_TYPE from 'buffer', reading up to the byte before 'buffer_end'
  /// and converts it TimestampValue. 'buffer' need not be aligned.
  template <parquet::Type::type PARQUET_TYPE>
  int Decode(const uint8_t* buffer, const uint8_t* buffer_end, TimestampValue* v) const;

  /// Batched version of Decode() that tries to decode 'num_values' values from the memory
  /// range [buffer, buffer_end) and writes them to 'v' with a stride of 'stride' bytes.
  /// Returns the number of bytes read from 'buffer' or -1 if there was an error
  /// decoding, e.g. invalid data or running out of input data before reading
  /// 'num_values'.
  template <parquet::Type::type PARQUET_TYPE>
  int64_t DecodeBatch(const uint8_t* buffer, const uint8_t* buffer_end,
      int64_t num_values, int64_t stride, TimestampValue* v);

  TimestampValue Int64ToTimestampValue(int64_t unix_time) const {
    switch (precision_) {
    case MILLI:
      return TimestampValue::UtcFromUnixTimeMillis(unix_time);
    case MICRO:
      return TimestampValue::UtcFromUnixTimeMicros(unix_time);
    case NANO:
      return TimestampValue::UtcFromUnixTimeLimitedRangeNanos(unix_time);
    default:
      DCHECK(false);
      return TimestampValue();
    }
  }

  void ConvertToLocalTime(TimestampValue* v) const {
    DCHECK(timezone_ != nullptr);
    if (v->HasDateAndTime()) v->UtcToLocal(*timezone_);
  }

  /// Timezone conversion of min/max stats need some extra logic because UTC->local
  /// conversion can change ordering near timezone rule changes. The max value is
  /// increased and min value is decreased to avoid incorrectly dropping column chunks
  /// (or pages once IMPALA-5843 is ready).

  /// If timestamp t >= v before conversion, then this function converts v in such a
  /// way that the same will be true after t is converted.
  void ConvertMinStatToLocalTime(TimestampValue* v) const;

  /// If timestamp t <= v before conversion, then this function converts v in such a
  /// way that the same will be true after t is converted.
  void ConvertMaxStatToLocalTime(TimestampValue* v) const;

  enum Precision { MILLI, MICRO, NANO };

  /// Processes the Parquet schema element 'e' and extracts timestamp related parameters.
  /// Returns true if the schema describes a valid timestamp column.
  /// 'precision': unit of the timestamp
  /// 'needs_conversion': whether utc->local conversion is necessary
  static bool GetTimestampInfoFromSchema(const parquet::SchemaElement& e,
      Precision& precision, bool& needs_conversion);

private:
  /// Timezone used for UTC->Local conversions. If it is UTCPTR, no conversion is needed.
  const Timezone* timezone_ = UTCPTR;

  /// Unit of the encoded timestamp. Used to decide between milli and microseconds during
  /// INT64 decoding. INT64 with nanosecond precision (and reduced range) is also planned
  /// to be implemented once it is added in Parquet (PARQUET-1387).
  Precision precision_ = NANO;
};

template <>
inline int ParquetTimestampDecoder::Decode<parquet::Type::INT64>(
    const uint8_t* buffer, const uint8_t* buffer_end, TimestampValue* v) const {
  int64_t unix_time;
  int bytes_read = ParquetPlainEncoder::Decode<int64_t, parquet::Type::INT64>(
      buffer, buffer_end, 0, &unix_time);
  if (UNLIKELY(bytes_read < 0)) {
    return bytes_read;
  }
  *v = Int64ToTimestampValue(unix_time);
  // TODO: It would be more efficient to do the timezone conversion in the same step
  //       as the int64_t -> TimestampValue conversion. This would be also needed to
  //       move conversion/validation to dictionary construction (IMPALA-4994) and to
  //       implement dictionary filtering for TimestampValues.
  return bytes_read;
}

template <>
inline int ParquetTimestampDecoder::Decode<parquet::Type::INT96>(
    const uint8_t* buffer, const uint8_t* buffer_end, TimestampValue* v) const {
  DCHECK_EQ(precision_, NANO);
  return ParquetPlainEncoder::Decode<TimestampValue, parquet::Type::INT96>(
      buffer, buffer_end, 0, v);
}

template <parquet::Type::type PARQUET_TYPE>
inline int64_t ParquetTimestampDecoder::DecodeBatch(const uint8_t* buffer,
    const uint8_t* buffer_end, int64_t num_values, int64_t stride,
    TimestampValue* v) {
  const uint8_t* buffer_pos = buffer;
  StrideWriter<TimestampValue> out(v, stride);
  for (int64_t i = 0; i < num_values; ++i) {
    int encoded_len = Decode<PARQUET_TYPE>(buffer_pos, buffer_end, out.Advance());
    if (UNLIKELY(encoded_len < 0)) return -1;
    buffer_pos += encoded_len;
  }
  return buffer_pos - buffer;
}
}
#endif
