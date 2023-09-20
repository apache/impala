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

#include "exec/parquet/parquet-bloom-filter-util.h"

#include <sstream>

#include "exprs/scalar-expr-evaluator.h"
#include "util/parquet-bloom-filter.h"

namespace impala {

bool IsParquetBloomFilterSupported(parquet::Type::type parquet_type,
    const ColumnType& impala_type) {
  switch (parquet_type) {
    case parquet::Type::INT32: {
      switch (impala_type.type) {
        case PrimitiveType::TYPE_TINYINT:
        case PrimitiveType::TYPE_SMALLINT:
        case PrimitiveType::TYPE_INT:
          return true;
        default:
          return false;
      }
    }
    case parquet::Type::INT64: {
      return impala_type.type == PrimitiveType::TYPE_BIGINT;
    }
    case parquet::Type::FLOAT: {
      return impala_type.type == PrimitiveType::TYPE_FLOAT;
    }
    case parquet::Type::DOUBLE: {
      return impala_type.type == PrimitiveType::TYPE_DOUBLE;
    }
    case parquet::Type::BYTE_ARRAY: {
      return impala_type.type == PrimitiveType::TYPE_STRING;
    }
    default:
      return false;
  }
}

Status ValidateBloomFilterHeader(const parquet::BloomFilterHeader& bloom_filter_header) {
  if (!bloom_filter_header.algorithm.__isset.BLOCK) {
    std::stringstream ss;
    ss << "Unsupported Bloom filter algorithm: "
        << bloom_filter_header.algorithm
        << ".";
    return Status(ss.str());
  }

  if (!bloom_filter_header.hash.__isset.XXHASH) {
    std::stringstream ss;
    ss << "Unsupported Bloom filter hash: "
        << bloom_filter_header.hash
        << ".";
    return Status(ss.str());
  }

  if (!bloom_filter_header.compression.__isset.UNCOMPRESSED) {
    std::stringstream ss;
    ss << "Unsupported Bloom filter compression: "
        << bloom_filter_header.compression
        << ".";
    return Status(ss.str());
  }

  if (bloom_filter_header.numBytes <= 0
      || bloom_filter_header.numBytes > ParquetBloomFilter::MAX_BYTES) {
    std::stringstream ss;
    ss << "Bloom filter size is incorrect: " << bloom_filter_header.numBytes
       << ". Must be in range (" << 0 << ", " << ParquetBloomFilter::MAX_BYTES << "].";
    return Status(ss.str());
  }

  return Status::OK();
}

Status GetUnsupportedConversionStatus(const ColumnType& from, parquet::Type::type to) {
  const char* parquet_type_name = parquet::_Type_VALUES_TO_NAMES.at(to);
  std::stringstream ss;
  ss << "Conversion from " << from << " to "
     << parquet_type_name << " is not supported.";
  return Status(ss.str());
}

template <typename T>
inline void ParquetBloomFilterEncodeValue(const void* value, vector<uint8_t>* storage,
    size_t expected_len) {
  static_assert(!std::is_same<T, StringValue>::value,
      "StringValues are encoded differently in ParquetBloom filters, "
      "their length is not included.");
  storage->resize(expected_len);
  const T* const cast_value = reinterpret_cast<const T*>(value);
  const int byte_len = ParquetPlainEncoder::Encode(*cast_value,
      -1 /* fixed_len_size */, storage->data());
  DCHECK_EQ(byte_len, expected_len);
}

void BytesToParquetInt(const void* value, const ColumnType& impala_type,
    const parquet::Type::type& type, vector<uint8_t>* storage,
    uint8_t** ptr, size_t* len) {
  DCHECK(type == parquet::Type::INT32 || type == parquet::Type::INT64);
  const int output_len = (type == parquet::Type::INT32) ?
            sizeof(int32_t) : sizeof(int64_t);

  if (type == parquet::Type::INT64) {
    // We only support encoding 8 and 16 bit ints as INT32.
    DCHECK_EQ(impala_type.type, TYPE_BIGINT);
  }

  if (type == parquet::Type::INT32) {
    DCHECK(impala_type.type == TYPE_TINYINT || impala_type.type == TYPE_SMALLINT
        || impala_type.type == TYPE_INT);
  }

  switch (impala_type.type) {
    case TYPE_TINYINT: {
      ParquetBloomFilterEncodeValue<int8_t>(value, storage, output_len);
      break;
    }
    case TYPE_SMALLINT: {
      ParquetBloomFilterEncodeValue<int16_t>(value, storage, output_len);
      break;
    }
    case TYPE_INT: {
      ParquetBloomFilterEncodeValue<int32_t>(value, storage, output_len);
      break;
    }
    case TYPE_BIGINT: {
      ParquetBloomFilterEncodeValue<int64_t>(value, storage, output_len);
      break;
    }
    default: {
      DCHECK(false);
    }
  }

  *ptr = storage->data();
  *len = output_len;
}

void BytesToParquetFloatingPoint(const void* value, const ColumnType& impala_type,
    const parquet::Type::type& type, vector<uint8_t>* storage,
    uint8_t** ptr, size_t* len) {
  DCHECK(type == parquet::Type::FLOAT || type == parquet::Type::DOUBLE);

  if (type == parquet::Type::FLOAT) {
    DCHECK_EQ(impala_type.type, PrimitiveType::TYPE_FLOAT);
    ParquetBloomFilterEncodeValue<float>(value, storage, sizeof(float));
  } else {
    DCHECK_EQ(impala_type.type, PrimitiveType::TYPE_DOUBLE);
    ParquetBloomFilterEncodeValue<double>(value, storage, sizeof(double));
  }

  *ptr = storage->data();
  *len = storage->size();
}

void BytesToParquetString(const void* value, const ColumnType& impala_type,
    const parquet::Type::type& type, uint8_t** ptr, size_t* len) {
  DCHECK(type == parquet::Type::BYTE_ARRAY);
  DCHECK_EQ(impala_type.type, PrimitiveType::TYPE_STRING);

  const StringValue* str_val = reinterpret_cast<const StringValue*>(value);
  DCHECK(str_val != nullptr);

  // Here we do not use the plain encoder because we do not include the length in the
  // hash of the string.
  *ptr = reinterpret_cast<uint8_t*>(const_cast<char*>(str_val->Ptr()));
  *len = str_val->Len();
}

Status BytesToParquetType(const void* value, const ColumnType& impala_type,
    const parquet::Type::type& parquet_type, vector<uint8_t>* storage,
    uint8_t** ptr, size_t* len) {
  DCHECK(storage != nullptr);

  if (!IsParquetBloomFilterSupported(parquet_type, impala_type)) {
    return GetUnsupportedConversionStatus(impala_type, parquet_type);
  }

  if (parquet_type == parquet::Type::INT32 || parquet_type == parquet::Type::INT64) {
    BytesToParquetInt(value, impala_type, parquet_type, storage, ptr, len);
  }

  if (parquet_type == parquet::Type::FLOAT || parquet_type == parquet::Type::DOUBLE) {
    BytesToParquetFloatingPoint(value, impala_type, parquet_type, storage, ptr, len);
  }

  if (parquet_type == parquet::Type::BYTE_ARRAY) {
    BytesToParquetString(value, impala_type, parquet_type, ptr, len);
  }

  return Status::OK();
}

Status LiteralToParquetType(const Literal& literal, ScalarExprEvaluator* eval,
    const parquet::Type::type& parquet_type, vector<uint8_t>* storage,
    uint8_t** ptr, size_t* len) {
  void* value = eval->GetValue(literal, nullptr);
  return BytesToParquetType(value, literal.type(), parquet_type, storage, ptr, len);
}

parquet::BloomFilterHeader CreateBloomFilterHeader(const ParquetBloomFilter& bloom_filter)
{
  parquet::BloomFilterHeader header;
  header.algorithm.__set_BLOCK(parquet::SplitBlockAlgorithm());
  header.hash.__set_XXHASH(parquet::XxHash());
  header.compression.__set_UNCOMPRESSED(parquet::Uncompressed());
  header.__set_numBytes(bloom_filter.directory_size());
  return header;
}

} // namespace impala
