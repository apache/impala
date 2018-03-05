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

#include "exec/parquet-common.h"

namespace impala {

/// Mapping of impala's internal types to parquet storage types. This is indexed by
/// PrimitiveType enum
const parquet::Type::type INTERNAL_TO_PARQUET_TYPES[] = {
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

const int INTERNAL_TO_PARQUET_TYPES_SIZE =
  sizeof(INTERNAL_TO_PARQUET_TYPES) / sizeof(INTERNAL_TO_PARQUET_TYPES[0]);

/// Mapping of Parquet codec enums to Impala enums
const THdfsCompression::type PARQUET_TO_IMPALA_CODEC[] = {
  THdfsCompression::NONE,
  THdfsCompression::SNAPPY,
  THdfsCompression::GZIP,
  THdfsCompression::LZO
};

const int PARQUET_TO_IMPALA_CODEC_SIZE =
    sizeof(PARQUET_TO_IMPALA_CODEC) / sizeof(PARQUET_TO_IMPALA_CODEC[0]);

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

const int IMPALA_TO_PARQUET_CODEC_SIZE =
    sizeof(IMPALA_TO_PARQUET_CODEC) / sizeof(IMPALA_TO_PARQUET_CODEC[0]);

parquet::Type::type ConvertInternalToParquetType(PrimitiveType type) {
  DCHECK_GE(type, 0);
  DCHECK_LT(type, INTERNAL_TO_PARQUET_TYPES_SIZE);
  return INTERNAL_TO_PARQUET_TYPES[type];
}

THdfsCompression::type ConvertParquetToImpalaCodec(
    parquet::CompressionCodec::type codec) {
  DCHECK_GE(codec, 0);
  DCHECK_LT(codec, PARQUET_TO_IMPALA_CODEC_SIZE);
  return PARQUET_TO_IMPALA_CODEC[codec];
}

parquet::CompressionCodec::type ConvertImpalaToParquetCodec(
    THdfsCompression::type codec) {
  DCHECK_GE(codec, 0);
  DCHECK_LT(codec, IMPALA_TO_PARQUET_CODEC_SIZE);
  return IMPALA_TO_PARQUET_CODEC[codec];
}
}
