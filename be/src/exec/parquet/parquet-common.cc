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

#include "exec/parquet/parquet-common.h"

namespace impala {

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

bool ParquetTimestampDecoder::GetTimestampInfoFromSchema(const parquet::SchemaElement& e,
    Precision& precision, bool& needs_conversion) {
  if (e.type == parquet::Type::INT96) {
    // Metadata does not contain information about being UTC normalized or not. The
    // caller may override 'needs_conversion' depending on flags and writer.
    needs_conversion = false;
    precision = NANO;
    return true;
  } else if (e.type != parquet::Type::INT64) {
    // Timestamps can be only encoded as INT64 or INT96, return false for other types.
    return false;
  }

  if (e.__isset.logicalType) {
    if (!e.logicalType.__isset.TIMESTAMP) return false;

    // Logical type (introduced in PARQUET-1253) contains explicit information about
    // being UTC normalized or not.
    needs_conversion = e.logicalType.TIMESTAMP.isAdjustedToUTC;

    if (e.logicalType.TIMESTAMP.unit.__isset.MILLIS) {
      precision = ParquetTimestampDecoder::MILLI;
    }
    else if (e.logicalType.TIMESTAMP.unit.__isset.MICROS) {
      precision = ParquetTimestampDecoder::MICRO;
    }
    else if (e.logicalType.TIMESTAMP.unit.__isset.NANOS) {
      precision = ParquetTimestampDecoder::NANO;
    } else {
      return false;
    }
  } else if (e.__isset.converted_type) {
    // Converted type does not contain information about being UTC normalized or not.
    // Timestamp with converted type but without logical type are/were never written
    // by Impala, so it is assumed that the writer is Parquet-mr and that timezone
    // conversion is needed.
    needs_conversion = true;
    if (e.converted_type == parquet::ConvertedType::TIMESTAMP_MILLIS) {
      precision = ParquetTimestampDecoder::MILLI;
    }
    else if (e.converted_type == parquet::ConvertedType::TIMESTAMP_MICROS) {
      precision = ParquetTimestampDecoder::MICRO;
    } else {
      // There is no TIMESTAMP_NANO converted type.
      return false;
    }
  } else {
    // Either logical or converted type must be set for int64 timestamps.
    return false;
  }
  return true;
}

ParquetTimestampDecoder::ParquetTimestampDecoder(const parquet::SchemaElement& e,
    const Timezone* timezone, bool convert_int96_timestamps) {
  bool needs_conversion = false;
  bool valid_schema = GetTimestampInfoFromSchema(e, precision_, needs_conversion);
  DCHECK(valid_schema); // Invalid schemas should be rejected in an earlier step.
  if (e.type == parquet::Type::INT96 && convert_int96_timestamps) needs_conversion = true;
  if (needs_conversion) timezone_ = timezone;
}

void ParquetTimestampDecoder::ConvertMinStatToLocalTime(TimestampValue* v) const {
  DCHECK(timezone_ != nullptr);
  if (!v->HasDateAndTime()) return;
  TimestampValue repeated_period_start;
  v->UtcToLocal(*timezone_, &repeated_period_start);
  if (repeated_period_start.HasDateAndTime()) *v = repeated_period_start;
}

void ParquetTimestampDecoder::ConvertMaxStatToLocalTime(TimestampValue* v) const {
  DCHECK(timezone_ != nullptr);
  if (!v->HasDateAndTime()) return;
  TimestampValue repeated_period_end;
  v->UtcToLocal(*timezone_, nullptr, &repeated_period_end);
  if (repeated_period_end.HasDateAndTime()) *v = repeated_period_end;
}
}
