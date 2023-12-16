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

#include "parquet-column-stats.inline.h"

#include <algorithm>
#include <cmath>
#include <limits>

#include "exec/parquet/parquet-data-converter.h"

#include "common/names.h"

namespace impala {

bool ColumnStatsReader::GetRequiredStatsField(const string& fn_name,
    StatsField* stats_field) {
  if (fn_name == "lt" || fn_name == "le") {
    *stats_field = StatsField::MIN;
    return true;
  } else if (fn_name == "gt" || fn_name == "ge") {
    *stats_field = StatsField::MAX;
    return true;
  }
  DCHECK(false) << "Unsupported function name for statistics evaluation: "
                << fn_name;
  return false;
}

bool ColumnStatsReader::ReadFromThrift(StatsField stats_field, void* slot) const {
  if (!(col_chunk_.__isset.meta_data && col_chunk_.meta_data.__isset.statistics)) {
    return false;
  }
  const parquet::Statistics& stats = col_chunk_.meta_data.statistics;

  // Try to read the requested stats field. If it is not set, we may fall back to reading
  // the old stats, based on the column type.
  const string* stat_value = nullptr;
  const string* paired_stats_value = nullptr;
  switch (stats_field) {
    case StatsField::MIN:
      if (stats.__isset.min_value && CanUseStats()) {
        stat_value = &stats.min_value;
        if (stats.__isset.max_value)
          paired_stats_value = &stats.max_value;
        break;
      }
      if (stats.__isset.min && CanUseDeprecatedStats()) {
        stat_value = &stats.min;
        if (stats.__isset.max)
          paired_stats_value = &stats.max;
      }
      break;
    case StatsField::MAX:
      if (stats.__isset.max_value && CanUseStats()) {
        stat_value = &stats.max_value;
        if (stats.__isset.min_value)
          paired_stats_value = &stats.min_value;
        break;
      }
      if (stats.__isset.max && CanUseDeprecatedStats()) {
        stat_value = &stats.max;
        if (stats.__isset.min)
          paired_stats_value = &stats.min;
      }
      break;
    default:
      DCHECK(false) << "Unsupported statistics field requested";
  }
  if (stat_value == nullptr) return false;

  return ReadFromString(stats_field, *stat_value, paired_stats_value, slot);
}

bool ColumnStatsReader::ReadMinMaxFromThrift(void* min_slot, void* max_slot) const {
  return ReadFromThrift(ColumnStatsReader::StatsField::MIN, min_slot) &&
         ReadFromThrift(ColumnStatsReader::StatsField::MAX, max_slot);
}

bool ColumnStatsReader::ReadFromString(StatsField stats_field,
    const string& encoded_value, const string* paired_stats_value, void* slot) const {
  switch (col_type_.type) {
    case TYPE_BOOLEAN:
      return ColumnStats<bool>::DecodePlainValue(encoded_value, slot,
          parquet::Type::BOOLEAN);
    case TYPE_TINYINT: {
      // parquet::Statistics encodes INT_8 values using 4 bytes.
      int32_t col_stats;
      int32_t paired_stats_val = 0;
      bool ret = ColumnStats<int32_t>::DecodePlainValue(encoded_value, &col_stats,
          parquet::Type::INT32);
      if (!ret || paired_stats_value == nullptr) return false;
      ret = ColumnStats<int32_t>::DecodePlainValue(*paired_stats_value,
          &paired_stats_val, parquet::Type::INT32);
      // Check if the values of the column stats and paired stats are in valid range.
      // The column stats values could be invalid if the column data type
      // has been changed.
      if (!ret ||
          col_stats < std::numeric_limits<int8_t>::min() ||
          col_stats > std::numeric_limits<int8_t>::max() ||
          paired_stats_val < std::numeric_limits<int8_t>::min() ||
          paired_stats_val > std::numeric_limits<int8_t>::max()) {
        return false;
      }
      *static_cast<int8_t*>(slot) = col_stats;
      return true;
    }
    case TYPE_SMALLINT: {
      // parquet::Statistics encodes INT_16 values using 4 bytes.
      int32_t col_stats;
      int32_t paired_stats_val = 0;
      bool ret = ColumnStats<int32_t>::DecodePlainValue(encoded_value, &col_stats,
          parquet::Type::INT32);
      if (!ret || paired_stats_value == nullptr) return false;
      ret = ColumnStats<int32_t>::DecodePlainValue(*paired_stats_value,
          &paired_stats_val, parquet::Type::INT32);
      if (!ret ||
          col_stats < std::numeric_limits<int16_t>::min() ||
          col_stats > std::numeric_limits<int16_t>::max() ||
          paired_stats_val < std::numeric_limits<int16_t>::min() ||
          paired_stats_val > std::numeric_limits<int16_t>::max()) {
        return false;
      }
      *static_cast<int16_t*>(slot) = col_stats;
      return true;
    }
    case TYPE_INT:
      return ColumnStats<int32_t>::DecodePlainValue(encoded_value, slot, element_.type);
    case TYPE_BIGINT:
      return ColumnStats<int64_t>::DecodePlainValue(encoded_value, slot, element_.type);
    case TYPE_FLOAT:
      // IMPALA-6527, IMPALA-6538: ignore min/max stats if NaN
      return ColumnStats<float>::DecodePlainValue(encoded_value, slot, element_.type) &&
          !std::isnan(*reinterpret_cast<float*>(slot));
    case TYPE_DOUBLE:
      // IMPALA-6527, IMPALA-6538: ignore min/max stats if NaN
      return ColumnStats<double>::DecodePlainValue(encoded_value, slot, element_.type) &&
          !std::isnan(*reinterpret_cast<double*>(slot));
    case TYPE_TIMESTAMP:
      return DecodeTimestamp(encoded_value, stats_field,
          static_cast<TimestampValue*>(slot));
    case TYPE_STRING:
    case TYPE_VARCHAR:
      return ColumnStats<StringValue>::DecodePlainValue(encoded_value, slot,
          element_.type);
    case TYPE_CHAR:
      /// We don't read statistics for CHAR columns, since CHAR support is broken in
      /// Impala (IMPALA-1652).
      return false;
    case TYPE_DECIMAL:
      switch (col_type_.GetByteSize()) {
        case 4:
          return DecodeDecimal<Decimal4Value>(encoded_value,
              static_cast<Decimal4Value*>(slot));
        case 8:
          return DecodeDecimal<Decimal8Value>(encoded_value,
              static_cast<Decimal8Value*>(slot));
        case 16:
          return DecodeDecimal<Decimal16Value>(encoded_value,
              static_cast<Decimal16Value*>(slot));
        }
      DCHECK(false) << "Unknown decimal byte size: " << col_type_.GetByteSize();
      break;
    case TYPE_DATE:
      return ColumnStats<DateValue>::DecodePlainValue(encoded_value, slot, element_.type);
    default:
      DCHECK(false) << col_type_.DebugString();
  }
  return false;
}

bool ColumnStatsReader::ReadFromStringsBatch(StatsField stats_field,
    const vector<string>& encoded_values, int64_t start_idx, int64_t end_idx,
    int fixed_len_size, void* slot) const {
  int64_t num_decoded = 0;
  switch (col_type_.type) {
    case TYPE_BOOLEAN:
      num_decoded = DecodeBatchOneBoundsCheck<int8_t>(encoded_values, start_idx, end_idx,
          fixed_len_size, (int8_t*)slot, element_.type);
      break;
    case TYPE_TINYINT:
      // parquet::Statistics encodes INT_8 values using 4 bytes.
      num_decoded = DecodeBatchOneBoundsCheck<int8_t>(encoded_values, start_idx, end_idx,
          fixed_len_size, (int8_t*)slot, parquet::Type::INT32);
      break;
    case TYPE_SMALLINT:
      // parquet::Statistics encodes INT_16 values using 4 bytes.
      num_decoded = DecodeBatchOneBoundsCheck<int16_t>(encoded_values, start_idx, end_idx,
          fixed_len_size, (int16_t*)slot, parquet::Type::INT32);
      break;
    case TYPE_INT:
      num_decoded = DecodeBatchOneBoundsCheckFastTrack<int32_t>(encoded_values, start_idx,
          end_idx, fixed_len_size, (int32_t*)slot);
      break;
    case TYPE_BIGINT:
      num_decoded = DecodeBatchOneBoundsCheckFastTrack<int64_t>(encoded_values, start_idx,
          end_idx, fixed_len_size, (int64_t*)slot);
      break;
    case TYPE_FLOAT:
      // IMPALA-6527, IMPALA-6538: ignore min/max stats if NaN
      num_decoded = DecodeBatchOneBoundsCheckFastTrack<float>(encoded_values, start_idx,
          end_idx, fixed_len_size, (float*)slot);
      for (int64_t i = 0; i < num_decoded; i++) {
        if (std::isnan(*(reinterpret_cast<float*>(slot)) + i)) return false;
      }
      break;
    case TYPE_DOUBLE:
      // IMPALA-6527, IMPALA-6538: ignore min/max stats if NaN
      num_decoded = DecodeBatchOneBoundsCheckFastTrack<double>(encoded_values, start_idx,
          end_idx, fixed_len_size, (double*)slot);
      for (int64_t i=0; i<num_decoded; i++) {
        if (std::isnan(*(reinterpret_cast<double*>(slot)) + i)) return false;
      }
      break;
    case TYPE_TIMESTAMP:
      num_decoded = DecodeBatchOneBoundsCheck<TimestampValue>(encoded_values, start_idx,
          end_idx, fixed_len_size, (TimestampValue*)slot, element_.type);
      break;
    case TYPE_STRING:
    case TYPE_VARCHAR:
      num_decoded = DecodeBatchOneBoundsCheck<StringValue>(encoded_values,
          start_idx, end_idx, fixed_len_size, (StringValue*)slot, element_.type);
      break;
    case TYPE_CHAR:
      /// We don't read statistics for CHAR columns, since CHAR support is broken in
      /// Impala (IMPALA-1652).
      return false;
    case TYPE_DECIMAL:
      switch (col_type_.GetByteSize()) {
        case 4:
          num_decoded = DecodeBatchOneBoundsCheck<Decimal4Value>(encoded_values,
              start_idx, end_idx, fixed_len_size, (Decimal4Value*)slot, element_.type);
          break;
        case 8:
          num_decoded = DecodeBatchOneBoundsCheck<Decimal8Value>(encoded_values,
              start_idx, end_idx, fixed_len_size, (Decimal8Value*)slot, element_.type);
          break;
        case 16:
          num_decoded = DecodeBatchOneBoundsCheck<Decimal16Value>(encoded_values,
              start_idx, end_idx, fixed_len_size, (Decimal16Value*)slot, element_.type);
          break;
        default:
          DCHECK(false) << "Unknown decimal byte size: " << col_type_.GetByteSize();
        }
      break;
    case TYPE_DATE:
      num_decoded = DecodeBatchOneBoundsCheck<DateValue>(encoded_values,
          start_idx, end_idx, fixed_len_size, (DateValue*)slot, element_.type);
      break;
    default:
      DCHECK(false) << col_type_.DebugString();
  }
  return num_decoded == end_idx - start_idx + 1;
}

// The basic version of batch read of stats.
template <typename InternalType>
inline int64_t ColumnStatsReader::DecodeBatchOneBoundsCheck(
    const vector<std::string>& source, int64_t start_idx, int64_t end_idx,
    int fixed_len_size, InternalType* v, parquet::Type::type PARQUET_TYPE) const {
  if (start_idx > end_idx || end_idx - start_idx + 1 > source.size()) return false;

  int64_t pos = start_idx;
  InternalType* output = v;
  ParquetDataConverter<InternalType, true> data_converter(&element_, &col_type_);
  //TODO (IMPALA-10793): set timestamp decoder to correctly convert timestamps.

  /// We unroll the loop manually in batches of 8.
  constexpr int batch = 8;
  int64_t num_values = (end_idx - start_idx + 1);
  const int64_t full_batches = num_values / batch;

  for (int64_t b = 0; b < full_batches; b++) {
#pragma push_macro("DECODE_NO_CHECK_UNROLL")
#define DECODE_NO_CHECK_UNROLL(ignore1, i, ignore2)           \
    ColumnStats<InternalType>::DecodePlainValue(              \
        source[pos + i], output + i, PARQUET_TYPE);           \
    if (UNLIKELY(data_converter.NeedsConversion())) {         \
      data_converter.ConvertSlot(output + i, output + i);     \
    }

    BOOST_PP_REPEAT_FROM_TO(0, 8 /* The value of `batch` */,
        DECODE_NO_CHECK_UNROLL, ignore);
#pragma pop_macro("DECODE_NO_CHECK_UNROLL")

    pos += batch;
    output += batch;
  }

  for (; pos < num_values; ++pos) {
    ColumnStats<InternalType>::DecodePlainValue(source[pos], output, PARQUET_TYPE);
    if (UNLIKELY(data_converter.NeedsConversion())) {
      data_converter.ConvertSlot(output, output);
    }
    output++;
  }

  DCHECK_EQ(pos, num_values);
  return num_values;
}

// A fast track version of batch read of stats.
template <typename InternalType>
inline int64_t ColumnStatsReader::DecodeBatchOneBoundsCheckFastTrack(
    const vector<std::string>& source, int64_t start_idx, int64_t end_idx,
    int fixed_len_size, InternalType* v) const {
  if (start_idx > end_idx || end_idx - start_idx + 1 > source.size()) return false;

  int64_t pos = start_idx;
  InternalType* output = v;

  /// We unroll the loop manually in batches of 8.
  constexpr int batch = 8;
  int64_t num_values = (end_idx - start_idx + 1);
  const int64_t full_batches = num_values / batch;

  for (int64_t b = 0; b < full_batches; b++) {
#pragma push_macro("DECODE_NO_CHECK_UNROLL")
#define DECODE_NO_CHECK_UNROLL(ignore1, i, ignore2) \
    ParquetPlainEncoder::DecodeNoBoundsCheck<InternalType>( \
        source[pos + i], output + i);

    BOOST_PP_REPEAT_FROM_TO(0, 8 /* The value of `batch` */,
        DECODE_NO_CHECK_UNROLL, ignore);
#pragma pop_macro("DECODE_NO_CHECK_UNROLL")

    pos += batch;
    output += batch;
  }

  for (; pos < num_values; ++pos) {
    ParquetPlainEncoder::DecodeNoBoundsCheck<InternalType>(source[pos], output);
    output++;
  }

  DCHECK_EQ(pos, num_values);
  return num_values;
}

template <typename DecimalType>
bool ColumnStatsReader::DecodeDecimal(const std::string& stat_value,
    DecimalType* slot) const {
  bool ret = ColumnStats<DecimalType>::DecodePlainValue(stat_value, slot,
      element_.type);
  if (!ret) return false;
  ParquetDataConverter<DecimalType, true> data_converter(&element_, &col_type_);
  if (LIKELY(!data_converter.NeedsConversion())) return true;
  // Let's convert the decimal value to the table's decimal type. It's OK to evaluate
  // filters and min/max conjuncts against the converted values as later we'd also
  // use the converted values anyways.
  // No need for an extra buffer, we can do the conversion in-place.
  return data_converter.ConvertSlot(slot, slot);
}

bool ColumnStatsReader::DecodeTimestamp(const std::string& stat_value,
    ColumnStatsReader::StatsField stats_field, TimestampValue* slot) const {
  bool stats_read = false;
  if (element_.type == parquet::Type::INT96) {
    stats_read =
        ColumnStats<TimestampValue>::DecodePlainValue(stat_value, slot, element_.type);
  } else if (element_.type == parquet::Type::INT64) {
    int64_t tmp;
    stats_read = ColumnStats<int64_t>::DecodePlainValue(stat_value, &tmp, element_.type);
    if (stats_read) *slot = timestamp_decoder_.Int64ToTimestampValue(tmp);
  } else {
    DCHECK(false) << element_.name;
    return false;
  }

  if (stats_read && timestamp_decoder_.NeedsConversion()) {
    if (stats_field == ColumnStatsReader::StatsField::MIN) {
      timestamp_decoder_.ConvertMinStatToLocalTime(slot);
    } else {
      timestamp_decoder_.ConvertMaxStatToLocalTime(slot);
    }
  }
  return stats_read && slot->HasDateAndTime();
}

bool ColumnStatsReader::ReadNullCountStat(int64_t* null_count) const {
  if (!(col_chunk_.__isset.meta_data && col_chunk_.meta_data.__isset.statistics)) {
    return false;
  }
  const parquet::Statistics& stats = col_chunk_.meta_data.statistics;
  if (stats.__isset.null_count) {
    *null_count = stats.null_count;
    return true;
  }
  return false;
}

bool ColumnStatsReader::AllNulls(bool* all_nulls) const {
  DCHECK(all_nulls);
  int64_t null_count = 0;
  bool null_count_read = ReadNullCountStat(&null_count);
  if (!null_count_read) {
    return false;
  }
  *all_nulls = (null_count == col_chunk_.meta_data.num_values);
  return true;
}

Status ColumnStatsBase::CopyToBuffer(StringBuffer* buffer, StringValue* value) {
  StringValue::SimpleString value_s = value->ToSimpleString();
  if (value_s.ptr == buffer->buffer()) return Status::OK();
  buffer->Clear();
  RETURN_IF_ERROR(buffer->Append(value_s.ptr, value_s.len));
  value->SetPtr(buffer->buffer());
  return Status::OK();
}

bool ColumnStatsReader::CanUseStats() const {
  // If column order is not set, only statistics for numeric types can be trusted.
  if (col_order_ == nullptr) {
    return col_type_.IsBooleanType() || col_type_.IsIntegerType()
        || col_type_.IsFloatingPointType();
  }
  // Stats can be used if the column order is TypeDefinedOrder (see parquet.thrift).
  return col_order_->__isset.TYPE_ORDER;
}

bool ColumnStatsReader::CanUseDeprecatedStats() const {
  // If column order is set to something other than TypeDefinedOrder, we shall not use the
  // stats (see parquet.thrift).
  if (col_order_ != nullptr && !col_order_->__isset.TYPE_ORDER) return false;
  return col_type_.IsBooleanType() || col_type_.IsIntegerType()
      || col_type_.IsFloatingPointType();
}

}  // end ns impala
