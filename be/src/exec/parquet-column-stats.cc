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

#include "common/names.h"

namespace impala {

bool ColumnStatsBase::ReadFromThrift(const parquet::ColumnChunk& col_chunk,
    const ColumnType& col_type, const parquet::ColumnOrder* col_order,
    StatsField stats_field, void* slot) {
  if (!(col_chunk.__isset.meta_data && col_chunk.meta_data.__isset.statistics)) {
    return false;
  }
  const parquet::Statistics& stats = col_chunk.meta_data.statistics;

  // Try to read the requested stats field. If it is not set, we may fall back to reading
  // the old stats, based on the column type.
  const string* stat_value = nullptr;
  switch (stats_field) {
    case StatsField::MIN:
      if (stats.__isset.min_value && CanUseStats(col_type, col_order)) {
        stat_value = &stats.min_value;
        break;
      }
      if (stats.__isset.min && CanUseDeprecatedStats(col_type, col_order)) {
        stat_value = &stats.min;
      }
      break;
    case StatsField::MAX:
      if (stats.__isset.max_value && CanUseStats(col_type, col_order)) {
        stat_value = &stats.max_value;
        break;
      }
      if (stats.__isset.max && CanUseDeprecatedStats(col_type, col_order)) {
        stat_value = &stats.max;
      }
      break;
    default:
      DCHECK(false) << "Unsupported statistics field requested";
  }
  if (stat_value == nullptr) return false;

  switch (col_type.type) {
    case TYPE_BOOLEAN:
      return ColumnStats<bool>::DecodePlainValue(*stat_value, slot,
          parquet::Type::BOOLEAN);
    case TYPE_TINYINT: {
      // parquet::Statistics encodes INT_8 values using 4 bytes.
      int32_t col_stats;
      bool ret = ColumnStats<int32_t>::DecodePlainValue(*stat_value, &col_stats,
          parquet::Type::INT32);
      if (!ret || col_stats < std::numeric_limits<int8_t>::min() ||
          col_stats > std::numeric_limits<int8_t>::max()) {
        return false;
      }
      *static_cast<int8_t*>(slot) = col_stats;
      return true;
    }
    case TYPE_SMALLINT: {
      // parquet::Statistics encodes INT_16 values using 4 bytes.
      int32_t col_stats;
      bool ret = ColumnStats<int32_t>::DecodePlainValue(*stat_value, &col_stats,
          parquet::Type::INT32);
      if (!ret || col_stats < std::numeric_limits<int16_t>::min() ||
          col_stats > std::numeric_limits<int16_t>::max()) {
        return false;
      }
      *static_cast<int16_t*>(slot) = col_stats;
      return true;
    }
    case TYPE_INT:
      return ColumnStats<int32_t>::DecodePlainValue(*stat_value, slot,
          col_chunk.meta_data.type);
    case TYPE_BIGINT:
      return ColumnStats<int64_t>::DecodePlainValue(*stat_value, slot,
          col_chunk.meta_data.type);
    case TYPE_FLOAT:
      // IMPALA-6527, IMPALA-6538: ignore min/max stats if NaN
      return ColumnStats<float>::DecodePlainValue(*stat_value, slot,
          col_chunk.meta_data.type) && !std::isnan(*reinterpret_cast<float*>(slot));
    case TYPE_DOUBLE:
      // IMPALA-6527, IMPALA-6538: ignore min/max stats if NaN
      return ColumnStats<double>::DecodePlainValue(*stat_value, slot,
          col_chunk.meta_data.type) && !std::isnan(*reinterpret_cast<double*>(slot));
    case TYPE_TIMESTAMP:
      return ColumnStats<TimestampValue>::DecodePlainValue(*stat_value, slot,
          col_chunk.meta_data.type);
    case TYPE_STRING:
    case TYPE_VARCHAR:
      return ColumnStats<StringValue>::DecodePlainValue(*stat_value, slot,
          col_chunk.meta_data.type);
    case TYPE_CHAR:
      /// We don't read statistics for CHAR columns, since CHAR support is broken in
      /// Impala (IMPALA-1652).
      return false;
    case TYPE_DECIMAL:
      switch (col_type.GetByteSize()) {
        case 4:
          return ColumnStats<Decimal4Value>::DecodePlainValue(*stat_value, slot,
              col_chunk.meta_data.type);
        case 8:
          return ColumnStats<Decimal8Value>::DecodePlainValue(*stat_value, slot,
              col_chunk.meta_data.type);
        case 16:
          return ColumnStats<Decimal16Value>::DecodePlainValue(*stat_value, slot,
              col_chunk.meta_data.type);
        }
      DCHECK(false) << "Unknown decimal byte size: " << col_type.GetByteSize();
    default:
      DCHECK(false) << col_type.DebugString();
  }
  return false;
}

bool ColumnStatsBase::ReadNullCountStat(const parquet::ColumnChunk& col_chunk,
    int64_t* null_count) {
  if (!(col_chunk.__isset.meta_data && col_chunk.meta_data.__isset.statistics)) {
    return false;
  }
  const parquet::Statistics& stats = col_chunk.meta_data.statistics;
  if (stats.__isset.null_count) {
    *null_count = stats.null_count;
    return true;
  }
  return false;
}

Status ColumnStatsBase::CopyToBuffer(StringBuffer* buffer, StringValue* value) {
  if (value->ptr == buffer->buffer()) return Status::OK();
  buffer->Clear();
  RETURN_IF_ERROR(buffer->Append(value->ptr, value->len));
  value->ptr = buffer->buffer();
  return Status::OK();
}

bool ColumnStatsBase::CanUseStats(
    const ColumnType& col_type, const parquet::ColumnOrder* col_order) {
  // If column order is not set, only statistics for numeric types can be trusted.
  if (col_order == nullptr) {
    return col_type.IsBooleanType() || col_type.IsIntegerType()
        || col_type.IsFloatingPointType();
  }
  // Stats can be used if the column order is TypeDefinedOrder (see parquet.thrift).
  return col_order->__isset.TYPE_ORDER;
}

bool ColumnStatsBase::CanUseDeprecatedStats(
    const ColumnType& col_type, const parquet::ColumnOrder* col_order) {
  // If column order is set to something other than TypeDefinedOrder, we shall not use the
  // stats (see parquet.thrift).
  if (col_order != nullptr && !col_order->__isset.TYPE_ORDER) return false;
  return col_type.IsBooleanType() || col_type.IsIntegerType()
      || col_type.IsFloatingPointType();
}

}  // end ns impala
