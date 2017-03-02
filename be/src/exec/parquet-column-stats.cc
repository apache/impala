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

#include <limits>

namespace impala {

bool ColumnStatsBase::ReadFromThrift(const parquet::Statistics& thrift_stats,
    const ColumnType& col_type, const StatsField& stats_field, void* slot) {
  switch (col_type.type) {
    case TYPE_BOOLEAN:
      return ColumnStats<bool>::ReadFromThrift(thrift_stats, stats_field, slot);
    case TYPE_TINYINT: {
        // parquet::Statistics encodes INT_8 values using 4 bytes.
        int32_t col_stats;
        bool ret = ColumnStats<int32_t>::ReadFromThrift(thrift_stats, stats_field,
            &col_stats);
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
        bool ret = ColumnStats<int32_t>::ReadFromThrift(thrift_stats, stats_field,
            &col_stats);
        if (!ret || col_stats < std::numeric_limits<int16_t>::min() ||
            col_stats > std::numeric_limits<int16_t>::max()) {
          return false;
        }
        *static_cast<int16_t*>(slot) = col_stats;
        return true;
      }
    case TYPE_INT:
      return ColumnStats<int32_t>::ReadFromThrift(thrift_stats, stats_field, slot);
    case TYPE_BIGINT:
      return ColumnStats<int64_t>::ReadFromThrift(thrift_stats, stats_field, slot);
    case TYPE_FLOAT:
      return ColumnStats<float>::ReadFromThrift(thrift_stats, stats_field, slot);
    case TYPE_DOUBLE:
      return ColumnStats<double>::ReadFromThrift(thrift_stats, stats_field, slot);
    case TYPE_TIMESTAMP:
      /// TODO add support for TimestampValue (IMPALA-4819)
      break;
    case TYPE_STRING:
    case TYPE_VARCHAR:
    case TYPE_CHAR:
      /// TODO add support for StringValue (IMPALA-4817)
      break;
    case TYPE_DECIMAL:
      /// TODO add support for DecimalValue (IMPALA-4815)
      switch (col_type.GetByteSize()) {
        case 4:
          break;
        case 8:
          break;
        case 16:
          break;
      }
      break;
    default:
      DCHECK(false) << col_type.DebugString();
  }
  return false;
}

}  // end ns impala
