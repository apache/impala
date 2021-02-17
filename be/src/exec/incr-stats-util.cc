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

#include "incr-stats-util.h"

#include <boost/unordered_set.hpp>
#include <gutil/strings/substitute.h>
#include <cmath>
#include <sstream>

#include "gen-cpp/CatalogService_types.h"
#include "gen-cpp/CatalogObjects_types.h"

#include "common/compiler-util.h"
#include "common/logging.h"
#include "exprs/aggregate-functions.h"
#include "service/hs2-util.h"
#include "udf/udf.h"

#include "common/names.h"

using namespace apache::hive::service::cli::thrift;
using namespace impala;
using namespace impala_udf;
using namespace strings;

// Finalize method for the NDV_NO_FINALIZE() UDA, which only copies the intermediate state
// of the NDV computation into its output StringVal.
StringVal IncrementNdvFinalize(FunctionContext* ctx, const StringVal& src) {
  if (UNLIKELY(src.is_null)) return src;
  DCHECK_EQ(src.len, AggregateFunctions::DEFAULT_HLL_LEN);
  StringVal result_str(ctx, src.len);
  if (UNLIKELY(result_str.is_null)) return result_str;
  memcpy(result_str.ptr, src.ptr, src.len);
  return result_str;
}

// To save space when sending NDV estimates around the cluster, we compress them using
// RLE, since they are often sparse. The resulting string has the form CVCVCVCV where C is
// the count, i.e. the number of times the subsequent V (value) should be repeated in the
// output string. C is between 0 and 255 inclusive, the count it represents is one more
// than the absolute value of C (since we never have a 0 count, and want to use the full
// range available to us).
//
// The output parameter is_encoded is set to true only if the RLE-compressed string is
// shorter than the input. Otherwise it is set to false, and the input is returned
// unencoded.
string EncodeNdv(const string& ndv, bool* is_encoded) {
  DCHECK_EQ(ndv.size(), AggregateFunctions::DEFAULT_HLL_LEN);
  string encoded_ndv(AggregateFunctions::DEFAULT_HLL_LEN, 0);
  int idx = 0;
  char last = ndv[0];

  // Keep a count of how many times a value appears in succession. We encode this count as
  // a byte 0-255, but the actual count is always one more than the encoded value
  // (i.e. in the range 1-256 inclusive).
  uint8_t count = 0;
  for (int i = 1; i < AggregateFunctions::DEFAULT_HLL_LEN; ++i) {
    if (ndv[i] != last || count == numeric_limits<uint8_t>::max()) {
      if (idx + 2 > AggregateFunctions::DEFAULT_HLL_LEN) break;
      // Write a (count, value) pair to two successive bytes
      encoded_ndv[idx++] = count;
      count = 0;
      encoded_ndv[idx++] = last;
      last = ndv[i];
    } else {
      ++count;
    }
  }

  // +2 for the remaining two bytes written below
  if (idx + 2 > AggregateFunctions::DEFAULT_HLL_LEN) {
    *is_encoded = false;
    return ndv;
  }

  encoded_ndv[idx++] = count;
  encoded_ndv[idx++] = last;

  *is_encoded = true;
  encoded_ndv.resize(idx);
  DCHECK_GT(encoded_ndv.size(), 0);
  DCHECK_LE(encoded_ndv.size(), AggregateFunctions::DEFAULT_HLL_LEN);
  return encoded_ndv;
}

string DecodeNdv(const string& ndv, bool is_encoded) {
  if (!is_encoded) return ndv;
  DCHECK_EQ(ndv.size() % 2, 0);
  string decoded_ndv(AggregateFunctions::DEFAULT_HLL_LEN, 0);
  int idx = 0;
  for (int i = 0; i < ndv.size(); i += 2) {
    for (int j = 0; j < (static_cast<uint8_t>(ndv[i])) + 1; ++j) {
      decoded_ndv[idx++] = ndv[i+1];
    }
  }
  DCHECK_EQ(idx, AggregateFunctions::DEFAULT_HLL_LEN);
  return decoded_ndv;
}

#define UPDATE_LOW_VALUE(TYPE)                                                      \
  if (!(low_value.__isset.TYPE##_val) || value.TYPE##_val < low_value.TYPE##_val) { \
    low_value.__set_##TYPE##_val(value.TYPE##_val);                                 \
  }

void PerColumnStats::UpdateLowValue(const impala::TColumnValue& value) {
  if (value.__isset.double_val) {
    UPDATE_LOW_VALUE(double);
  } else if (value.__isset.byte_val) {
    UPDATE_LOW_VALUE(byte);
  } else if (value.__isset.int_val) {
    UPDATE_LOW_VALUE(int);
  } else if (value.__isset.short_val) {
    UPDATE_LOW_VALUE(short);
  } else if (value.__isset.long_val) {
    UPDATE_LOW_VALUE(long);
  }
}

#define UPDATE_HIGH_VALUE(TYPE)                                                       \
  if (!(high_value.__isset.TYPE##_val) || value.TYPE##_val > high_value.TYPE##_val) { \
    high_value.__set_##TYPE##_val(value.TYPE##_val);                                  \
  }

void PerColumnStats::UpdateHighValue(const impala::TColumnValue& value) {
  if (value.__isset.double_val) {
    UPDATE_HIGH_VALUE(double);
  } else if (value.__isset.byte_val) {
    UPDATE_HIGH_VALUE(byte);
  } else if (value.__isset.int_val) {
    UPDATE_HIGH_VALUE(int);
  } else if (value.__isset.short_val) {
    UPDATE_HIGH_VALUE(short);
  } else if (value.__isset.long_val) {
    UPDATE_HIGH_VALUE(long);
  }
}

void PerColumnStats::Update(const string& ndv, int64_t num_new_rows, double new_avg_width,
    int32_t max_new_width, int64_t num_new_nulls, int64_t num_new_trues,
    int64_t num_new_falses, const impala::TColumnValue& low_value_new,
    const impala::TColumnValue& high_value_new) {
  DCHECK_EQ(intermediate_ndv.size(), ndv.size()) << "Incompatible intermediate NDVs";
  DCHECK_GE(num_new_rows, 0);
  DCHECK_GE(max_new_width, 0);
  DCHECK_GE(new_avg_width, 0);
  DCHECK_GE(num_new_nulls, -1); // '-1' needed to be backward compatible
  DCHECK_GE(num_trues, 0);
  DCHECK_GE(num_falses, 0);
  for (int j = 0; j < ndv.size(); ++j) {
    intermediate_ndv[j] = ::max(intermediate_ndv[j], ndv[j]);
  }
  // Earlier the 'num_nulls' were initialized and persisted with '-1', this condition
  // ensures metadata backward compatibility between releases
  if (num_nulls >= 0) {
    if (num_new_nulls >= 0) {
      num_nulls += num_new_nulls;
    } else {
      num_nulls = -1;
    }
  }

  num_trues += num_new_trues;
  num_falses += num_new_falses;
  max_width = ::max(max_width, max_new_width);
  total_width += (new_avg_width * num_new_rows);
  num_rows += num_new_rows;

  UpdateLowValue(low_value_new);
  UpdateHighValue(high_value_new);
}

void PerColumnStats::Finalize() {
  ndv_estimate = AggregateFunctions::HllFinalEstimate(
      reinterpret_cast<const uint8_t*>(intermediate_ndv.data()));
  avg_width = num_rows == 0 ? 0 : total_width / num_rows;
}

TColumnStats PerColumnStats::ToTColumnStats() const {
  TColumnStats col_stats;
  col_stats.__set_num_distinct_values(ndv_estimate);
  col_stats.__set_num_nulls(num_nulls);
  col_stats.__set_max_size(max_width);
  col_stats.__set_avg_size(avg_width);
  col_stats.__set_num_trues(num_trues);
  col_stats.__set_num_falses(num_falses);
  col_stats.__set_low_value(low_value);
  col_stats.__set_high_value(high_value);
  return col_stats;
}

string PerColumnStats::DebugString() const {
  stringstream ss_low_value;
  ss_low_value << low_value;
  stringstream ss_high_value;
  ss_high_value << high_value;
  return Substitute("ndv: $0, num_nulls: $1, max_width: $2, avg_width: $3, num_rows: "
                    "$4, num_trues: $5, num_falses: $6, low_value: $7, high_value: $8",
      ndv_estimate, num_nulls, max_width, avg_width, num_rows, num_trues, num_falses,
      ss_low_value.str(), ss_high_value.str());
}

namespace impala {

void FinalizePartitionedColumnStats(const TTableSchema& col_stats_schema,
    const vector<TPartitionStats>& existing_part_stats,
    const vector<vector<string>>& expected_partitions, const TRowSet& rowset,
    int32_t num_partition_cols, TAlterTableUpdateStatsParams* params) {
  // The rowset should have the following schema: for every column in the source table,
  // seven columns are produced, one row per partition.
  // <ndv buckets>, <num nulls>, <max width>, <avg width>, <count rows>,
  // <num trues>, <num falses>, <low value>, <high value>
  static const int COLUMNS_PER_STAT = 9;

  const int num_cols =
      (col_stats_schema.columns.size() - num_partition_cols) / COLUMNS_PER_STAT;
  unordered_set<vector<string>> seen_partitions;
  vector<PerColumnStats> stats(num_cols);

  if (rowset.rows.size() > 0) {
    DCHECK_GE(rowset.rows[0].colVals.size(), COLUMNS_PER_STAT);
    params->__isset.partition_stats = true;
    for (const TRow& col_stats_row: rowset.rows) {
      // The last few columns are partition columns that the results are grouped by, and
      // so uniquely identify the partition that these stats belong to.
      vector<string> partition_key_vals;
      partition_key_vals.reserve(col_stats_row.colVals.size());
      for (int j = num_cols * COLUMNS_PER_STAT; j < col_stats_row.colVals.size(); ++j) {
        stringstream ss;
        PrintTColumnValue(col_stats_row.colVals[j], &ss);
        partition_key_vals.push_back(ss.str());
      }
      seen_partitions.insert(partition_key_vals);

      TPartitionStats* part_stat = &params->partition_stats[partition_key_vals];
      part_stat->__isset.intermediate_col_stats = true;
      for (int i = 0; i < num_cols * COLUMNS_PER_STAT; i += COLUMNS_PER_STAT) {
        PerColumnStats* stat = &stats[i / COLUMNS_PER_STAT];
        const string& ndv = col_stats_row.colVals[i].stringVal.value;
        int64_t num_rows = col_stats_row.colVals[i + 4].i64Val.value;
        double avg_width = col_stats_row.colVals[i + 3].doubleVal.value;
        int32_t max_width = col_stats_row.colVals[i + 2].i32Val.value;
        int64_t num_nulls = col_stats_row.colVals[i + 1].i64Val.value;
        int64_t num_trues = col_stats_row.colVals[i + 5].i64Val.value;
        int64_t num_falses = col_stats_row.colVals[i + 6].i64Val.value;
        TColumnValueHive low_value = col_stats_row.colVals[i + 7];
        TColumnValueHive high_value = col_stats_row.colVals[i + 8];

        impala::TColumnValue low_value_impala =
            ConvertToTColumnValue(col_stats_schema.columns[i + 7], low_value);
        impala::TColumnValue high_value_impala =
            ConvertToTColumnValue(col_stats_schema.columns[i + 8], high_value);

        VLOG(3) << "Updated statistics for column=["
                << col_stats_schema.columns[i].columnName << "]," << " statistics={"
                << ndv << "," << num_rows << "，" << avg_width << "," << num_trues
                << "," << max_width << "," << num_nulls << "," << num_falses
                << PrintTColumnValue(low_value_impala) << ","
                << PrintTColumnValue(high_value_impala) << "}";
        stat->Update(ndv, num_rows, avg_width, max_width, num_nulls, num_trues,
            num_falses, low_value_impala, high_value_impala);

        // Save the intermediate state per-column, per-partition
        TIntermediateColumnStats int_stats;
        bool is_encoded;
        int_stats.__set_intermediate_ndv(EncodeNdv(ndv, &is_encoded));
        int_stats.__set_is_ndv_encoded(is_encoded);
        int_stats.__set_num_nulls(num_nulls);
        int_stats.__set_max_width(max_width);
        int_stats.__set_avg_width(avg_width);
        int_stats.__set_num_rows(num_rows);
        int_stats.__set_num_trues(num_trues);
        int_stats.__set_num_falses(num_falses);
        int_stats.__set_low_value(low_value_impala);
        int_stats.__set_high_value(high_value_impala);

        part_stat->intermediate_col_stats[col_stats_schema.columns[i].columnName] =
            int_stats;
      }
    }
  }

  // Make sure there's a zeroed entry for all partitions that were included in the query -
  // empty partitions will not have a row in the GROUP BY, but should still emit a
  // TPartitionStats.
  TIntermediateColumnStats empty_column_stats;
  bool is_encoded;
  empty_column_stats.__set_intermediate_ndv(
      EncodeNdv(string(AggregateFunctions::DEFAULT_HLL_LEN, 0), &is_encoded));
  empty_column_stats.__set_is_ndv_encoded(is_encoded);
  empty_column_stats.__set_num_nulls(0);
  empty_column_stats.__set_max_width(0);
  empty_column_stats.__set_avg_width(0);
  empty_column_stats.__set_num_rows(0);
  empty_column_stats.__set_num_trues(0);
  empty_column_stats.__set_num_falses(0);
  TPartitionStats empty_part_stats;
  for (int i = 0; i < num_cols * COLUMNS_PER_STAT; i += COLUMNS_PER_STAT) {
    empty_part_stats.intermediate_col_stats[col_stats_schema.columns[i].columnName] =
        empty_column_stats;
  }
  empty_part_stats.__isset.intermediate_col_stats = true;
  TTableStats empty_table_stats;
  empty_table_stats.__set_num_rows(0);
  empty_part_stats.stats = empty_table_stats;
  for (const vector<string>& part_key_vals: expected_partitions) {
    DCHECK_EQ(part_key_vals.size(), num_partition_cols);
    if (seen_partitions.find(part_key_vals) != seen_partitions.end()) continue;
    params->partition_stats[part_key_vals] = empty_part_stats;
  }

  // Now aggregate the existing statistics. The FE will ensure that the set of
  // partitions accessed by the query and this list are disjoint and cover the entire
  // set of partitions.
  for (const TPartitionStats& existing_stats: existing_part_stats) {
    DCHECK_LE(existing_stats.intermediate_col_stats.size(),
        col_stats_schema.columns.size());
    for (int i = 0; i < num_cols; ++i) {
      const string& col_name = col_stats_schema.columns[i * COLUMNS_PER_STAT].columnName;
      map<string, TIntermediateColumnStats>::const_iterator it =
          existing_stats.intermediate_col_stats.find(col_name);
      if (it == existing_stats.intermediate_col_stats.end()) {
        VLOG(2) << "Could not find column in existing column stat state: " << col_name;
        continue;
      }

      const TIntermediateColumnStats& int_stats = it->second;
      VLOG(3) << "Updated intermediate value for column=[" << col_name << "], "
              << "statistics={" << int_stats.intermediate_ndv << ","
              << int_stats.num_rows << "，" << int_stats.avg_width << ","
              << int_stats.max_width << ","<< int_stats.num_nulls << ","
              << int_stats.num_trues << "," << int_stats.num_falses << ","
              << int_stats.low_value << "," << int_stats.high_value << "}";
      stats[i].Update(DecodeNdv(int_stats.intermediate_ndv, int_stats.is_ndv_encoded),
          int_stats.num_rows, int_stats.avg_width, int_stats.max_width,
          int_stats.num_nulls, int_stats.num_trues, int_stats.num_falses,
          int_stats.low_value, int_stats.high_value);
    }
  }

  // Compute the final results now that all aggregations are done, and save those as
  // column stats for each column in turn.
  for (int i = 0; i < stats.size(); ++i) {
    stats[i].Finalize();
    const string& col_name = col_stats_schema.columns[i * COLUMNS_PER_STAT].columnName;
    params->column_stats[col_name] = stats[i].ToTColumnStats();

    VLOG(3) << "Incremental stats result for column: " << col_name << ": "
            << stats[i].DebugString();
  }

  params->__isset.column_stats = true;
}

}
