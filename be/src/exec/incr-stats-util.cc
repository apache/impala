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
  DCHECK_EQ(src.len, AggregateFunctions::HLL_LEN);
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
  DCHECK_EQ(ndv.size(), AggregateFunctions::HLL_LEN);
  string encoded_ndv(AggregateFunctions::HLL_LEN, 0);
  int idx = 0;
  char last = ndv[0];

  // Keep a count of how many times a value appears in succession. We encode this count as
  // a byte 0-255, but the actual count is always one more than the encoded value
  // (i.e. in the range 1-256 inclusive).
  uint8_t count = 0;
  for (int i = 1; i < AggregateFunctions::HLL_LEN; ++i) {
    if (ndv[i] != last || count == numeric_limits<uint8_t>::max()) {
      if (idx + 2 > AggregateFunctions::HLL_LEN) break;
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
  if (idx + 2 > AggregateFunctions::HLL_LEN) {
    *is_encoded = false;
    return ndv;
  }

  encoded_ndv[idx++] = count;
  encoded_ndv[idx++] = last;

  *is_encoded = true;
  encoded_ndv.resize(idx);
  DCHECK_GT(encoded_ndv.size(), 0);
  DCHECK_LE(encoded_ndv.size(), AggregateFunctions::HLL_LEN);
  return encoded_ndv;
}

string DecodeNdv(const string& ndv, bool is_encoded) {
  if (!is_encoded) return ndv;
  DCHECK_EQ(ndv.size() % 2, 0);
  string decoded_ndv(AggregateFunctions::HLL_LEN, 0);
  int idx = 0;
  for (int i = 0; i < ndv.size(); i += 2) {
    for (int j = 0; j < (static_cast<uint8_t>(ndv[i])) + 1; ++j) {
      decoded_ndv[idx++] = ndv[i+1];
    }
  }
  DCHECK_EQ(idx, AggregateFunctions::HLL_LEN);
  return decoded_ndv;
}

// A container for statistics for a single column that are aggregated partition by
// partition during the incremental computation of column stats. The aggregations are
// updated during Update(), and the final statistics are computed by Finalize().
struct PerColumnStats {
  // Should have length AggregateFunctions::HLL_PRECISION. Intermediate buckets for the
  // HLL calculation.
  string intermediate_ndv;

  // The total number of nulls counted, or -1 for no sample.
  int64_t num_nulls;

  // The maximum width of the column, in bytes.
  int32_t max_width;

  // The total number of rows
  int64_t num_rows;

  // The sum of avg_width * num_rows for each partition, so that avg_width can be
  // correctly computed during Finalize()
  double total_width;

  // Populated after Finalize(), the result of the HLL computation
  int64_t ndv_estimate;

  // The average column width, in bytes (but may have non-integer value)
  double avg_width;

  PerColumnStats()
      : intermediate_ndv(AggregateFunctions::HLL_LEN, 0), num_nulls(-1),
        max_width(0), num_rows(0), avg_width(0) { }

  // Updates all aggregate statistics with a new set of measurements.
  void Update(const string& ndv, int64_t num_new_rows, double new_avg_width,
      int32_t max_new_width, int64_t num_new_nulls) {
    DCHECK_EQ(intermediate_ndv.size(), ndv.size()) << "Incompatible intermediate NDVs";
    DCHECK_GE(num_new_rows, 0);
    DCHECK_GE(max_new_width, 0);
    DCHECK_GE(new_avg_width, 0);
    DCHECK_GE(num_new_nulls, -1);
    for (int j = 0; j < ndv.size(); ++j) {
      intermediate_ndv[j] = ::max(intermediate_ndv[j], ndv[j]);
    }
    if (num_new_nulls >= 0) num_nulls += num_new_nulls;
    max_width = ::max(max_width, max_new_width);
    avg_width += (new_avg_width * num_new_rows);
    num_rows += num_new_rows;
  }

  // Performs any stats computations that are not distributive, that is they may not be
  // computed in part during Update(). After this method returns, ndv_estimate and
  // avg_width contain valid values.
  void Finalize() {
    ndv_estimate = AggregateFunctions::HllFinalEstimate(
        reinterpret_cast<const uint8_t*>(intermediate_ndv.data()),
        intermediate_ndv.size());
    avg_width = num_rows == 0 ? 0 : avg_width / num_rows;
  }

  TColumnStats ToTColumnStats() const {
    TColumnStats col_stats;
    col_stats.__set_num_distinct_values(ndv_estimate);
    col_stats.__set_num_nulls(num_nulls);
    col_stats.__set_max_size(max_width);
    col_stats.__set_avg_size(avg_width);
    return col_stats;
  }

  // Returns a string with debug information for this
  string DebugString() const {
    return Substitute(
        "ndv: $0, num_nulls: $1, max_width: $2, avg_width: $3, num_rows: $4",
        ndv_estimate, num_nulls, max_width, avg_width, num_rows);
  }
};

namespace impala {

void FinalizePartitionedColumnStats(const TTableSchema& col_stats_schema,
    const vector<TPartitionStats>& existing_part_stats,
    const vector<vector<string>>& expected_partitions, const TRowSet& rowset,
    int32_t num_partition_cols, TAlterTableUpdateStatsParams* params) {
  // The rowset should have the following schema: for every column in the source table,
  // five columns are produced, one row per partition.
  // <ndv buckets>, <num nulls>, <max width>, <avg width>, <count rows>
  static const int COLUMNS_PER_STAT = 5;

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

        stat->Update(ndv, num_rows, avg_width, max_width, num_nulls);

        // Save the intermediate state per-column, per-partition
        TIntermediateColumnStats int_stats;
        bool is_encoded;
        int_stats.__set_intermediate_ndv(EncodeNdv(ndv, &is_encoded));
        int_stats.__set_is_ndv_encoded(is_encoded);
        int_stats.__set_num_nulls(num_nulls);
        int_stats.__set_max_width(max_width);
        int_stats.__set_avg_width(avg_width);
        int_stats.__set_num_rows(num_rows);

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
      EncodeNdv(string(AggregateFunctions::HLL_LEN, 0), &is_encoded));
  empty_column_stats.__set_is_ndv_encoded(is_encoded);
  empty_column_stats.__set_num_nulls(0);
  empty_column_stats.__set_max_width(0);
  empty_column_stats.__set_avg_width(0);
  empty_column_stats.__set_num_rows(0);
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
      stats[i].Update(DecodeNdv(int_stats.intermediate_ndv, int_stats.is_ndv_encoded),
          int_stats.num_rows, int_stats.avg_width, int_stats.max_width,
          int_stats.num_nulls);
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
