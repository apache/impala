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

#include "gen-cpp/TCLIService_types.h"
#include "gen-cpp/CatalogService_types.h"
#include "gen-cpp/CatalogObjects_types.h"

#include <vector>
#include "exprs/aggregate-functions.h"
#include "common/names.h"

using namespace impala;

/// A container for statistics for a single column that are aggregated partition by
/// partition during the incremental computation of column stats. The aggregations are
/// updated during Update(), and the final statistics are computed by Finalize().
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

  // The total number of true
  int64_t num_trues;

  // The total number of false
  int64_t num_falses;

  // The sum of avg_width * num_rows for each partition, so that avg_width can be
  // correctly computed during Finalize()
  double total_width;

  // Populated after Finalize(), the result of the HLL computation
  int64_t ndv_estimate;

  // Populated after Finalize(), the average column width, in bytes (but may have
  // non-integer value)
  double avg_width;

  // The low value which is undefined if all fields in TColumnValue are not defined.
  // Otherwise, it is the one associated with the field defined (set).
  TColumnValue low_value;

  // The high value which is undefined if all fields in TColumnValue are not defined.
  // Otherwise, it is the one associated with the field defined (set).
  TColumnValue high_value;

  PerColumnStats()
    : intermediate_ndv(AggregateFunctions::DEFAULT_HLL_LEN, 0),
      num_nulls(0),
      max_width(0),
      num_rows(0),
      num_trues(0),
      num_falses(0),
      total_width(0) {}

  /// Updates all aggregate statistics with a new set of measurements.
  void Update(const string& ndv, int64_t num_new_rows, double new_avg_width,
      int32_t max_new_width, int64_t num_new_nulls, int64_t num_new_trues,
      int64_t num_new_falses, const impala::TColumnValue& low_value,
      const impala::TColumnValue& high_value);

  /// Performs any stats computations that are not distributive, that is they may not be
  /// computed in part during Update(). After this method returns, ndv_estimate and
  /// avg_width contain valid values.
  void Finalize();

  /// Creates a TColumnStats object from PerColumnStats
  TColumnStats ToTColumnStats() const;

  /// Returns a string with debug information for this
  string DebugString() const;

  /// Updates the low and the high value
  void UpdateLowValue(const TColumnValue& value);
  void UpdateHighValue(const TColumnValue& value);
};

namespace impala {

/// Populates the supplied TAlterTableUpdateStatsParams argument with per-column statistics
/// that are computed by aggregating the per-partition results from the COMPUTE STATS child
/// queries, contained in the 'rowset', and any pre-existing per-partition measurements,
/// contained in 'existing_part_stats'. The schema for 'rowset' is in 'col_stats_schema'.
void FinalizePartitionedColumnStats(
    const apache::hive::service::cli::thrift::TTableSchema& col_stats_schema,
    const std::vector<TPartitionStats>& existing_part_stats,
    const std::vector<std::vector<std::string>>& expected_partitions,
    const apache::hive::service::cli::thrift::TRowSet& rowset,
    int32_t num_partition_cols, TAlterTableUpdateStatsParams* params);
}
