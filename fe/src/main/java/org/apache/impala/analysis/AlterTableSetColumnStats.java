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

package org.apache.impala.analysis;

import java.util.Map;

import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.ColumnStats;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TAlterTableType;
import org.apache.impala.thrift.TAlterTableUpdateStatsParams;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
* Represents an ALTER TABLE [<dbName>.]<tableName> SET COLUMN STATS <colName>
* ('statsKey'='val','statsKey2',='val2') statement.
*
* The keys as well as the values are specified as string literals to be consistent
* with the existing DDL for setting TBLPROPERTIES/SERDEPROPERTIES, in particular,
* setting the 'numRows' table/partition property.
*
* Stats key comparisons are case-insensitive.
*/
public class AlterTableSetColumnStats extends AlterTableStmt {
  private final String colName_;
  private final Map<String, String> statsMap_;

  // Complete column stats reflecting this alteration. Existing stats values
  // are preserved. Result of analysis.
  private ColumnStats colStats_;

  public AlterTableSetColumnStats(TableName tableName, String colName,
      Map<String, String> statsMap) {
    super(tableName);
    colName_ = colName;
    statsMap_ = statsMap;
  }

  @Override
  public String getOperation() { return "SET COLUMN STATS"; }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);

    Column col = getTargetTable().getColumn(colName_);
    if (col == null) {
      throw new AnalysisException(
          String.format("Column '%s' does not exist in table: %s",
              colName_, getTargetTable().getFullName()));
    }
    // Cannot update stats on partition columns because the HMS has no entries
    // for them, and the stats can be computed directly from the metadata.
    if (col.getPosition() < getTargetTable().getNumClusteringCols()) {
      throw new AnalysisException(
          "Updating the stats of a partition column is not allowed: " + colName_);
    }
    // Cannot update the stats if they are not supported for the column's type.
    if (!ColumnStats.isSupportedColType(col.getType())) {
      throw new AnalysisException(String.format(
          "Statistics for column '%s' are not supported because " +
          "it has type '%s'.", col.getName(), col.getType().toSql()));
    }

    // Copy the existing stats and then change the values according to the
    // stats map of this stmt. The existing stats are first copied to preserve
    // those stats values that are not changed by this stmt because all stats
    // values are updated when altering the stats in the HMS.
    colStats_ = col.getStats().clone();
    for (Map.Entry<String, String> entry: statsMap_.entrySet()) {
      ColumnStats.StatsKey statsKey = ColumnStats.StatsKey.fromString(entry.getKey());
      if (statsKey == null) {
        throw new AnalysisException(String.format(
            "Invalid column stats key: %s\nValid keys are: %s",
            entry.getKey(), Joiner.on(',').join(ColumnStats.StatsKey.values())));
      }
      setStatsValue(statsKey, entry.getValue(), col, colStats_);
    }
  }

  /**
   * Updates the given column stats based on statsKey and statsValue.
   * Throws an AnalysisException if the statsValue is invalid or not applicable to the
   * column (e.g., trying to update the avg/max size of a fixed-length column).
   */
  private void setStatsValue(ColumnStats.StatsKey statsKey, String statsValue,
      Column col, ColumnStats stats) throws AnalysisException {
    // Updating max/avg size is only allowed for variable length columns.
    if (col.getType().isFixedLengthType()
        && (statsKey == ColumnStats.StatsKey.AVG_SIZE
            || statsKey == ColumnStats.StatsKey.MAX_SIZE)) {
      throw new AnalysisException(String.format(
          "Cannot update the '%s' stats of column '%s' with type '%s'.\n" +
          "Changing '%s' is only allowed for variable-length columns.",
          statsKey, col.getName(), col.getType().toSql(), statsKey));
    }

    if (statsKey == ColumnStats.StatsKey.NUM_DISTINCT_VALUES
        || statsKey == ColumnStats.StatsKey.NUM_NULLS
        || statsKey == ColumnStats.StatsKey.MAX_SIZE
        || statsKey == ColumnStats.StatsKey.NUM_TRUES
        || statsKey == ColumnStats.StatsKey.NUM_FALSES) {
      Long statsVal = null;
      try {
        statsVal = Long.parseLong(statsValue);
      } catch (Exception e) {
      }
      if (statsVal == null || statsVal < -1) {
        throw new AnalysisException(String.format(
            "Invalid stats value '%s' for column stats key: %s\n" +
            "Expected a non-negative integer or -1 for unknown.",
            statsValue, statsKey));
      }
      stats.update(col.getType(), statsKey, statsVal);
    } else if (statsKey == ColumnStats.StatsKey.AVG_SIZE) {
      Float statsVal = null;
      try {
        statsVal = Float.parseFloat(statsValue);
      } catch (Exception e) {
      }
      if (statsVal == null || (statsVal < 0 && statsVal != -1) ||
          statsVal.isNaN() || statsVal.isInfinite()) {
        throw new AnalysisException(String.format(
            "Invalid stats value '%s' for column stats key: %s\n" +
            "Expected a non-negative floating-point number or -1 for unknown.",
            statsValue, statsKey));
      }
      stats.update(col.getType(), statsKey, statsVal);
    } else {
      Preconditions.checkState(false, "Unhandled StatsKey value: " + statsKey);
    }
  }

  @Override
  public TAlterTableParams toThrift() {
   TAlterTableParams params = super.toThrift();
   params.setAlter_type(TAlterTableType.UPDATE_STATS);
   TAlterTableUpdateStatsParams updateStatsParams =
       new TAlterTableUpdateStatsParams();
   updateStatsParams.setTable_name(getTargetTable().getTableName().toThrift());
   updateStatsParams.putToColumn_stats(colName_.toString(), colStats_.toThrift());
   params.setUpdate_stats_params(updateStatsParams);
   return params;
  }
}
