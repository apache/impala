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

package org.apache.impala.catalog;

import com.google.common.base.Preconditions;

import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nullable;

/**
 * Class that encapsulate numRows, totalSize, and column stats of a table for planner
 * test. This stats is used in-place of stats from HMS. See an example at
 * TpcdsCpuCostPlannerTest.java.
 */
public class SideloadTableStats {
  private final String tableName_;
  private final long numRows_;
  private final long totalSize_;
  private final Map<String, ColumnStatisticsData> columnStats_;

  public SideloadTableStats(String tableName, long numRows, long totalSize) {
    tableName_ = tableName;
    numRows_ = numRows;
    totalSize_ = totalSize;
    columnStats_ = new HashMap<>();
  }

  public void addColumnStats(String colName, ColumnStatisticsData colStats) {
    Preconditions.checkNotNull(colName);
    Preconditions.checkNotNull(colStats);
    Preconditions.checkArgument(!colName.isEmpty());
    columnStats_.put(colName, colStats);
  }

  public long getNumRows() { return numRows_; }
  public long getTotalSize() { return totalSize_; }
  public boolean hasColumn(String colName) { return columnStats_.containsKey(colName); }

  public @Nullable ColumnStatisticsData getColumnStats(String colName) {
    return columnStats_.get(colName);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("SideloadTableStats(");
    sb.append("tableName:").append(tableName_);
    sb.append(", numRows:").append(numRows_);
    sb.append(", columns:{");
    int numCols = 0;
    for (Map.Entry<String, ColumnStatisticsData> entry : columnStats_.entrySet()) {
      if (numCols > 0) sb.append(", ");
      sb.append(entry.getKey()).append(": ").append(entry.getValue());
      numCols++;
    }
    sb.append("})");
    return sb.toString();
  }
}
