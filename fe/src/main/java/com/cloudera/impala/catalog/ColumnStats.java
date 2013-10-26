// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.catalog;

import java.util.EnumSet;

import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.SlotRef;
import com.cloudera.impala.thrift.TColumnStats;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * Statistics for a single column.
 */
public class ColumnStats {
  private final static Logger LOG = LoggerFactory.getLogger(ColumnStats.class);
  private TColumnStats colStats;

  // Set of the currently supported column stats column types.
  private final static EnumSet<PrimitiveType> SUPPORTED_COL_TYPES = EnumSet.of(
      PrimitiveType.BIGINT, PrimitiveType.BINARY, PrimitiveType.BOOLEAN,
      PrimitiveType.DOUBLE, PrimitiveType.FLOAT, PrimitiveType.INT,
      PrimitiveType.SMALLINT, PrimitiveType.STRING, PrimitiveType.TIMESTAMP,
      PrimitiveType.TINYINT);

  // in bytes: excludes serialization overhead
  private double avgSize;
  // in bytes; includes serialization overhead.
  private double avgSerializedSize;
  private long maxSize;  // in bytes
  private long numDistinctValues;
  private long numNulls;

  public ColumnStats(PrimitiveType colType) {
    initColStats(colType);
  }

  /**
   * Initializes all column stats values as "unknown". For fixed-length type
   * (those which don't need additional storage besides the slot they occupy),
   * sets avgSerializedSize and maxSize to their slot size.
   */
  private void initColStats(PrimitiveType colType) {
    avgSize = -1;
    avgSerializedSize = -1;
    maxSize = -1;
    numDistinctValues = -1;
    numNulls = -1;
    if (colType.isFixedLengthType()) {
      avgSerializedSize = colType.getSlotSize();
      avgSize = colType.getSlotSize();
      maxSize = colType.getSlotSize();
    }
  }

  /**
   * Creates ColumnStats from the given expr. Sets numDistinctValues and if the expr
   * is a SlotRef also numNulls.
   */
  public static ColumnStats fromExpr(Expr expr) {
    Preconditions.checkNotNull(expr);
    Preconditions.checkState(expr.getType().isValid());
    ColumnStats stats = new ColumnStats(expr.getType());
    stats.setNumDistinctValues(expr.getNumDistinctValues());
    SlotRef slotRef = expr.unwrapSlotRef();
    if (slotRef == null) return stats;
    stats.numNulls = slotRef.getDesc().getStats().getNumNulls();
    return stats;
  }

  /**
   * Adds other's numDistinctValues and numNulls to this ColumnStats.
   * If this or other's stats are invalid, sets the corresponding stat to invalid,
   * Returns this with the updated stats.
   * This method is used to aggregate stats for slots that originate from multiple
   * source slots, e.g., those produced by union queries.
   */
  public ColumnStats add(ColumnStats other) {
    if (numDistinctValues != -1 || other.numDistinctValues == -1) {
      numDistinctValues = -1;
    } else {
      numDistinctValues += other.numDistinctValues;
    }
    if (numNulls != -1 || other.numNulls != -1) {
      numNulls = -1;
    } else {
      numNulls += other.numNulls;
    }
    return this;
  }

  public void setAvgSerializedSize(float avgSize) { this.avgSerializedSize = avgSize; }
  public void setMaxSize(long maxSize) { this.maxSize = maxSize; }
  public long getNumDistinctValues() { return numDistinctValues; }
  public void setNumDistinctValues(long numDistinctValues) {
    this.numDistinctValues = numDistinctValues;
  }
  public void setNumNulls(long numNulls) { this.numNulls = numNulls; }
  public double getAvgSerializedSize() { return avgSerializedSize; }
  public double getAvgSize() { return avgSize; }
  public long getMaxSize() { return maxSize; }
  public boolean hasNulls() { return numNulls > 0; }
  public long getNumNulls() { return numNulls; }
  public boolean hasAvgSerializedSize() { return avgSerializedSize >= 0; }
  public boolean hasMaxSize() { return maxSize >= 0; }
  public boolean hasNumDistinctValues() { return numDistinctValues >= 0; }
  public boolean hasStats() { return numNulls != -1 || numDistinctValues != -1; }

  /**
   * Updates the stats with the given ColumnStatisticsData. If the ColumnStatisticsData
   * is not compatible with the given colType, all stats are initialized based on
   * initColStats().
   * Returns false if the ColumnStatisticsData data was incompatible with the given
   * column type, otherwise returns true.
   */
  public boolean update(PrimitiveType colType, ColumnStatisticsData statsData) {
    Preconditions.checkState(SUPPORTED_COL_TYPES.contains(colType));
    initColStats(colType);
    boolean isCompatible = false;
    switch (colType) {
      case BOOLEAN:
        isCompatible = statsData.isSetBooleanStats();
        if (isCompatible) {
          BooleanColumnStatsData boolStats = statsData.getBooleanStats();
          numNulls = boolStats.getNumNulls();
          numDistinctValues = (numNulls > 0) ? 3 : 2;
        }
        break;
      case TINYINT:
      case SMALLINT:
      case INT:
      case BIGINT:
      case TIMESTAMP: // Hive and Impala use LongColumnStatsData for timestamps.
        isCompatible = statsData.isSetLongStats();
        if (isCompatible) {
          LongColumnStatsData longStats = statsData.getLongStats();
          numDistinctValues = longStats.getNumDVs();
          numNulls = longStats.getNumNulls();
        }
        break;
      case FLOAT:
      case DOUBLE:
        isCompatible = statsData.isSetDoubleStats();
        if (isCompatible) {
          DoubleColumnStatsData doubleStats = statsData.getDoubleStats();
          numDistinctValues = doubleStats.getNumDVs();
          numNulls = doubleStats.getNumNulls();
        }
        break;
      case STRING:
        isCompatible = statsData.isSetStringStats();
        if (isCompatible) {
          StringColumnStatsData stringStats = statsData.getStringStats();
          numDistinctValues = stringStats.getNumDVs();
          numNulls = stringStats.getNumNulls();
          maxSize = stringStats.getMaxColLen();
          avgSize = Double.valueOf(stringStats.getAvgColLen()).floatValue();
          avgSerializedSize = avgSize + PrimitiveType.STRING.getSlotSize();
        }
        break;
      case BINARY:
        isCompatible = statsData.isSetStringStats();
        if (isCompatible) {
          BinaryColumnStatsData binaryStats = statsData.getBinaryStats();
          numNulls = binaryStats.getNumNulls();
          maxSize = binaryStats.getMaxColLen();
          avgSize = Double.valueOf(binaryStats.getAvgColLen()).floatValue();
          avgSerializedSize = avgSize + PrimitiveType.BINARY.getSlotSize();
        }
        break;
      default:
        Preconditions.checkState(false,
            "Unexpected column type: " + colType.toString());
        break;
    }
    return isCompatible;
  }

  /**
   * Returns true if the given PrimitiveType supports column stats updates.
   */
  public static boolean isSupportedColType(PrimitiveType colType) {
    return SUPPORTED_COL_TYPES.contains(colType);
  }

  public void update(PrimitiveType colType, TColumnStats stats) {
    avgSize = Double.valueOf(stats.getAvg_size()).floatValue();
    avgSerializedSize = colType.getSlotSize() + avgSize;
    maxSize = stats.getMax_size();
    numDistinctValues = stats.getNum_distinct_values();
    numNulls = stats.getNum_nulls();
  }

  public TColumnStats toThrift() {
    TColumnStats colStats = new TColumnStats();
    colStats.setAvg_size(avgSize);
    colStats.setMax_size(maxSize);
    colStats.setNum_distinct_values(numDistinctValues);
    colStats.setNum_nulls(numNulls);
    return colStats;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this.getClass())
        .add("avgSerializedSize", avgSerializedSize)
        .add("maxSize", maxSize)
        .add("numDistinct", numDistinctValues)
        .add("numNulls", numNulls)
        .toString();
  }
}
