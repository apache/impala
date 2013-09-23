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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * Statistics for a single column.
 */
public class ColumnStats {
  private final static Logger LOG = LoggerFactory.getLogger(ColumnStats.class);

  // Set of the currently supported column stats column types.
  private final static EnumSet<PrimitiveType> SUPPORTED_COL_TYPES = EnumSet.of(
      PrimitiveType.BIGINT, PrimitiveType.BINARY, PrimitiveType.BOOLEAN,
      PrimitiveType.DOUBLE, PrimitiveType.FLOAT, PrimitiveType.INT,
      PrimitiveType.SMALLINT, PrimitiveType.STRING, PrimitiveType.TINYINT);

  private float avgSerializedSize;  // in bytes; includes serialization overhead
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
    avgSerializedSize = -1;
    maxSize = -1;
    numDistinctValues = -1;
    numNulls = -1;
    if (colType.isFixedLengthType()) {
      avgSerializedSize = colType.getSlotSize();
      maxSize = colType.getSlotSize();
    }
  }

  public void setAvgSerializedSize(float avgSize) { this.avgSerializedSize = avgSize; }
  public void setMaxSize(long maxSize) { this.maxSize = maxSize; }
  public long getNumDistinctValues() { return numDistinctValues; }
  public void setNumDistinctValues(long numDistinctValues) {
    this.numDistinctValues = numDistinctValues;
  }
  public void setNumNulls(long numNulls) { this.numNulls = numNulls; }
  public float getAvgSerializedSize() { return avgSerializedSize; }
  public long getMaxSize() { return maxSize; }
  public boolean hasNulls() { return numNulls > 0; }
  public long getNumNulls() { return numNulls; }
  public boolean hasAvgSerializedSize() { return avgSerializedSize >= 0; }
  public boolean hasMaxSize() { return maxSize >= 0; }
  public boolean hasNumDistinctValues() { return numDistinctValues >= 0; }

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
        }
        break;
      case TINYINT:
      case SMALLINT:
      case INT:
      case BIGINT:
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
          avgSerializedSize = PrimitiveType.STRING.getSlotSize()
              + Double.valueOf(stringStats.getAvgColLen()).floatValue();
        }
        break;
      case BINARY:
        isCompatible = statsData.isSetStringStats();
        if (isCompatible) {
          BinaryColumnStatsData binaryStats = statsData.getBinaryStats();
          numNulls = binaryStats.getNumNulls();
          maxSize = binaryStats.getMaxColLen();
          avgSerializedSize = PrimitiveType.BINARY.getSlotSize()
              + Double.valueOf(binaryStats.getAvgColLen()).floatValue();
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
