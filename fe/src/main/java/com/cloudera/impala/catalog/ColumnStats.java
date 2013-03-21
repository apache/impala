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

  private float avgSerializedSize;  // in bytes; includes serialization overhead
  private long maxSize;  // in bytes
  private long numDistinctValues;
  private boolean hasNulls;

  /**
   * For fixed-length type (those which don't need additional storage besides
   * the slot they occupy), sets avgSerializedSize and maxSize to their slot size.
   */
  public ColumnStats(PrimitiveType colType) {
    avgSerializedSize = -1;
    maxSize = -1;
    numDistinctValues = -1;
    hasNulls = true;
    if (colType.isFixedLengthType()) {
      avgSerializedSize = colType.getSlotSize();
      maxSize = colType.getSlotSize();
    }
  }

  public void setAvgSerializedSize(float avgSize) {
    this.avgSerializedSize = avgSize;
  }

  public void setMaxSize(long maxSize) {
    this.maxSize = maxSize;
  }

  public void setNumDistinctValues(long numDistinctValues) {
    this.numDistinctValues = numDistinctValues;
  }

  public void setHasNulls(boolean hasNulls) {
    this.hasNulls = hasNulls;
  }

  public float getAvgSerializedSize() {
    return avgSerializedSize;
  }

  public long getMaxSize() {
    return maxSize;
  }

  public long getNumDistinctValues() {
    return numDistinctValues;
  }

  public boolean hasNulls() {
    return hasNulls;
  }

  public boolean hasAvgSerializedSize() {
    return avgSerializedSize >= 0;
  }

  public boolean hasMaxSize() {
    return maxSize >= 0;
  }

  public boolean hasNumDistinctValues() {
    return numDistinctValues >= 0;
  }

  public void update(PrimitiveType colType, ColumnStatisticsData statsData) {
    switch (colType) {
      case BOOLEAN:
        Preconditions.checkState(statsData.isSetBooleanStats());
        BooleanColumnStatsData boolStats = statsData.getBooleanStats();
        hasNulls = boolStats.getNumNulls() > 0;
        break;
      case TINYINT:
      case SMALLINT:
      case INT:
      case BIGINT:
        Preconditions.checkState(statsData.isSetLongStats());
        LongColumnStatsData longStats = statsData.getLongStats();
        numDistinctValues = longStats.getNumDVs();
        hasNulls = longStats.getNumNulls() > 0;
        break;
      case FLOAT:
      case DOUBLE:
        Preconditions.checkState(statsData.isSetDoubleStats());
        DoubleColumnStatsData doubleStats = statsData.getDoubleStats();
        numDistinctValues = doubleStats.getNumDVs();
        hasNulls = doubleStats.getNumNulls() > 0;
        break;
      case STRING:
        Preconditions.checkState(statsData.isSetStringStats());
        StringColumnStatsData stringStats = statsData.getStringStats();
        numDistinctValues = stringStats.getNumDVs();
        hasNulls = stringStats.getNumNulls() > 0;
        maxSize = stringStats.getMaxColLen();
        avgSerializedSize = PrimitiveType.STRING.getSlotSize()
            + Double.valueOf(stringStats.getAvgColLen()).floatValue();
        break;
      case BINARY:
        Preconditions.checkState(statsData.isSetBinaryStats());
        BinaryColumnStatsData binaryStats = statsData.getBinaryStats();
        hasNulls = binaryStats.getNumNulls() > 0;
        maxSize = binaryStats.getMaxColLen();
        avgSerializedSize = PrimitiveType.BINARY.getSlotSize()
            + Double.valueOf(binaryStats.getAvgColLen()).floatValue();
        break;
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this.getClass())
        .add("avgSerializedSize", avgSerializedSize)
        .add("maxSize", maxSize)
        .add("numDistinct", numDistinctValues)
        .add("hasNulls", hasNulls)
        .toString();
  }
}
