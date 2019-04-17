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

import java.util.Set;

import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.thrift.TColumnStats;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.math.LongMath;

/**
 * Statistics for a single column.
 */
public class ColumnStats {
  // Set of the currently supported column stats column types.
  private final static Set<PrimitiveType> SUPPORTED_COL_TYPES = Sets.newHashSet(
      PrimitiveType.BIGINT, PrimitiveType.BINARY, PrimitiveType.BOOLEAN,
      PrimitiveType.DATE, PrimitiveType.DOUBLE, PrimitiveType.FLOAT,
      PrimitiveType.INT, PrimitiveType.SMALLINT, PrimitiveType.CHAR,
      PrimitiveType.VARCHAR, PrimitiveType.STRING, PrimitiveType.TIMESTAMP,
      PrimitiveType.TINYINT, PrimitiveType.DECIMAL);

  public enum StatsKey {
    NUM_DISTINCT_VALUES("numDVs"),
    NUM_NULLS("numNulls"),
    AVG_SIZE("avgSize"),
    MAX_SIZE("maxSize");

    private final String name_;

    private StatsKey(String name) { name_ = name; }

    /**
     * Returns the StatsKey whose name matches 'key'. The comparison is
     * case insensitive. Returns null if there is no matching StatsKey.
     */
    public static StatsKey fromString(String key) {
      for (StatsKey k: values()) {
        if (key.equalsIgnoreCase(k.name_)) return k;
      }
      return null;
    }

    @Override
    public String toString() { return name_; }
  }

  // in bytes: excludes serialization overhead
  private double avgSize_;
  // in bytes; includes serialization overhead.
  private double avgSerializedSize_;
  private long maxSize_;  // in bytes
  private long numDistinctValues_;
  private long numNulls_;

  public ColumnStats(Type colType) {
    initColStats(colType);
  }

  /**
   * C'tor for clone().
   */
  private ColumnStats(ColumnStats other) {
    avgSize_ = other.avgSize_;
    avgSerializedSize_ = other.avgSerializedSize_;
    maxSize_ = other.maxSize_;
    numDistinctValues_ = other.numDistinctValues_;
    numNulls_ = other.numNulls_;
  }

  /**
   * Initializes all column stats values as "unknown". For fixed-length type
   * (those which don't need additional storage besides the slot they occupy),
   * sets avgSerializedSize and maxSize to their slot size.
   */
  private void initColStats(Type colType) {
    avgSize_ = -1;
    avgSerializedSize_ = -1;
    maxSize_ = -1;
    numDistinctValues_ = -1;
    numNulls_ = -1;
    if (colType.isFixedLengthType()) {
      avgSerializedSize_ = colType.getSlotSize();
      avgSize_ = colType.getSlotSize();
      maxSize_ = colType.getSlotSize();
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
    SlotRef slotRef = expr.unwrapSlotRef(false);
    if (slotRef == null) return stats;
    ColumnStats slotStats = slotRef.getDesc().getStats();
    if (slotStats == null) return stats;
    stats.numNulls_ = slotStats.getNumNulls();
    stats.avgSerializedSize_ = slotStats.getAvgSerializedSize();
    stats.avgSize_ = slotStats.getAvgSize();
    stats.maxSize_ = slotStats.getMaxSize();
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
    if (numDistinctValues_ == -1 || other.numDistinctValues_ == -1) {
      numDistinctValues_ = -1;
    } else {
      numDistinctValues_ += other.numDistinctValues_;
    }
    if (numNulls_ == -1 || other.numNulls_ == -1) {
      numNulls_ = -1;
    } else {
      numNulls_ += other.numNulls_;
    }
    return this;
  }

  public long getNumDistinctValues() { return numDistinctValues_; }
  public void setNumDistinctValues(long numDistinctValues) {
    numDistinctValues_ = numDistinctValues;
  }
  public void setNumNulls(long numNulls) { numNulls_ = numNulls; }
  public double getAvgSerializedSize() { return avgSerializedSize_; }
  public double getAvgSize() { return avgSize_; }
  public long getMaxSize() { return maxSize_; }
  public boolean hasNulls() { return numNulls_ > 0; }
  public long getNumNulls() { return numNulls_; }
  public boolean hasAvgSize() { return avgSize_ >= 0; }
  public boolean hasNumDistinctValues() { return numDistinctValues_ >= 0; }
  public boolean hasStats() { return numNulls_ != -1 || numDistinctValues_ != -1; }

  /**
   * Updates the stats with the given ColumnStatisticsData. If the ColumnStatisticsData
   * is not compatible with the given colType, all stats are initialized based on
   * initColStats().
   * Returns false if the ColumnStatisticsData data was incompatible with the given
   * column type, otherwise returns true.
   */
  public boolean update(Type colType, ColumnStatisticsData statsData) {
    Preconditions.checkState(isSupportedColType(colType));
    initColStats(colType);
    boolean isCompatible = false;
    switch (colType.getPrimitiveType()) {
      case BOOLEAN:
        isCompatible = statsData.isSetBooleanStats();
        if (isCompatible) {
          BooleanColumnStatsData boolStats = statsData.getBooleanStats();
          numNulls_ = boolStats.getNumNulls();
          numDistinctValues_ = (numNulls_ > 0) ? 3 : 2;
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
          numDistinctValues_ = longStats.getNumDVs();
          numNulls_ = longStats.getNumNulls();
        }
        break;
      case DATE:
        isCompatible = statsData.isSetDateStats();
        if (isCompatible) {
          DateColumnStatsData dateStats = statsData.getDateStats();
          numDistinctValues_ = dateStats.getNumDVs();
          numNulls_ = dateStats.getNumNulls();
        }
        break;
      case FLOAT:
      case DOUBLE:
        isCompatible = statsData.isSetDoubleStats();
        if (isCompatible) {
          DoubleColumnStatsData doubleStats = statsData.getDoubleStats();
          numDistinctValues_ = doubleStats.getNumDVs();
          numNulls_ = doubleStats.getNumNulls();
        }
        break;
      case CHAR:
      case VARCHAR:
      case STRING:
        isCompatible = statsData.isSetStringStats();
        if (isCompatible) {
          StringColumnStatsData stringStats = statsData.getStringStats();
          numDistinctValues_ = stringStats.getNumDVs();
          numNulls_ = stringStats.getNumNulls();
          maxSize_ = stringStats.getMaxColLen();
          avgSize_ = Double.valueOf(stringStats.getAvgColLen()).floatValue();
          avgSerializedSize_ = avgSize_ + PrimitiveType.STRING.getSlotSize();
        }
        break;
      case BINARY:
        isCompatible = statsData.isSetStringStats();
        if (isCompatible) {
          BinaryColumnStatsData binaryStats = statsData.getBinaryStats();
          numNulls_ = binaryStats.getNumNulls();
          maxSize_ = binaryStats.getMaxColLen();
          avgSize_ = Double.valueOf(binaryStats.getAvgColLen()).floatValue();
          avgSerializedSize_ = avgSize_ + PrimitiveType.BINARY.getSlotSize();
        }
        break;
      case DECIMAL:
        isCompatible = statsData.isSetDecimalStats();
        if (isCompatible) {
          DecimalColumnStatsData decimalStats = statsData.getDecimalStats();
          numNulls_ = decimalStats.getNumNulls();
          numDistinctValues_ = decimalStats.getNumDVs();
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
   * Convert the statistics back into an HMS-compatible ColumnStatisticsData object.
   * This is essentially the inverse of {@link #update(Type, ColumnStatisticsData)
   * above.
   *
   * Returns null if statistics for the specified type are not supported.
   */
  public static ColumnStatisticsData createHiveColStatsData(
      long capNdv, TColumnStats colStats, Type colType) {
    ColumnStatisticsData colStatsData = new ColumnStatisticsData();
    long ndv = colStats.getNum_distinct_values();
    // Cap NDV at row count if available.
    if (capNdv >= 0) ndv = Math.min(ndv, capNdv);

    long numNulls = colStats.getNum_nulls();
    switch(colType.getPrimitiveType()) {
      case BOOLEAN:
        colStatsData.setBooleanStats(new BooleanColumnStatsData(1, -1, numNulls));
        break;
      case TINYINT:
        ndv = Math.min(ndv, LongMath.pow(2, Byte.SIZE));
        colStatsData.setLongStats(new LongColumnStatsData(numNulls, ndv));
        break;
      case SMALLINT:
        ndv = Math.min(ndv, LongMath.pow(2, Short.SIZE));
        colStatsData.setLongStats(new LongColumnStatsData(numNulls, ndv));
        break;
      case INT:
        ndv = Math.min(ndv, LongMath.pow(2, Integer.SIZE));
        colStatsData.setLongStats(new LongColumnStatsData(numNulls, ndv));
        break;
      case DATE:
        // Number of distinct dates in the 0000-01-01..9999-12-31 inclusive range is
        // 3652425.
        ndv = Math.min(ndv, 3652425);
        colStatsData.setDateStats(new DateColumnStatsData(numNulls, ndv));
        break;
      case BIGINT:
      case TIMESTAMP: // Hive and Impala use LongColumnStatsData for timestamps.
        colStatsData.setLongStats(new LongColumnStatsData(numNulls, ndv));
        break;
      case FLOAT:
      case DOUBLE:
        colStatsData.setDoubleStats(new DoubleColumnStatsData(numNulls, ndv));
        break;
      case CHAR:
      case VARCHAR:
      case STRING:
        long maxStrLen = colStats.getMax_size();
        double avgStrLen = colStats.getAvg_size();
        colStatsData.setStringStats(
            new StringColumnStatsData(maxStrLen, avgStrLen, numNulls, ndv));
        break;
      case DECIMAL:
        double decMaxNdv = Math.pow(10, colType.getPrecision());
        ndv = (long) Math.min(ndv, decMaxNdv);
        colStatsData.setDecimalStats(new DecimalColumnStatsData(numNulls, ndv));
        break;
      default:
        return null;
    }
    return colStatsData;
  }

  public ColumnStatisticsData toHmsCompatibleThrift(Type colType) {
    return createHiveColStatsData(-1, toThrift(), colType);
  }

  /**
   * Sets the member corresponding to the given stats key to 'value'.
   * Requires that the given value is of a type appropriate for the
   * member being set. Throws if that is not the case.
   */
  public void update(StatsKey key, Number value) {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(value);
    if (key == StatsKey.AVG_SIZE) {
      Preconditions.checkArgument(value instanceof Float);
    } else {
      Preconditions.checkArgument(value instanceof Long);
    }
    switch (key) {
      case NUM_DISTINCT_VALUES: {
        numDistinctValues_ = (Long) value;
        break;
      }
      case NUM_NULLS: {
        numNulls_ = (Long) value;
        break;
      }
      case AVG_SIZE: {
        avgSize_ = (Float) value;
        break;
      }
      case MAX_SIZE: {
        maxSize_ = (Long) value;
        break;
      }
      default: Preconditions.checkState(false);
    }
  }

  /**
   * Returns true if the given PrimitiveType supports column stats updates.
   */
  public static boolean isSupportedColType(Type colType) {
    if (!colType.isScalarType()) return false;
    ScalarType scalarType = (ScalarType) colType;
    return SUPPORTED_COL_TYPES.contains(scalarType.getPrimitiveType());
  }

  public void update(Type colType, TColumnStats stats) {
    initColStats(colType);
    avgSize_ = Double.valueOf(stats.getAvg_size()).floatValue();
    if (colType.getPrimitiveType() == PrimitiveType.STRING ||
        colType.getPrimitiveType() == PrimitiveType.BINARY) {
      avgSerializedSize_ = colType.getSlotSize() + avgSize_;
    }
    maxSize_ = stats.getMax_size();
    numDistinctValues_ = stats.getNum_distinct_values();
    numNulls_ = stats.getNum_nulls();
  }

  public TColumnStats toThrift() {
    TColumnStats colStats = new TColumnStats();
    colStats.setAvg_size(avgSize_);
    colStats.setMax_size(maxSize_);
    colStats.setNum_distinct_values(numDistinctValues_);
    colStats.setNum_nulls(numNulls_);
    return colStats;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this.getClass())
        .add("avgSerializedSize_", avgSerializedSize_)
        .add("maxSize_", maxSize_)
        .add("numDistinct_", numDistinctValues_)
        .add("numNulls_", numNulls_)
        .toString();
  }

  @Override
  public ColumnStats clone() { return new ColumnStats(this); }
}
