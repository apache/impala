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

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.Date;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.analysis.DateLiteral;
import org.apache.impala.analysis.NumericLiteral;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.util.MetaStoreUtil;
import org.apache.impala.thrift.TColumnStats;
import org.apache.impala.thrift.TColumnValue;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.math.LongMath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private final static Logger LOG = LoggerFactory.getLogger(ColumnStats.class);

  public enum StatsKey {
    NUM_DISTINCT_VALUES("numDVs"),
    NUM_NULLS("numNulls"),
    AVG_SIZE("avgSize"),
    MAX_SIZE("maxSize"),
    NUM_TRUES("numTrues"),
    NUM_FALSES("numFalses");

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

  // in bytes: excludes serialization overhead.
  // -1 if unknown. Always has a valid value for fixed-length types.
  private double avgSize_;
  // in bytes; includes serialization overhead.
  // -1 if unknown. Always has a valid value for fixed-length types.
  // avgSerializedSize_ is valid iff avgSize_ is valid.
  private double avgSerializedSize_;
  private long maxSize_;  // in bytes
  private long numDistinctValues_;
  private long numNulls_;
  private long numTrues_;
  private long numFalses_;
  private TColumnValue lowValue_;
  private TColumnValue highValue_;

  public ColumnStats(Type colType) {
    initColStats(colType);
    validate(colType);
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
    numTrues_ = other.numTrues_;
    numFalses_ = other.numFalses_;
    lowValue_ = other.lowValue_;
    highValue_ = other.highValue_;
    validate(null);
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
    numTrues_ = -1;
    numFalses_ = -1;
    lowValue_ = null;
    highValue_ = null;
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
  public static ColumnStats fromExpr(Expr expr) { return fromExpr(expr, null); }

  /**
   * A variant of {@link #fromExpr(Expr)} that may reduce numDistinctValues
   * if 'ignoreColumn' contains expr's column.
   */
  public static ColumnStats fromExpr(Expr expr, @Nullable Set<Column> ignoreColumn) {
    Preconditions.checkNotNull(expr);
    Preconditions.checkState(expr.getType().isValid(), expr);
    Type colType = expr.getType();
    ColumnStats stats = new ColumnStats(colType);
    stats.setNumDistinctValues(expr.getNumDistinctValues());
    SlotRef slotRef = expr.unwrapSlotRef(false);
    if (slotRef == null) return stats;
    ColumnStats slotStats = ignoreColumn != null ?
        slotRef.getDesc().getStats(ignoreColumn) :
        slotRef.getDesc().getStats();
    if (slotStats == null) return stats;
    if (ignoreColumn != null && slotStats.hasNumDistinctValues()
        && slotStats.getNumDistinctValues() < stats.getNumDistinctValues()) {
      stats.setNumDistinctValues(slotStats.getNumDistinctValues());
    }
    stats.numNulls_ = slotStats.getNumNulls();
    if (!colType.isFixedLengthType()) {
      stats.avgSerializedSize_ = slotStats.getAvgSerializedSize();
      stats.avgSize_ = slotStats.getAvgSize();
      stats.maxSize_ = slotStats.getMaxSize();
    }
    stats.numTrues_ = slotStats.getNumTrues();
    stats.numFalses_ = slotStats.getNumFalses();
    stats.lowValue_ = slotStats.getLowValue();
    stats.highValue_ = slotStats.getHighValue();
    stats.validate(colType);
    return stats;
  }

  /**
   * Adds other's numDistinctValues, numNulls, numTrues, numFalses to this ColumnStats.
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
    if (numTrues_ == -1 || other.numTrues_ == -1) {
      numTrues_ = -1;
    } else {
      numTrues_ += other.numTrues_;
    }
    if (numFalses_ == -1 || other.numFalses_ == -1) {
      numFalses_ = -1;
    } else {
      numFalses_ += other.numFalses_;
    }
    validate(null);
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
  public boolean hasNullsStats() { return numNulls_ != -1; }
  public long getNumNulls() { return numNulls_; }
  // True iff getAvgSize() and getAvgSerializedSize() will return valid values.
  public boolean hasAvgSize() { return avgSize_ >= 0; }
  public long getNumTrues() { return numTrues_; }
  public long getNumFalses() { return numFalses_; }
  public boolean hasNumDistinctValues() { return numDistinctValues_ >= 0; }
  public boolean hasStats() { return numNulls_ != -1 || numDistinctValues_ != -1; }
  public TColumnValue getLowValue() { return lowValue_; }
  public TColumnValue getHighValue() { return highValue_; }

  /**
   * Return the value of a defined field as a string. Return -1 if 'value' is null,
   * all fields are undefined, or a field is defined but its value is null.
   */
  public String getTColumnValueAsString(TColumnValue value) {
    if (value==null) return "-1";
    StringBuilder sb = new StringBuilder();

    if (value.isSetBool_val()) {
      sb.append(value.bool_val);
    } else if (value.isSetByte_val()) {
      sb.append(value.byte_val);
    } else if (value.isSetShort_val()) {
      sb.append(value.short_val);
    } else if (value.isSetInt_val()) {
      sb.append(value.int_val);
    } else if (value.isSetLong_val()) {
      sb.append(value.long_val);
    } else if (value.isSetDouble_val()) {
      sb.append(value.double_val);
    } else if (value.isSetString_val()) {
      if (value.string_val == null) {
        sb.append("-1");
      } else {
        sb.append(value.string_val);
      }
    } else if (value.isSetBinary_val()) {
      if (value.binary_val == null) {
        sb.append("-1");
      } else {
        org.apache.thrift.TBaseHelper.toString(value.binary_val, sb);
      }
    } else if (value.isSetTimestamp_val()) {
      if (value.timestamp_val == null) {
        sb.append("-1");
      } else {
        org.apache.thrift.TBaseHelper.toString(value.timestamp_val, sb);
      }
    } else if (value.isSetDecimal_val()) {
      if (value.decimal_val == null) {
        sb.append("-1");
      } else {
        sb.append(new String(value.getDecimal_val()));
      }
    } else if (value.isSetDate_val()) {
      LocalDate d = LocalDate.ofEpochDay(value.date_val);
      sb.append(d.toString());
    } else {
      sb.append("-1");
    }
    return sb.toString();
  }

  /*
   * Return the low and high value as a string.
   */
  public String getLowValueAsString() { return getTColumnValueAsString(lowValue_); }
  public String getHighValueAsString() { return getTColumnValueAsString(highValue_); }

  /*
   * Update the low value with a numeric literal
   */
  protected void updateLowValue(NumericLiteral literal) {
    if (lowValue_ == null) lowValue_ = new TColumnValue();
    if (literal.getType().isScalarType(PrimitiveType.TINYINT)) {
      int value = literal.getIntValue();
      if (!lowValue_.isSetByte_val() || value < lowValue_.getByte_val()) {
        lowValue_.setByte_val((byte) value);
      }
    } else if (literal.getType().isScalarType(PrimitiveType.SMALLINT)) {
      int value = literal.getIntValue();
      if (!lowValue_.isSetShort_val() || value < lowValue_.getShort_val()) {
        lowValue_.setShort_val((short) value);
      }
    } else if (literal.getType().isScalarType(PrimitiveType.INT)) {
      int value = literal.getIntValue();
      if (!lowValue_.isSetInt_val() || value < lowValue_.getInt_val()) {
        lowValue_.setInt_val(value);
      }
    } else if (literal.getType().isScalarType(PrimitiveType.BIGINT)) {
      long value = literal.getLongValue();
      if (!lowValue_.isSetLong_val() || value < lowValue_.getLong_val()) {
        lowValue_.setLong_val(value);
      }
    } else if (literal.getType().isFloatingPointType()) {
      double value = literal.getDoubleValue();
      if (!lowValue_.isSetDouble_val() || value < lowValue_.getDouble_val()) {
        lowValue_.setDouble_val(value);
      }
    } else if (literal.getType().isDecimal()) {
      // Decimals are represented as ASCII strings in bytes[] in lowValue_.
      if (!lowValue_.isSetDecimal_val() ) {
        lowValue_.setDecimal_val(literal.getValue().toString().getBytes());
      } else {
        BigDecimal value = literal.getValue();
        BigDecimal lValue = new BigDecimal(new String(lowValue_.getDecimal_val()));
        if (value.compareTo(lValue) < 0) {
          lowValue_.setDecimal_val(value.toString().getBytes());
        }
      }
    }
  }

  /*
   * Update the high value with a numeric literal
   */
  protected void updateHighValue(NumericLiteral literal) {
    if (highValue_ == null) highValue_ = new TColumnValue();
    if (literal.getType().isScalarType(PrimitiveType.TINYINT)) {
      int value = literal.getIntValue();
      if (!highValue_.isSetByte_val() || value > highValue_.getByte_val()) {
        highValue_.setByte_val((byte) value);
      }
    } else if (literal.getType().isScalarType(PrimitiveType.SMALLINT)) {
      int value = literal.getIntValue();
      if (!highValue_.isSetShort_val() || value > highValue_.getShort_val()) {
        highValue_.setShort_val((short) value);
      }
    } else if (literal.getType().isScalarType(PrimitiveType.INT)) {
      int value = literal.getIntValue();
      if (!highValue_.isSetInt_val() || value > highValue_.getInt_val()) {
        highValue_.setInt_val(value);
      }
    } else if (literal.getType().isScalarType(PrimitiveType.BIGINT)) {
      long value = literal.getLongValue();
      if (!highValue_.isSetLong_val() || value > highValue_.getLong_val()) {
        highValue_.setLong_val(value);
      }
    } else if (literal.getType().isFloatingPointType()) {
      double value = literal.getDoubleValue();
      if (!highValue_.isSetDouble_val() || value > highValue_.getDouble_val()) {
        highValue_.setDouble_val(value);
      }
    } else if (literal.getType().isDecimal()) {
      // Decimals are represented as ASCII strings in bytes[] in highValue_.
      if (!highValue_.isSetDecimal_val() ) {
        highValue_.setDecimal_val(literal.getValue().toString().getBytes());
      } else {
        BigDecimal value = literal.getValue();
        BigDecimal hValue = new BigDecimal(new String(highValue_.getDecimal_val()));
        if (value.compareTo(hValue) > 0) {
          highValue_.setDecimal_val(value.toString().getBytes());
        }
      }
    }
  }

  /*
   * Update the low value with a date literal
   */
  protected void updateLowValue(DateLiteral literal) {
    if (lowValue_ == null) lowValue_ = new TColumnValue();
    int value = literal.getValue();
    if (!lowValue_.isSetDate_val() || value < lowValue_.getDate_val()) {
      lowValue_.setDate_val(value);
    }
  }

  /*
   * Update the high value with a date literal
   */
  protected void updateHighValue(DateLiteral literal) {
    if (highValue_ == null) highValue_ = new TColumnValue();
    int value = literal.getValue();
    if (!highValue_.isSetDate_val() || value > highValue_.getDate_val()) {
      highValue_.setDate_val(value);
    }
  }

  /*
   * Update the low and the high value with 'literal'. If 'literal' is NULL or not a type
   * supported by HMS for storage, no update will be done. This method is mainly called
   * to update the low and high value for partition columns in HDFS table.
   * TODO: handle DECIMAL.
   */
  public void updateLowAndHighValue(LiteralExpr literal) {
    if (Expr.IS_NULL_LITERAL.apply(literal)) return;
    if (!MetaStoreUtil.canStoreMinmaxInHMS(literal.getType())) return;
    if (literal instanceof NumericLiteral) {
      updateLowValue((NumericLiteral) literal);
      updateHighValue((NumericLiteral) literal);
    } else if (literal instanceof DateLiteral) {
      updateLowValue((DateLiteral) literal);
      updateHighValue((DateLiteral) literal);
    }
  }

  /*
   * From the source 'longStats', set the low and high value for 'type' (one of the
   * integer types). Does not handle TIMESTAMP columns.
   */
  protected void setLowAndHighValue(PrimitiveType type, LongColumnStatsData longStats) {
    if (!longStats.isSetLowValue()) {
      lowValue_ = null;
    } else {
      long value = longStats.getLowValue();
      lowValue_ = new TColumnValue();
      switch (type) {
        case TINYINT:
          lowValue_.setByte_val((byte) value);
          break;
        case SMALLINT:
          lowValue_.setShort_val((short) value);
          break;
        case INT:
          lowValue_.setInt_val((int) value);
          break;
        case BIGINT:
          lowValue_.setLong_val(value);
          break;
        case TIMESTAMP:
          throw new IllegalStateException(
              "TIMESTAMP columns are not supported by setLowAndHighValue()");
        default:
          throw new IllegalStateException(
              "Unsupported type encountered in setLowAndHighValue()");
      }
    }

    if (!longStats.isSetHighValue()) {
      highValue_ = null;
    } else {
      long value = longStats.getHighValue();
      highValue_ = new TColumnValue();
      switch (type) {
        case TINYINT:
          highValue_.setByte_val((byte) value);
          break;
        case SMALLINT:
          highValue_.setShort_val((short) value);
          break;
        case INT:
          highValue_.setInt_val((int) value);
          break;
        case BIGINT:
          highValue_.setLong_val(value);
          break;
        case TIMESTAMP:
          throw new IllegalStateException(
              "TIMESTAMP columns are not supported by setLowAndHighValue()");
        default:
          throw new IllegalStateException(
              "Unsupported type encountered in setLowAndHighValue()");
      }
    }
  }

  /*
   * From the source 'doubleStats', set the low and high value.
   */
  protected void setLowAndHighValue(DoubleColumnStatsData doubleStats) {
    if (!doubleStats.isSetLowValue()) {
      lowValue_ = null;
    } else {
      lowValue_ = new TColumnValue();
      lowValue_.setDouble_val(doubleStats.getLowValue());
    }

    if (!doubleStats.isSetHighValue()) {
      highValue_ = null;
    } else {
      highValue_ = new TColumnValue();
      highValue_.setDouble_val(doubleStats.getHighValue());
    }
  }

  /*
   * From the source 'dateStats', set the low and high value.
   */
  protected void setLowAndHighValue(DateColumnStatsData dateStats) {
    if (!dateStats.isSetLowValue()) {
      lowValue_ = null;
    } else {
      lowValue_ = new TColumnValue();
      lowValue_.setDate_val((int)dateStats.getLowValue().getDaysSinceEpoch());
    }

    if (!dateStats.isSetHighValue()) {
      highValue_ = null;
    } else {
      highValue_ = new TColumnValue();
      highValue_.setDate_val((int)dateStats.getHighValue().getDaysSinceEpoch());
    }
  }

  /*
   * From the source 'decimalStats', set the low and high value.
   */
  protected void setLowAndHighValue(DecimalColumnStatsData decimalStats) {
    if (!decimalStats.isSetLowValue()) {
      lowValue_ = null;
    } else {
      lowValue_ = new TColumnValue();
      lowValue_.setDecimal_val(decimalStats.getLowValue().getUnscaled());
    }

    if (!decimalStats.isSetHighValue()) {
      highValue_ = null;
    } else {
      highValue_ = new TColumnValue();
      highValue_.setDecimal_val(decimalStats.getHighValue().getUnscaled());
    }
  }

  private long normalizeValue(String colName, StatsKey key, long value) {
    if (value < -1) {
      LOG.warn("Invalid {} of column {}: {}. Normalized to -1.", key, colName, value);
      return -1;
    }
    return value;
  }

  private float normalizeAvgSize(String colName, float value) {
    if (value < -1) {
      LOG.warn("Invalid avgSize of column {}: {}. Normalized to -1.", colName, value);
      return -1;
    }
    return value;
  }

  /**
   * Updates the stats with the given ColumnStatisticsData. If the ColumnStatisticsData
   * is not compatible with the given colType, all stats are initialized based on
   * initColStats().
   * Returns false if the ColumnStatisticsData data was incompatible with the given
   * column type, otherwise returns true.
   */
  public boolean update(String colName, Type colType, ColumnStatisticsData statsData) {
    Preconditions.checkState(isSupportedColType(colType));
    initColStats(colType);
    boolean isCompatible = false;

    /// Since the low and high value exist only in the following Hive stats objects:
    ///   DateColumnStatsData
    ///   LongColumnStatsData
    ///   DoubleColumnStatsData
    ///   DecimalColumnStatsData
    /// assume no low or high values are available until one with min/max values is
    /// encountered. At that point of time, setLowAndHighValue() will be called.
    lowValue_ = null;
    highValue_ = null;
    switch (colType.getPrimitiveType()) {
      case BOOLEAN:
        isCompatible = statsData.isSetBooleanStats();
        if (isCompatible) {
          BooleanColumnStatsData boolStats = statsData.getBooleanStats();
          numNulls_ = normalizeValue(colName, StatsKey.NUM_NULLS,
              boolStats.getNumNulls());
          // If we have numNulls, we can infer NDV from that.
          if (numNulls_ > 0) {
            numDistinctValues_ = 3;
          } else if (numNulls_ == 0) {
            numDistinctValues_ = 2;
          } else {
            numDistinctValues_ = -1;
          }
          numTrues_ = normalizeValue(colName, StatsKey.NUM_TRUES,
              boolStats.getNumTrues());
          numFalses_ = normalizeValue(colName, StatsKey.NUM_FALSES,
              boolStats.getNumFalses());
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
          numDistinctValues_ = normalizeValue(colName, StatsKey.NUM_DISTINCT_VALUES,
              longStats.getNumDVs());
          numNulls_ = normalizeValue(colName, StatsKey.NUM_NULLS,
              longStats.getNumNulls());
          if (colType.getPrimitiveType() != PrimitiveType.TIMESTAMP) {
            // Low/high value handling is not yet implemented for timestamps.
            setLowAndHighValue(colType.getPrimitiveType(), longStats);
          }
        }
        break;
      case DATE:
        isCompatible = statsData.isSetDateStats();
        if (isCompatible) {
          DateColumnStatsData dateStats = statsData.getDateStats();
          numDistinctValues_ = normalizeValue(colName, StatsKey.NUM_DISTINCT_VALUES,
              dateStats.getNumDVs());
          numNulls_ = normalizeValue(colName, StatsKey.NUM_NULLS,
              dateStats.getNumNulls());
          setLowAndHighValue(dateStats);
        }
        break;
      case FLOAT:
      case DOUBLE:
        isCompatible = statsData.isSetDoubleStats();
        if (isCompatible) {
          DoubleColumnStatsData doubleStats = statsData.getDoubleStats();
          numDistinctValues_ = normalizeValue(colName, StatsKey.NUM_DISTINCT_VALUES,
              doubleStats.getNumDVs());
          numNulls_ = normalizeValue(colName, StatsKey.NUM_NULLS,
              doubleStats.getNumNulls());
          setLowAndHighValue(doubleStats);
        }
        break;
      case CHAR:
        // Ignore CHAR length stats, since it is fixed length internally.
        isCompatible = statsData.isSetStringStats();
        if (isCompatible) {
          StringColumnStatsData stringStats = statsData.getStringStats();
          numDistinctValues_ = normalizeValue(colName, StatsKey.NUM_DISTINCT_VALUES,
              stringStats.getNumDVs());
          numNulls_ = normalizeValue(colName, StatsKey.NUM_NULLS,
              stringStats.getNumNulls());
        }
        break;
      case VARCHAR:
      case STRING:
        isCompatible = statsData.isSetStringStats();
        if (isCompatible) {
          StringColumnStatsData stringStats = statsData.getStringStats();
          numDistinctValues_ = normalizeValue(colName, StatsKey.NUM_DISTINCT_VALUES,
              stringStats.getNumDVs());
          numNulls_ = normalizeValue(colName, StatsKey.NUM_NULLS,
              stringStats.getNumNulls());
          maxSize_ = normalizeValue(colName, StatsKey.MAX_SIZE,
              stringStats.getMaxColLen());
          avgSize_ = normalizeAvgSize(colName,
              Double.valueOf(stringStats.getAvgColLen()).floatValue());
          if (avgSize_ >= 0) {
            avgSerializedSize_ = avgSize_ + PrimitiveType.STRING.getSlotSize();
          } else {
            avgSerializedSize_ = -1;
          }
        }
        break;
      case BINARY:
        isCompatible = statsData.isSetBinaryStats();
        if (isCompatible) {
          BinaryColumnStatsData binaryStats = statsData.getBinaryStats();
          numNulls_ = normalizeValue(colName, StatsKey.NUM_NULLS,
              binaryStats.getNumNulls());
          maxSize_ = normalizeValue(colName, StatsKey.MAX_SIZE,
              binaryStats.getMaxColLen());
          avgSize_ = normalizeAvgSize(colName,
              Double.valueOf(binaryStats.getAvgColLen()).floatValue());
          if (avgSize_ >= 0) {
            avgSerializedSize_ = avgSize_ + PrimitiveType.BINARY.getSlotSize();
          } else {
            avgSerializedSize_ = -1;
          }
        }
        break;
      case DECIMAL:
        isCompatible = statsData.isSetDecimalStats();
        if (isCompatible) {
          DecimalColumnStatsData decimalStats = statsData.getDecimalStats();
          numNulls_ = normalizeValue(colName, StatsKey.NUM_NULLS,
              decimalStats.getNumNulls());
          numDistinctValues_ = normalizeValue(colName, StatsKey.NUM_DISTINCT_VALUES,
              decimalStats.getNumDVs());
          setLowAndHighValue(decimalStats);
        }
        break;
      default:
        throw new IllegalStateException("Unexpected column type: " + colType);
    }
    validate(colType);
    return isCompatible;
  }

  /**
   * Set the low and high value for an Hive LongColumnStatsData object.
   */
  public static void updateLowAndHighForHiveColumnStatsData(
      Long low_value, Long high_value, LongColumnStatsData longColStatsData) {
    if (low_value != null) {
      longColStatsData.setLowValue(low_value);
    } else {
      longColStatsData.unsetLowValue();
    }
    if (high_value != null) {
      longColStatsData.setHighValue(high_value);
    } else {
      longColStatsData.unsetHighValue();
    }
  }

  /**
   * Set the low and high value for an Hive DoubleColumnStatsData object.
   */
  public static void updateLowAndHighForHiveColumnStatsData(
      Double low_value, Double high_value, DoubleColumnStatsData doubleColStatsData) {
    if (low_value != null) {
      doubleColStatsData.setLowValue(low_value);
    } else {
      doubleColStatsData.unsetLowValue();
    }
    if (high_value != null) {
      doubleColStatsData.setHighValue(high_value);
    } else {
      doubleColStatsData.unsetHighValue();
    }
  }

  /**
   * Set the low and high value for an Hive DateColumnStatsData object.
   */
  public static void updateLowAndHighForHiveColumnStatsData(
      Date low_value, Date high_value, DateColumnStatsData dateColStatsData) {
    if (low_value != null) {
      dateColStatsData.setLowValue(low_value);
    } else {
      dateColStatsData.unsetLowValue();
    }
    if (high_value != null) {
      dateColStatsData.setHighValue(high_value);
    } else {
      dateColStatsData.unsetHighValue();
    }
  }

  /**
   * Set the low and high value for an Hive DecimalColumnStatsData object.
   */
  public static void updateLowAndHighForHiveColumnStatsData(
      Decimal low_value, Decimal high_value, DecimalColumnStatsData decimalColStatsData) {
    if (low_value != null) {
      decimalColStatsData.setLowValue(low_value);
    } else {
      decimalColStatsData.unsetLowValue();
    }
    if (high_value != null) {
      decimalColStatsData.setHighValue(high_value);
    } else {
      decimalColStatsData.unsetHighValue();
    }
  }

  /**
   * Convert the statistics back into an HMS-compatible ColumnStatisticsData object.
   * This is essentially the inverse of {@link #update(String, Type, ColumnStatisticsData)
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
    long numTrues = colStats.getNum_trues();
    long numFalses = colStats.getNum_falses();
    boolean isLowValueSet = colStats.isSetLow_value();
    boolean isHighValueSet = colStats.isSetHigh_value();
    long maxStrLen = colStats.getMax_size();
    double avgStrLen = colStats.getAvg_size();
    switch(colType.getPrimitiveType()) {
      case BOOLEAN:
        colStatsData.setBooleanStats(
            new BooleanColumnStatsData(numTrues, numFalses, numNulls));
        break;
      case TINYINT:
        {
          ndv = Math.min(ndv, LongMath.pow(2, Byte.SIZE));
          LongColumnStatsData longColStatsData = new LongColumnStatsData(numNulls, ndv);
          Long lowValue = null;
          Long highValue = null;
          if (isLowValueSet && colStats.low_value.isSetByte_val()) {
            lowValue = (long) colStats.low_value.getByte_val();
          }
          if (isHighValueSet && colStats.high_value.isSetByte_val()) {
            highValue = (long) colStats.high_value.getByte_val();
          }
          updateLowAndHighForHiveColumnStatsData(lowValue, highValue, longColStatsData);
          colStatsData.setLongStats(longColStatsData);
        }
        break;
      case SMALLINT:
        {
          ndv = Math.min(ndv, LongMath.pow(2, Short.SIZE));
          LongColumnStatsData longColStatsData = new LongColumnStatsData(numNulls, ndv);

          Long lowValue = null;
          Long highValue = null;
          if (isLowValueSet && colStats.low_value.isSetShort_val()) {
            lowValue = (long) colStats.low_value.getShort_val();
          }
          if (isHighValueSet && colStats.high_value.isSetShort_val()) {
            highValue = (long) colStats.high_value.getShort_val();
          }
          updateLowAndHighForHiveColumnStatsData(lowValue, highValue, longColStatsData);

          colStatsData.setLongStats(longColStatsData);
        }
        break;
      case INT:
        {
          ndv = Math.min(ndv, LongMath.pow(2, Integer.SIZE));
          LongColumnStatsData longColStatsData = new LongColumnStatsData(numNulls, ndv);

          Long lowValue = null;
          Long highValue = null;
          if (isLowValueSet && colStats.low_value.isSetInt_val()) {
            lowValue = (long) colStats.low_value.getInt_val();
          }
          if (isHighValueSet && colStats.high_value.isSetInt_val()) {
            highValue = (long) colStats.high_value.getInt_val();
          }
          updateLowAndHighForHiveColumnStatsData(lowValue, highValue, longColStatsData);

          colStatsData.setLongStats(longColStatsData);
        }
        break;
      case DATE:
        {
          // Number of distinct dates in the 0001-01-01..9999-12-31 inclusive range is
          // 3652059.
          ndv = Math.min(ndv, 3652059);
          DateColumnStatsData dateColStatsData = new DateColumnStatsData(numNulls, ndv);
          Date lowValue = null;
          Date highValue = null;
          if (isLowValueSet && colStats.low_value.isSetDate_val()) {
            lowValue = new Date(colStats.low_value.getDate_val());
          }
          if (isHighValueSet && colStats.high_value.isSetDate_val()) {
            highValue = new Date(colStats.high_value.getDate_val());
          }
          updateLowAndHighForHiveColumnStatsData(lowValue, highValue, dateColStatsData);
          colStatsData.setDateStats(dateColStatsData);
        }
        break;
      case BIGINT:
        {
          LongColumnStatsData longColStatsData = new LongColumnStatsData(numNulls, ndv);

          Long lowValue = null;
          Long highValue = null;
          if (isLowValueSet && colStats.low_value.isSetLong_val()) {
            lowValue = colStats.low_value.getLong_val();
          }
          if (isHighValueSet && colStats.high_value.isSetLong_val()) {
            highValue = colStats.high_value.getLong_val();
          }
          updateLowAndHighForHiveColumnStatsData(lowValue, highValue, longColStatsData);

          colStatsData.setLongStats(longColStatsData);
        }
        break;
      case TIMESTAMP: // Hive and Impala use LongColumnStatsData for timestamps.
        colStatsData.setLongStats(new LongColumnStatsData(numNulls, ndv));
        break;
      case FLOAT:
      case DOUBLE:
        {
          DoubleColumnStatsData doubleColStatsData =
              new DoubleColumnStatsData(numNulls, ndv);

          Double lowValue = null;
          Double highValue = null;
          if (isLowValueSet && colStats.low_value.isSetDouble_val()) {
            lowValue = colStats.low_value.getDouble_val();
          }
          if (isHighValueSet && colStats.high_value.isSetDouble_val()) {
            highValue = colStats.high_value.getDouble_val();
          }
          updateLowAndHighForHiveColumnStatsData(lowValue, highValue, doubleColStatsData);

          colStatsData.setDoubleStats(doubleColStatsData);
        }
        break;
      case CHAR:
      case VARCHAR:
      case STRING:
        colStatsData.setStringStats(
            new StringColumnStatsData(maxStrLen, avgStrLen, numNulls, ndv));
        break;
      case BINARY:
        // No NDV is stored for BINARY.
        colStatsData.setBinaryStats(
            new BinaryColumnStatsData(maxStrLen, avgStrLen, numNulls));
        break;
      case DECIMAL:
        {
          double decMaxNdv = Math.pow(10, colType.getPrecision());
          ndv = (long) Math.min(ndv, decMaxNdv);
          DecimalColumnStatsData decimalStatsData =
              new DecimalColumnStatsData(numNulls, ndv);
          Decimal lowValue = null;
          Decimal highValue = null;
          ScalarType colTypeScalar = (ScalarType) colType;
          if (isLowValueSet && colStats.low_value.isSetDecimal_val()) {
            lowValue = new Decimal((short) colTypeScalar.decimalScale(),
                colStats.low_value.bufferForDecimal_val());
          }
          if (isHighValueSet && colStats.high_value.isSetDecimal_val()) {
            highValue = new Decimal((short) colTypeScalar.decimalScale(),
                colStats.high_value.bufferForDecimal_val());
          }
          updateLowAndHighForHiveColumnStatsData(lowValue, highValue, decimalStatsData);
          colStatsData.setDecimalStats(decimalStatsData);
        }
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
  public void update(Type colType, StatsKey key, Number value) {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(value);
    if (key == StatsKey.AVG_SIZE) {
      Preconditions.checkArgument(value instanceof Float);
      Float floatValue = (Float) value;
      Preconditions.checkArgument(floatValue >= 0 || floatValue == -1, floatValue);
    } else {
      Preconditions.checkArgument(value instanceof Long);
      Long longValue = (Long) value;
      Preconditions.checkArgument(longValue >= 0 || longValue == -1, longValue);
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
        Preconditions.checkArgument(!colType.isFixedLengthType(), colType);
        avgSize_ = (Float) value;
        // Ensure avgSerializedSize_ stays in sync with avgSize_.
        if (avgSize_ >= 0) {
          avgSerializedSize_ = colType.getSlotSize() + avgSize_;
        } else {
          avgSerializedSize_ = -1;
        }
        break;
      }
      case MAX_SIZE: {
        Preconditions.checkArgument(!colType.isFixedLengthType(), colType);
        maxSize_ = (Long) value;
        break;
      }
      case NUM_TRUES: {
        numTrues_ = (Long) value;
        break;
      }
      case NUM_FALSES: {
        numFalses_ = (Long) value;
        break;
      }
      default: throw new IllegalStateException("Unknown StatsKey " + key);
    }
    validate(colType);
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
    if (!colType.isFixedLengthType() && stats.getAvg_size() >= 0) {
      // Update size estimates based on average size. Fixed length types already include
      // size estimates.
      avgSize_ = Double.valueOf(stats.getAvg_size()).floatValue();
      avgSerializedSize_ = colType.getSlotSize() + avgSize_;
    }
    if (colType.getPrimitiveType() == PrimitiveType.BOOLEAN) {
      numTrues_ = stats.getNum_trues();
      numFalses_ = stats.getNum_falses();
    }
    maxSize_ = stats.getMax_size();
    numDistinctValues_ = stats.getNum_distinct_values();
    numNulls_ = stats.getNum_nulls();
    lowValue_ = stats.getLow_value();
    highValue_ = stats.getHigh_value();
    validate(colType);
  }

  public TColumnStats toThrift() {
    TColumnStats colStats = new TColumnStats();
    colStats.setAvg_size(avgSize_);
    colStats.setMax_size(maxSize_);
    colStats.setNum_distinct_values(numDistinctValues_);
    colStats.setNum_nulls(numNulls_);
    colStats.setNum_trues(numTrues_);
    colStats.setNum_falses(numFalses_);
    colStats.setLow_value(lowValue_);
    colStats.setHigh_value(highValue_);
    return colStats;
  }

  /**
   * Check that the stats obey expected invariants.
   * 'colType' is optional, but should be passed in if it is available in the caller.
   */
  public void validate(Type colType) {
    // avgSize_ and avgSerializedSize_ must be set together.
    Preconditions.checkState(avgSize_ >= 0 == avgSerializedSize_ >= 0, this);

    // Values must be either valid or -1.
    Preconditions.checkState(avgSize_ == -1 || avgSize_ >= 0, this);
    Preconditions.checkState(avgSerializedSize_ == -1 || avgSerializedSize_ >= 0, this);
    Preconditions.checkState(maxSize_ == -1 || maxSize_ >= 0, this);
    Preconditions.checkState(numDistinctValues_ == -1 || numDistinctValues_ >= 0, this);
    Preconditions.checkState(numNulls_ == -1 || numNulls_ >= 0, this);
    Preconditions.checkState(numTrues_ == -1 || numTrues_ >= 0, this);
    Preconditions.checkState(numFalses_ == -1 || numFalses_ >= 0, this);
    if (colType != null && colType.isFixedLengthType()) {
      Preconditions.checkState(avgSize_ == colType.getSlotSize(), this);
      Preconditions.checkState(avgSerializedSize_ == colType.getSlotSize(), this);
      Preconditions.checkState(maxSize_ == colType.getSlotSize(), this);
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this.getClass())
        .add("avgSize_", avgSize_)
        .add("avgSerializedSize_", avgSerializedSize_)
        .add("maxSize_", maxSize_)
        .add("numDistinct_", numDistinctValues_)
        .add("numNulls_", numNulls_)
        .add("numTrues", numTrues_)
        .add("numFalses", numFalses_)
        .add("lowValue", getLowValueAsString())
        .add("highValue", getHighValueAsString())
        .toString();
  }

  @Override
  public ColumnStats clone() { return new ColumnStats(this); }
}
