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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TDistributeByHashParam;
import org.apache.impala.thrift.TDistributeByRangeParam;
import org.apache.impala.thrift.TDistributeParam;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Represents the distribution of a Kudu table as defined in the DISTRIBUTE BY
 * clause of a CREATE TABLE statement. The distribution can be hash-based or
 * range-based or both. See RangePartition for details on the supported range partitions.
 *
 * Examples:
 * - Hash-based:
 *   DISTRIBUTE BY HASH(id) INTO 10 BUCKETS
 * - Single column range-based:
 *   DISTRIBUTE BY RANGE(age)
 *   (
 *     PARTITION VALUES < 10,
 *     PARTITION 10 <= VALUES < 20,
 *     PARTITION 20 <= VALUES < 30,
 *     PARTITION VALUE = 100
 *   )
 * - Combination of hash and range based:
 *   DISTRIBUTE BY HASH (id) INTO 3 BUCKETS,
 *   RANGE (age)
 *   (
 *     PARTITION 10 <= VALUES < 20,
 *     PARTITION VALUE = 100
 *   )
 * - Multi-column range based:
 *   DISTRIBUTE BY RANGE (year, quarter)
 *   (
 *     PARTITION VALUE = (2001, 1),
 *     PARTITION VALUE = (2001, 2),
 *     PARTITION VALUE = (2002, 1)
 *   )
 *
 */
public class DistributeParam implements ParseNode {

  /**
   * Creates a hash-based DistributeParam.
   */
  public static DistributeParam createHashParam(List<String> cols, int buckets) {
    return new DistributeParam(Type.HASH, cols, buckets, null);
  }

  /**
   * Creates a range-based DistributeParam.
   */
  public static DistributeParam createRangeParam(List<String> cols,
      List<RangePartition> rangePartitions) {
    return new DistributeParam(Type.RANGE, cols, NO_BUCKETS, rangePartitions);
  }

  private static final int NO_BUCKETS = -1;

  /**
   * The distribution type.
   */
  public enum Type {
    HASH, RANGE
  }

  // Columns of this distribution. If no columns are specified, all
  // the primary key columns of the associated table are used.
  private final List<String> colNames_ = Lists.newArrayList();

  // Map of primary key column names to the associated column definitions. Must be set
  // before the call to analyze().
  private Map<String, ColumnDef> pkColumnDefByName_;

  // Distribution scheme type
  private final Type type_;

  // Only relevant for hash-based distribution, -1 otherwise
  private final int numBuckets_;

  // List of range partitions specified in a range-based distribution.
  private List<RangePartition> rangePartitions_;

  private DistributeParam(Type t, List<String> colNames, int buckets,
      List<RangePartition> partitions) {
    type_ = t;
    for (String name: colNames) colNames_.add(name.toLowerCase());
    rangePartitions_ = partitions;
    numBuckets_ = buckets;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    Preconditions.checkState(!colNames_.isEmpty());
    Preconditions.checkNotNull(pkColumnDefByName_);
    Preconditions.checkState(!pkColumnDefByName_.isEmpty());
    // Validate that the columns specified in this distribution are primary key columns.
    for (String colName: colNames_) {
      if (!pkColumnDefByName_.containsKey(colName)) {
        throw new AnalysisException(String.format("Column '%s' in '%s' is not a key " +
            "column. Only key columns can be used in DISTRIBUTE BY.", colName, toSql()));
      }
    }
    if (type_ == Type.RANGE) analyzeRangeParam(analyzer);
  }

  /**
   * Analyzes a range-based distribution. This function does not check for overlapping
   * range partitions; these checks are performed by Kudu and an error is reported back
   * to the user.
   */
  public void analyzeRangeParam(Analyzer analyzer) throws AnalysisException {
    List<ColumnDef> pkColDefs = Lists.newArrayListWithCapacity(colNames_.size());
    for (String colName: colNames_) pkColDefs.add(pkColumnDefByName_.get(colName));
    for (RangePartition rangePartition: rangePartitions_) {
      rangePartition.analyze(analyzer, pkColDefs);
    }
  }

  @Override
  public String toSql() {
    StringBuilder builder = new StringBuilder(type_.toString());
    if (!colNames_.isEmpty()) {
      builder.append(" (");
      Joiner.on(", ").appendTo(builder, colNames_).append(")");
    }
    if (type_ == Type.HASH) {
      builder.append(" INTO ");
      Preconditions.checkState(numBuckets_ != NO_BUCKETS);
      builder.append(numBuckets_).append(" BUCKETS");
    } else {
      builder.append(" (");
      if (rangePartitions_ != null) {
        List<String> partsSql = Lists.newArrayList();
        for (RangePartition rangePartition: rangePartitions_) {
          partsSql.add(rangePartition.toSql());
        }
        builder.append(Joiner.on(", ").join(partsSql));
      } else {
        builder.append("...");
      }
      builder.append(")");
    }
    return builder.toString();
  }

  @Override
  public String toString() { return toSql(); }

  public TDistributeParam toThrift() {
    TDistributeParam result = new TDistributeParam();
    // TODO: Add a validate() function to ensure the validity of distribute params.
    if (type_ == Type.HASH) {
      TDistributeByHashParam hash = new TDistributeByHashParam();
      Preconditions.checkState(numBuckets_ != NO_BUCKETS);
      hash.setNum_buckets(numBuckets_);
      hash.setColumns(colNames_);
      result.setBy_hash_param(hash);
    } else {
      Preconditions.checkState(type_ == Type.RANGE);
      TDistributeByRangeParam rangeParam = new TDistributeByRangeParam();
      rangeParam.setColumns(colNames_);
      if (rangePartitions_ == null) {
        result.setBy_range_param(rangeParam);
        return result;
      }
      for (RangePartition rangePartition: rangePartitions_) {
        rangeParam.addToRange_partitions(rangePartition.toThrift());
      }
      result.setBy_range_param(rangeParam);
    }
    return result;
  }

  void setPkColumnDefMap(Map<String, ColumnDef> pkColumnDefByName) {
    pkColumnDefByName_ = pkColumnDefByName;
  }

  boolean hasColumnNames() { return !colNames_.isEmpty(); }
  public List<String> getColumnNames() { return ImmutableList.copyOf(colNames_); }
  void setColumnNames(Collection<String> colNames) {
    Preconditions.checkState(colNames_.isEmpty());
    colNames_.addAll(colNames);
  }

  public Type getType() { return type_; }
}
