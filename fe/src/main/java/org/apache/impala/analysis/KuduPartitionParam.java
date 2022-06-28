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

import static org.apache.impala.analysis.ToSqlOptions.DEFAULT;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TKuduPartitionByHashParam;
import org.apache.impala.thrift.TKuduPartitionByRangeParam;
import org.apache.impala.thrift.TKuduPartitionParam;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Represents the partitioning of a Kudu table as defined in the PARTITION BY
 * clause of a CREATE TABLE statement. The partitioning can be hash-based or
 * range-based or both. See RangePartition for details on the supported range partitions.
 *
 * Examples:
 * - Hash-based:
 *   PARTITION BY HASH(id) PARTITIONS 10
 * - Single column range-based:
 *   PARTITION BY RANGE(age)
 *   (
 *     PARTITION VALUES < 10,
 *     PARTITION 10 <= VALUES < 20,
 *     PARTITION 20 <= VALUES < 30,
 *     PARTITION VALUE = 100
 *   )
 * - Combination of hash and range based:
 *   PARTITION BY HASH (id) PARTITIONS 3,
 *   RANGE (age)
 *   (
 *     PARTITION 10 <= VALUES < 20,
 *     PARTITION VALUE = 100
 *   )
 * - Multi-column range based:
 *   PARTITION BY RANGE (year, quarter)
 *   (
 *     PARTITION VALUE = (2001, 1),
 *     PARTITION VALUE = (2001, 2),
 *     PARTITION VALUE = (2002, 1)
 *   )
 *
 */
public class KuduPartitionParam extends StmtNode {

  /**
   * Creates a hash-based KuduPartitionParam.
   */
  public static KuduPartitionParam createHashParam(List<String> cols, int numPartitions) {
    return new KuduPartitionParam(Type.HASH, cols, numPartitions, null);
  }

  /**
   * Creates a range-based KuduPartitionParam.
   */
  public static KuduPartitionParam createRangeParam(List<String> cols,
      List<RangePartition> rangePartitions) {
    return new KuduPartitionParam(Type.RANGE, cols, NO_HASH_PARTITIONS, rangePartitions);
  }

  private static final int NO_HASH_PARTITIONS = -1;

  /**
   * The partitioning type.
   */
  public enum Type {
    HASH, RANGE
  }

  // Columns of this partitioning. If no columns are specified, all
  // the primary key columns of the associated table are used.
  private final List<String> colNames_ = new ArrayList<>();

  // Map of primary key column names to the associated column definitions. Must be set
  // before the call to analyze().
  private Map<String, ColumnDef> pkColumnDefByName_;

  // partitioning scheme type
  private final Type type_;

  // Only relevant for hash-based partitioning, -1 otherwise
  private final int numHashPartitions_;

  // List of range partitions specified in a range-based partitioning.
  private List<RangePartition> rangePartitions_;

  private KuduPartitionParam(Type t, List<String> colNames, int numHashPartitions,
      List<RangePartition> partitions) {
    type_ = t;
    for (String name: colNames) colNames_.add(name.toLowerCase());
    rangePartitions_ = partitions;
    numHashPartitions_ = numHashPartitions;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    Preconditions.checkNotNull(pkColumnDefByName_);
    Preconditions.checkState(!pkColumnDefByName_.isEmpty());
    // If no column names were specified in this partitioning scheme, use all the
    // primary key columns.
    if (!hasColumnNames()) {
      setColumnNames(pkColumnDefByName_.keySet());
    }
    // Validate that the columns specified in this partitioning are primary key columns.
    for (String colName: colNames_) {
      if (!pkColumnDefByName_.containsKey(colName)) {
        throw new AnalysisException(String.format("Column '%s' in '%s' is not a key " +
            "column. Only key columns can be used in PARTITION BY.", colName, toSql()));
      }
    }
    if (type_ == Type.RANGE) analyzeRangeParam(analyzer);
  }

  /**
   * Analyzes a range-based partitioning. This function does not check for overlapping
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
  public final String toSql() {
    return toSql(DEFAULT);
  }

  @Override
  public String toSql(ToSqlOptions options) {
    StringBuilder builder = new StringBuilder(type_.toString());
    if (!colNames_.isEmpty()) {
      builder.append(" (");
      Joiner.on(", ").appendTo(builder, colNames_).append(")");
    }
    if (type_ == Type.HASH) {
      Preconditions.checkState(numHashPartitions_ != NO_HASH_PARTITIONS);
      builder.append(" PARTITIONS ").append(numHashPartitions_);
    } else {
      builder.append(" (");
      if (rangePartitions_ != null) {
        List<String> partsSql = new ArrayList<>();
        for (RangePartition rangePartition: rangePartitions_) {
          partsSql.add(rangePartition.toSql(options));
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
  public String toString() {
    return toSql(DEFAULT);
  }

  public TKuduPartitionParam toThrift() {
    TKuduPartitionParam result = new TKuduPartitionParam();
    // TODO: Add a validate() function to ensure the validity of distribute params.
    if (type_ == Type.HASH) {
      TKuduPartitionByHashParam hash = new TKuduPartitionByHashParam();
      Preconditions.checkState(numHashPartitions_ != NO_HASH_PARTITIONS);
      hash.setNum_partitions(numHashPartitions_);
      hash.setColumns(colNames_);
      result.setBy_hash_param(hash);
    } else {
      Preconditions.checkState(type_ == Type.RANGE);
      TKuduPartitionByRangeParam rangeParam = new TKuduPartitionByRangeParam();
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
    if (rangePartitions_ != null) {
      for (RangePartition rangePartition: rangePartitions_) {
        rangePartition.setPkColumnDefMap(pkColumnDefByName);
      }
    }
  }

  boolean hasColumnNames() { return !colNames_.isEmpty(); }
  public List<String> getColumnNames() { return ImmutableList.copyOf(colNames_); }
  void setColumnNames(Collection<String> colNames) {
    Preconditions.checkState(colNames_.isEmpty());
    colNames_.addAll(colNames);
  }

  public Type getType() { return type_; }
}
