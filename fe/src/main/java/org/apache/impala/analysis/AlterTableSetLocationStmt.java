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

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.permission.FsAction;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.HdfsPartition;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TAlterTableSetLocationParams;
import org.apache.impala.thrift.TAlterTableType;
import org.apache.impala.thrift.TPartitionKeyValue;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Represents an ALTER TABLE [PARTITION partitionSpec] SET LOCATION statement.
 */
public class AlterTableSetLocationStmt extends AlterTableSetStmt {
  // max num of partitions printed during error reporting.
  private static final int NUM_PARTITION_LOG_LIMIT = 3;
  private final HdfsUri location_;

  public AlterTableSetLocationStmt(TableName tableName,
      PartitionSet partitionSet, HdfsUri location) {
    super(tableName, partitionSet);
    Preconditions.checkNotNull(location);
    this.location_ = location;
  }

  public HdfsUri getLocation() { return location_; }

  @Override
  public String getOperation() { return "SET LOCATION"; }

  @Override
  public TAlterTableParams toThrift() {
    TAlterTableParams params = super.toThrift();
    params.setAlter_type(TAlterTableType.SET_LOCATION);
    TAlterTableSetLocationParams locationParams =
        new TAlterTableSetLocationParams(location_.toString());
    PartitionSet partitionSet = getPartitionSet();
    if (partitionSet != null && !partitionSet.getPartitions().isEmpty()) {
      List<List<TPartitionKeyValue>> tPartitionSet = partitionSet.toThrift();
      Preconditions.checkState(tPartitionSet.size() == 1);
      locationParams.setPartition_spec(tPartitionSet.get(0));
    }
    params.setSet_location_params(locationParams);
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    location_.analyze(analyzer, Privilege.ALL, FsAction.READ_WRITE);

    FeTable table = getTargetTable();
    Preconditions.checkNotNull(table);
    if (table instanceof FeIcebergTable) {
      throw new AnalysisException("ALTER TABLE SET LOCATION is not supported on Iceberg "
          + "tables: " + table.getFullName());
    }

    if (table instanceof FeFsTable) {
      FeFsTable hdfsTable = (FeFsTable) table;
      if (getPartitionSet() != null) {
        // Targeting a partition rather than a table.
        List<? extends FeFsPartition> partitions = getPartitionSet().getPartitions();
        if (partitions.isEmpty()) { return; }
        if (partitions.size() != 1) {
          // Sort the partitions to get a consistent error reporting.
          List<FeFsPartition> sortedPartitions = Lists.newArrayList(partitions);
          Collections.sort(sortedPartitions, HdfsPartition.KV_COMPARATOR);
          List<String> sortedPartitionNames =
              Lists.transform(sortedPartitions.subList(0, NUM_PARTITION_LOG_LIMIT),
                  new Function<FeFsPartition, String>() {
                    @Override
                    public String apply(FeFsPartition hdfsPartition) {
                      return hdfsPartition.getPartitionName();
                    }
                  });
          throw new AnalysisException(String.format(
              "Partition expr in set location statements can only match " +
              "one partition. Too many matched partitions %s %s",
              Joiner.on(",").join(sortedPartitionNames),
              sortedPartitions.size() < partitions.size() ? "..." : "."));
        }
        if (partitions.get(0).isMarkedCached()) {
          throw new AnalysisException(String.format("Target partition is cached, " +
              "please uncache before changing the location using: ALTER TABLE %s %s " +
              "SET UNCACHED", table.getFullName(), getPartitionSet().toSql()));
        }
      } else if (hdfsTable.isMarkedCached()) {
        throw new AnalysisException(String.format("Target table is cached, please " +
            "uncache before changing the location using: ALTER TABLE %s SET UNCACHED",
            table.getFullName()));
      }
    } else if (table instanceof FeKuduTable) {
      throw new AnalysisException("ALTER TABLE SET LOCATION is not supported on Kudu " +
          "tables: " + table.getFullName());
    }
  }
}
