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

package com.cloudera.impala.analysis;

import java.util.List;

import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TAlterTableParams;
import com.cloudera.impala.thrift.TAlterTableSetLocationParams;
import com.cloudera.impala.thrift.TAlterTableType;
import com.cloudera.impala.thrift.TPartitionKeyValue;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Represents an ALTER TABLE [PARTITION partitionSpec] SET LOCATION statement.
 */
public class AlterTableSetLocationStmt extends AlterTableStmt {
  private final String location;
  private final List<PartitionKeyValue> partitionSpec;

  // The value Hive is configured to use for NULL partition key values.
  // Set during analysis.
  private String nullPartitionKeyValue;

  public AlterTableSetLocationStmt(TableName tableName,
      List<PartitionKeyValue> partitionSpec, String location) {
    super(tableName);
    Preconditions.checkNotNull(location);
    Preconditions.checkNotNull(partitionSpec);
    this.location = location;
    this.partitionSpec = ImmutableList.copyOf(partitionSpec);
  }

  public String getLocation() {
    return location;
  }

  public List<PartitionKeyValue> getPartitionSpec() {
    return partitionSpec;
  }

  private String getNullPartitionKeyValue() {
    Preconditions.checkNotNull(nullPartitionKeyValue);
    return nullPartitionKeyValue;
  }

  @Override
  public TAlterTableParams toThrift() {
    TAlterTableParams params = super.toThrift();
    params.setAlter_type(TAlterTableType.SET_LOCATION);
    TAlterTableSetLocationParams locationParams =
        new TAlterTableSetLocationParams(location);
    for (PartitionKeyValue kv: partitionSpec) {
      String value = kv.getPartitionKeyValueString(getNullPartitionKeyValue());
      locationParams.addToPartition_spec(new TPartitionKeyValue(kv.getColName(), value));
    }
    params.setSet_location_params(locationParams);
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    // Alterting the table, rather than the partition.
    if (partitionSpec.size() == 0) {
      return;
    }

    Table table = getTargetTable();
    String tableName = getDb() + "." + getTbl();

    // Make sure the target table is partitioned.
    if (table.getMetaStoreTable().getPartitionKeysSize() == 0) {
      throw new AnalysisException("Table is not partitioned: " + tableName);
    }

    // Make sure static partition key values only contain constant exprs.
    for (PartitionKeyValue kv: partitionSpec) {
      kv.analyze(analyzer);
    }

    // If the table is partitioned it should be an HdfsTable
    Preconditions.checkState(table instanceof HdfsTable);
    HdfsTable hdfsTable = (HdfsTable) table;
    if (hdfsTable.getPartition(partitionSpec) == null) {
      throw new AnalysisException("No matching partition spec found: (" +
          Joiner.on(", ").join(partitionSpec) + ")");
    }
    nullPartitionKeyValue = hdfsTable.getNullPartitionKeyValue();
  }
}
