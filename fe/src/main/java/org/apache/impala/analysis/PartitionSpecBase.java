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
// under the License.package com.cloudera.impala.analysis;

package org.apache.impala.analysis;

import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.common.AnalysisException;
import com.google.common.base.Preconditions;

import static org.apache.impala.analysis.ToSqlOptions.DEFAULT;

/**
 * Base class for PartitionSpec and PartitionSet containing the partition
 * specifications of related DDL operations.
 */
public abstract class PartitionSpecBase extends StmtNode {
  protected FeFsTable table_;
  protected TableName tableName_;
  protected Boolean partitionShouldExist_;
  protected Privilege privilegeRequirement_;
  // The value Hive is configured to use for NULL partition key values.
  // Set during analysis.
  protected String nullPartitionKeyValue_;

  public void setTableName(TableName tableName) {this.tableName_ = tableName; }
  public TableName getTableName() { return tableName_; }

  // The value Hive is configured to use for NULL partition key values.
  // Set during analysis.
  public String getNullPartitionKeyValue() {
    Preconditions.checkNotNull(nullPartitionKeyValue_);
    return nullPartitionKeyValue_;
  }

  // If set, an additional analysis check will be performed to validate the target table
  // contains the given partition spec.
  public void setPartitionShouldExist() { partitionShouldExist_ = Boolean.TRUE; }

  // If set, an additional analysis check will be performed to validate the target table
  // does not contain the given partition spec.
  public void setPartitionShouldNotExist() { partitionShouldExist_ = Boolean.FALSE; }

  public boolean getPartitionShouldExist() {
    return partitionShouldExist_ != null && partitionShouldExist_;
  }

  // Set the privilege requirement for this partition spec. Must be set prior to
  // analysis.
  public void setPrivilegeRequirement(Privilege privilege) {
    privilegeRequirement_ = privilege;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    Preconditions.checkNotNull(tableName_);
    Preconditions.checkNotNull(privilegeRequirement_);

    // Skip adding an audit event when analyzing partitions. The parent table should
    // be audited outside of the PartitionSpec.
    FeTable table;
    try {
      table = analyzer.getTable(tableName_, /* add access event */ false,
          /* add column-level privilege */ false, privilegeRequirement_);
    } catch (TableLoadingException e) {
      throw new AnalysisException(e.getMessage(), e);
    }

    // Make sure the target table is partitioned.
    if (!isPartitioned(table)) {
      throw new AnalysisException("Table is not partitioned: " + tableName_);
    }

    // Only HDFS tables are partitioned.
    Preconditions.checkState(table instanceof FeFsTable);
    table_ = (FeFsTable) table;
    nullPartitionKeyValue_ = table_.getNullPartitionKeyValue();
  }

  private boolean isPartitioned(FeTable table) {
    if (table instanceof FeIcebergTable) {
      return ((FeIcebergTable) table).isPartitioned();
    } else {
      return table.getMetaStoreTable().getPartitionKeysSize() != 0;
    }
  }

  @Override
  public final String toSql() {
    return toSql(DEFAULT);
  }

  @Override
  public abstract String toSql(ToSqlOptions options);
}
