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

import java.util.List;

import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.paimon.FePaimonTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TDropStatsParams;
import org.apache.impala.thrift.TTableName;

import com.google.common.base.Preconditions;

/**
 * Represents both a DROP STATS statement, and the DROP INCREMENTAL STATS <tbl> PARTITION
 * <part_spec> variant.
 */
public class DropStatsStmt extends StatementBase implements SingleTableStmt {
  protected final TableName tableName_;

  // Set during analysis
  protected TableRef tableRef_;

  // If non-null, only drop the statistics for a given partition
  private final PartitionSet partitionSet_;

  /**
   * Constructor for building the DROP TABLE/VIEW statement
   */
  public DropStatsStmt(TableName tableName) {
    this.tableName_ = Preconditions.checkNotNull(tableName);
    this.partitionSet_ = null;
  }

  public DropStatsStmt(TableName tableName, PartitionSet partitionSet) {
    this.tableName_ = Preconditions.checkNotNull(tableName);;
    this.partitionSet_ = partitionSet;
  }

  @Override
  public TableName getTableName() { return tableName_; }

  @Override
  public String toSql(ToSqlOptions options) {
    StringBuilder sb = new StringBuilder("DROP ");
    if (partitionSet_ == null) {
      sb.append(" STATS ");
      if (tableName_.getDb() != null) sb.append(tableName_.getDb() + ".");
      sb.append(tableName_.toSql());
    } else {
      sb.append(" INCREMENTAL STATS ");
      if (tableName_.getDb() != null) sb.append(tableName_.getDb() + ".");
      sb.append(tableName_.toSql());
      sb.append(partitionSet_.toSql(options));
    }
    return sb.toString();
  }

  public TDropStatsParams toThrift() {
    TDropStatsParams params = new TDropStatsParams();
    params.setTable_name(new TTableName(getDb(), getTbl()));
    if (partitionSet_ != null) {
      params.setPartition_set(partitionSet_.toThrift());
    }
    return params;
  }

  @Override
  public void collectTableRefs(List<TableRef> tblRefs) {
    tblRefs.add(new TableRef(tableName_.toPath(), null));
  }

  /**
   * Checks that the given table exists and the user has privileges
   * to drop stats on this table.
   */
  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    // Resolve and analyze table ref to register privilege and audit events
    // and to allow us to evaluate partition predicates.
    tableRef_ = new TableRef(tableName_.toPath(), null, Privilege.ALTER);
    tableRef_ = analyzer.resolveTableRef(tableRef_);
    Preconditions.checkNotNull(tableRef_);
    if (tableRef_ instanceof InlineViewRef) {
      throw new AnalysisException(
          String.format("DROP STATS not allowed on a view: %s", tableName_));
    }
    if (tableRef_ instanceof CollectionTableRef) {
      throw new AnalysisException(
          String.format("DROP STATS not allowed on a nested collection: %s", tableName_));
    }
    if (tableRef_.getTable() instanceof FePaimonTable) {
      throw new AnalysisException(
          String.format("DROP STATS not allowed on a PAIMON table: %s", tableName_));
    }
    tableRef_.analyze(analyzer);

    FeTable table_ = analyzer.getTable(tableName_, Privilege.ALTER);
    if (!(table_ instanceof FeFsTable)) {
      if (partitionSet_ != null) {
        throw new AnalysisException("DROP INCREMENTAL STATS ... PARTITION " +
            "not supported for non-filesystem-based table " + tableName_);
      }
    }
    if (table_ instanceof FeIcebergTable) {
      if (partitionSet_ != null) {
        throw new AnalysisException("DROP INCREMENTAL STATS ... PARTITION " +
            "not supported for Iceberg table " + tableName_);
      }
    }

    // There is no transactional HMS API to drop stats at the moment (HIVE-22104).
    analyzer.ensureTableNotTransactional(tableRef_.getTable(), "DROP STATS");
    if (partitionSet_ != null) {
      partitionSet_.setTableName(tableRef_.getTable().getTableName());
      partitionSet_.setPrivilegeRequirement(Privilege.ALTER);
      partitionSet_.setPartitionShouldExist();
      partitionSet_.analyze(analyzer);
    }
  }

  /**
   * Can only be called after analysis. Returns the name of the database that
   * the target drop table resides in.
   */
  public String getDb() {
    Preconditions.checkNotNull(tableRef_);
    return tableRef_.getTable().getDb().getName();
  }

  public String getTbl() { return tableName_.getTbl(); }
}
