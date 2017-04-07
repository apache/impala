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
import org.apache.impala.catalog.DataSourceTable;
import org.apache.impala.catalog.Table;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TTableName;

import com.google.common.base.Preconditions;

/**
 * Base class for all ALTER TABLE statements.
 */
public abstract class AlterTableStmt extends StatementBase {
  protected final TableName tableName_;

  // Set during analysis.
  protected Table table_;

  protected AlterTableStmt(TableName tableName) {
    Preconditions.checkState(tableName != null && !tableName.isEmpty());
    tableName_ = tableName;
    table_ = null;
  }

  public String getTbl() { return tableName_.getTbl(); }

  /**
   * Can only be called after analysis, returns the parent database name of the target
   * table for this ALTER TABLE statement.
   */
  public String getDb() {
    return getTargetTable().getDb().getName();
  }

  /**
   * Can only be called after analysis, returns the Table object of the target of this
   * ALTER TABLE statement.
   */
  protected Table getTargetTable() {
    Preconditions.checkNotNull(table_);
    return table_;
  }

  public TAlterTableParams toThrift() {
    TAlterTableParams params = new TAlterTableParams();
    params.setTable_name(new TTableName(getDb(), getTbl()));
    return params;
  }

  @Override
  public void collectTableRefs(List<TableRef> tblRefs) {
    tblRefs.add(new TableRef(tableName_.toPath(), null));
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    // Resolve and analyze this table ref so we can evaluate partition predicates.
    TableRef tableRef = new TableRef(tableName_.toPath(), null, Privilege.ALTER);
    tableRef = analyzer.resolveTableRef(tableRef);
    Preconditions.checkNotNull(tableRef);
    tableRef.analyze(analyzer);
    if (tableRef instanceof InlineViewRef) {
      throw new AnalysisException(String.format(
          "ALTER TABLE not allowed on a view: %s", tableName_));
    }
    if (tableRef instanceof CollectionTableRef) {
      throw new AnalysisException(String.format(
          "ALTER TABLE not allowed on a nested collection: %s", tableName_));
    }
    Preconditions.checkState(tableRef instanceof BaseTableRef);
    table_ = tableRef.getTable();
    if (table_ instanceof DataSourceTable
        && !(this instanceof AlterTableSetColumnStats)) {
      throw new AnalysisException(String.format(
          "ALTER TABLE not allowed on a table produced by a data source: %s",
          tableName_));
    }
  }
}
