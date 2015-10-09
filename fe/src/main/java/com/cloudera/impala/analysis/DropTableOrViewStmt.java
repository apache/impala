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

import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.catalog.View;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TDropTableOrViewParams;
import com.cloudera.impala.thrift.TTableName;
import com.google.common.base.Preconditions;

/**
 * Represents a DROP TABLE/VIEW [IF EXISTS] statement
 */
public class DropTableOrViewStmt extends StatementBase {
  protected final TableName tableName_;
  protected final boolean ifExists_;

  // True if we are dropping a table. False if we are dropping a view.
  protected final boolean dropTable_;

  // Setting this value causes dropped tables to be permanently
  // deleted. For example, for hdfs tables it skips the trash directory
  protected final boolean purgeTable_;

  // Set during analysis
  protected String dbName_;

  /**
   * Constructor for building the DROP TABLE/VIEW statement
   */
  public DropTableOrViewStmt(TableName tableName, boolean ifExists,
      boolean dropTable, boolean purgeTable) {
    tableName_ = tableName;
    ifExists_ = ifExists;
    dropTable_ = dropTable;
    purgeTable_ = purgeTable;
    // PURGE with a view is not allowed.
    Preconditions.checkState(!(!dropTable_ && purgeTable_));
  }

  @Override
  public String toSql() {
    StringBuilder sb = new StringBuilder("DROP " + ((dropTable_) ? "TABLE " : "VIEW "));
    if (ifExists_) sb.append("IF EXISTS ");
    if (tableName_.getDb() != null) sb.append(tableName_.getDb() + ".");
    sb.append(tableName_.getTbl());
    if (purgeTable_) sb.append(" PURGE");
    return sb.toString();
  }

  public TDropTableOrViewParams toThrift() {
    TDropTableOrViewParams params = new TDropTableOrViewParams();
    params.setTable_name(new TTableName(getDb(), getTbl()));
    params.setIf_exists(ifExists_);
    params.setPurge(purgeTable_);
    return params;
  }

  /**
   * 1. Checks that the user has privileges to DROP the given table/view
   * 2. Checks that the database and table exists
   * 3. Checks that the table type (TABLE/VIEW) matches the DROP TABLE/VIEW statement
   * Note: Do not analyze tableName because we prefer to report an error indicating
   * that the table/view does not exist even if the table/view name is invalid.
   */
  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    dbName_ = analyzer.getTargetDbName(tableName_);
    try {
      Table table = analyzer.getTable(tableName_, Privilege.DROP);
      Preconditions.checkNotNull(table);
      if (table instanceof View && dropTable_) {
        throw new AnalysisException(String.format(
            "DROP TABLE not allowed on a view: %s.%s", dbName_, getTbl()));
      }
      if (!(table instanceof View) && !dropTable_) {
        throw new AnalysisException(String.format(
            "DROP VIEW not allowed on a table: %s.%s", dbName_, getTbl()));
      }
    } catch (AnalysisException e) {
      if (ifExists_ && analyzer.getMissingTbls().isEmpty()) return;
      throw e;
    }
  }

  /**
   * Can only be called after analysis. Returns the name of the database that
   * the target drop table resides in.
   */
  public String getDb() {
    Preconditions.checkNotNull(dbName_);
    return dbName_;
  }

  public String getTbl() { return tableName_.getTbl(); }
  public boolean isDropTable() { return dropTable_; }
}
