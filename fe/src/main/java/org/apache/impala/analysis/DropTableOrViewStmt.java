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
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.View;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TAccessEvent;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TDropTableOrViewParams;
import org.apache.impala.thrift.TTableName;

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
    tableName_ = Preconditions.checkNotNull(tableName);
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
    params.setIs_table(dropTable_);
    return params;
  }

  @Override
  public void collectTableRefs(List<TableRef> tblRefs) {
    tblRefs.add(new TableRef(tableName_.toPath(), null));
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
      Table table = analyzer.getTable(tableName_, Privilege.DROP, true);
      Preconditions.checkNotNull(table);
      if (table instanceof View && dropTable_) {
        throw new AnalysisException(String.format(
            "DROP TABLE not allowed on a view: %s.%s", dbName_, getTbl()));
      }
      if (!(table instanceof View) && !dropTable_) {
        throw new AnalysisException(String.format(
            "DROP VIEW not allowed on a table: %s.%s", dbName_, getTbl()));
      }
    } catch (TableLoadingException e) {
      // We should still try to DROP tables that failed to load, so that tables that are
      // in a bad state, eg. deleted externally from Kudu, can be dropped.
      // We still need an access event - we don't know if this is a TABLE or a VIEW, so
      // we set it as TABLE as VIEW loading is unlikely to fail and even if it does
      // TABLE -> VIEW is a small difference.
      analyzer.addAccessEvent(new TAccessEvent(
          analyzer.getFqTableName(tableName_).toString(), TCatalogObjectType.TABLE,
          Privilege.DROP.toString()));
    } catch (AnalysisException e) {
      if (!ifExists_) throw e;
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
