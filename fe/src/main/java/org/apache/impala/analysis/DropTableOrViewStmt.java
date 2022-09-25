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
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.FeView;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TAccessEvent;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TDropTableOrViewParams;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.util.MetaStoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Represents a DROP TABLE/VIEW [IF EXISTS] statement
 */
public class DropTableOrViewStmt extends StatementBase {
  private static final Logger LOG = LoggerFactory.getLogger(DropTableOrViewStmt.class);

  protected final TableName tableName_;
  protected final boolean ifExists_;

  // True if we are dropping a table. False if we are dropping a view.
  protected final boolean dropTable_;

  // Setting this value causes dropped tables to be permanently
  // deleted. For example, for hdfs tables it skips the trash directory
  protected final boolean purgeTable_;

  // Set during analysis
  protected String dbName_;

  // Server name needed for privileges. Set during analysis.
  private String serverName_;

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
  public String toSql(ToSqlOptions options) {
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
    params.setServer_name(serverName_);
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
    // Set the servername here if authorization is enabled because analyzer_ is not
    // available in the toThrift() method.
    serverName_ = analyzer.getServerName();
    try {
      // Fetch the table owner information, without registering any privileges.
      FeTable table = analyzer.getTableNoThrow(dbName_, tableName_.getTbl());
      String tblOwnerUser = table == null ? null : table.getOwnerUser();
      if (ifExists_) {
        // Start with ANY privilege in case of IF EXISTS, and register DROP privilege
        // later only if the table exists. See IMPALA-8851 for more explanation.
        analyzer.registerPrivReq(builder ->
            builder.allOf(Privilege.ANY)
            .onTable(dbName_, getTbl(), tblOwnerUser)
            .build());
        if (table == null) return;
      }
      // Register the DROP privilege on the table.
      table = analyzer.getTable(tableName_, /* add access event */ true,
          /* add column-level privilege */ false, Privilege.DROP);
      Preconditions.checkNotNull(table);
      if (table instanceof FeView && dropTable_) {
        // DROP VIEW IF EXISTS 'table' succeeds, similarly to Hive, but unlike postgres.
        if (ifExists_) return;
        throw new AnalysisException(String.format(
            "DROP TABLE not allowed on a view: %s.%s", dbName_, getTbl()));
      }
      if (!(table instanceof FeView) && !dropTable_) {
        // DROP TABLE IF EXISTS 'view' succeeds, similarly to Hive, but unlike postgres.
        if (ifExists_) return;
        throw new AnalysisException(String.format(
            "DROP VIEW not allowed on a table: %s.%s", dbName_, getTbl()));
      }
      // It currently supports create bucketed tables,
      // but it should also support drop bucketed tables.
      if (dropTable_ && !MetaStoreUtil.isBucketedTable(table.getMetaStoreTable())) {
        // To drop a view needs not write capabilities, only checks for tables.
        analyzer.checkTableCapability(table, Analyzer.OperationType.WRITE);
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
      LOG.info("Ignoring TableLoadingException for {}", tableName_);
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
