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

package org.apache.impala.catalog;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.impala.analysis.QueryStmt;
import org.apache.impala.analysis.TableName;
import org.apache.impala.authorization.AuthorizationChecker;
import org.apache.impala.authorization.TableMask;
import org.apache.impala.authorization.User;
import org.apache.impala.common.InternalException;
import org.apache.impala.thrift.TTable;
import org.apache.impala.util.EventSequence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * A Materialized View of type HdfsTable. An MV also has a reference
 * to the QueryStmt from the view definition. This is useful for applying
 * access control policies on the MV based on the policies defined on the
 * source tables.
 */
public class MaterializedViewHdfsTable extends HdfsTable {

  private final static Logger LOG =
      LoggerFactory.getLogger(MaterializedViewHdfsTable.class);

  // View definition created by parsing inlineViewDef_ into a QueryStmt.
  private QueryStmt queryStmt_;

  // List of source tables referenced by this materialized view
  private List<TableName>  srcTblNames_;

  public MaterializedViewHdfsTable(org.apache.hadoop.hive.metastore.api.Table msTable,
      Db db, String name, String owner) {
    super(msTable, db, name, owner);
    srcTblNames_ = new ArrayList<>();
  }

  public QueryStmt getQueryStmt() { return queryStmt_; }

  /**
   * Returns true if any of the source tables associated with this
   * materialized view have a masking or row filtering policy defined
   * on them. False otherwise.
   */
  public boolean isReferencesMaskedTables(AuthorizationChecker authChecker,
    FeCatalog catalog, User user) throws InternalException {
    for (TableName srcTblName : srcTblNames_) {
      FeDb db = catalog.getDb(srcTblName.getDb());
      Preconditions.checkNotNull(db);
      FeTable srcTbl = db.getTable(srcTblName.getTbl());
      if (srcTbl == null) continue;
      Preconditions.checkArgument(srcTbl.isLoaded());
      List<Column> columns = srcTbl.getColumnsInHiveOrder();
      TableMask tableMask = new TableMask(authChecker, db.getName(),
          srcTblName.getTbl(), columns, user);
      if (tableMask.needsMaskingOrFiltering()) return true;
    }
    return false;
  }

  public void addSrcTables(Set<TableName> srcTableNames) {
    srcTblNames_.addAll(srcTableNames);
  }

  public List<TableName> getSrcTables() { return srcTblNames_; }

  @Override
  public void load(boolean reuseMetadata, IMetaStoreClient client,
      org.apache.hadoop.hive.metastore.api.Table msTbl, String reason,
      EventSequence catalogTimeline) throws TableLoadingException {
    super.load(reuseMetadata, client, msTbl, reason, catalogTimeline);
    initQueryStmt();
  }

  @Override
  protected void loadFromThrift(TTable thriftTable) throws TableLoadingException {
    super.loadFromThrift(thriftTable);
    initQueryStmt();
  }

  private void initQueryStmt() throws TableLoadingException {
    org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable();
    queryStmt_ = View.parseViewDef(new View(msTbl, getDb(), msTbl.getTableName(),
        msTbl.getOwner()));
  }

}
