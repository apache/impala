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
import org.apache.impala.catalog.View;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TTableName;

import com.google.common.base.Preconditions;

/**
 * Representation of a SHOW CREATE TABLE statement which returns either the
 * "CREATE TABLE ..." string that re-creates the table or the "CREATE VIEW ..."
 * string that re-creates the view as appropriate.
 *
 * Syntax: SHOW CREATE (TABLE|VIEW) <table or view>
 */
public class ShowCreateTableStmt extends StatementBase {
  private TableName tableName_;

  // The object type keyword used, e.g. TABLE or VIEW, needed to output matching SQL.
  private final TCatalogObjectType objectType_;

  public ShowCreateTableStmt(TableName table, TCatalogObjectType objectType) {
    Preconditions.checkNotNull(table);
    this.tableName_ = table;
    this.objectType_ = objectType;
  }

  @Override
  public String toSql() {
    return "SHOW CREATE " + objectType_.name() + " " + tableName_;
  }

  @Override
  public void collectTableRefs(List<TableRef> tblRefs) {
    tblRefs.add(new TableRef(tableName_.toPath(), null));
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    tableName_ = analyzer.getFqTableName(tableName_);
    Table table = analyzer.getTable(tableName_, Privilege.VIEW_METADATA);
    if (table instanceof View) {
      View view = (View)table;
      // Analyze the view query statement with its own analyzer for authorization.
      Analyzer viewAnalyzer = new Analyzer(analyzer);
      // Only show the view's query if the user has permissions to execute the query, to
      // avoid revealing information, e.g. tables the user does not have access to.
      // Report a masked authorization message if authorization fails.
      viewAnalyzer.setMaskPrivChecks(String.format("User '%s' does not have privileges " +
          "to see the definition of view '%s'.", analyzer.getUser().getName(),
          view.getFullName()));
      QueryStmt viewQuery = view.getQueryStmt().clone();
      // Views from the Hive metastore may rely on Hive's column naming if the SQL
      // statement references a column by its implicitly defined column names.
      viewAnalyzer.setUseHiveColLabels(true);
      viewQuery.analyze(viewAnalyzer);
    }
  }

  public TTableName toThrift() {
    TTableName params = new TTableName();
    params.setTable_name(tableName_.getTbl());
    params.setDb_name(tableName_.getDb());
    return params;
  }
}
