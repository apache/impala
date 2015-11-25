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

import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.Table;
import org.apache.impala.common.AnalysisException;
import com.google.common.base.Preconditions;

/**
 * Represents a reference to an actual table, such as an Hdfs or HBase table.
 * BaseTableRefs are instantiated as a result of table resolution during analysis
 * of a SelectStmt.
 */
public class BaseTableRef extends TableRef {

  /**
   * Create a BaseTableRef from the original unresolved table ref as well as
   * its resolved path. Sets table aliases and join-related attributes.
   */
  public BaseTableRef(TableRef tableRef, Path resolvedPath) {
    super(tableRef);
    Preconditions.checkState(resolvedPath.isResolved());
    Preconditions.checkState(resolvedPath.isRootedAtTable());
    resolvedPath_ = resolvedPath;
    // Set implicit aliases if no explicit one was given.
    if (hasExplicitAlias()) return;
    aliases_ = new String[] {
        getTable().getTableName().toString().toLowerCase(),
        getTable().getName().toLowerCase() };
  }

  /**
   * C'tor for cloning.
   */
  private BaseTableRef(BaseTableRef other) {
    super(other);
  }

  /**
   * Register this table ref and then analyze the Join clause.
   */
  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed_) return;
    analyzer.registerAuthAndAuditEvent(resolvedPath_.getRootTable(), priv_);
    desc_ = analyzer.registerTableRef(this);
    isAnalyzed_ = true;
    analyzeHints(analyzer);
    analyzeJoin(analyzer);
    analyzeSkipHeaderLineCount();
  }

  @Override
  protected String tableRefToSql() {
    // Enclose the alias in quotes if Hive cannot parse it without quotes.
    // This is needed for view compatibility between Impala and Hive.
    String aliasSql = null;
    String alias = getExplicitAlias();
    if (alias != null) aliasSql = ToSqlUtils.getIdentSql(alias);
    String tableHintsSql = ToSqlUtils.getPlanHintsSql(tableHints_);
    return getTable().getTableName().toSql() +
        ((aliasSql != null) ? " " + aliasSql : "") +
        (tableHintsSql != "" ? " " + tableHintsSql : "");
  }

  public String debugString() { return tableRefToSql(); }
  @Override
  protected TableRef clone() { return new BaseTableRef(this); }

  /**
   * Analyze the 'skip.header.line.count' property.
   */
  private void analyzeSkipHeaderLineCount() throws AnalysisException {
    Table table = getTable();
    if (!(table instanceof HdfsTable)) return;
    HdfsTable hdfsTable = (HdfsTable)table;

    StringBuilder error = new StringBuilder();
    hdfsTable.parseSkipHeaderLineCount(error);
    if (error.length() > 0) throw new AnalysisException(error.toString());
  }
}
