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

import com.google.common.base.Preconditions;
import org.apache.commons.lang.NotImplementedException;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.planner.DataSink;
import org.apache.impala.rewrite.ExprRewriter;
import org.apache.impala.thrift.TSortingOrder;

import java.util.ArrayList;
import java.util.List;

/**
 * Representation of an OPTIMIZE statement used to execute table maintenance tasks in
 * Iceberg tables, such as:
 * 1. compacting small files,
 * 2. merging delete deltas.
 * Currently, it executes these tasks as an alias for INSERT OVERWRITE:
 * OPTIMIZE TABLE tbl; -->> INSERT OVERWRITE TABLE tbl SELECT * FROM tbl;
 */
public class OptimizeStmt extends DmlStatementBase {

  // INSERT OVERWRITE statement that this OPTIMIZE statement is translated to.
  private InsertStmt insertStmt_;
  // Target table name as seen by the parser.
  private final TableName originalTableName_;
  // Target table that should be compacted. May be qualified by analyze().
  private TableName tableName_;

  public OptimizeStmt(TableName tableName) {
    tableName_ = tableName;
    originalTableName_ = tableName_;
    List<SelectListItem> selectListItems = new ArrayList<>();
    selectListItems.add(SelectListItem.createStarItem(null));
    SelectList selectList = new SelectList(selectListItems);
    List<TableRef> tableRefs = new ArrayList<>();
    tableRefs.add(new TableRef(tableName.toPath(), null));
    QueryStmt queryStmt = new SelectStmt(selectList, new FromClause(tableRefs), null,
        null, null, null, null);
    insertStmt_ = new InsertStmt(null, tableName, true, null, null, null, queryStmt,
        null, false);
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed()) return;
    super.analyze(analyzer);
    insertStmt_.analyze(analyzer);

    Preconditions.checkState(table_ == null);
    if (!tableName_.isFullyQualified()) {
      tableName_ = new TableName(analyzer.getDefaultDb(), tableName_.getTbl());
    }
    table_ = analyzer.getTable(tableName_, Privilege.ALL);
    Preconditions.checkState(table_ == insertStmt_.getTargetTable());
    if (!(table_ instanceof FeIcebergTable)) {
      throw new AnalysisException("OPTIMIZE is only supported for Iceberg tables.");
    }
  }

  @Override
  public void reset() {
    super.reset();
    tableName_ = originalTableName_;
    insertStmt_.reset();
  }

  public InsertStmt getInsertStmt() { return insertStmt_; }

  @Override
  public DataSink createDataSink() {
    throw new NotImplementedException();
  }

  @Override
  public void substituteResultExprs(ExprSubstitutionMap smap, Analyzer analyzer) {
    throw new NotImplementedException();
  }

  @Override
  public TSortingOrder getSortingOrder() {
    throw new NotImplementedException();
  }

  @Override
  public List<Expr> getPartitionKeyExprs() {
    return insertStmt_.getPartitionKeyExprs();
  }

  @Override
  public List<Expr> getSortExprs() {
    return insertStmt_.getSortExprs();
  }

  @Override
  public void collectTableRefs(List<TableRef> tblRefs) {
    insertStmt_.collectTableRefs(tblRefs);
  }

  @Override
  public void rewriteExprs(ExprRewriter rewriter) throws AnalysisException {
    Preconditions.checkState(isAnalyzed());
    insertStmt_.rewriteExprs(rewriter);
  }

}