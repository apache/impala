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

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.List;

import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.Pair;
import org.apache.impala.rewrite.ExprRewriter;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.impala.thrift.TSortingOrder;

/**
 * Abstract super class for statements that modify existing data like
 * UPDATE and DELETE.
 *
 * The ModifyStmt has four major parts:
 *   - targetTablePath (not null)
 *   - fromClause (not null)
 *   - assignmentExprs (not null, can be empty)
 *   - wherePredicate (nullable)
 *
 * This class holds information from parsing and semantic analysis. Then it delegates
 * implementation logic to the *Impl classes, e.g. KuduDeleteImpl, IcebergDeleteImpl,
 * etc.
 * In the analysis phase, the impl object creates a SelectStmt with the result expressions
 * which hold information about the modified records (e.g. primary keys of Kudu tables,
 * file_path / pos information of Iceberg data records).
 * During query execution, the plan that is generated from this SelectStmt produces
 * all rows that need to be modified.
 *
 * UPDATEs:
 * The result of the SelectStmt contain the right-hand side of the assignments in addition
 * to projecting the key columns of the underlying table.
 */
public abstract class ModifyStmt extends DmlStatementBase {

  // List of explicitly mentioned assignment expressions in the UPDATE's SET clause
  protected final List<Pair<SlotRef, Expr>> assignments_;

  // Optional WHERE clause of the statement
  protected final Expr wherePredicate_;

  // Path identifying the target table.
  protected final List<String> targetTablePath_;

  // TableRef identifying the target table, set during analysis.
  protected TableRef targetTableRef_;

  // FROM clause of the statement
  protected FromClause fromClause_;

  /////////////////////////////////////////
  // START: Members that are set in first run of analyze().

  // Implementation of the modify statement. Depends on the target table type.
  protected ModifyImpl modifyImpl_;

  // END: Members that are set in first run of analyze
  /////////////////////////////////////////

  // SQL string of the ModifyStmt. Set in analyze().
  protected String sqlString_;

  public ModifyStmt(List<String> targetTablePath, FromClause fromClause,
      List<Pair<SlotRef, Expr>> assignmentExprs, Expr wherePredicate) {
    targetTablePath_ = Preconditions.checkNotNull(targetTablePath);
    fromClause_ = Preconditions.checkNotNull(fromClause);
    assignments_ = Preconditions.checkNotNull(assignmentExprs);
    wherePredicate_ = wherePredicate;
  }

  @Override
  public void collectTableRefs(List<TableRef> tblRefs) {
    tblRefs.add(new TableRef(targetTablePath_, null));
    fromClause_.collectTableRefs(tblRefs);
    if (wherePredicate_ != null) {
      // Collect TableRefs in WHERE-clause subqueries.
      List<Subquery> subqueries = new ArrayList<>();
      wherePredicate_.collect(Subquery.class, subqueries);
      for (Subquery sq : subqueries) {
        sq.getStatement().collectTableRefs(tblRefs);
      }
    }
  }

  /**
   * The analysis of the ModifyStmt proceeds as follows: First, the FROM clause is
   * analyzed and the targetTablePath is verified to be a valid alias into the FROM
   * clause. It also identifies the target table. Raises errors for unsupported table
   * types.
   */
  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    fromClause_.analyze(analyzer);

    List<Path> candidates = analyzer.getTupleDescPaths(targetTablePath_);
    if (candidates.isEmpty()) {
      throw new AnalysisException(format("'%s' is not a valid table alias or reference.",
          Joiner.on(".").join(targetTablePath_)));
    }

    Preconditions.checkState(candidates.size() == 1);
    Path path = candidates.get(0);
    path.resolve();

    if (!path.isResolved()) {
      throw new AnalysisException(format("Cannot resolve path '%s' for DML statement.",
          path.toString()));
    }

    if (path.destTupleDesc() == null) {
      throw new AnalysisException(format(
          "'%s' is not a table alias. Using the FROM clause requires the target table " +
              "to be a table alias.",
          Joiner.on(".").join(targetTablePath_)));
    }

    targetTableRef_ = analyzer.getTableRef(path.getRootDesc().getId());
    if (targetTableRef_ instanceof InlineViewRef) {
      throw new AnalysisException(format("Cannot modify view: '%s'",
          targetTableRef_.toSql()));
    }

    Preconditions.checkNotNull(targetTableRef_);
    FeTable dstTbl = targetTableRef_.getTable();
    // Only Kudu and Iceberg tables can be updated.
    if (!(dstTbl instanceof FeKuduTable) && !(dstTbl instanceof FeIcebergTable)) {
      throw new AnalysisException(
          format("Impala only supports modifying Kudu and Iceberg tables, " +
              "but the following table is neither: %s",
              dstTbl.getFullName()));
    }
    if (dstTbl instanceof FeIcebergTable) {
      setMaxTableSinks(analyzer_.getQueryOptions().getMax_fs_writers());
    }
    // Make sure that the user is allowed to modify the target table. Use ALL because no
    // UPDATE / DELETE privilege exists yet (IMPALA-3840).
    analyzer.registerAuthAndAuditEvent(dstTbl, Privilege.ALL);
    table_ = dstTbl;
    if (modifyImpl_ == null) createModifyImpl();
    modifyImpl_.analyze(analyzer);
    // Create and analyze the source statement.
    modifyImpl_.createSourceStmt(analyzer);
    // Add target table to descriptor table.
    analyzer.getDescTbl().setTargetTable(table_);

    sqlString_ = toSql();
  }

  /**
   * Creates the implementation class for this statement. Ony called once during the
   * first run of analyze().
   */
  abstract protected void createModifyImpl();

  @Override
  public void reset() {
    super.reset();
    fromClause_.reset();
    modifyImpl_.reset();
  }

  @Override
  public List<Expr> getPartitionKeyExprs() { return modifyImpl_.getPartitionKeyExprs(); }
  @Override
  public List<Expr> getSortExprs() { return modifyImpl_.getSortExprs(); }

  public Expr getWherePredicate() { return wherePredicate_; }

  public List<Pair<SlotRef, Expr>> getAssignments() { return assignments_; }

  @Override
  public TSortingOrder getSortingOrder() { return modifyImpl_.getSortingOrder(); }

  @Override
  public boolean resolveTableMask(Analyzer analyzer) throws AnalysisException {
    return getQueryStmt().resolveTableMask(analyzer);
  }

  @Override
  public List<Expr> getResultExprs() {
    return modifyImpl_.getQueryStmt().getResultExprs();
  }

  @Override
  public void castResultExprs(List<Type> types) throws AnalysisException {
    modifyImpl_.castResultExprs(types);
  }

  @Override
  public void rewriteExprs(ExprRewriter rewriter) throws AnalysisException {
    Preconditions.checkState(isAnalyzed());
    for (Pair<SlotRef, Expr> assignment: assignments_) {
      assignment.second = rewriter.rewrite(assignment.second, analyzer_);
    }
    modifyImpl_.rewriteExprs(rewriter);
  }

  public QueryStmt getQueryStmt() { return modifyImpl_.getQueryStmt(); }

  /**
   * Return true if the target table is Kudu table.
   */
  public boolean isTargetTableKuduTable() { return (table_ instanceof FeKuduTable); }

  @Override
  public abstract String toSql(ToSqlOptions options);
}
