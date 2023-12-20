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

import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.planner.DataSink;
import org.apache.impala.rewrite.ExprRewriter;
import org.apache.impala.thrift.TSortingOrder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

/**
 * Abstract class for implementing a Modify statement such as DELETE or UPDATE. Child
 * classes implement logic specific to target table types.
 */
abstract class ModifyImpl {
  abstract void addCastsToAssignmentsInSourceStmt(Analyzer analyzer)
      throws AnalysisException;

  abstract void analyze(Analyzer analyzer) throws AnalysisException;

  /**
   * This method is invoked during the creation of the source SELECT statement.
   * It populates 'selectList' with expressions we want to write during the operation.
   * (E.g. primary keys for Kudu, or file path and row position for Iceberg DELETEs, etc.)
   * This also sets the partition expressions and sort expressions if required.
   */
  abstract void buildAndValidateSelectExprs(Analyzer analyzer,
      List<SelectListItem> selectList) throws AnalysisException;

  abstract DataSink createDataSink();

  /**
   * Substitutes the result expressions, partition key expressions with smap.
   * Preserves the original types of those expressions during the substitution.
   * It is usually invoked when a SORT node or a VIEW is involved.
   * SORT node materializes sort expressions into the sort tuple, so after the
   * SORT node we only need to have slot refs to the materialized exprs. 'smap'
   * contains the mapping from the original exprs to the materialized slot refs.
   * When VIEWs are involved, the slot references also need to be substituted with
   * references to the actual base tables.
   */
  void substituteResultExprs(ExprSubstitutionMap smap, Analyzer analyzer) {
    sourceStmt_.substituteResultExprs(smap, analyzer);
  }

  // The Modify statement for this modify impl. The ModifyStmt class holds information
  // about the statement (e.g. target table type, FROM, WHERE clause, etc.)
  ModifyStmt modifyStmt_;
  /////////////////////////////////////////
  // START: Members that are set in createSourceStmt().

  // Result of the analysis of the internal SelectStmt that produces the rows that
  // will be modified.
  protected SelectStmt sourceStmt_;
  // END: Members that are set in createSourceStmt().
  /////////////////////////////////////////

  public ModifyImpl(ModifyStmt modifyStmt) {
    modifyStmt_ = modifyStmt;
  }

  public void reset() {
    if (sourceStmt_ != null) sourceStmt_.reset();
  }

  /**
   * Builds and validates the sourceStmt_. The select list of the sourceStmt_ contains
   * first the SlotRefs for the key Columns, followed by the expressions representing the
   * assignments. This method sets the member variables for the sourceStmt_ and the
   * referencedColumns_.
   *
   * This only creates the sourceStmt_ once, following invocations will reuse the
   * previously created statement.
   */
  protected void createSourceStmt(Analyzer analyzer)
      throws AnalysisException {
    if (sourceStmt_ == null) {
      // Builds the select list and column position mapping for the target table.
      ArrayList<SelectListItem> selectList = new ArrayList<>();
      buildAndValidateSelectExprs(analyzer, selectList);

      // Analyze the generated select statement.
      sourceStmt_ = new SelectStmt(new SelectList(selectList), modifyStmt_.fromClause_,
          modifyStmt_.wherePredicate_,
          null, null, null, null);
      sourceStmt_.analyze(analyzer);
      addCastsToAssignmentsInSourceStmt(analyzer);
    } else {
      sourceStmt_.analyze(analyzer);
    }
  }

  abstract public List<Expr> getPartitionKeyExprs();

  protected void checkSubQuery(SlotRef lhsSlotRef, Expr rhsExpr)
      throws AnalysisException {
    if (rhsExpr.contains(Subquery.class)) {
      throw new AnalysisException(
          format("Subqueries are not supported as update expressions for column '%s'",
              lhsSlotRef.toSql()));
    }
  }

  protected void checkCorrectTargetTable(SlotRef lhsSlotRef, Expr rhsExpr)
      throws AnalysisException {
    if (!lhsSlotRef.isBoundByTupleIds(modifyStmt_.targetTableRef_.getId().asList())) {
      throw new AnalysisException(
          format("Left-hand side column '%s' in assignment expression '%s=%s' does not "
              + "belong to target table '%s'", lhsSlotRef.toSql(), lhsSlotRef.toSql(),
              rhsExpr.toSql(),
              modifyStmt_.targetTableRef_.getDesc().getTable().getFullName()));
    }
  }

  protected void checkLhsIsColumnRef(SlotRef lhsSlotRef, Expr rhsExpr)
      throws AnalysisException {
    Column c = lhsSlotRef.getResolvedPath().destColumn();
    if (c == null) {
      throw new AnalysisException(
          format("Left-hand side in assignment expression '%s=%s' must be a column " +
              "reference", lhsSlotRef.toSql(), rhsExpr.toSql()));
    }
  }

  protected Expr checkTypeCompatiblity(Analyzer analyzer, Column c, Expr rhsExpr)
      throws AnalysisException {
    return StatementBase.checkTypeCompatibility(
        modifyStmt_.targetTableRef_.getDesc().getTable().getFullName(),
        c, rhsExpr, analyzer, null /*widestTypeSrcExpr*/);
  }

  protected void checkLhsOnlyAppearsOnce(Map<Integer, Expr> colToExprs, Column c,
      SlotRef lhsSlotRef, Expr rhsExpr) throws AnalysisException {
    if (colToExprs.containsKey(c.getPosition())) {
      throw new AnalysisException(
          format("Left-hand side in assignment appears multiple times '%s=%s'",
              lhsSlotRef.toSql(), rhsExpr.toSql()));
    }
  }

  public List<Expr> getSortExprs() {
    return Collections.emptyList();
  }

  public QueryStmt getQueryStmt() {
    return sourceStmt_;
  }

  public void castResultExprs(List<Type> types) throws AnalysisException {
    sourceStmt_.castResultExprs(types);
  }

  public void rewriteExprs(ExprRewriter rewriter) throws AnalysisException {
    sourceStmt_.rewriteExprs(rewriter);
  }

  public TSortingOrder getSortingOrder() { return TSortingOrder.LEXICAL; }
}
