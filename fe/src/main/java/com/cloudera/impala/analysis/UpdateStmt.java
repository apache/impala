// Copyright 2015 Cloudera Inc.
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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.authorization.PrivilegeRequestBuilder;
import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.KuduTable;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.Pair;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;

/**
 * Representation of an Update statement.
 *
 * Example UPDATE statement:
 *
 *     UPDATE target_table
 *       SET slotRef=expr, [slotRef=expr, ...]
 *       FROM table_ref_list
 *       WHERE conjunct_list
 *
 * An update statement consists of four major parts. First, the target table path,
 * second, the list of assignments, the optional FROM clause, and the optional where
 * clause. The type of the right-hand side of each assignments must be
 * assignment compatible with the left-hand side column type.
 *
 * In the analysis phase, a SelectStmt is created with the result expressions set to
 * match the right-hand side of the assignments. During query execution, the plan that
 * is generated from this SelectStmt produces all rows that need to be updated.
 *
 * Currently, only Kudu tables can be updated.
 */
public class UpdateStmt extends StatementBase {
  private final static org.slf4j.Logger LOG = LoggerFactory.getLogger(UpdateStmt.class);

  // List of explicitly mentioned assignment expressions in the UPDATE's SET clause
  private final List<Pair<SlotRef, Expr>> assignments_;

  // Optional WHERE clause of the statement
  private final Expr wherePredicate_;

  // Path identifying the target table.
  private final List<String> targetTablePath_;

  // TableRef identifying the target table, set during analysis.
  private TableRef targetTableRef_;

  private FromClause fromClause_;

  // Result of the analysis of the internal SelectStmt that produces the rows that
  // will be updated.
  private SelectStmt sourceStmt_;

  // Target Kudu table. Since currently only Kudu tables are supported, we use a
  // concrete table class. Result of analysis.
  private KuduTable table_;

  // Position mapping of output expressions of the sourceStmt_ to column indices in the
  // target table. The i'th position in this list maps to the referencedColumns_[i]'th
  // position in the target table. Set in createSourceStmt() during analysis.
  private ArrayList<Integer> referencedColumns_;

  public UpdateStmt(List<String> targetTablePath, FromClause tableRefs,
      List<Pair<SlotRef, Expr>> assignmentExprs,
      Expr wherePredicate) {
    targetTablePath_ = Preconditions.checkNotNull(targetTablePath);
    fromClause_ = Preconditions.checkNotNull(tableRefs);
    assignments_ = Preconditions.checkNotNull(assignmentExprs);
    wherePredicate_ = wherePredicate;
  }

  /**
   * The analysis of the UPDATE progresses as follows. First, the FROM clause is analyzed
   * and the targetTablePath is verified to be a valid alias into the FROM clause. When
   * the target table is identified, the assignment expressions are validated and as a
   * last step the internal SelectStmt is produced and analyzed. Potential query rewrites
   * for the select statement are implemented here and are not triggered externally
   * by the statement rewriter.
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

    if (path.destTupleDesc() == null) {
      throw new AnalysisException(format(
          "'%s' is not a table alias. Using the FROM clause requires the target table " +
              "to be a table alias.",
          Joiner.on(".").join(targetTablePath_)));
    }

    targetTableRef_ = analyzer.getTableRef(path.getRootDesc().getId());
    if (targetTableRef_ instanceof InlineViewRef) {
      throw new AnalysisException(format("Cannot update view: '%s'",
          targetTableRef_.toSql()));
    }

    Preconditions.checkNotNull(targetTableRef_);
    Table dstTbl = targetTableRef_.getTable();
    // Only Kudu tables can be updated
    if (!(dstTbl instanceof KuduTable)) {
      throw new AnalysisException(
          format("Impala does not support updating a non-Kudu table: %s",
              dstTbl.getFullName()));
    }
    table_ = (KuduTable) dstTbl;

    // Make sure that the user is allowed to update the target table, since no UPDATE
    // privilege exists, we reuse the INSERT one.
    analyzer.registerPrivReq(new PrivilegeRequestBuilder()
        .onTable(table_.getDb().getName(), table_.getName())
        .allOf(Privilege.INSERT).toRequest());

    // Validates the assignments_ and creates the sourceStmt_.
    createSourceStmt(analyzer);
  }

  /**
   * Builds and validates the sourceStmt_. The select list of the sourceStmt_ contains
   * first the SlotRefs for the key Columns, followed by the expressions representing the
   * assignments. This method sets the member variables for the sourceStmt_ and the
   * referencedColumns_.
   */
  private void createSourceStmt(Analyzer analyzer)
      throws AnalysisException {
    // Builds the select list and column position mapping for the target table.
    ArrayList<SelectListItem> selectList = Lists.newArrayList();
    referencedColumns_ = Lists.newArrayList();
    buildAndValidateAssignmentExprs(analyzer, selectList, referencedColumns_);

    // Analyze the generated select statement.
    SelectStmt srcStmt =
        new SelectStmt(new SelectList(selectList), fromClause_, wherePredicate_,
            null, null, null, null);
    srcStmt.analyze(analyzer);

    if (analyzer.containsSubquery()) {
      StmtRewriter.rewriteQueryStatement(srcStmt, analyzer);
      srcStmt = (SelectStmt) srcStmt.clone();

      // After the clone, the state of all table refs is reset. Use a child analyzer
      // to avoid clashing with registered table refs in 'analyzer'
      // TODO(Kudu) After merge, clean this up by implementing proper reset() / clone()
      srcStmt.analyze(new Analyzer(analyzer));

      // Force no additional rewrite
      // TODO(Kudu) Remove method after merge as rewrite is handled with reset() / clone()
      analyzer.resetSubquery();
    }

    // cast result expressions to the correct type of the referenced slot of the
    // target table
    int keyColumnsOffset = table_.getKuduKeyColumnNames().size();
    for(int i = keyColumnsOffset; i < srcStmt.resultExprs_.size(); ++i) {
      srcStmt.resultExprs_.set(i, srcStmt.resultExprs_.get(i).castTo(
          assignments_.get(i - keyColumnsOffset).first.getType()));
    }
    sourceStmt_ = srcStmt;
  }

  /**
   * Validates the list of value assignments that should be used to update the target
   * table. It verifies that only those columns are referenced that belong to the target
   * table, no key columns are updated, and that a single column is not updated multiple
   * times. Analyzes the Exprs and SlotRefs of assignments_ and writes a list of
   * SelectListItems to the out parameter selectList that is used to build the select list
   * for sourceStmt_. A list of integers indicating the column position of an entry in the
   * select list in the target table is written to the out parameter referencedColumns.
   *
   * In addition to the expressions that are generated for each assignment, the
   * expression list contains an expression for each key column. The key columns
   * are always prepended to the list of expression representing the assignments.
   */
  private void buildAndValidateAssignmentExprs(Analyzer analyzer,
      ArrayList<SelectListItem> selectList, ArrayList<Integer> referencedColumns)
      throws AnalysisException {
    // The order of the referenced columns equals the order of the result expressions
    HashSet<SlotId> uniqueSlots = Sets.newHashSet();
    HashSet<SlotId> keySlots = Sets.newHashSet();

    // Mapping from column name to index
    ArrayList<Column> cols = table_.getColumnsInHiveOrder();
    HashMap<String, Integer> colIndexMap = Maps.newHashMap();
    for (int i = 0; i < cols.size(); i++) {
      colIndexMap.put(cols.get(i).getName(), i);
    }

    // Add the key columns as slot refs
    for (String k : table_.getKuduKeyColumnNames()) {
      ArrayList<String> path = Path.createRawPath(targetTableRef_.getUniqueAlias(), k);
      SlotRef ref = new SlotRef(path);
      ref.analyze(analyzer);
      selectList.add(new SelectListItem(ref, null));
      uniqueSlots.add(ref.getSlotId());
      keySlots.add(ref.getSlotId());
      referencedColumns.add(colIndexMap.get(k));
    }

    for (Pair<SlotRef, Expr> valueAssignment : assignments_) {
      Expr rhsExpr = valueAssignment.second;
      SlotRef lhsSlotRef = valueAssignment.first;

      rhsExpr.analyze(analyzer);
      lhsSlotRef.analyze(analyzer);

      // Correct target table
      if (!lhsSlotRef.isBoundByTupleIds(targetTableRef_.getId().asList())) {
        throw new AnalysisException(
            format(
                "Left-hand side column '%s' in assignment expression '%s=%s' does not " +
                    "belong to target table '%s'",
                lhsSlotRef.toSql(),
                lhsSlotRef.toSql(),
                rhsExpr.toSql(),
                targetTableRef_.getDesc().getTable().getFullName()));
      }

      // No subqueries for rhs expression
      if (rhsExpr.contains(Subquery.class)) {
        throw new AnalysisException(
            format("Subqueries are not supported as update expressions for column '%s'",
                lhsSlotRef.toSql()));
      }

      Column c = lhsSlotRef.getResolvedPath().destColumn();
      // TODO(Kudu) Add test for this code-path when Kudu supports nested types
      if (c == null) {
        throw new AnalysisException(
            format("Left-hand side in assignment expression '%s=%s' must be a column " +
                    "reference", lhsSlotRef.toSql(), rhsExpr.toSql()));
      }

      if (keySlots.contains(lhsSlotRef.getSlotId())) {
        throw new AnalysisException(format("Key column '%s' cannot be updated.",
            lhsSlotRef.toSql()));
      }

      if (uniqueSlots.contains(lhsSlotRef.getSlotId())) {
        throw new AnalysisException(
            format("Duplicate value assignment to column: '%s'",
                lhsSlotRef.toSql()));
      }

      // Check type of the expression
      if (!Type.isImplicitlyCastable(rhsExpr.getType(), c.getType())) {
        throw new AnalysisException(format(
            "Column '%s' of type %s is not compatible with the expression %s of type " +
                "%s",
            c.getName(), c.getType().toSql(), rhsExpr.toSql(),
            rhsExpr.getType().toSql()));
      }

      uniqueSlots.add(lhsSlotRef.getSlotId());
      selectList.add(new SelectListItem(rhsExpr, null));
      referencedColumns.add(colIndexMap.get(c.getName()));
    }
  }

  /**
   * String representation of the UPDATE stmt, Does not generate the SQL matching the
   * underlying select statement but only the SQL for the UPDATE stmt.
   */
  @Override
  public String toSql() {
    StringBuilder b = new StringBuilder();
    b.append("UPDATE ");

    if (fromClause_ == null) {
      b.append(targetTableRef_.toSql());
    } else {
      if (targetTableRef_.hasExplicitAlias()) {
        b.append(targetTableRef_.getExplicitAlias());
      } else {
        b.append(targetTableRef_.toSql());
      }
    }
    b.append(" SET");

    boolean first = true;
    for (Pair<SlotRef, Expr> i : assignments_) {
      if (!first) {
        b.append(",");
      } else {
        first = false;
      }
      b.append(format(" %s = %s",
          i.first.toSql(),
          i.second.toSql()));
    }

    b.append(fromClause_.toSql());

    if (wherePredicate_ != null) {
      b.append(" WHERE ");
      b.append(wherePredicate_.toSql());
    }
    return b.toString();
  }
}
