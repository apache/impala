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

import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.KuduColumn;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.Pair;
import org.apache.impala.planner.DataSink;
import org.apache.impala.rewrite.ExprRewriter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Abstract class for implementing a Modify statement such as DELETE or UPDATE. Child
 * classes implement logic specific to target table types.
 */
abstract class ModifyImpl {
  abstract void addCastsToAssignmentsInSourceStmt(Analyzer analyzer)
      throws AnalysisException;

  abstract void addKeyColumns(Analyzer analyzer,
      List<SelectListItem> selectList, List<Integer> referencedColumns,
      Set<SlotId> uniqueSlots, Set<SlotId> keySlots, Map<String, Integer> colIndexMap)
      throws AnalysisException;

  abstract void analyze(Analyzer analyzer) throws AnalysisException;

  abstract DataSink createDataSink();

  // The Modify statement for this modify impl. The ModifyStmt class holds information
  // about the statement (e.g. target table type, FROM, WHERE clause, etc.)
  ModifyStmt modifyStmt_;
  /////////////////////////////////////////
  // START: Members that are set in createSourceStmt().

  // Result of the analysis of the internal SelectStmt that produces the rows that
  // will be modified.
  protected SelectStmt sourceStmt_;

  // Output expressions that produce the final results to write to the target table. May
  // include casts.
  //
  // In case of DELETE statements it contains the columns that identify the deleted
  // rows (Kudu primary keys, Iceberg file_path / position).
  protected List<Expr> resultExprs_ = new ArrayList<>();

  // Exprs corresponding to the partitionKeyValues, if specified, or to the partition
  // columns for tables.
  protected List<Expr> partitionKeyExprs_ = new ArrayList<>();

  // For every column of the target table that is referenced in the optional
  // 'sort.columns' table property, this list will contain the corresponding result expr
  // from 'resultExprs_'. Before insertion, all rows will be sorted by these exprs. If the
  // list is empty, no additional sorting by non-partitioning columns will be performed.
  // The column list must not contain partition columns and must be empty for non-Hdfs
  // tables.
  protected List<Expr> sortExprs_ = new ArrayList<>();

  // Position mapping of output expressions of the sourceStmt_ to column indices in the
  // target table. The i'th position in this list maps to the referencedColumns_[i]'th
  // position in the target table.
  protected List<Integer> referencedColumns_ = new ArrayList<>();
  // END: Members that are set in first run of analyze
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
      buildAndValidateAssignmentExprs(analyzer, selectList);

      // Analyze the generated select statement.
      sourceStmt_ = new SelectStmt(new SelectList(selectList), modifyStmt_.fromClause_,
          modifyStmt_.wherePredicate_,
          null, null, null, null);

      addCastsToAssignmentsInSourceStmt(analyzer);
    }
    sourceStmt_.analyze(analyzer);
  }

  /**
   * Validates the list of value assignments that should be used to modify the target
   * table. It verifies that only those columns are referenced that belong to the target
   * table, no key columns are modified, and that a single column is not modified multiple
   * times. Analyzes the Exprs and SlotRefs of assignments_ and writes a list of
   * SelectListItems to the out parameter selectList that is used to build the select list
   * for sourceStmt_. A list of integers indicating the column position of an entry in the
   * select list in the target table is written to the field referencedColumns_.
   *
   * In addition to the expressions that are generated for each assignment, the
   * expression list contains an expression for each key column. The key columns
   * are always prepended to the list of expression representing the assignments.
   */
  private void buildAndValidateAssignmentExprs(Analyzer analyzer,
      List<SelectListItem> selectList)
      throws AnalysisException {
    // The order of the referenced columns equals the order of the result expressions
    Set<SlotId> uniqueSlots = new HashSet<>();
    Set<SlotId> keySlots = new HashSet<>();

    // Mapping from column name to index
    List<Column> cols = modifyStmt_.table_.getColumnsInHiveOrder();
    Map<String, Integer> colIndexMap = new HashMap<>();
    for (int i = 0; i < cols.size(); i++) {
      colIndexMap.put(cols.get(i).getName(), i);
    }

    addKeyColumns(analyzer, selectList, referencedColumns_, uniqueSlots,
        keySlots, colIndexMap);

    // Assignments are only used in the context of updates.
    for (Pair<SlotRef, Expr> valueAssignment : modifyStmt_.assignments_) {
      SlotRef lhsSlotRef = valueAssignment.first;
      lhsSlotRef.analyze(analyzer);

      Expr rhsExpr = valueAssignment.second;
      // No subqueries for rhs expression
      if (rhsExpr.contains(Subquery.class)) {
        throw new AnalysisException(
            format("Subqueries are not supported as update expressions for column '%s'",
                lhsSlotRef.toSql()));
      }
      rhsExpr.analyze(analyzer);

      // Correct target table
      if (!lhsSlotRef.isBoundByTupleIds(modifyStmt_.targetTableRef_.getId().asList())) {
        throw new AnalysisException(
            format("Left-hand side column '%s' in assignment expression '%s=%s' does not "
                + "belong to target table '%s'", lhsSlotRef.toSql(), lhsSlotRef.toSql(),
                rhsExpr.toSql(),
                modifyStmt_.targetTableRef_.getDesc().getTable().getFullName()));
      }

      Column c = lhsSlotRef.getResolvedPath().destColumn();
      // TODO(Kudu) Add test for this code-path when Kudu supports nested types
      if (c == null) {
        throw new AnalysisException(
            format("Left-hand side in assignment expression '%s=%s' must be a column " +
                "reference", lhsSlotRef.toSql(), rhsExpr.toSql()));
      }

      if (keySlots.contains(lhsSlotRef.getSlotId())) {
        boolean isSystemGeneratedColumn =
            c instanceof KuduColumn && ((KuduColumn)c).isAutoIncrementing();
        throw new AnalysisException(format("%s column '%s' cannot be updated.",
            isSystemGeneratedColumn ? "System generated key" : "Key",
            lhsSlotRef.toSql()));
      }

      if (uniqueSlots.contains(lhsSlotRef.getSlotId())) {
        throw new AnalysisException(
            format("Duplicate value assignment to column: '%s'", lhsSlotRef.toSql()));
      }

      rhsExpr = StatementBase.checkTypeCompatibility(
          modifyStmt_.targetTableRef_.getDesc().getTable().getFullName(),
          c, rhsExpr, analyzer, null /*widestTypeSrcExpr*/);
      uniqueSlots.add(lhsSlotRef.getSlotId());
      selectList.add(new SelectListItem(rhsExpr, null));
      referencedColumns_.add(colIndexMap.get(c.getName()));
    }
  }

  protected void addKeyColumn(Analyzer analyzer, List<SelectListItem> selectList,
      List<Integer> referencedColumns, Set<SlotId> uniqueSlots, Set<SlotId> keySlots,
      Map<String, Integer> colIndexMap, String colName, boolean isSortingColumn)
      throws AnalysisException {
    SlotRef ref = addSlotRef(analyzer, selectList, referencedColumns, uniqueSlots,
        keySlots, colIndexMap, colName);
    resultExprs_.add(ref);
    if (isSortingColumn) sortExprs_.add(ref);
  }

  protected void addPartitioningColumn(Analyzer analyzer, List<SelectListItem> selectList,
  List<Integer> referencedColumns, Set<SlotId> uniqueSlots, Set<SlotId> keySlots,
  Map<String, Integer> colIndexMap, String colName) throws AnalysisException {
    SlotRef ref = addSlotRef(analyzer, selectList, referencedColumns, uniqueSlots,
        keySlots, colIndexMap, colName);
    partitionKeyExprs_.add(ref);
    sortExprs_.add(ref);
  }

  private SlotRef addSlotRef(Analyzer analyzer, List<SelectListItem> selectList,
      List<Integer> referencedColumns, Set<SlotId> uniqueSlots, Set<SlotId> keySlots,
      Map<String, Integer> colIndexMap, String colName) throws AnalysisException {
    List<String> path = Path.createRawPath(modifyStmt_.targetTableRef_.getUniqueAlias(),
        colName);
    SlotRef ref = new SlotRef(path);
    ref.analyze(analyzer);
    selectList.add(new SelectListItem(ref, null));
    uniqueSlots.add(ref.getSlotId());
    keySlots.add(ref.getSlotId());
    referencedColumns.add(colIndexMap.get(colName));
    return ref;
  }

  public List<Expr> getPartitionKeyExprs() {
     return partitionKeyExprs_;
  }

  public List<Expr> getSortExprs() {
    return sortExprs_;
  }

  public QueryStmt getQueryStmt() {
    return sourceStmt_;
  }

  public List<Integer> getReferencedColumns() {
    return referencedColumns_;
  }

  public void castResultExprs(List<Type> types) throws AnalysisException {
    sourceStmt_.castResultExprs(types);
  }

  public void rewriteExprs(ExprRewriter rewriter) throws AnalysisException {
    sourceStmt_.rewriteExprs(rewriter);
  }

  public void substituteResultExprs(ExprSubstitutionMap smap, Analyzer analyzer) {
    resultExprs_ = Expr.substituteList(resultExprs_, smap, analyzer, true);
    partitionKeyExprs_ = Expr.substituteList(partitionKeyExprs_, smap, analyzer, true);
  }
}
