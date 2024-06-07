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
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.KuduColumn;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.Pair;
import org.apache.impala.util.ExprUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

abstract class KuduModifyImpl extends ModifyImpl {
  // Target Kudu table.
  FeKuduTable kuduTable_;

  /////////////////////////////////////////
  // START: Members that are set in buildAndValidateSelectExprs().

  // Output expressions that produce the final results to write to the target table. May
  // include casts.
  //
  // In case of DELETE statements it contains the columns that identify the deleted
  // rows (Kudu primary keys, Iceberg file_path / position).
  protected List<Expr> resultExprs_ = new ArrayList<>();

  // Position mapping of output expressions of the sourceStmt_ to column indices in the
  // target table. The i'th position in this list maps to the referencedColumns_[i]'th
  // position in the target table.
  protected List<Integer> referencedColumns_ = new ArrayList<>();

  // END: Members that are set in buildAndValidateSelectExprs().
  /////////////////////////////////////////

  public KuduModifyImpl(ModifyStmt modifyStmt) {
    super(modifyStmt);
    kuduTable_ = (FeKuduTable)modifyStmt.getTargetTable();
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {}

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
  @Override
  protected void buildAndValidateSelectExprs(Analyzer analyzer,
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

    boolean convertToUtc = analyzer.getQueryOptions().isWrite_kudu_utc_timestamps();

    // Assignments are only used in the context of updates.
    for (Pair<SlotRef, Expr> valueAssignment : modifyStmt_.assignments_) {
      SlotRef lhsSlotRef = valueAssignment.first;
      lhsSlotRef.analyze(analyzer);

      Expr rhsExpr = valueAssignment.second;
      checkSubQuery(lhsSlotRef, rhsExpr);
      rhsExpr.analyze(analyzer);

      checkCorrectTargetTable(lhsSlotRef, rhsExpr);
      // TODO(Kudu) Add test for this code-path when Kudu supports nested types
      checkLhsIsColumnRef(lhsSlotRef, rhsExpr);

      Column c = lhsSlotRef.getResolvedPath().destColumn();

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

      if (convertToUtc && rhsExpr.getType().isTimestamp()) {
        rhsExpr = ExprUtil.toUtcTimestampExpr(
            analyzer, rhsExpr, true /*expectPreIfNonUnique*/);
      }
      uniqueSlots.add(lhsSlotRef.getSlotId());
      selectList.add(new SelectListItem(rhsExpr, null));
      referencedColumns_.add(colIndexMap.get(c.getName()));
    }
  }

  @Override
  public List<Expr> getPartitionKeyExprs() { return Collections.emptyList(); }

  public List<Integer> getReferencedColumns() {
    return referencedColumns_;
  }

  @Override
  public void substituteResultExprs(ExprSubstitutionMap smap, Analyzer analyzer) {
    super.substituteResultExprs(smap, analyzer);
    resultExprs_ = Expr.substituteList(resultExprs_, smap, analyzer, true);
  }

  protected void addKeyColumn(Analyzer analyzer, List<SelectListItem> selectList,
      List<Integer> referencedColumns, Set<SlotId> uniqueSlots, Set<SlotId> keySlots,
      Map<String, Integer> colIndexMap, String colName)
      throws AnalysisException {
    Expr ref = addSlotRef(analyzer, selectList, referencedColumns, uniqueSlots,
        keySlots, colIndexMap, colName);
    resultExprs_.add(ref);
  }

  private Expr addSlotRef(Analyzer analyzer, List<SelectListItem> selectList,
      List<Integer> referencedColumns, Set<SlotId> uniqueSlots, Set<SlotId> keySlots,
      Map<String, Integer> colIndexMap, String colName) throws AnalysisException {
    List<String> path = Path.createRawPath(modifyStmt_.targetTableRef_.getUniqueAlias(),
        colName);
    SlotRef ref = new SlotRef(path);
    ref.analyze(analyzer);
    Expr expr = ref;
    boolean convertToUtc = analyzer.getQueryOptions().isWrite_kudu_utc_timestamps();
    if (convertToUtc && expr.getType().isTimestamp()) {
      expr = ExprUtil.toUtcTimestampExpr(
          analyzer, expr, true /*expectPreIfNonUnique*/);
    }
    selectList.add(new SelectListItem(expr, null));
    uniqueSlots.add(ref.getSlotId());
    keySlots.add(ref.getSlotId());
    referencedColumns.add(colIndexMap.get(colName));
    return expr;
  }

  @Override
  public void addCastsToAssignmentsInSourceStmt(Analyzer analyzer)
      throws AnalysisException {
    // cast result expressions to the correct type of the referenced slot of the
    // target table
    List<Pair<SlotRef, Expr>> assignments = modifyStmt_.getAssignments();
    int keyColumnsOffset = kuduTable_.getPrimaryKeyColumnNames().size();
    for (int i = keyColumnsOffset; i < sourceStmt_.resultExprs_.size(); ++i) {
      sourceStmt_.resultExprs_.set(i, sourceStmt_.resultExprs_.get(i).castTo(
          assignments.get(i - keyColumnsOffset).first.getType()));
    }
  }

  public void addKeyColumns(Analyzer analyzer, List<SelectListItem> selectList,
      List<Integer> referencedColumns, Set<SlotId> uniqueSlots, Set<SlotId> keySlots,
      Map<String, Integer> colIndexMap) throws AnalysisException {
    // Add the key columns as slot refs
    for (String k : kuduTable_.getPrimaryKeyColumnNames()) {
      addKeyColumn(analyzer, selectList, referencedColumns, uniqueSlots, keySlots,
          colIndexMap, k);
    }
  }
}
