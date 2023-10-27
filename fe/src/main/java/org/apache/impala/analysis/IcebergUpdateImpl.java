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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.iceberg.TableProperties;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.IcebergColumn;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.Pair;
import org.apache.impala.planner.DataSink;
import org.apache.impala.planner.IcebergDeleteSink;
import org.apache.impala.planner.MultiDataSink;
import org.apache.impala.planner.TableSink;
import org.apache.impala.thrift.TIcebergFileFormat;
import org.apache.impala.thrift.TSortingOrder;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.impala.util.ExprUtil;
import org.apache.impala.util.IcebergUtil;

public class IcebergUpdateImpl extends IcebergModifyImpl {
  // Id of the delete table in the descriptor table. Set in analyze().
  private int deleteTableId_ = -1;

  /////////////////////////////////////////
  // START: Members that are set in buildAndValidateSelectExprs().
  private List<Expr> insertResultExprs_ = new ArrayList<>();
  private List<Expr> insertPartitionKeyExprs_ = new ArrayList<>();

  // END: Members that are set in buildAndValidateSelectExprs().
  /////////////////////////////////////////

  public IcebergUpdateImpl(ModifyStmt modifyStmt) {
    super(modifyStmt);
  }

  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    deleteTableId_ = analyzer.getDescTbl().addTargetTable(icePosDelTable_);
    IcebergUtil.validateIcebergColumnsForInsert(originalTargetTable_);
    if (originalTargetTable_.getPartitionSpecs().size() > 1) {
      throw new AnalysisException(
          String.format("Table '%s' has multiple partition specs, therefore " +
              "cannot be used as a target table in an UPDATE statement",
              originalTargetTable_.getFullName()));
    }
    String updateMode = originalTargetTable_.getIcebergApiTable().properties().get(
        TableProperties.UPDATE_MODE);
    if (updateMode != null && !updateMode.equals("merge-on-read")) {
      throw new AnalysisException(String.format("Unsupported update mode: '%s' for " +
          "Iceberg table: %s", updateMode, originalTargetTable_.getFullName()));
    }
    for (Column c : originalTargetTable_.getColumns()) {
      if (c.getType().isComplexType()) {
        throw new AnalysisException(String.format("Impala does not support updating " +
            "tables with complex types. Table '%s' has column '%s' " +
            "with type: %s", originalTargetTable_.getFullName(), c.getName(),
            c.getType().toSql()));
      }
    }
    Pair<List<Integer>, TSortingOrder> sortProperties =
        AlterTableSetTblProperties.analyzeSortColumns(originalTargetTable_,
            originalTargetTable_.getMetaStoreTable().getParameters());
    if (!sortProperties.first.isEmpty()) {
      throw new AnalysisException(String.format("Impala does not support updating " +
              "sorted tables. Data files in table '%s' are sorted by the " +
              "following column(s): %s", originalTargetTable_.getFullName(),
          originalTargetTable_.getMetaStoreTable().getParameters().get(
              AlterTableSortByStmt.TBL_PROP_SORT_COLUMNS)));
    }
    if (originalTargetTable_.getIcebergFileFormat() != TIcebergFileFormat.PARQUET) {
      throw new AnalysisException(String.format("Impala can only write Parquet data " +
          "files, while table '%s' expects '%s' data files.",
          originalTargetTable_.getFullName(),
          originalTargetTable_.getIcebergFileFormat().toString()));
    }
  }

  @Override
  protected void buildAndValidateSelectExprs(Analyzer analyzer,
      List<SelectListItem> selectList) throws AnalysisException {
    Map<Integer, Expr> colToExprs = new HashMap<>();

    for (Pair<SlotRef, Expr> valueAssignment : modifyStmt_.assignments_) {
      SlotRef lhsSlotRef = valueAssignment.first;
      lhsSlotRef.analyze(analyzer);

      Expr rhsExpr = valueAssignment.second;
      checkSubQuery(lhsSlotRef, rhsExpr);
      rhsExpr.analyze(analyzer);

      checkCorrectTargetTable(lhsSlotRef, rhsExpr);
      checkLhsIsColumnRef(lhsSlotRef, rhsExpr);

      IcebergColumn c = (IcebergColumn)lhsSlotRef.getResolvedPath().destColumn();
      rhsExpr = checkTypeCompatiblity(analyzer, c, rhsExpr);
      if (IcebergUtil.isPartitionColumn(
          c, originalTargetTable_.getDefaultPartitionSpec())) {
        throw new AnalysisException(
            String.format("Left-hand side in assignment '%s = %s' refers to a " +
                "partitioning column", lhsSlotRef.toSql(), rhsExpr.toSql()));
      }

      checkLhsOnlyAppearsOnce(colToExprs, c, lhsSlotRef, rhsExpr);
      colToExprs.put(c.getPosition(), rhsExpr);
    }

    List<Column> columns = modifyStmt_.table_.getColumns();
    for (Column col : columns) {
      Expr expr = colToExprs.get(col.getPosition());
      if (expr == null) expr = createSlotRef(analyzer, col.getName());
      insertResultExprs_.add(expr);
    }
    selectList.addAll(ExprUtil.exprsAsSelectList(insertResultExprs_));
    IcebergUtil.populatePartitionExprs(analyzer, null, columns,
        insertResultExprs_, originalTargetTable_, insertPartitionKeyExprs_, null);

    deletePartitionKeyExprs_ = getDeletePartitionExprs(analyzer);
    deleteResultExprs_ = getDeleteResultExprs(analyzer);
    selectList.addAll(ExprUtil.exprsAsSelectList(deletePartitionKeyExprs_));
    selectList.addAll(ExprUtil.exprsAsSelectList(deleteResultExprs_));

    sortExprs_.addAll(deleteResultExprs_);
  }

  @Override
  public void addCastsToAssignmentsInSourceStmt(Analyzer analyzer)
      throws AnalysisException {
    // Cast result expressions to the correct type of the referenced slot of the
    // target table.
    List<Column> columns = modifyStmt_.table_.getColumns();
    for (int i = 0; i < insertResultExprs_.size(); ++i) {
      Column col = columns.get(i);
      Expr resultExpr = sourceStmt_.resultExprs_.get(i);
      if (!col.getType().equals(resultExpr.getType())) {
        Expr castTo = resultExpr.castTo(col.getType());
        sourceStmt_.resultExprs_.set(i, castTo);
      }
    }
  }

  @Override
  public List<Expr> getPartitionKeyExprs() {
    return deletePartitionKeyExprs_;
  }

  public void substituteResultExprs(ExprSubstitutionMap smap, Analyzer analyzer) {
    super.substituteResultExprs(smap, analyzer);
    insertResultExprs_ = Expr.substituteList(insertResultExprs_, smap, analyzer, true);
    insertPartitionKeyExprs_ = Expr.substituteList(
        insertPartitionKeyExprs_, smap, analyzer, true);
  }

  @Override
  public DataSink createDataSink() {
    // analyze() must have been called before.
    Preconditions.checkState(modifyStmt_.table_ instanceof FeIcebergTable);

    TableSink insertSink = TableSink.create(modifyStmt_.table_, TableSink.Op.INSERT,
        insertPartitionKeyExprs_, insertResultExprs_, Collections.emptyList(), false,
        false, new Pair<>(ImmutableList.<Integer>of(), TSortingOrder.LEXICAL), -1, null,
        modifyStmt_.maxTableSinks_);
    TableSink deleteSink = new IcebergDeleteSink(
        icePosDelTable_, deletePartitionKeyExprs_, deleteResultExprs_, deleteTableId_);

    MultiDataSink ret = new MultiDataSink();
    ret.addDataSink(insertSink);
    ret.addDataSink(deleteSink);
    return ret;
  }
}
