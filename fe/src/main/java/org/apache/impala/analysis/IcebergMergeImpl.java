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
import com.google.common.collect.Lists;
import java.util.Objects;
import org.apache.iceberg.TableProperties;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.VirtualColumn;
import org.apache.impala.catalog.IcebergPositionDeleteTable;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.Pair;
import org.apache.impala.planner.DataSink;
import org.apache.impala.planner.IcebergBufferedDeleteSink;
import org.apache.impala.planner.IcebergMergeNode;
import org.apache.impala.planner.IcebergMergeSink;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.PlannerContext;
import org.apache.impala.planner.TableSink;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TSortingOrder;
import org.apache.impala.util.IcebergUtil;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class implements analysis of MERGE statements on Iceberg tables.
 * The implementation is creating a SELECT statement that enumerates all columns
 * from the target table and from the source table reference, joins the target
 * and the source by the given ON clause, and adds an auxiliary column
 * called 'row_present' which evaluates to a flag that signals whether the
 * target, the source, or both rows are present in the result set. This marker
 * helps the merge operator to decide which merge case should be evaluated for
 * each row. The implementation also analyzes the merge cases (WHEN MATCHED /
 * NOT MATCHED), and validates the references listed in them. The Iceberg merge
 * uses a specialized data sink called IcebergMergeSink which handles the
 * update/delete/insert operations for each row. The result set of the merge
 * query also contains an auxiliary tuple called merge action tuple, this tuple
 * holds the information that is required for the IcebergMergeSink to decide if
 * the incoming row should be marked as deleted, written as a new row, or both.
 */
public class IcebergMergeImpl implements MergeImpl {
  private static final String MERGE_ACTION_TUPLE_NAME = "merge-action";

  private final MergeStmt mergeStmt_;
  private TableRef targetTableRef_;
  private TableRef sourceTableRef_;
  private final Expr on_;
  private FeIcebergTable icebergTable_;
  private IcebergPositionDeleteTable icebergPositionalDeleteTable_;
  private SelectStmt queryStmt_;
  private int deleteTableId_;
  private final FeTable table_;
  private TupleDescriptor mergeActionTuple_;
  private TupleId targetTupleId_;

  private List<Expr> targetExpressions_;
  private List<Expr> targetRowMetaExpressions_;
  private List<Expr> targetPartitionMetaExpressions_;
  private List<Expr> targetPartitionExpressions_;
  private List<Expr> shuffleExprs_;
  private MergeSorting targetSorting_;
  private Expr rowPresentExpression_;
  private Expr mergeActionExpression_;

  public IcebergMergeImpl(MergeStmt stmt, TableRef target, TableRef source, Expr on) {
    mergeStmt_ = stmt;
    targetTableRef_ = target;
    sourceTableRef_ = source;
    on_ = on;
    table_ = targetTableRef_.getTable();
    targetPartitionExpressions_ = Lists.newArrayList();
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (queryStmt_ != null ) {
      Preconditions.checkState(queryStmt_.fromClause_.size() == 2);
      sourceTableRef_ = analyzer.resolveTableRef(sourceTableRef_);
      targetTableRef_ = analyzer.resolveTableRef(targetTableRef_);
      queryStmt_.fromClause_.set(0, targetTableRef_);
      queryStmt_.fromClause_.set(1, sourceTableRef_);
    }
    setJoinParams();
    targetTableRef_.analyze(analyzer);

    if (targetTableRef_.isTableMaskingView()) {
      targetTableRef_= ((InlineViewRef)targetTableRef_).getUnMaskedTableRef();
    }

    FeTable table = targetTableRef_.getTable();
    Preconditions.checkState(table instanceof FeIcebergTable);
    icebergTable_ = (FeIcebergTable) table;
    IcebergUtil.validateIcebergTableForInsert(icebergTable_);
    int formatVersion = icebergTable_.getFormatVersion();
    if (formatVersion > IcebergUtil.FORMAT_VERSION_3) {
      throw new AnalysisException(String.format(
          "Impala does not support MERGE statements on Iceberg tables with format " +
              "version %d", formatVersion));
    }
    if (formatVersion >= IcebergUtil.FORMAT_VERSION_3
        && !mergeStmt_.hasOnlyInsertCases()) {
      int positionDeletes =
          icebergTable_.getContentFileStore().getPositionDeleteFiles().size();
      if (positionDeletes > 0) {
        throw new AnalysisException(String.format(
            "MERGE is not allowed on Iceberg format version 3 table '%s' as it "
                + "has existing version 2 position delete file(s): "
                + "%d position delete file(s). Run 'OPTIMIZE TABLE %s' to rewrite the "
                + "files and remove deletion files before attempting MERGE.",
            icebergTable_.getFullName(), positionDeletes,
            icebergTable_.getFullName()));
      }
    }
    String modifyWriteMode = icebergTable_.getIcebergApiTable().properties()
        .get(TableProperties.MERGE_MODE);
    if (modifyWriteMode != null && !Objects.equals(modifyWriteMode, "merge-on-read")
        && !BackendConfig.INSTANCE.icebergAlwaysAllowMergeOnReadOperations()) {
      throw new AnalysisException(String.format(
          "Unsupported '%s': '%s' for Iceberg table: %s",
          TableProperties.MERGE_MODE, modifyWriteMode, icebergTable_.getFullName()));
    }
    for (Column column : icebergTable_.getColumnsInHiveOrder()) {
      Path slotPath =
          new Path(targetTableRef_.desc_, Collections.singletonList(column.getName()));
      slotPath.resolve();
      analyzer.registerSlotRef(slotPath);
    }

    sourceTableRef_.analyze(analyzer);

    IcebergMergeQueryGenerator.MergeQuery mergeQuery =
        IcebergMergeQueryGenerator.generate(
            targetTableRef_, sourceTableRef_, icebergTable_, mergeStmt_, analyzer);
    queryStmt_ = mergeQuery.queryStmt;
    rowPresentExpression_ = mergeQuery.rowPresentExpression;
    targetExpressions_ = mergeQuery.targetExpressions;
    targetRowMetaExpressions_ = mergeQuery.targetRowMetaExpressions;
    targetPartitionMetaExpressions_ = mergeQuery.targetPartitionMetaExpressions;
    queryStmt_.analyze(analyzer);

    targetTupleId_ = targetTableRef_.getId();

    addMergeActionTuple(analyzer);

    if (!mergeStmt_.hasOnlyInsertCases()) {
      icebergPositionalDeleteTable_ = new IcebergPositionDeleteTable(icebergTable_);
      deleteTableId_ = analyzer.getDescTbl().addTargetTable(
          icebergPositionalDeleteTable_);
    }

    IcebergUtil.populatePartitionExprs(analyzer, null, table_.getColumnsInHiveOrder(),
        getResultExprs(), icebergTable_, targetPartitionExpressions_, null);

    analyzer.registerPrivReq(
        builder -> builder.onTable(icebergTable_).allOf(Privilege.ALL).build());

    shuffleExprs_ = buildShuffleExprs(analyzer);
    targetSorting_ = getSorting();
    analyzer.getDescTbl().setTargetTable(icebergTable_);
  }

  /**
   * Creating a join between target table and the source expression. If the merge
   * statement contains only matched cases, the join can be an inner join, if
   * both matched and not matched cases are present, it must be a
   * full outer join.
   */
  private void setJoinParams() {
    if (mergeStmt_.hasOnlyMatchedCases()) {
      sourceTableRef_.setJoinOp(JoinOperator.INNER_JOIN);
    } else {
      sourceTableRef_.setJoinOp(JoinOperator.FULL_OUTER_JOIN);
    }
    sourceTableRef_.setOnClause(on_);
    sourceTableRef_.setLeftTblRef(targetTableRef_);
  }

  /**
   * Creates a new tuple descriptor that will hold a TINYINT slot which will
   * store the result of whether the resulted row after evaluation should be
   * deleted, inserted, or updated. This tuple is null until the rows pass
   * the IcebergMergeNode.
   * @param analyzer analyzer of the merge statement
   */
  private void addMergeActionTuple(Analyzer analyzer) {
    mergeActionTuple_ =
        analyzer.getDescTbl().createTupleDescriptor(MERGE_ACTION_TUPLE_NAME);

    SlotDescriptor sd = analyzer.addSlotDescriptor(mergeActionTuple_);
    sd.setType(Type.TINYINT);
    sd.setIsMaterialized(true);
    sd.setIsNullable(false);

    mergeActionExpression_ = new SlotRef(sd);
    sd.setSourceExpr(mergeActionExpression_);
  }

  @Override
  public void substituteResultExprs(ExprSubstitutionMap smap, Analyzer analyzer) {
    targetExpressions_ = Expr.substituteList(targetExpressions_, smap, analyzer, true);
    targetPartitionMetaExpressions_ =
        Expr.substituteList(targetPartitionMetaExpressions_, smap, analyzer, true);
    targetRowMetaExpressions_ =
        Expr.substituteList(targetRowMetaExpressions_, smap, analyzer, true);
    mergeActionExpression_ = mergeActionExpression_.substitute(smap, analyzer, true);
    targetPartitionExpressions_ =
        Expr.substituteList(targetPartitionExpressions_, smap, analyzer, true);
    shuffleExprs_ = Expr.substituteList(shuffleExprs_, smap, analyzer, true);
    for (MergeCase mergeCase : mergeStmt_.getCases()) {
      mergeCase.substituteResultExprs(smap, analyzer);
    }
  }

  @Override
  public List<Expr> getResultExprs() {
    List<Expr> result = Lists.newArrayList(targetExpressions_);
    result.addAll(targetRowMetaExpressions_);
    result.addAll(targetPartitionMetaExpressions_);
    result.add(mergeActionExpression_);
    return result;
  }

  @Override
  public List<Expr> getPartitionKeyExprs() { return targetPartitionExpressions_; }

  /**
   * Returns expressions used to shuffle rows across fragment instances.
   *
   * Partitioned tables:
   *   V3 — [COALESCE(PARTITION__SPEC__ID, murmur_hash(e1) ^ murmur_hash(e2) ^ ...),
   *          ICEBERG__PARTITION__SERIALIZED]
   *         For matched rows PARTITION__SPEC__ID is non-NULL and routes by the actual
   *         spec+partition virtual columns so each sink instance sees at most one DV per
   *         data file. For INSERT (WHEN NOT MATCHED) rows the target tuple is absent
   *         from the full outer join so PARTITION__SPEC__ID is NULL; the fallback
   *         XOR of per-expression murmur_hash values spreads inserts across instances
   *         by logical partition.
   *   V2 — computed partition transform expressions (e.g. year(ts), bucket(s, 5)).
   *
   * Unpartitioned tables:
   *   V3 — input__file__name, so all deletes for the same data file land in the same
   *         sink instance, producing exactly one DV. Skipped for INSERT-only statements
   *         since no deletion vectors are produced.
   *         WHEN NOT MATCHED rows have a NULL file name and hash to the same bucket,
   *         which is acceptable for correctness.
   *   V2 — no shuffle needed.
   */
  @Override
  public List<Expr> getShuffleExprs() { return shuffleExprs_; }

  private List<Expr> buildShuffleExprs(Analyzer analyzer) throws AnalysisException {
    if (icebergTable_.isPartitioned()) {
      if (icebergTable_.getFormatVersion() >= IcebergUtil.FORMAT_VERSION_3) {
        return buildV3PartitionedShuffleExprs(analyzer);
      }
      return targetPartitionExpressions_;
    }
    if (icebergTable_.getFormatVersion() >= IcebergUtil.FORMAT_VERSION_3
        && !mergeStmt_.hasOnlyInsertCases()) {
      return Collections.singletonList(DmlStatementBase.createSlotRef(
          analyzer, targetTableRef_.getUniqueAlias(), "INPUT__FILE__NAME"));
    }
    return Collections.emptyList();
  }

  /**
   * Builds shuffle expressions for V3 partitioned tables:
   *   [COALESCE(PARTITION__SPEC__ID, murmur_hash(e1) ^ murmur_hash(e2) ^ ...),
   *    ICEBERG__PARTITION__SERIALIZED]
   * murmur_hash takes a single argument, so N partition transform expressions are
   * hashed individually and XOR'd together into one BIGINT.
   */
  private List<Expr> buildV3PartitionedShuffleExprs(Analyzer analyzer)
      throws AnalysisException {
    SlotRef specId = DmlStatementBase.createSlotRef(
        analyzer, targetTableRef_.getUniqueAlias(),
        VirtualColumn.PARTITION_SPEC_ID.getName());
    Expr xorExpr = null;
    for (Expr partExpr : Expr.cloneList(targetPartitionExpressions_)) {
      FunctionCallExpr hashExpr = new FunctionCallExpr(
          "murmur_hash", Lists.newArrayList(partExpr));
      hashExpr.analyze(analyzer);
      xorExpr = (xorExpr == null) ? hashExpr
          : new ArithmeticExpr(ArithmeticExpr.Operator.BITXOR, xorExpr, hashExpr);
      if (xorExpr instanceof ArithmeticExpr) xorExpr.analyze(analyzer);
    }
    FunctionCallExpr coalesceExpr = new FunctionCallExpr(
        "coalesce", Lists.newArrayList(specId, xorExpr));
    coalesceExpr.analyze(analyzer);
    SlotRef partitionSerialized = DmlStatementBase.createSlotRef(
        analyzer, targetTableRef_.getUniqueAlias(),
        VirtualColumn.ICEBERG_PARTITION_SERIALIZED.getName());
    return Lists.newArrayList(coalesceExpr, partitionSerialized);
  }

  @Override
  public List<Expr> getSortExprs() { return targetSorting_.sortingExpressions_; }

  @Override
  public TSortingOrder getSortingOrder() { return targetSorting_.order_; }

  @Override
  public QueryStmt getQueryStmt() { return queryStmt_; }

  @Override
  public PlanNode getPlanNode(PlannerContext ctx, PlanNode child, Analyzer analyzer)
      throws ImpalaException {
    // Passing copies to the IcebergMergeNode to handle cases where the row descriptor of
    // the sink and the merge node differs.
    List<MergeCase> copyOfCases =
        mergeStmt_.getCases().stream().map(MergeCase::clone).collect(Collectors.toList());
    List<Expr> rowMetaExprs = Expr.cloneList(targetRowMetaExpressions_);
    List<Expr> partitionMetaExprs = Expr.cloneList(targetPartitionMetaExpressions_);
    IcebergMergeNode mergeNode = new IcebergMergeNode(ctx.getNextNodeId(), child,
        copyOfCases, rowPresentExpression_.clone(), rowMetaExprs, partitionMetaExprs,
        mergeActionTuple_, targetTupleId_);
    mergeNode.init(analyzer);
    return mergeNode;
  }

  @Override
  public void reset() {
    queryStmt_.reset();
    // TableRef resets are replacing references instead of resetting the object state
    targetTableRef_ = queryStmt_.fromClause_.get(0);
    sourceTableRef_ = queryStmt_.fromClause_.get(1);
    targetPartitionExpressions_.clear();
  }

  @Override
  public DataSink createDataSink() {
    if (mergeStmt_.hasOnlyDeleteCases()) { return createDeleteSink(); }
    if (mergeStmt_.hasOnlyInsertCases()) { return createInsertSink(); }

    TableSink insertSink = createInsertSink();
    TableSink deleteSink = createDeleteSink();

    return new IcebergMergeSink(
        insertSink, deleteSink, Collections.singletonList(mergeActionExpression_));
  }

  public TableSink createDeleteSink() {
    List<Expr> deletePartitionKeys = Collections.emptyList();
    if (icebergTable_.isPartitioned()) {
      deletePartitionKeys = targetPartitionMetaExpressions_;
    }
    return new IcebergBufferedDeleteSink(icebergPositionalDeleteTable_,
        deletePartitionKeys, targetRowMetaExpressions_, deleteTableId_);
  }

  public TableSink createInsertSink() {
    return TableSink.create(icebergTable_, TableSink.Op.INSERT,
        targetPartitionExpressions_, targetExpressions_, Collections.emptyList(), false,
        true, targetSorting_.sortingColumnsAndOrder(), -1, null,
        mergeStmt_.maxTableSinks_);
  }

  private MergeSorting getSorting() throws AnalysisException {
    Pair<List<Integer>, TSortingOrder> sortProperties =
        AlterTableSetTblProperties.analyzeSortColumns(
            table_, table_.getMetaStoreTable().getParameters());
    // Assign sortExprs based on sortColumnPositions.
    List<Integer> sortColumnPositions = sortProperties.first;
    List<Expr> sortExpressions =
        sortColumnPositions.stream().map(getResultExprs()::get)
            .collect(Collectors.toList());
    return new MergeSorting(sortColumnPositions, sortExpressions,
        sortProperties.second);
  }

  protected static class MergeSorting {
    private final List<Integer> sortingColumnPositions_;
    private final List<Expr> sortingExpressions_;
    private final TSortingOrder order_;

    public MergeSorting(List<Integer> sortingColumnPositions,
        List<Expr> sortingExpressions, TSortingOrder order) {
      sortingColumnPositions_ = sortingColumnPositions;
      sortingExpressions_ = sortingExpressions;
      order_ = order;
    }

    public Pair<List<Integer>, TSortingOrder> sortingColumnsAndOrder() {
      return Pair.create(sortingColumnPositions_, order_);
    }
  }
}
