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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.VirtualColumn;

/**
 * Generates the SELECT statement driving a MERGE operation on an Iceberg table.
 *
 * <p>The assembled SELECT statement has the form:
 * <pre>
 * SELECT /&#42; +straight_join &#42;/
 *   CAST(TupleIsNull(target) + TupleIsNull(source) * 2 AS TINYINT) row_present,
 *   target.*,
 *   target.input__file__name,
 *   target.file__position,
 *   [target.iceberg__data__sequence__number,]   -- only when equality delete files exist
 *   [target.__partition__spec__id,              -- only for partitioned tables
 *    target.iceberg__partition__serialized,]
 *   source.*
 * FROM target
 * [FULL OUTER | INNER] JOIN source ON &lt;on-clause&gt;
 * </pre>
 */
public class IcebergMergeQueryGenerator {

  private static final String ROW_PRESENT = "row_present";

  /**
   * Generates the {@link MergeQuery} for the given target/source table refs and
   * Iceberg table. Orchestrates: row-present → expression target expressions →
   * row-meta expressions → partition-meta expressions → SELECT assembly.
   */
  public static MergeQuery generate(TableRef targetTableRef, TableRef sourceTableRef,
      FeIcebergTable icebergTable) {
    CastExpr rowPresentExpression =
        buildRowPresentExpression(targetTableRef, sourceTableRef);
    List<Expr> targetExpressions =
        buildTargetExpressions(targetTableRef);
    List<Expr> rowMetaExpressions =
        buildRowMetaExpressions(targetTableRef, icebergTable);
    List<Expr> partitionMetaExpressions =
        buildPartitionMetaExpressions(targetTableRef, icebergTable);

    List<SelectListItem> selectListItems = Lists.newArrayList();
    SelectList selectList = new SelectList(selectListItems);
    // Straight join hint is required to fix the join sides.
    selectList.setPlanHints(Collections.singletonList(new PlanHint("straight_join")));

    selectListItems.add(new SelectListItem(rowPresentExpression, ROW_PRESENT));
    targetExpressions.stream()
        .map(expr -> new SelectListItem(expr, null))
        .forEach(selectListItems::add);
    rowMetaExpressions.stream()
        .map(expr -> new SelectListItem(expr, null))
        .forEach(selectListItems::add);
    partitionMetaExpressions.stream()
        .map(expr -> new SelectListItem(expr, null))
        .forEach(selectListItems::add);
    selectListItems.add(SelectListItem.createStarItem(
        Collections.singletonList(sourceTableRef.getUniqueAlias())));

    FromClause fromClause =
        new FromClause(Lists.newArrayList(targetTableRef, sourceTableRef));
    SelectStmt queryStmt =
        new SelectStmt(selectList, fromClause, null, null, null, null, null);

    return new MergeQuery(queryStmt, rowPresentExpression, targetExpressions,
        rowMetaExpressions, partitionMetaExpressions);
  }

  /**
   * Builds one expression per column in the target table.
   */
  protected static List<Expr> buildTargetExpressions(
      TableRef targetTableRef) {
    return targetTableRef.getTable().getColumns().stream()
        .map(column -> new SlotRef(
            ImmutableList.of(targetTableRef.getUniqueAlias(), column.getName())))
        .collect(Collectors.toList());
  }

  /**
   * Builds the {@code row_present} expression:
   * {@code CAST(TupleIsNull(target) + TupleIsNull(source) * 2 AS TINYINT)}.
   * First bit signals target presence, second bit signals source presence.
   */
  protected static CastExpr buildRowPresentExpression(
      TableRef targetTableRef, TableRef sourceTableRef) {
    TupleIsNullPredicate targetPresent =
        new TupleIsNullPredicate(targetTableRef.getMaterializedTupleIds());
    TupleIsNullPredicate sourcePresent =
        new TupleIsNullPredicate(sourceTableRef.getMaterializedTupleIds());
    CastExpr targetPresentAsTinyInt =
        new CastExpr(new TypeDef(Type.TINYINT), targetPresent);
    CastExpr sourcePresentAsTinyInt =
        new CastExpr(new TypeDef(Type.TINYINT), sourcePresent);
    ArithmeticExpr sourcePresentShifted =
        new ArithmeticExpr(ArithmeticExpr.Operator.MULTIPLY, sourcePresentAsTinyInt,
            NumericLiteral.create(2));
    return new CastExpr(new TypeDef(Type.TINYINT),
        new ArithmeticExpr(
            ArithmeticExpr.Operator.ADD, targetPresentAsTinyInt, sourcePresentShifted));
  }

  /**
   * Builds virtual column expressions for writing delete files:
   * {@code input__file__name}, {@code file__position}, and conditionally
   * {@code iceberg__data__sequence__number} when equality delete files are present.
   */
  protected static List<Expr> buildRowMetaExpressions(
      TableRef targetTableRef, FeIcebergTable icebergTable) {
    List<Expr> rowMetaExpressions = Lists.newArrayList();

    // Required for duplicate checks and for UPDATE and DELETE clauses.
    rowMetaExpressions.add(new SlotRef(ImmutableList.of(
        targetTableRef.getUniqueAlias(), VirtualColumn.INPUT_FILE_NAME.getName())));
    rowMetaExpressions.add(new SlotRef(ImmutableList.of(
        targetTableRef.getUniqueAlias(), VirtualColumn.FILE_POSITION.getName())));

    boolean hasEqualityDeleteFiles =
        !icebergTable.getContentFileStore().getEqualityDeleteFiles().isEmpty();
    if (hasEqualityDeleteFiles) {
      rowMetaExpressions.add(new SlotRef(ImmutableList.of(
          targetTableRef.getUniqueAlias(),
          VirtualColumn.ICEBERG_DATA_SEQUENCE_NUMBER.getName())));
    }

    return rowMetaExpressions;
  }

  /**
   * Builds virtual column expressions for partition-aware writes:
   * {@code __partition__spec__id} and {@code iceberg__partition__serialized}.
   * Returns empty list for unpartitioned tables.
   */
  protected static List<Expr> buildPartitionMetaExpressions(
      TableRef targetTableRef, FeIcebergTable icebergTable) {
    if (!icebergTable.isPartitioned()) return Collections.emptyList();

    return ImmutableList.of(
        new SlotRef(ImmutableList.of(
            targetTableRef.getUniqueAlias(),
            VirtualColumn.PARTITION_SPEC_ID.getName())),
        new SlotRef(ImmutableList.of(
            targetTableRef.getUniqueAlias(),
            VirtualColumn.ICEBERG_PARTITION_SERIALIZED.getName())));
  }

  /**
   * Holds the result of {@link #generate}: the SELECT statement and every expression
   * list that {@link IcebergMergeImpl} stores back into its own fields.
   */
  public static class MergeQuery {
    public final SelectStmt queryStmt;
    public final Expr rowPresentExpression;
    public final List<Expr> targetExpressions;
    public final List<Expr> targetRowMetaExpressions;
    public final List<Expr> targetPartitionMetaExpressions;

    public MergeQuery(SelectStmt queryStmt, Expr rowPresentExpression,
        List<Expr> targetExpressions, List<Expr> targetRowMetaExpressions,
        List<Expr> targetPartitionMetaExpressions) {
      this.queryStmt = queryStmt;
      this.rowPresentExpression = rowPresentExpression;
      this.targetExpressions = targetExpressions;
      this.targetRowMetaExpressions = targetRowMetaExpressions;
      this.targetPartitionMetaExpressions = targetPartitionMetaExpressions;
    }
  }
}
