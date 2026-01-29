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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.StructField;
import org.apache.impala.catalog.StructType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * An UnpivotTableRef is created when there is an UNPIVOT clause in the FROM clause of a
 * query. An UNPIVOT clause following a TableRef can be used to merge multiple columns of
 * the table into one.
 *
 * As an example, for the following query:
 *
 * SELECT * FROM s UNPIVOT (a FOR b IN (x AS 'x', y AS 'y')) AS t;
 *
 * the UNPIVOT clause merges the "unpivot columns" x and y in the "source table" s by
 * putting all the data of both columns into the "data column" a in the result table t.
 * To help identify the source column of the value in the data column for each row, the
 * value of the expression corresponding to the souce column specified in the AS clause
 * will be written to the slot for the "header column" b in the result table t.
 *
 * The SingleNodePlanner will generate an UnpivotNode for each UnpivotTableRef, with the
 * plan for its source table as the subtree.
 */
public class UnpivotTableRef extends TableRef {
  private final String dataColumn_;
  private final String headerColumn_;
  private final SelectList unpivotColumnList_;

  private final boolean resolvedSourceTableRef_;
  private TableRef sourceTableRef_;
  private SlotDescriptor dataSlotDesc_;
  private SlotDescriptor headerSlotDesc_;

  // Maps the unpivot column names to the LiteralExprs to fill the header column.
  private final Map<String, LiteralExpr> unpivotColumnNameMap_ = new HashMap<>();

  // Maps the SlotRefs for the unpivot columns to the LiteralExprs to fill the
  // header column.
  private final Map<Expr, LiteralExpr> unpivotExprMap_ = new HashMap<>();

  // Maps the id of each slot in the TupleDescriptor of this table to the SlotRef on
  // the source table except for the slots for the header or the data column.
  private Map<SlotId, Expr> sourceExprMap_ = new HashMap<>();
  private Type headerColumnType_;

  private final ExprSubstitutionMap smapToSourceTableOutput_ = new ExprSubstitutionMap();

  private ExprSubstitutionMap baseTableSmap_ = null;

  UnpivotTableRef(List<String> path, String alias,
      String dataColumn, String headerColumn, SelectList unpivotColumnList) {
    super(path, alias);
    resolvedSourceTableRef_ = false;
    headerColumn_ = headerColumn;
    dataColumn_ = dataColumn;
    unpivotColumnList_ = unpivotColumnList;
  }

  UnpivotTableRef(TableRef sourceTableRef, String alias,
      String dataColumn, String headerColumn, SelectList unpivotColumnList) {
    super(sourceTableRef.getPath(), alias);
    sourceTableRef_ = sourceTableRef;
    resolvedSourceTableRef_ = true;
    headerColumn_ = headerColumn;
    dataColumn_ = dataColumn;
    unpivotColumnList_ = unpivotColumnList;
  }

  // Creates an unanalyzed clone.
  // We cannot create a full clone since a SlotDescriptor cannot be cloned without an
  // Analyzer.
  @Override
  protected UnpivotTableRef clone() {
    if (!resolvedSourceTableRef_) {
      return new UnpivotTableRef(
          new ArrayList<>(rawPath_),
          getUniqueAlias(),
          dataColumn_,
          headerColumn_,
          unpivotColumnList_.clone()
      );
    }
    TableRef sourceTableRefClone = sourceTableRef_.clone();
    sourceTableRefClone.reset();
    return new UnpivotTableRef(sourceTableRefClone,
        getUniqueAlias(), dataColumn_, headerColumn_, unpivotColumnList_.clone());
  }

  @Override
  public TupleDescriptor createTupleDescriptor(Analyzer analyzer)
      throws AnalysisException {
    Preconditions.checkNotNull(sourceTableRef_);

    TupleDescriptor result = analyzer.getDescTbl().createTupleDescriptor(
        getClass().getSimpleName() + " " + getUniqueAlias());
    List<StructField> fields = new ArrayList<>();
    Type dataColumnType = null;
    for (StructField field : sourceTableRef_.getDesc().getType().getFields()) {
      if (!unpivotColumnNameMap_.containsKey(field.getName())) {
        fields.add(new StructField(field.getName(), field.getType()));
        continue;
      }
      // The unpivot columns are not in the result table.
      if (dataColumnType == null) {
        dataColumnType = field.getType();
      } else if (!dataColumnType.equals(field.getType())) {
         // TODO(IMPALA-14927): Support different but compatible types.
        throw new AnalysisException(
            "Columns in the UNPIVOT clause should have the same type");
      }
    }
    if (dataColumnType == null) {
      throw new AnalysisException(String.format(
        "Table '%s' does not contain the columns for UNPIVOT",
        Joiner.on(".").join(getPath())));
    }
    fields.add(new StructField(dataColumn_, dataColumnType));
    fields.add(new StructField(
        headerColumn_, Preconditions.checkNotNull(headerColumnType_)));
    result.setType(new StructType(fields));
    return result;
  }

  private static String generateUniqueAlias() {
    return "_" + UUID.randomUUID().toString().replace("-", "_");
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed_) return;
    isAnalyzed_ = true;

    if (!resolvedSourceTableRef_) {
      // TODO(IMPALA-14928): Support multiple UNPIVOT clauses following the same path.
      sourceTableRef_ = analyzer.resolveTableRef(new TableRef(rawPath_, null));
    }
    sourceTableRef_.analyze(analyzer);
    for (SelectListItem item : unpivotColumnList_.getItems()) {
      Preconditions.checkState(item.getExpr() instanceof LiteralExpr);
      // Use clone() to keep unpivotColumnList_ read-only.
      Expr e = item.getExpr().clone();
      e.analyze(analyzer);
      LiteralExpr prevExpr =
          unpivotColumnNameMap_.put(item.getAlias(), (LiteralExpr)e);
      if (prevExpr != null) {
        throw new AnalysisException(String.format(
            "Duplicate column name '%s' in the UNPIVOT clause", item.getAlias()));
      }
      if (headerColumnType_ == null) {
        headerColumnType_ = e.getType();
      } else if (!headerColumnType_.equals(e.getType())) {
        throw new AnalysisException(
          "Expressions for the UNPIVOT header column should have the same type");
      }
    }
    Preconditions.checkState(getNumUnpivotColumns() > 0);
    // The source table is not visible outside the UNPIVOT clause when resolving a Path.
    sourceTableRef_.setHidden(true);
    desc_ = analyzer.registerTableRef(this);
    analyzeHints(analyzer);
    analyzeJoin(analyzer);
  }

  public TableRef getSourceTableRef() {
    return Preconditions.checkNotNull(sourceTableRef_);
  }

  private SlotRef registerSourceTableSlotRef(Analyzer analyzer, List<String> rawPath)
      throws AnalysisException {
    Path sourceTableSlotPath = new Path(sourceTableRef_.getDesc(), rawPath);
    if (!sourceTableSlotPath.resolve()) {
      // The error message is the same as in Analyzer.resolvePaths().
      throw new AnalysisException(String.format(
          "Could not resolve column/field reference: '%s'",
          Joiner.on(".").join(rawPath)));
    }
    SlotRef sourceSlotRef = new SlotRef(
        analyzer.registerSlotRef(sourceTableSlotPath, false));
    sourceSlotRef.analyze(analyzer);
    sourceTableRef_.registerColumn(new Column(
        String.join(".", rawPath), sourceSlotRef.getType(), -1));
    return sourceSlotRef;
  }

  // Registers the corresponding SlotRef against the source table.
  @Override
  void notifySlotRefRegistered(Analyzer analyzer, SlotDescriptor slot)
      throws AnalysisException {
    String columnName = slot.getPath().getRawPath().get(0);
    if (columnName.equals(dataColumn_) || columnName.equals(headerColumn_)) {
      Preconditions.checkState(slot.getPath().getRawPath().size() == 1);
      if (unpivotExprMap_.isEmpty()) {
        // Registers SlotRefs for the names in the UNPIVOT clause against the source
        // table.
        for (Map.Entry<String, LiteralExpr> entry : unpivotColumnNameMap_.entrySet()) {
          String sourceTableColumnName = entry.getKey();
          LiteralExpr headerExpr = entry.getValue();
          SlotRef sourceTableSlotRef = registerSourceTableSlotRef(
              analyzer, Lists.newArrayList(sourceTableColumnName));
          unpivotExprMap_.put(sourceTableSlotRef, headerExpr);
        }
      }
      if (columnName.equals(headerColumn_)) {
        headerSlotDesc_ = slot;
      } else {
        dataSlotDesc_ = slot;
        for (Expr sourceTableSlotRef : unpivotExprMap_.keySet()) {
          dataSlotDesc_.addSourceExpr(sourceTableSlotRef);
        }
      }
    } else {
      Path sourceTableSlotPath = new Path(
          sourceTableRef_.getDesc(), slot.getPath().getRawPath());
      SlotRef sourceSlotRef = registerSourceTableSlotRef(
          analyzer, slot.getPath().getRawPath());
      sourceExprMap_.put(slot.getId(), sourceSlotRef);
      smapToSourceTableOutput_.put(new SlotRef(slot), sourceSlotRef);
    }
  }

  @Override
  public TableRef applyTableMask(Analyzer analyzer) throws AnalysisException {
    TableRef sourceTableRefClone = sourceTableRef_.clone();
    sourceTableRefClone.setHidden(false);
    TableRef maskedSourceTableRef = sourceTableRefClone.applyTableMask(analyzer);
    UnpivotTableRef maskedUnpivotTableRef = new UnpivotTableRef(
        maskedSourceTableRef,
        generateUniqueAlias(),
        dataColumn_, headerColumn_, unpivotColumnList_);
    SelectList selectList = new SelectList(
        Lists.newArrayList(SelectListItem.createStarItem(null)));
    FromClause fromClause = new FromClause(Lists.newArrayList(maskedUnpivotTableRef));
    SelectStmt tableMaskStmt = new SelectStmt(selectList, fromClause, null,
        null, null, null, null);
    InlineViewRef result = new InlineViewRef(
        null, tableMaskStmt, (TableSampleClause)null);
    maskedUnpivotTableRef.migratePropertiesTo(result);
    result.setIsTableMaskingView();
    return result;
  }

  private ExprSubstitutionMap getBaseTableSmap(Analyzer analyzer) {
    if (baseTableSmap_ == null) {
      baseTableSmap_ = smapToSourceTableOutput_;
      if (sourceTableRef_ instanceof InlineViewRef) {
        InlineViewRef inlineViewRef = (InlineViewRef)sourceTableRef_;
        baseTableSmap_ = ExprSubstitutionMap.compose(
            smapToSourceTableOutput_, inlineViewRef.getBaseTblSmap(), analyzer);
      }
    }
    return baseTableSmap_;
  }

  @Override
  void notifySlotsMaterialized(Analyzer analyzer, Expr expr) {
    // Exprs in which the referenced slots needs to be materialized.
    List<Expr> exprs = Lists.newArrayList(expr);
    if (dataSlotDesc_ != null) {
      // Checks whether the slot for the data column is referenced in 'expr'.
      List<SlotId> slotIds = new ArrayList<>();
      Preconditions.checkState(expr.isAnalyzed());
      expr.getIds(null, slotIds);
      for (SlotId slotId : slotIds) {
        if (slotId.equals(dataSlotDesc_.getId())) {
          exprs.addAll(unpivotExprMap_.keySet());
          break;
        }
      }
    }
    analyzer.materializeSlots(Expr.substituteList(
        exprs, getBaseTableSmap(analyzer), analyzer, false));
  }

  // Pushes the conjuncts down by re-binding them to the source table tuple.
  // This method needs to be called after registering all the conjuncts and before
  // creating any plan node.
  public List<Expr> pushdownConjuncts(Analyzer analyzer) throws AnalysisException {
    Preconditions.checkState(isAnalyzed_);
    List<Expr> conjuncts = collectConjuncts(analyzer);
    List<Expr> remainingConjuncts = new ArrayList<>();
    for (Expr conjunct : conjuncts) {
      Expr substConjunct = conjunct.substitute(
          getBaseTableSmap(analyzer), analyzer, false);
      if (substConjunct.equals(conjunct)) {
        // The conjunct cannot be pushed down.
        remainingConjuncts.add(conjunct);
      } else {
        analyzer.registerConjuncts(substConjunct, true);
        analyzer.materializeSlots(substConjunct);
      }
    }
    return remainingConjuncts;
  }


  public int getNumUnpivotColumns() {
    return unpivotColumnNameMap_.size();
  }

  public Map<Expr, LiteralExpr> getUnpivotExprMap() {
    Preconditions.checkState(isAnalyzed_);
    return unpivotExprMap_;
  }

  public Map<SlotId, Expr> getSourceExprMap() {
    Preconditions.checkState(isAnalyzed_);
    return sourceExprMap_;
  }

  public SlotDescriptor getDataSlotDescriptor() {
    Preconditions.checkState(isAnalyzed_);
    return dataSlotDesc_;
  }

  public SlotDescriptor getHeaderSlotDescriptor() {
    Preconditions.checkState(isAnalyzed_);
    return headerSlotDesc_;
  }

  @Override
  public String tableRefToSql(ToSqlOptions options) {
    List<String> unpivotColumnList = new ArrayList<>();
    for (SelectListItem item : unpivotColumnList_.getItems()) {
      unpivotColumnList.add(item.toSql());
    }
    return String.format("%s UNPIVOT (%s FOR %s IN (%s)) %s",
        resolvedSourceTableRef_ ? sourceTableRef_.toSql() : String.join(".", getPath()),
        dataColumn_,
        headerColumn_,
        String.join(",", unpivotColumnList),
        getUniqueAlias());
  }
}
