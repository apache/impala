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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.impala.analysis.BinaryPredicate.Operator;
import org.apache.impala.analysis.GroupByClause.GroupingSetsType;
import org.apache.impala.catalog.AggregateFunction;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.StructField;
import org.apache.impala.catalog.StructType;
import org.apache.impala.common.AnalysisException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * A PivotTableRef is created when there is a PIVOT clause in the FROM clause of a query.
 * A PIVOT clause following a TableRef can be used to split an aggregated column into
 * multiple columns.
 *
 * As an example, for the following query:
 *
 * SELECT * FROM s PIVOT ( count(a) for b in (1 AS v1, 2 AS v2) ) AS t;
 *
 * The PIVOT clause splits the aggregated column count(a) into two columns:
 * - column v1 contains the aggregation results when b = 1, and
 * - column v2 contains the aggregation results when b = 2.
 *
 * The result table t contains
 * - columns v1 and v2 created from the split, and
 * - all the columns in the source table s except column b.
 *
 * Column b in this example is called the "header column" since its values are used for
 * the header of the result table to identify the columns created from the split.
 *
 * In the analysis phase, a `PivotTableRef` will be rewritten to an `InlineViewRef` to a
 * nested subquery with the `toInlineViewRef()` method.
 */
public class PivotTableRef extends TableRef {
  private final SelectList aggList_; // read-only
  private final String headerColumn_; // read-only
  private final SelectList headerValueList_; // read-only

  private final boolean resolvedSourceTableRef_;
  private TableRef sourceTableRef_ = null;

  // Use LinkedHashMaps here to keep the order of the exprs stable since it determines
  // the order of slots in the output tuple of the aggregation.

  // Maps each alias to the Agg Expr in the PIVOT clause.
  private final Map<String, FunctionCallExpr> analyzedAggList_ = new LinkedHashMap<>();

  // Maps each alias to the Literal Expr in the PIVOT clause.
  private final Map<String, LiteralExpr> headerValueMap_ = new LinkedHashMap<>();

  // Maps each column name to the SlotDescriptor in the TupleDescriptor of the source
  // table of the PIVOT clause.
  private final Map<String, SlotDescriptor> groupingSlotMap_ = new LinkedHashMap<>();

  PivotTableRef(List<String> path, String alias,
      SelectList aggList, String headerColumn, SelectList headerValueList) {
    super(path, alias);
    resolvedSourceTableRef_ = false;
    aggList_ = aggList;
    headerColumn_ = headerColumn;
    headerValueList_ = headerValueList;
  }

  PivotTableRef(TableRef sourceTableRef, String alias,
      SelectList aggList, String headerColumn, SelectList headerValueList) {
    super(sourceTableRef.getPath(), alias);
    resolvedSourceTableRef_ = true;
    sourceTableRef_ = sourceTableRef;
    aggList_ = aggList;
    headerColumn_ = headerColumn;
    headerValueList_ = headerValueList;
  }

  // Creates an unanalyzed clone.
  // We cannot create a full clone since a SlotDescriptor cannot be cloned without an
  // Analyzer.
  @Override
  protected PivotTableRef clone() {
    if (!resolvedSourceTableRef_) {
      return new PivotTableRef(
          new ArrayList<>(rawPath_),
          getUniqueAlias(),
          aggList_.clone(),
          headerColumn_,
          headerValueList_.clone()
      );
    }
    TableRef sourceTableRefClone = sourceTableRef_.clone();
    sourceTableRefClone.reset();
    return new PivotTableRef(
        sourceTableRefClone,
        getUniqueAlias(),
        aggList_.clone(),
        headerColumn_,
        headerValueList_.clone()
    );
  }

  @Override
  public TupleDescriptor createTupleDescriptor(Analyzer analyzer)
      throws AnalysisException {
    Preconditions.checkNotNull(sourceTableRef_);

    TupleDescriptor result = analyzer.getDescTbl().createTupleDescriptor(
        getClass().getSimpleName() + " " + getUniqueAlias());
    result.setIsMaterialized(false);
    result.setHidden(true);
    List<StructField> fields = new ArrayList<>();
    Set<String> uniqueColAliases = Sets.newHashSetWithExpectedSize(
        sourceTableRef_.getDesc().getType().getFields().size() +
        (analyzedAggList_.size() * headerValueMap_.size()));
    for (StructField field : sourceTableRef_.getDesc().getType().getFields()) {
      if (field.getName().equals(headerColumn_)) continue;
      if (!uniqueColAliases.add(field.getName())) {
        throw new AnalysisException("Duplicate column name: " + field.getName());
      }
      fields.add(new StructField(field.getName(), field.getType()));
    }
    Preconditions.checkState(analyzedAggList_.size() >= 1);
    for (String alias : headerValueMap_.keySet()) {
      for (Map.Entry<String, FunctionCallExpr> entry : analyzedAggList_.entrySet()) {
        String fieldName = analyzedAggList_.size() == 1 ?
            alias : alias + "_" + entry.getKey();
        if (!uniqueColAliases.add(fieldName)) {
          throw new AnalysisException("Duplicate column name: " + fieldName);
        }
        fields.add(new StructField(fieldName, entry.getValue().getType()));
      }
    }
    result.setType(new StructType(fields));
    return result;
  }

  private void createHeaderValueMap(Analyzer analyzer) throws AnalysisException {
    HashSet<LiteralExpr> headerValueSet = new HashSet<>();
    for (SelectListItem item : headerValueList_.getItems()) {
      if (!(item.getExpr() instanceof LiteralExpr)) {
        throw new AnalysisException(
            "Only literals are supported in the header value list of the PIVOT clause");
      }
      LiteralExpr e = (LiteralExpr)item.getExpr();
      e.analyze(analyzer);
      String alias = item.getAlias();
      if (alias == null) {
        alias = e.getStringValue();
      }
      if (headerValueMap_.put(alias, e) != null) {
        throw new AnalysisException(
            "Duplicate alias for header values in the PIVOT clause: " + alias);
      }
      if (!headerValueSet.add(e)) {
        throw new AnalysisException(
            "Duplicate value in the PIVOT clause: " + e.getStringValue());
      }
    }
  }

  private void analyzeAggList(Analyzer analyzer) throws AnalysisException {
    for (SelectListItem item : aggList_.getItems()) {
      // Use clone() to keep aggList_ read-only.
      Expr e = item.getExpr().clone();
      e.analyze(analyzer);
      if (!e.isAggregate()) {
        throw new AnalysisException(
            "The function called in the PIVOT clause should be an aggregate");
      }
      if (item.getAlias() == null && aggList_.getItems().size() > 1) {
        throw new AnalysisException(
            "Aliases are required when multiple aggregate expressions are specified " +
            "in the PIVOT clause");
      }
      FunctionCallExpr aggExpr = (FunctionCallExpr)e;
      AggregateFunction aggFn = (AggregateFunction)(aggExpr.getFn());
      if (aggExpr.returnsNonNullOnEmpty() && aggFn.getReturnExprOnEmpty() == null) {
        throw new AnalysisException(
            "Aggregate function in the PIVOT clause is not supported: " +
            aggFn.getName());
      }
      if (analyzedAggList_.put(item.getAlias(), (FunctionCallExpr)e) != null) {
        throw new AnalysisException(
            "Duplicate alias for aggregations in the PIVOT clause: " + item.getAlias());
      }
    }
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed_) return;
    isAnalyzed_ = true;

    sourceTableRef_ = analyzer.resolveTableRef(new TableRef(rawPath_, null));
    sourceTableRef_.analyze(analyzer);
    registerSourceTableSlotRef(analyzer, Lists.newArrayList(headerColumn_));
    createHeaderValueMap(analyzer);
    analyzeAggList(analyzer);
    // The source table is not visible outside the PIVOT clause when resolving a Path.
    sourceTableRef_.setHidden(true);
    desc_ = analyzer.registerTableRef(this);
  }

  private SlotDescriptor registerSourceTableSlotRef(
      Analyzer analyzer, List<String> rawPath) throws AnalysisException {
    String columnName = rawPath.get(0);
    Path sourceTableSlotPath = new Path(sourceTableRef_.getDesc(), rawPath);
    if (!sourceTableSlotPath.resolve()) return null;
    SlotDescriptor sourceTableSlot = analyzer.registerSlotRef(sourceTableSlotPath);
    if (sourceTableSlot.getType().isComplexType()) {
      // Throw an exception since GROUP BY expression cannot be used on complex types.
      throw new AnalysisException(
          "Referencing complex columns in the source table of the PIVOT clause " +
          "is not supported");
    }
    sourceTableRef_.registerColumn(new Column(
        String.join(".", rawPath), sourceTableSlot.getType(), -1));
    groupingSlotMap_.put(columnName, sourceTableSlot);
    return sourceTableSlot;
  }

  @Override
  void notifySlotRefRegistered(Analyzer analyzer, SlotDescriptor slot)
      throws AnalysisException {
    List<String> rawPath = slot.getPath().getRawPath();
    registerSourceTableSlotRef(analyzer, rawPath);
  }

  @Override
  public String tableRefToSql(ToSqlOptions options) {
    List<String> headerValueList = new ArrayList<>();
    for (SelectListItem item : headerValueList_.getItems()) {
      headerValueList.add(item.toSql());
    }
    List<String> aggList = new ArrayList<>();
    for (SelectListItem item : aggList_.getItems()) {
      aggList.add(item.toSql());
    }
    return String.format("%s PIVOT (%s FOR %s IN (%s)) %s",
        resolvedSourceTableRef_ ? sourceTableRef_.toSql() : String.join(".", getPath()),
        String.join(", ", aggList),
        headerColumn_,
        String.join(", ", headerValueList),
        getUniqueAlias());
  }

  TableRef getSourceTableRef() { return sourceTableRef_; }

  InlineViewRef toInlineViewRef(TableRef sourceTableRef) {
    List<Expr> groupingExprs = new ArrayList<>();
    List<SelectListItem> multiAggSelectList = new ArrayList<>();
    int headerSlotRefIndex = -1;
    int slotIdx = 0;
    for (Map.Entry<String, SlotDescriptor> entry : groupingSlotMap_.entrySet()) {
      Expr e = new SlotRef(entry.getValue().getPath().getRawPath());
      groupingExprs.add(e);
      multiAggSelectList.add(new SelectListItem(e, null));
      String columnName = entry.getKey();
      if (columnName.equals(headerColumn_)) {
        Preconditions.checkState(headerSlotRefIndex == -1);
        headerSlotRefIndex = slotIdx;
      }
      ++slotIdx;
    }
    Preconditions.checkState(headerSlotRefIndex >= 0);
    String multiAggInlineViewRefAlias = getUniqueAlias() + "_" +
        UUID.randomUUID().toString().replace("-", "_");
    // singleAggAlias is used only when there is only one Agg Expr and its alias is not
    // specified.
    String singleAggAlias = multiAggInlineViewRefAlias + "_agg";
    for (SelectListItem aggItem : aggList_.getItems()) {
      // Use clone() to keep aggList_ read-only.
      multiAggSelectList.add(aggItem.getAlias() != null ?
          aggItem.clone() :
          new SelectListItem(aggItem.getExpr().clone(), singleAggAlias));
    }
    if (sourceTableRef == null) {
      sourceTableRef_.setHidden(false);
      sourceTableRef = sourceTableRef_;
    }
    SelectStmt multiAggStmt = new SelectStmt(new SelectList(multiAggSelectList),
        new FromClause(Lists.newArrayList(sourceTableRef)), null,
        new GroupByClause(groupingExprs, GroupingSetsType.NONE), null, null, null);

    InlineViewRef multiAggInlineViewRef = new InlineViewRef(
        multiAggInlineViewRefAlias, multiAggStmt, (TableSampleClause) null);

    List<Expr> transposeGroupingExprs = Expr.cloneList(groupingExprs);
    transposeGroupingExprs.remove(headerSlotRefIndex);
    List<SelectListItem> transposeSelectList = new ArrayList<>();
    for (Expr e : transposeGroupingExprs) {
      transposeSelectList.add(new SelectListItem(e, null));
    }
    SlotRef headerColumnSlotRef = new SlotRef(Lists.newArrayList(headerColumn_));
    for (Map.Entry<String, LiteralExpr> entry : headerValueMap_.entrySet()) {
      for (Map.Entry<String, FunctionCallExpr> aggEntry : analyzedAggList_.entrySet()) {
        String aggAlias =
            aggEntry.getKey() == null ? singleAggAlias : aggEntry.getKey();
        // clone() is needed to keep the headerValueMap read-only.
        Expr cond = Expr.IS_NULL_LITERAL.apply(entry.getValue()) ?
            new IsNullPredicate(headerColumnSlotRef, false) :
            new BinaryPredicate(
                Operator.EQ, headerColumnSlotRef, entry.getValue().clone());
        FunctionCallExpr transAggExpr = new FunctionCallExpr("aggif",
            Lists.newArrayList(cond, new SlotRef(Lists.newArrayList(aggAlias))));
        if (aggEntry.getValue().returnsNonNullOnEmpty()) {
          AggregateFunction aggFn = (AggregateFunction)(aggEntry.getValue().getFn());
          Expr returnExpr = aggFn.getReturnExprOnEmpty();
          transAggExpr = new FunctionCallExpr("ifnull",
              Lists.newArrayList(transAggExpr, returnExpr));
        }
        String alias = entry.getKey();
        if (aggList_.getItems().size() > 1) {
          alias = alias + "_" + aggAlias;
        }
        transposeSelectList.add(new SelectListItem(transAggExpr, alias));
      }
    }
    GroupByClause transposeGroupByClause = transposeGroupingExprs.isEmpty() ? null :
        new GroupByClause(transposeGroupingExprs, GroupingSetsType.NONE);
    SelectStmt transposeSelectStmt = new SelectStmt(new SelectList(transposeSelectList),
        new FromClause(Lists.newArrayList(multiAggInlineViewRef)), null,
        transposeGroupByClause, null, null, null);
    return new InlineViewRef(
        getUniqueAlias(), transposeSelectStmt, (TableSampleClause) null);
  }

  @Override
  public TableRef applyTableMask(Analyzer analyzer) throws AnalysisException {
    TableRef sourceTableRefClone = sourceTableRef_.clone();
    sourceTableRefClone.setHidden(false);
    TableRef maskedSourceTableRef = sourceTableRefClone.applyTableMask(analyzer);
    InlineViewRef result = toInlineViewRef(maskedSourceTableRef);
    result.setIsTableMaskingView();
    return result;
  }
}
