// Copyright 2012 Cloudera Inc.
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

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.authorization.User;
import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.ColumnStats;
import com.cloudera.impala.catalog.InlineView;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.TreeNode;
import com.cloudera.impala.service.FeSupport;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;


/**
 * Inline view is a query statement with an alias
 */
public class InlineViewRef extends TableRef {
  private final static Logger LOG = LoggerFactory.getLogger(SelectStmt.class);

  // The select or union statement of the inline view
  protected final QueryStmt queryStmt_;

  // queryStmt has its own analysis context
  protected Analyzer inlineViewAnalyzer_;

  // list of tuple ids materialized by queryStmt
  protected final ArrayList<TupleId> materializedTupleIds_ = Lists.newArrayList();

  // Map inline view's output slots to the corresponding resultExpr of queryStmt.
  // Some rhs exprs are wrapped into IF(TupleIsNull(), NULL, expr) by calling
  // makeOutputNullable() if this inline view is a nullable side of an outer join.
  protected final Expr.SubstitutionMap smap_ = new Expr.SubstitutionMap();

  // Map inline view's output slots to the corresponding baseTblResultExpr of queryStmt.
  // Some rhs exprs are wrapped into IF(TupleIsNull(), NULL, expr) by calling
  // makeOutputNullable() if this inline view is a nullable side of an outer join.
  protected final Expr.SubstitutionMap baseTblSmap_ = new Expr.SubstitutionMap();

  /**
   * Constructor with alias and inline view select statement
   * @param alias inline view alias
   * @param inlineViewStmt the select statement of the inline view
   */
  public InlineViewRef(String alias, QueryStmt queryStmt) {
    super(alias);
    Preconditions.checkNotNull(queryStmt);
    this.queryStmt_ = queryStmt;
  }

  /**
   * C'tor for cloning.
   */
  public InlineViewRef(InlineViewRef other) {
    super(other);
    Preconditions.checkNotNull(other.queryStmt_);
    this.queryStmt_ = other.queryStmt_.clone();
  }

  /**
   * Analyzes the inline view query block in a child analyzer of 'analyzer', creates
   * a new tuple descriptor for the inline view and registers auxiliary eq predicates
   * between the slots of that descriptor and the select list exprs of the inline view;
   * then performs join clause analysis.
   */
  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    analyzeAsUser(analyzer, analyzer.getUser(), analyzer.useHiveColLabels());
  }

  protected void analyzeAsUser(Analyzer analyzer, User user, boolean useHiveColLabels)
      throws AuthorizationException, AnalysisException {
    // Analyze the inline view query statement with its own analyzer
    inlineViewAnalyzer_ = new Analyzer(analyzer, user);
    inlineViewAnalyzer_.setUseHiveColLabels(useHiveColLabels);
    queryStmt_.analyze(inlineViewAnalyzer_);

    inlineViewAnalyzer_.setHasLimitOffsetClause(
        queryStmt_.hasLimit() || queryStmt_.hasOffset());
    queryStmt_.getMaterializedTupleIds(materializedTupleIds_);
    desc_ = analyzer.registerInlineViewRef(this);
    isAnalyzed_ = true;  // true now that we have assigned desc

    // For constant selects we materialize its exprs into a tuple.
    if (materializedTupleIds_.isEmpty()) {
      Preconditions.checkState(queryStmt_ instanceof SelectStmt);
      Preconditions.checkState(((SelectStmt) queryStmt_).getTableRefs().isEmpty());
      desc_.setIsMaterialized(true);
      materializedTupleIds_.add(desc_.getId());
    }

    // create smap_ and baseTblSmap_ and register auxiliary eq predicates between our
    // tuple descriptor's slots and our *unresolved* select list exprs;
    // we create these auxiliary predicates so that the analyzer can compute the value
    // transfer graph through this inline view correctly (ie, predicates can get
    // propagated through the view)
    for (int i = 0; i < queryStmt_.getColLabels().size(); ++i) {
      String colName = queryStmt_.getColLabels().get(i);
      Expr colExpr = queryStmt_.getResultExprs().get(i);
      SlotDescriptor slotDesc = analyzer.registerColumnRef(getAliasAsName(), colName);
      slotDesc.setStats(ColumnStats.fromExpr(colExpr));
      SlotRef slotRef = new SlotRef(slotDesc);
      smap_.addMapping(slotRef, colExpr);
      baseTblSmap_.addMapping(slotRef, queryStmt_.getBaseTblResultExprs().get(i));

      analyzer.createAuxEquivPredicate(new SlotRef(slotDesc), colExpr.clone(null));
    }
    LOG.info("inline view " + getAlias() + " smap: " + smap_.debugString());
    LOG.info("inline view " + getAlias() + " baseTblSmap: " + baseTblSmap_.debugString());

    // Now do the remaining join analysis
    try {
      analyzeJoin(analyzer);
    } catch (InternalException e) {
      throw new AnalysisException(e.getMessage(), e);
    }
  }

  /**
   * Create a non-materialized tuple descriptor in descTbl for this inline view.
   * This method is called from the analyzer when registering this inline view.
   */
  public TupleDescriptor createTupleDescriptor(DescriptorTable descTbl)
      throws AnalysisException {
    // Create a fake catalog table for the inline view
    InlineView inlineView = new InlineView(alias_);
    for (int i = 0; i < queryStmt_.getColLabels().size(); ++i) {
      // inline view select statement has been analyzed. Col label should be filled.
      Expr selectItemExpr = queryStmt_.getResultExprs().get(i);
      String colAlias = queryStmt_.getColLabels().get(i);

      // inline view col cannot have duplicate name
      if (inlineView.getColumn(colAlias) != null) {
        throw new AnalysisException("duplicated inline view column alias: '" +
            colAlias + "'" + " in inline view " + "'" + alias_ + "'");
      }

      // create a column and add it to the inline view
      Column col = new Column(colAlias, selectItemExpr.getType(), i);
      inlineView.addColumn(col);
    }

    // Create the non-materialized tuple and set the fake table in it.
    TupleDescriptor result = descTbl.createTupleDescriptor();
    result.setIsMaterialized(false);
    result.setTable(inlineView);
    return result;
  }

  /**
   * Makes each rhs expr in baseTblSmap_ nullable, if necessary by wrapping as follows:
   * IF(TupleIsNull(), NULL, rhs expr)
   * Should be called only if this inline view is on the nullable side of an outer join.
   *
   * We need to make an rhs exprs nullable if it evaluates to a non-NULL value
   * when all of its contained SlotRefs evaluate to NULL.
   * For example, constant exprs need to be wrapped or an expr such as
   * 'case slotref is null then 1 else 2 end'
   */
  protected void makeOutputNullable(Analyzer analyzer)
      throws AnalysisException, InternalException, AuthorizationException {
    makeOutputNullableHelper(analyzer, smap_);
    makeOutputNullableHelper(analyzer, baseTblSmap_);
  }

  protected void makeOutputNullableHelper(Analyzer analyzer, Expr.SubstitutionMap smap)
      throws AnalysisException, InternalException, AuthorizationException {
    // Gather all unique rhs SlotRefs into rhsSlotRefs
    List<SlotRef> rhsSlotRefs = Lists.newArrayList();
    TreeNode.collect(smap.getRhs(), Predicates.instanceOf(SlotRef.class), rhsSlotRefs);
    // Map for substituting SlotRefs with NullLiterals.
    Expr.SubstitutionMap nullSMap = new Expr.SubstitutionMap();
    for (SlotRef rhsSlotRef: rhsSlotRefs) {
      nullSMap.addMapping(rhsSlotRef.clone(null), new NullLiteral());
    }

    // Make rhs exprs nullable if necessary.
    for (int i = 0; i < smap.getRhs().size(); ++i) {
      List<Expr> params = Lists.newArrayList();
      if (!requiresNullWrapping(analyzer, smap.getRhs().get(i), nullSMap)) continue;
      params.add(new TupleIsNullPredicate(materializedTupleIds_));
      params.add(new NullLiteral());
      params.add(smap.getRhs().get(i));
      Expr ifExpr = new FunctionCallExpr("if", params);
      ifExpr.analyze(analyzer);
      smap.getRhs().set(i, ifExpr);
    }
  }

  /**
   * Replaces all SloRefs in expr with a NullLiteral using nullSMap, and evaluates the
   * resulting constant expr. Returns true if the constant expr yields a non-NULL value,
   * false otherwise.
   */
  private boolean requiresNullWrapping(Analyzer analyzer, Expr expr,
      Expr.SubstitutionMap nullSMap) throws InternalException, AuthorizationException {
    // If the expr is already wrapped in an IF(TupleIsNull(), NULL, expr)
    // then do not try to execute it.
    // TODO: return true in this case?
    if (expr.contains(Predicates.instanceOf(TupleIsNullPredicate.class))) return true;

    // Replace all SlotRefs in expr with NullLiterals, and wrap the result
    // with an IS NOT NULL predicate.
    Expr isNotNullLiteralPred = new IsNullPredicate(expr.clone(nullSMap), true);
    Preconditions.checkState(isNotNullLiteralPred.isConstant());
    // analyze to insert casts, etc.
    try {
      isNotNullLiteralPred.reanalyze(analyzer);
    } catch (AnalysisException e) {
      // this should never happen
      throw new InternalException(
          "couldn't analyze predicate " + isNotNullLiteralPred.toSql(), e);
    }
    return FeSupport.EvalPredicate(isNotNullLiteralPred, analyzer.getQueryCtx());
  }

  @Override
  public List<TupleId> getMaterializedTupleIds() {
    Preconditions.checkState(isAnalyzed_);
    Preconditions.checkState(materializedTupleIds_.size() > 0);
    return materializedTupleIds_;
  }

  public Analyzer getAnalyzer() {
    Preconditions.checkState(isAnalyzed_);
    return inlineViewAnalyzer_;
  }

  public Expr.SubstitutionMap getSmap() {
    Preconditions.checkState(isAnalyzed_);
    return smap_;
  }

  public Expr.SubstitutionMap getBaseTblSmap() {
    Preconditions.checkState(isAnalyzed_);
    return baseTblSmap_;
  }

  public QueryStmt getViewStmt() { return queryStmt_; }
  @Override
  public String getAlias() { return alias_; }
  @Override
  public TableName getAliasAsName() { return new TableName(null, alias_); }
  @Override
  public TableRef clone() { return new InlineViewRef(this); }

  @Override
  protected String tableRefToSql() {
    // Enclose the alias in quotes if Hive cannot parse it without quotes.
    // This is needed for view compatibility between Impala and Hive.
    String aliasSql = ToSqlUtils.getIdentSql(alias_);
    return "(" + queryStmt_.toSql() + ") " + aliasSql;
  }
}
