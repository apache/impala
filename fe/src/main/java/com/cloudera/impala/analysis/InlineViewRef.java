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

import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.ColumnStats;
import com.cloudera.impala.catalog.InlineView;
import com.cloudera.impala.catalog.View;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.TreeNode;
import com.cloudera.impala.service.FeSupport;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;

/**
 * An inline view is a query statement with an alias. Inline views can be parsed directly
 * from a query string or represent a reference to a local or catalog view.
 */
public class InlineViewRef extends TableRef {
  private final static Logger LOG = LoggerFactory.getLogger(SelectStmt.class);

  // Catalog or local view that is referenced.
  // Null for inline views parsed directly from a query string.
  private final View view_;

  // The select or union statement of the inline view
  protected QueryStmt queryStmt_;

  // queryStmt has its own analysis context
  protected Analyzer inlineViewAnalyzer_;

  // list of tuple ids materialized by queryStmt
  protected final ArrayList<TupleId> materializedTupleIds_ = Lists.newArrayList();

  // Map inline view's output slots to the corresponding resultExpr of queryStmt.
  // Some rhs exprs are wrapped into IF(TupleIsNull(), NULL, expr) by calling
  // makeOutputNullable() if this inline view is a nullable side of an outer join.
  protected final ExprSubstitutionMap smap_ = new ExprSubstitutionMap();

  // Map inline view's output slots to the corresponding baseTblResultExpr of queryStmt.
  // Some rhs exprs are wrapped into IF(TupleIsNull(), NULL, expr) by calling
  // makeOutputNullable() if this inline view is a nullable side of an outer join.
  protected final ExprSubstitutionMap baseTblSmap_ = new ExprSubstitutionMap();

  // If not null, these will serve as the column labels for the inline view. This provides
  // a layer of separation between column labels visible from outside the inline view
  // and column labels used in the query definition. Either all or none of the column
  // labels must be overridden.
  private List<String> explicitColLabels_;

  /**
   * C'tor for creating inline views parsed directly from the a query string.
   */
  public InlineViewRef(String alias, QueryStmt queryStmt) {
    super(null, alias);
    Preconditions.checkNotNull(queryStmt);
    queryStmt_ = queryStmt;
    view_ = null;
  }

  public InlineViewRef(String alias, QueryStmt queryStmt, List<String> colLabels) {
    this(alias, queryStmt);
    explicitColLabels_ = Lists.newArrayList(colLabels);
  }

  /**
   * C'tor for creating inline views that replace a local or catalog view ref.
   */
  public InlineViewRef(View view, TableRef origTblRef) {
    super(view.getTableName(), origTblRef.getExplicitAlias());
    queryStmt_ = view.getQueryStmt().clone();
    view_ = view;
    setJoinAttrs(origTblRef);
  }

  /**
   * C'tor for cloning.
   */
  public InlineViewRef(InlineViewRef other) {
    super(other);
    Preconditions.checkNotNull(other.queryStmt_);
    queryStmt_ = other.queryStmt_.clone();
    view_ = other.view_;
    explicitColLabels_ = other.explicitColLabels_;
  }

  /**
   * Analyzes the inline view query block in a child analyzer of 'analyzer', creates
   * a new tuple descriptor for the inline view and registers auxiliary eq predicates
   * between the slots of that descriptor and the select list exprs of the inline view;
   * then performs join clause analysis.
   */
  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    // Analyze the inline view query statement with its own analyzer
    inlineViewAnalyzer_ = new Analyzer(analyzer);

    // Catalog views refs require special analysis settings for authorization.
    boolean isCatalogView = (view_ != null && !view_.isLocalView());
    if (isCatalogView) {
      if (inlineViewAnalyzer_.isExplain()) {
        // If the user does not have privileges on the view's definition
        // then we report a masked authorization error so as not to reveal
        // privileged information (e.g., the existence of a table).
        inlineViewAnalyzer_.setAuthErrMsg(
            String.format("User '%s' does not have privileges to " +
            "EXPLAIN this statement.", analyzer.getUser().getName()));
      } else {
        // If this is not an EXPLAIN statement, auth checks for the view
        // definition should be disabled.
        inlineViewAnalyzer_.setEnablePrivChecks(false);
      }
    }

    inlineViewAnalyzer_.setUseHiveColLabels(
        isCatalogView ? true : analyzer.useHiveColLabels());
    queryStmt_.analyze(inlineViewAnalyzer_);
    if (explicitColLabels_ != null) {
      Preconditions.checkState(
          explicitColLabels_.size() == queryStmt_.getColLabels().size());
    }

    inlineViewAnalyzer_.setHasLimitOffsetClause(
        queryStmt_.hasLimit() || queryStmt_.hasOffset());
    queryStmt_.getMaterializedTupleIds(materializedTupleIds_);
    desc_ = analyzer.registerTableRef(this);
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
    // propagated through the view);
    // if the view stmt contains analytic functions, we cannot propagate predicates
    // into the view, unless the predicates are compatible with the analytic
    // function's partition by clause, because those extra filters
    // would alter the results of the analytic functions (see IMPALA-1243)
    // TODO: relax this a bit by allowing propagation out of the inline view (but
    // not into it)
    for (int i = 0; i < getColLabels().size(); ++i) {
      String colName = getColLabels().get(i);
      Expr colExpr = queryStmt_.getResultExprs().get(i);
      SlotDescriptor slotDesc = analyzer.registerColumnRef(getAliasAsName(), colName);
      slotDesc.setSourceExpr(colExpr);
      slotDesc.setStats(ColumnStats.fromExpr(colExpr));
      SlotRef slotRef = new SlotRef(slotDesc);
      smap_.put(slotRef, colExpr);
      baseTblSmap_.put(slotRef, queryStmt_.getBaseTblResultExprs().get(i));
      if (createAuxPredicate(colExpr)) {
        analyzer.createAuxEquivPredicate(new SlotRef(slotDesc), colExpr.clone());
      }
    }
    LOG.trace("inline view " + getAlias() + " smap: " + smap_.debugString());
    LOG.trace("inline view " + getAlias() + " baseTblSmap: " +
        baseTblSmap_.debugString());

    // Now do the remaining join analysis
    analyzeJoin(analyzer);
  }

  /**
   * Checks if an auxiliary predicate should be created for an expr. Returns False if the
   * inline view has a SELECT stmt with analytic functions and the expr is not in the
   * common partition exprs of all the analytic functions computed by this inline view.
   */
  public boolean createAuxPredicate(Expr e) {
    if (!(queryStmt_ instanceof SelectStmt)
        || !((SelectStmt) queryStmt_).hasAnalyticInfo()) {
      return true;
    }
    AnalyticInfo analyticInfo = ((SelectStmt) queryStmt_).getAnalyticInfo();
    return analyticInfo.getCommonPartitionExprs().contains(e);
  }

  /**
   * Create a non-materialized tuple descriptor in descTbl for this inline view.
   * This method is called from the analyzer when registering this inline view.
   */
  @Override
  public TupleDescriptor createTupleDescriptor(Analyzer analyzer)
      throws AnalysisException {
    InlineView inlineView =
        (view_ != null) ? new InlineView(view_) : new InlineView(alias_);
    for (int i = 0; i < getColLabels().size(); ++i) {
      // inline view select statement has been analyzed. Col label should be filled.
      Expr selectItemExpr = queryStmt_.getResultExprs().get(i);
      String colAlias = getColLabels().get(i);

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
    TupleDescriptor result =
        analyzer.getDescTbl().createTupleDescriptor("inl-view-" + alias_);
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
  protected void makeOutputNullable(Analyzer analyzer) {
    try {
      makeOutputNullableHelper(analyzer, smap_);
      makeOutputNullableHelper(analyzer, baseTblSmap_);
    } catch (Exception e) {
      // should never happen
      throw new IllegalStateException(e);
    }
  }

  protected void makeOutputNullableHelper(Analyzer analyzer, ExprSubstitutionMap smap)
      throws InternalException {
    // Gather all unique rhs SlotRefs into rhsSlotRefs
    List<SlotRef> rhsSlotRefs = Lists.newArrayList();
    TreeNode.collect(smap.getRhs(), Predicates.instanceOf(SlotRef.class), rhsSlotRefs);
    // Map for substituting SlotRefs with NullLiterals.
    ExprSubstitutionMap nullSMap = new ExprSubstitutionMap();
    for (SlotRef rhsSlotRef: rhsSlotRefs) {
      // The rhs null literal should have the same type as the lhs SlotRef to ensure
      // exprs resolve to the same signature after applying this smap.
      nullSMap.put(rhsSlotRef.clone(), NullLiteral.create(rhsSlotRef.getType()));
    }

    // Make rhs exprs nullable if necessary.
    for (int i = 0; i < smap.getRhs().size(); ++i) {
      List<Expr> params = Lists.newArrayList();
      if (!requiresNullWrapping(analyzer, smap.getRhs().get(i), nullSMap)) continue;
      params.add(new TupleIsNullPredicate(materializedTupleIds_));
      params.add(new NullLiteral());
      params.add(smap.getRhs().get(i));
      Expr ifExpr = new FunctionCallExpr("if", params);
      ifExpr.analyzeNoThrow(analyzer);
      smap.getRhs().set(i, ifExpr);
    }
  }

  /**
   * Replaces all SloRefs in expr with a NullLiteral using nullSMap, and evaluates the
   * resulting constant expr. Returns true if the constant expr yields a non-NULL value,
   * false otherwise.
   */
  private boolean requiresNullWrapping(Analyzer analyzer, Expr expr,
      ExprSubstitutionMap nullSMap) throws InternalException {
    // If the expr is already wrapped in an IF(TupleIsNull(), NULL, expr)
    // then do not try to execute it.
    // TODO: return true in this case?
    if (expr.contains(Predicates.instanceOf(TupleIsNullPredicate.class))) return true;

    // Replace all SlotRefs in expr with NullLiterals, and wrap the result
    // with an IS NOT NULL predicate.
    Expr isNotNullLiteralPred =
        new IsNullPredicate(expr.substitute(nullSMap, analyzer, false), true);
    Preconditions.checkState(isNotNullLiteralPred.isConstant());
    // analyze to insert casts, etc.
    isNotNullLiteralPred.analyzeNoThrow(analyzer);
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

  public ExprSubstitutionMap getSmap() {
    Preconditions.checkState(isAnalyzed_);
    return smap_;
  }

  public ExprSubstitutionMap getBaseTblSmap() {
    Preconditions.checkState(isAnalyzed_);
    return baseTblSmap_;
  }

  public QueryStmt getViewStmt() { return queryStmt_; }
  public void setRewrittenViewStmt(QueryStmt stmt) {
    Preconditions.checkState(getAnalyzer().containsSubquery());
    queryStmt_ = stmt;
  }

  public List<String> getExplicitColLabels() { return explicitColLabels_; }

  public List<String> getColLabels() {
    if (explicitColLabels_ != null) {
      return explicitColLabels_;
    }
    return queryStmt_.getColLabels();
  }

  @Override
  public TableRef clone() { return new InlineViewRef(this); }

  @Override
  protected String tableRefToSql() {
    // Enclose the alias in quotes if Hive cannot parse it without quotes.
    // This is needed for view compatibility between Impala and Hive.
    String aliasSql = (alias_ == null) ? null : ToSqlUtils.getIdentSql(alias_);
    if (view_ != null) {
      return view_.getTableName().toSql() + (aliasSql == null ? "" : " " + aliasSql);
    }
    Preconditions.checkNotNull(aliasSql);
    StringBuilder sql = new StringBuilder()
        .append("(")
        .append(queryStmt_.toSql())
        .append(") ")
        .append(aliasSql);
    // Add explicit col labels for debugging even though this syntax isn't supported.
    if (explicitColLabels_ != null) {
      sql.append(" (");
      for (int i = 0; i < getExplicitColLabels().size(); i++) {
        if (i > 0) sql.append(", ");
        sql.append(ToSqlUtils.getIdentSql(getExplicitColLabels().get(i)));
      }
      sql.append(")");
    }
    return sql.toString();
  }
}
