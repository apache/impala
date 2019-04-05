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
import java.util.List;
import java.util.Set;

import org.apache.impala.catalog.ColumnStats;
import org.apache.impala.catalog.FeView;
import org.apache.impala.catalog.StructField;
import org.apache.impala.catalog.StructType;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.rewrite.ExprRewriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * An inline view is a query statement with an alias. Inline views can be parsed directly
 * from a query string or represent a reference to a local or catalog view.
 */
public class InlineViewRef extends TableRef {
  private final static Logger LOG = LoggerFactory.getLogger(SelectStmt.class);

  // Catalog or local view that is referenced.
  // Null for inline views parsed directly from a query string.
  private final FeView view_;

  // If not null, these will serve as the column labels for the inline view. This provides
  // a layer of separation between column labels visible from outside the inline view
  // and column labels used in the query definition. Either all or none of the column
  // labels must be overridden.
  private List<String> explicitColLabels_;

  /////////////////////////////////////////
  // BEGIN: Members that need to be reset()

  // The select or union statement of the inline view
  protected QueryStmt queryStmt_;

  // queryStmt has its own analysis context
  protected Analyzer inlineViewAnalyzer_;

  // list of tuple ids materialized by queryStmt
  protected final List<TupleId> materializedTupleIds_ = new ArrayList<>();

  // Map inline view's output slots to the corresponding resultExpr of queryStmt.
  protected final ExprSubstitutionMap smap_;

  // Map inline view's output slots to the corresponding baseTblResultExpr of queryStmt.
  protected final ExprSubstitutionMap baseTblSmap_;

  // END: Members that need to be reset()
  /////////////////////////////////////////

  /**
   * C'tor for creating inline views parsed directly from the a query string.
   */
  public InlineViewRef(String alias, QueryStmt queryStmt,
      TableSampleClause sampleParams) {
    super(null, alias, sampleParams);
    Preconditions.checkNotNull(queryStmt);
    queryStmt_ = queryStmt;
    view_ = null;
    smap_ = new ExprSubstitutionMap();
    baseTblSmap_ = new ExprSubstitutionMap();
  }

  public InlineViewRef(String alias, QueryStmt queryStmt, List<String> colLabels) {
    this(alias, queryStmt, (TableSampleClause) null);
    explicitColLabels_ = Lists.newArrayList(colLabels);
  }

  /**
   * C'tor for creating inline views that replace a local or catalog view ref.
   */
  public InlineViewRef(FeView view, TableRef origTblRef) {
    super(view.getTableName().toPath(), origTblRef.getExplicitAlias(),
        origTblRef.getPrivilege(), origTblRef.requireGrantOption());
    queryStmt_ = view.getQueryStmt().clone();
    queryStmt_.reset();
    if (view.isLocalView()) queryStmt_.reset();
    view_ = view;
    smap_ = new ExprSubstitutionMap();
    baseTblSmap_ = new ExprSubstitutionMap();
    setJoinAttrs(origTblRef);
    explicitColLabels_ = view.getColLabels();
    // Set implicit aliases if no explicit one was given.
    if (hasExplicitAlias()) return;
    aliases_ = new String[] {
        view_.getTableName().toString().toLowerCase(), view_.getName().toLowerCase()
    };
    sampleParams_ = origTblRef.getSampleParams();
  }

  /**
   * C'tor for cloning.
   */
  public InlineViewRef(InlineViewRef other) {
    super(other);
    Preconditions.checkNotNull(other.queryStmt_);
    view_ = other.view_;
    queryStmt_ = other.queryStmt_.clone();
    inlineViewAnalyzer_ = other.inlineViewAnalyzer_;
    if (other.explicitColLabels_ != null) {
      explicitColLabels_ = Lists.newArrayList(other.explicitColLabels_);
    }
    materializedTupleIds_.addAll(other.materializedTupleIds_);
    smap_ = other.smap_.clone();
    baseTblSmap_ = other.baseTblSmap_.clone();
  }

  /**
   * Analyzes the inline view query block in a child analyzer of 'analyzer', creates
   * a new tuple descriptor for the inline view and registers auxiliary eq predicates
   * between the slots of that descriptor and the select list exprs of the inline view;
   * then performs join clause analysis.
   */
  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed_) return;

    // Analyze the inline view query statement with its own analyzer
    inlineViewAnalyzer_ = new Analyzer(analyzer);

    // Catalog views refs require special analysis settings for authorization.
    boolean isCatalogView = (view_ != null && !view_.isLocalView());
    if (isCatalogView) {
      analyzer.registerAuthAndAuditEvent(view_, priv_, requireGrantOption_);
      if (inlineViewAnalyzer_.isExplain()) {
        // If the user does not have privileges on the view's definition
        // then we report a masked authorization error so as not to reveal
        // privileged information (e.g., the existence of a table).
        inlineViewAnalyzer_.setMaskPrivChecks(
            String.format("User '%s' does not have privileges to " +
            "EXPLAIN this statement.", analyzer.getUser().getName()));
      } else {
        // If this is not an EXPLAIN statement, auth checks for the view
        // definition are still performed in order to determine if the user has access
        // to the runtime profile but don't trigger authorization errors.
        inlineViewAnalyzer_.setMaskPrivChecks(null);
      }
    }

    inlineViewAnalyzer_.setUseHiveColLabels(
        isCatalogView ? true : analyzer.useHiveColLabels());
    queryStmt_.analyze(inlineViewAnalyzer_);
    correlatedTupleIds_.addAll(queryStmt_.getCorrelatedTupleIds());
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
      String colName = getColLabels().get(i).toLowerCase();
      Expr colExpr = queryStmt_.getResultExprs().get(i);
      Path p = new Path(desc_, Lists.newArrayList(colName));
      Preconditions.checkState(p.resolve());
      SlotDescriptor slotDesc = analyzer.registerSlotRef(p);
      slotDesc.setSourceExpr(colExpr);
      slotDesc.setStats(ColumnStats.fromExpr(colExpr));
      SlotRef slotRef = new SlotRef(slotDesc);
      smap_.put(slotRef, colExpr);
      baseTblSmap_.put(slotRef, queryStmt_.getBaseTblResultExprs().get(i));
      if (createAuxPredicate(colExpr)) {
        analyzer.createAuxEqPredicate(new SlotRef(slotDesc), colExpr.clone());
      }
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("inline view " + getUniqueAlias() + " smap: " + smap_.debugString());
      LOG.trace("inline view " + getUniqueAlias() + " baseTblSmap: " +
          baseTblSmap_.debugString());
    }
    Preconditions.checkState(baseTblSmap_.checkComposedFrom(smap_));

    analyzeTableSample(analyzer);
    analyzeHints(analyzer);
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
   * Create and register a non-materialized tuple descriptor for this inline view.
   * This method is called from the analyzer when registering this inline view.
   * Create a non-materialized tuple descriptor for this inline view.
   */
  @Override
  public TupleDescriptor createTupleDescriptor(Analyzer analyzer)
      throws AnalysisException {
    int numColLabels = getColLabels().size();
    Preconditions.checkState(numColLabels > 0);
    Set<String> uniqueColAliases = Sets.newHashSetWithExpectedSize(numColLabels);
    List<StructField> fields = Lists.newArrayListWithCapacity(numColLabels);
    for (int i = 0; i < numColLabels; ++i) {
      // inline view select statement has been analyzed. Col label should be filled.
      Expr selectItemExpr = queryStmt_.getResultExprs().get(i);
      String colAlias = getColLabels().get(i).toLowerCase();

      // inline view col cannot have duplicate name
      if (!uniqueColAliases.add(colAlias)) {
        throw new AnalysisException("duplicated inline view column alias: '" +
            colAlias + "'" + " in inline view " + "'" + getUniqueAlias() + "'");
      }
      fields.add(new StructField(colAlias, selectItemExpr.getType(), null));
    }

    // Create the non-materialized tuple and set its type.
    TupleDescriptor result = analyzer.getDescTbl().createTupleDescriptor(
        getClass().getSimpleName() + " " + getUniqueAlias());
    result.setIsMaterialized(false);
    result.setType(new StructType(fields));
    return result;
  }

  @Override
  public void rewriteExprs(ExprRewriter rewriter, Analyzer analyzer)
      throws AnalysisException {
    super.rewriteExprs(rewriter, analyzer);
    queryStmt_.rewriteExprs(rewriter);
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

  public List<String> getExplicitColLabels() { return explicitColLabels_; }

  public List<String> getColLabels() {
    if (explicitColLabels_ != null) return explicitColLabels_;
    return queryStmt_.getColLabels();
  }

  public FeView getView() { return view_; }

  @Override
  protected TableRef clone() { return new InlineViewRef(this); }

  @Override
  public void reset() {
    super.reset();
    queryStmt_.reset();
    inlineViewAnalyzer_ = null;
    materializedTupleIds_.clear();
    smap_.clear();
    baseTblSmap_.clear();
  }

  @Override
  protected String tableRefToSql(ToSqlOptions options) {
    // Enclose the alias in quotes if Hive cannot parse it without quotes.
    // This is needed for view compatibility between Impala and Hive.
    String alias = getExplicitAlias();
    if (view_ != null) {
      return view_.getTableName().toSql() + ToSqlUtils.formatAlias(alias);
    }
    StringBuilder sql = new StringBuilder()
        .append("(")
        .append(queryStmt_.toSql(options))
        .append(")")
        .append(ToSqlUtils.formatAlias(alias));
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
