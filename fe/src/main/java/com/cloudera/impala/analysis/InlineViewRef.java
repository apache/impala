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
import com.cloudera.impala.catalog.InlineView;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.service.FeSupport;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;


/**
 * Inline view is a query statement with an alias
 */
public class InlineViewRef extends TableRef {
  private final static Logger LOG = LoggerFactory.getLogger(SelectStmt.class);

  // The select or union statement of the inline view
  protected final QueryStmt queryStmt;

  // queryStmt has its own analysis context
  protected Analyzer inlineViewAnalyzer;

  // list of tuple ids materialized by queryStmt
  protected final ArrayList<TupleId> materializedTupleIds = Lists.newArrayList();

  // Map inline view colname to the underlying, fully substituted expression.
  // This map is built bottom-up, by recursively applying the substitution
  // maps of all enclosed inlined views; in other words, all SlotRefs
  // contained in rhs exprs reference base tables, not contained inline views
  // (and therefore can be evaluated at runtime).
  // Some rhs exprs are wrapped into IF(TupleIsNull(), NULL, expr) by calling
  // makeOutputNullable() if this inline view is a nullable side of an outer join.
  protected final Expr.SubstitutionMap sMap = new Expr.SubstitutionMap();

  /**
   * Constructor with alias and inline view select statement
   * @param alias inline view alias
   * @param inlineViewStmt the select statement of the inline view
   */
  public InlineViewRef(String alias, QueryStmt queryStmt) {
    super(alias);
    Preconditions.checkNotNull(queryStmt);
    this.queryStmt = queryStmt;
  }

  /**
   * C'tor for cloning.
   */
  public InlineViewRef(InlineViewRef other) {
    super(other);
    Preconditions.checkNotNull(other.queryStmt);
    this.queryStmt = other.queryStmt.clone();
  }

  /**
   * Create a new analyzer to analyze the inline view query block.
   * Then perform the join clause analysis as usual.
   *
   * By the time the inline view query block analysis returns, all the expressions of the
   * enclosed inline view select statment have already been substituted all the way down.
   * Then create a substitution map, mapping the SlotRef of this inline view to
   * the underlying, fully substituted query block select list expressions.
   */
  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    analyzeAsUser(analyzer, analyzer.getUser(), analyzer.useHiveColLabels());
  }

  protected void analyzeAsUser(Analyzer analyzer, User user, boolean useHiveColLabels)
      throws AuthorizationException, AnalysisException {
    // Analyze the inline view query statement with its own analyzer
    inlineViewAnalyzer = new Analyzer(analyzer, user);
    inlineViewAnalyzer.setUseHiveColLabels(useHiveColLabels);
    queryStmt.analyze(inlineViewAnalyzer);
    queryStmt.getMaterializedTupleIds(materializedTupleIds);
    desc = analyzer.registerInlineViewRef(this);
    isAnalyzed = true;  // true now that we have assigned desc

    // For constant selects we materialize its exprs into a tuple.
    if (materializedTupleIds.isEmpty()) {
      Preconditions.checkState(queryStmt instanceof SelectStmt);
      Preconditions.checkState(((SelectStmt) queryStmt).getTableRefs().isEmpty());
      desc.setIsMaterialized(true);
      materializedTupleIds.add(desc.getId());
    }

    // create sMap
    for (int i = 0; i < queryStmt.getColLabels().size(); ++i) {
      String colName = queryStmt.getColLabels().get(i);
      Expr colExpr = queryStmt.getResultExprs().get(i);
      SlotDescriptor slotDesc = analyzer.registerColumnRef(getAliasAsName(), colName);
      SlotRef slotRef = new SlotRef(slotDesc);
      sMap.lhs.add(slotRef);
      sMap.rhs.add(colExpr);
    }
    LOG.debug("inline view smap: " + sMap.debugString());

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
    InlineView inlineView = new InlineView(alias);
    for (int i = 0; i < queryStmt.getColLabels().size(); ++i) {
      // inline view select statement has been analyzed. Col label should be filled.
      Expr selectItemExpr = queryStmt.getResultExprs().get(i);
      String colAlias = queryStmt.getColLabels().get(i);

      // inline view col cannot have duplicate name
      if (inlineView.getColumn(colAlias) != null) {
        throw new AnalysisException("duplicated inline view column alias: '" +
            colAlias + "'" + " in inline view " + "'" + alias + "'");
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
   * Makes each rhs expr in sMap nullable if necessary by wrapping as follows:
   * IF(TupleIsNull(), NULL, rhs expr)
   * Should be called only if this inline view is a nullable side of an outer join.
   *
   * We need to make an rhs exprs nullable if it evaluates to a non-NULL value
   * when all of its contained SlotRefs evaluate to NULL.
   * For example, constant exprs need to be wrapped or an expr such as
   * 'case slotref is null then 1 else 2 end'
   */
  protected void makeOutputNullable(Analyzer analyzer)
      throws AnalysisException, InternalException, AuthorizationException {
    // Gather all unique rhs SlotRefs into rhsSlotRefs
    List<SlotRef> rhsSlotRefs = Lists.newArrayList();
    Expr.collectList(sMap.rhs, SlotRef.class, rhsSlotRefs);
    // Map for substituting SlotRefs with NullLiterals.
    Expr.SubstitutionMap nullSMap = new Expr.SubstitutionMap();
    for (SlotRef rhsSlotRef: rhsSlotRefs) {
      nullSMap.lhs.add(rhsSlotRef.clone(null));
      nullSMap.rhs.add(new NullLiteral());
    }

    // Make rhs exprs nullable if necessary.
    for (int i = 0; i < sMap.rhs.size(); ++i) {
      List<Expr> params = Lists.newArrayList();
      if (!requiresNullWrapping(analyzer, sMap.rhs.get(i), nullSMap)) continue;
      params.add(new TupleIsNullPredicate(materializedTupleIds));
      params.add(new NullLiteral());
      params.add(sMap.rhs.get(i));
      Expr ifExpr = new FunctionCallExpr("if", params);
      ifExpr.analyze(analyzer);
      sMap.rhs.set(i, ifExpr);
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
    if (expr.contains(TupleIsNullPredicate.class)) return true;

    // Replace all SlotRefs in expr with NullLiterals, and wrap the result
    // into an IS NOT NULL predicate.
    Expr isNotNullLiteralPred = new IsNullPredicate(expr.clone(nullSMap), true);
    Preconditions.checkState(isNotNullLiteralPred.isConstant());
    // analyze to insert casts, etc.
    try {
      isNotNullLiteralPred.analyze(analyzer);
    } catch (AnalysisException e) {
      // this should never happen
      throw new InternalException(
          "couldn't analyze predicate " + isNotNullLiteralPred.toSql(), e);
    }
    return FeSupport.EvalPredicate(isNotNullLiteralPred, analyzer.getQueryGlobals());
  }

  @Override
  public List<TupleId> getMaterializedTupleIds() {
    Preconditions.checkState(isAnalyzed);
    Preconditions.checkState(materializedTupleIds.size() > 0);
    return materializedTupleIds;
  }

  public QueryStmt getViewStmt() {
    return queryStmt;
  }

  public Analyzer getAnalyzer() {
    Preconditions.checkState(isAnalyzed);
    return inlineViewAnalyzer;
  }

  public Expr.SubstitutionMap getExprSMap() {
    Preconditions.checkState(isAnalyzed);
    return sMap;
  }

  @Override
  public String getAlias() {
    return alias;
  }

  @Override
  public TableName getAliasAsName() {
    return new TableName(null, alias);
  }

  @Override
  protected String tableRefToSql() {
    // Enclose the alias in quotes if Hive cannot parse it without quotes.
    // This is needed for view compatibility between Impala and Hive.
    String aliasSql = ToSqlUtils.getHiveIdentSql(alias);
    return "(" + queryStmt.toSql() + ") " + aliasSql;
  }

  @Override
  public TableRef clone() { return new InlineViewRef(this); }
}
