// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import java.util.ArrayList;
import java.util.List;

import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;


/**
 * Inline view is a query statement with an alias
 */
public class InlineViewRef extends TableRef {
  // The select or union statement of the inline view
  private final QueryStmt queryStmt;

  // queryStmt has its own analysis context
  private Analyzer inlineViewAnalyzer;

  // list of tuple ids materialized by queryStmt
  private final ArrayList<TupleId> materializedTupleIds = Lists.newArrayList();

  // Map inline view colname to the underlying, fully substituted expression.
  // This map is built bottom-up, by recursively applying the substitution
  // maps of all enclosed inlined views; in other words, all SlotRefs
  // contained in rhs exprs reference base tables, not contained inline views
  // (and therefore can be evaluated at runtime).
  private final Expr.SubstitutionMap sMap = new Expr.SubstitutionMap();

  /**
   * Constructor with alias and inline view select statement
   * @param alias inline view alias
   * @param inlineViewStmt the select statement of the inline view
   */
  public InlineViewRef(String alias, QueryStmt queryStmt) {
    super(alias);
    this.queryStmt = queryStmt;
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
  public void analyze(Analyzer analyzer) throws AnalysisException, InternalException {
    // Analyze the inline view query statement with its own Analyzer
    inlineViewAnalyzer = new Analyzer(analyzer);
    queryStmt.analyze(inlineViewAnalyzer);
    desc = analyzer.registerInlineViewRef(this);
    queryStmt.getMaterializedTupleIds(materializedTupleIds);

    // Now do the remaining join analysis
    analyzeJoin(analyzer);

    // SlotRef of this inline view is mapped to the underlying expression
    for (int i = 0; i < queryStmt.getColLabels().size(); ++i) {
      String colName = queryStmt.getColLabels().get(i);
      SlotDescriptor slotD = analyzer.registerColumnRef(getAliasAsName(), colName);
      Expr colexpr = queryStmt.getResultExprs().get(i);
      sMap.lhs.add(new SlotRef(slotD));
      sMap.rhs.add(colexpr);
    }

    isAnalyzed = true;
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

  public Expr.SubstitutionMap getSelectListExprSMap() {
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
    return "(" + queryStmt.toSql() + ") " + alias;
  }
}
