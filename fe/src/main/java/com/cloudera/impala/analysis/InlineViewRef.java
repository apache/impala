// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import java.util.ArrayList;
import java.util.List;

import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;


/**
 * Inline view is a select statement with an alias
 */
public class InlineViewRef extends TableRef {
  // The select statement of the inline view
  private final SelectStmt viewSelectStmt;

  // Inner query block has its own analysis context
  private Analyzer inlineViewAnalyzer;

  // list of tuple ids from this inline view
  private final ArrayList<TupleId> tupleIdList = Lists.newArrayList();

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
  public InlineViewRef(String alias, SelectStmt inlineViewStmt) {
    super(alias);
    this.viewSelectStmt = inlineViewStmt;
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
    // Analyze the inline view select statement with its own Analyzer
    inlineViewAnalyzer = new Analyzer(analyzer);
    viewSelectStmt.analyze(inlineViewAnalyzer);

    // Register the inline view
    desc = analyzer.registerInlineViewRef(this);

    // Update the tupleIdList
    // If select statement has an aggregate, then the aggregate tuple id is materialized.
    // Otherwise, all the tables referenced by the inline view are materialized.
    if (viewSelectStmt.getAggInfo() != null) {
      tupleIdList.add(viewSelectStmt.getAggInfo().getAggTupleId());
    } else {
      for (TableRef tblRef: viewSelectStmt.getTableRefs()) {
        tupleIdList.addAll(tblRef.getIdList());
      }
    }

    // Now do the remaining join analysis
    analyzeJoin(analyzer);

    // SlotRef of this inline view is mapped to the underlying expression
    for (int i = 0; i < viewSelectStmt.getColLabels().size(); ++i) {
      String colName = viewSelectStmt.getColLabels().get(i);
      SlotDescriptor slotD = analyzer.registerColumnRef(getAliasAsName(), colName);
      Expr colexpr = viewSelectStmt.getSelectListExprs().get(i);
      sMap.lhs.add(new SlotRef(slotD));
      sMap.rhs.add(colexpr);
    }

    isAnalyzed = true;
  }

  @Override
  public List<TupleId> getIdList() {
    Preconditions.checkState(isAnalyzed);
    Preconditions.checkState(tupleIdList.size() > 0);
    return tupleIdList;
  }

  public SelectStmt getViewSelectStmt() {
    return viewSelectStmt;
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
    return "(" + viewSelectStmt.toSql() + ") " + alias;
  }
}
