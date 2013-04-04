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

import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;


/**
 * Inline view is a query statement with an alias
 */
public class InlineViewRef extends TableRef {
  private final static Logger LOG = LoggerFactory.getLogger(SelectStmt.class);

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
    // Analyze the inline view query statement with its own analyzer
    inlineViewAnalyzer = new Analyzer(analyzer);
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

    // Now do the remaining join analysis
    analyzeJoin(analyzer);

    // create sMap
    for (int i = 0; i < queryStmt.getColLabels().size(); ++i) {
      String colName = queryStmt.getColLabels().get(i);
      SlotDescriptor slotD = analyzer.registerColumnRef(getAliasAsName(), colName);
      Expr colExpr = queryStmt.getResultExprs().get(i);
      sMap.lhs.add(new SlotRef(slotD));
      sMap.rhs.add(colExpr);
    }
    LOG.debug("inline view smap: " + sMap.debugString());
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
