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

import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.common.AnalysisException;
import com.google.common.collect.Lists;

/**
 * Abstract base class for any statement that returns results
 * via a list of result expressions, for example a
 * SelectStmt or UnionStmt. Also maintains a map of expression substitutions
 * for replacing expressions from ORDER BY or GROUP BY clauses with
 * their corresponding result expressions.
 * Used for sharing members/methods and some of the analysis code, in particular the
 * analysis of the ORDER BY and LIMIT clauses.
 *
 */
public abstract class QueryStmt extends StatementBase {
  protected WithClause withClause;

  protected ArrayList<OrderByElement> orderByElements;
  protected final long limit;

  /**
   * For a select statment:
   * List of executable exprs in select clause (star-expanded, ordinals and
   * aliases substituted, agg output substituted
   * For a union statement:
   * List of slotrefs into the tuple materialized by the union.
   */
  protected final ArrayList<Expr> resultExprs = Lists.newArrayList();

  /**
   * Map of expression substitutions for replacing aliases
   * in "order by" or "group by" clauses with their corresponding result expr.
   */
  protected final Expr.SubstitutionMap aliasSMap = new Expr.SubstitutionMap();

  /**
   * Select list item alias does not have to be unique.
   * This list contains all the non-unique aliases. For example,
   *   select int_col a, string_col a from alltypessmall;
   * Both columns are using the same alias "a".
   */
  protected final ArrayList<Expr> ambiguousAliasList = Lists.newArrayList();

  protected SortInfo sortInfo;

  // True if this QueryStmt is the top level query from an EXPLAIN <query>
  protected boolean isExplain = false;

  // Analyzer that was used to analyze this query statement.
  protected Analyzer analyzer;

  QueryStmt(ArrayList<OrderByElement> orderByElements, long limit) {
    this.orderByElements = orderByElements;
    this.limit = limit;
    this.sortInfo = null;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    this.analyzer = analyzer;
    if (hasWithClause()) withClause.analyze(analyzer);
    analyzer.setIsExplain(isExplain);
  }

  /**
   * Creates sortInfo by resolving aliases and ordinals in the orderingExprs.
   */
  protected void createSortInfo(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    if (orderByElements == null) {
      // not computing order by
      return;
    }

    ArrayList<Expr> orderingExprs = Lists.newArrayList();
    ArrayList<Boolean> isAscOrder = Lists.newArrayList();

    // extract exprs
    for (OrderByElement orderByElement: orderByElements) {
      // create copies, we don't want to modify the original parse node, in case
      // we need to print it
      orderingExprs.add(orderByElement.getExpr().clone(null));
      isAscOrder.add(Boolean.valueOf(orderByElement.getIsAsc()));
    }
    substituteOrdinals(orderingExprs, "ORDER BY");
    Expr ambiguousAlias = getFirstAmbiguousAlias(orderingExprs);
    if (ambiguousAlias != null) {
      throw new AnalysisException("Column " + ambiguousAlias.toSql() +
          " in order clause is ambiguous");
    }
    Expr.substituteList(orderingExprs, aliasSMap);
    Expr.analyze(orderingExprs, analyzer);

    sortInfo = new SortInfo(orderingExprs, isAscOrder);
  }

  /**
   * Return the first expr in exprs that is a non-unique alias. Return null if none of
   * exprs is an ambiguous alias.
   */
  protected Expr getFirstAmbiguousAlias(List<Expr> exprs) {
    for (Expr exp: exprs) {
      if (ambiguousAliasList.contains(exp)) {
        return exp;
      }
    }
    return null;
  }

  /**
   * Substitute exprs of the form "<number>"  with the corresponding
   * expressions.
   * @param exprs
   * @param errorPrefix
   * @throws AnalysisException
   */
  protected abstract void substituteOrdinals(List<Expr> exprs, String errorPrefix)
      throws AnalysisException;

  /**
   * UnionStmt and SelectStmt have different implementations.
   */
  public abstract ArrayList<String> getColLabels();

  /**
   * Returns the materialized tuple ids of the output of this stmt.
   * Used in case this stmt is part of an @InlineViewRef,
   * since we need to know the materialized tupls ids of a TableRef.
   */
  public abstract void getMaterializedTupleIds(ArrayList<TupleId> tupleIdList);

  public void setWithClause(WithClause withClause) {
    this.withClause = withClause;
  }

  public boolean hasWithClause() {
    return withClause != null;
  }

  public WithClause getWithClause() {
    return withClause;
  }

  public ArrayList<OrderByElement> getOrderByElements() {
    return orderByElements;
  }

  public void removeOrderByElements() {
    orderByElements = null;
  }

  public boolean hasOrderByClause() {
    return orderByElements != null;
  }

  public long getLimit() {
    return limit;
  }

  public boolean hasLimitClause() {
    return limit != -1;
  }

  public SortInfo getSortInfo() {
    return sortInfo;
  }

  public ArrayList<Expr> getResultExprs() {
    return resultExprs;
  }

  public void setResultExprs(List<Expr> resultExprs) {
    this.resultExprs.clear();
    this.resultExprs.addAll(resultExprs);
  }

  public void setIsExplain(boolean isExplain) {
    this.isExplain = isExplain;
  }

  public boolean isExplain() {
    return isExplain;
  }

  public ArrayList<OrderByElement> cloneOrderByElements() {
    return orderByElements != null ? Lists.newArrayList(orderByElements) : null;
  }

  public WithClause cloneWithClause() {
    return withClause != null ? withClause.clone() : null;
  }

  @Override
  public abstract QueryStmt clone();
}
