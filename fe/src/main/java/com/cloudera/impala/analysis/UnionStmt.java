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
import java.util.ListIterator;

import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.ColumnStats;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Representation of a union with its list of operands,
 * and optional order by and limit.
 * A union materializes its results, and its resultExprs
 * are slotrefs into the materialized tuple.
 */
public class UnionStmt extends QueryStmt {

  public static enum Qualifier {
    ALL,
    DISTINCT
  }

  /**
   * Represents an operand to a union, created by the parser.
   * Contains a query statement and the all/distinct qualifier
   * of the union operator (null for the first queryStmt).
   */
  public static class UnionOperand {
    private final QueryStmt queryStmt;
    // Null for the first operand.
    private Qualifier qualifier;

    // Analyzer used for this operand. Set in analyze().
    // We must preserve the conjuncts registered in the analyzer for partition pruning.
    private Analyzer analyzer;

    public UnionOperand(QueryStmt queryStmt, Qualifier qualifier) {
      this.queryStmt = queryStmt;
      this.qualifier = qualifier;
    }

    public void analyze(Analyzer parent) throws AnalysisException,
        AuthorizationException {
      analyzer = new Analyzer(parent, parent.getUser());
      queryStmt.analyze(analyzer);
    }

    public QueryStmt getQueryStmt() {
      return queryStmt;
    }

    public Qualifier getQualifier() {
      return qualifier;
    }

    // Used for propagating DISTINCT.
    public void setQualifier(Qualifier qualifier) {
      this.qualifier = qualifier;
    }

    public Analyzer getAnalyzer() {
      return analyzer;
    }

    @Override
    public UnionOperand clone() {
      return new UnionOperand(queryStmt.clone(), qualifier);
    }
  }

  protected final List<UnionOperand> operands;

  // Single tuple materialized by the union. Set in analyze().
  protected TupleId tupleId;

  public UnionStmt(List<UnionOperand> operands,
      ArrayList<OrderByElement> orderByElements, long limit) {
    super(orderByElements, limit);
    this.operands = operands;
  }

  public List<UnionOperand> getUnionOperands() {
    return operands;
  }

  /**
   * Propagates DISTINCT from left to right, and checks that all
   * union operands are union compatible, adding implicit casts if necessary.
   */
  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    super.analyze(analyzer);

    Preconditions.checkState(operands.size() > 0);

    // Propagates DISTINCT from left to right,
    propagateDistinct();

    // Make sure all operands return an equal number of exprs.
    QueryStmt firstQuery = operands.get(0).getQueryStmt();
    operands.get(0).analyze(analyzer);
    List<Expr> firstQueryExprs = firstQuery.getResultExprs();
    for (int i = 1; i < operands.size(); ++i) {
      QueryStmt query = operands.get(i).getQueryStmt();
      operands.get(i).analyze(analyzer);
      List<Expr> exprs = query.getResultExprs();
      if (firstQueryExprs.size() != exprs.size()) {
        throw new AnalysisException("Operands have unequal number of columns:\n" +
            "'" + queryStmtToSql(firstQuery) + "' has " +
            firstQueryExprs.size() + " column(s)\n" +
            "'" + queryStmtToSql(query) + "' has " + exprs.size() + " column(s)");
      }
    }

    // Determine compatible types for exprs, position by position.
    for (int i = 0; i < firstQueryExprs.size(); ++i) {
      // Type compatible with the i-th exprs of all selects.
      // Initialize with type of i-th expr in first select.
      PrimitiveType compatibleType = firstQueryExprs.get(i).getType();
      // Remember last compatible expr for error reporting.
      Expr lastCompatibleExpr = firstQueryExprs.get(i);
      for (int j = 1; j < operands.size(); ++j) {
        List<Expr> resultExprs = operands.get(j).getQueryStmt().getResultExprs();
        compatibleType = analyzer.getCompatibleType(compatibleType,
            lastCompatibleExpr, resultExprs.get(i));
        lastCompatibleExpr = resultExprs.get(i);
      }
      // Now that we've found a compatible type, add implicit casts if necessary.
      for (int j = 0; j < operands.size(); ++j) {
        List<Expr> resultExprs = operands.get(j).getQueryStmt().getResultExprs();
        if (resultExprs.get(i).getType() != compatibleType) {
          Expr castExpr = resultExprs.get(i).castTo(compatibleType);
          resultExprs.set(i, castExpr);
        }
      }
    }

    // Create tuple descriptor materialized by this UnionStmt,
    // its resultExprs, and its sortInfo if necessary.
    createTupleAndResultExprs(analyzer);
    createSortInfo(analyzer);
  }

  /**
   * String representation of queryStmt used in reporting errors.
   * Allow subclasses to override this.
   */
  protected String queryStmtToSql(QueryStmt queryStmt) {
    return queryStmt.toSql();
  }

  /**
   * Propagates DISTINCT (if present) from left to right.
   */
  private void propagateDistinct() {
    int firstDistinctPos = -1;
    for (int i = operands.size() - 1; i > 0; --i) {
      UnionOperand operand = operands.get(i);
      if (firstDistinctPos != -1) {
        // There is a DISTINCT somewhere to the right.
        operand.setQualifier(Qualifier.DISTINCT);
      } else if (operand.getQualifier() == Qualifier.DISTINCT) {
        firstDistinctPos = i;
      }
    }
  }

  /**
   * Create a descriptor for the tuple materialized by the union.
   * Set resultExprs to be slot refs into that tuple.
   * Also fills the substitution map, such that "order by" can properly resolve
   * column references from the result of the union.
   */
  private void createTupleAndResultExprs(Analyzer analyzer) throws AnalysisException {
    // Create tuple descriptor for materialized tuple created by the union.
    TupleDescriptor tupleDesc = analyzer.getDescTbl().createTupleDescriptor();
    tupleDesc.setIsMaterialized(true);
    tupleId = tupleDesc.getId();

    // One slot per expr in the select blocks. Use first select block as representative.
    List<Expr> firstSelectExprs = operands.get(0).getQueryStmt().getResultExprs();

    // Compute column stats for the materialized slots from the source exprs.
    List<ColumnStats> columnStats = Lists.newArrayList();
    for (int i = 0; i < operands.size(); ++i) {
      List<Expr> selectExprs = operands.get(i).getQueryStmt().getResultExprs();
      for (int j = 0; j < selectExprs.size(); ++j) {
        ColumnStats statsToAdd = ColumnStats.fromExpr(selectExprs.get(j));
        if (i == 0) {
          columnStats.add(statsToAdd);
        } else {
          columnStats.get(j).add(statsToAdd);
        }
      }
    }

    // Create tuple descriptor and slots.
    for (int i = 0; i < firstSelectExprs.size(); ++i) {
      Expr expr = firstSelectExprs.get(i);
      SlotDescriptor slotDesc = analyzer.getDescTbl().addSlotDescriptor(tupleDesc);
      slotDesc.setLabel(getColLabels().get(i));
      slotDesc.setType(expr.getType());
      slotDesc.setStats(columnStats.get(i));
      SlotRef slotRef = new SlotRef(slotDesc);
      resultExprs.add(slotRef);
      // Add to the substitution map so that column refs in "order by" can be resolved.
      if (orderByElements != null) {
        SlotRef aliasRef = new SlotRef(null, getColLabels().get(i));
        if (aliasSMap.lhs.contains(aliasRef)) {
          ambiguousAliasList.add(aliasRef);
        } else {
          aliasSMap.lhs.add(aliasRef);
          aliasSMap.rhs.add(slotRef);
        }
      }
    }
  }

  /**
   * Substitute exprs of the form "<number>" with the corresponding
   * expressions from the resultExprs.
   * @param exprs
   * @param errorPrefix
   * @throws AnalysisException
   */
  @Override
  protected void substituteOrdinals(List<Expr> exprs, String errorPrefix)
      throws AnalysisException {
    // Substitute ordinals.
    ListIterator<Expr> i = exprs.listIterator();
    while (i.hasNext()) {
      Expr expr = i.next();
      if (!(expr instanceof IntLiteral)) {
        continue;
      }
      long pos = ((IntLiteral) expr).getValue();
      if (pos < 1) {
        throw new AnalysisException(
            errorPrefix + ": ordinal must be >= 1: " + expr.toSql());
      }
      if (pos > resultExprs.size()) {
        throw new AnalysisException(
            errorPrefix + ": ordinal exceeds number of items in select list: "
            + expr.toSql());
      }
      // Create copy to protect against accidentally shared state.
      i.set(resultExprs.get((int) pos - 1).clone(null));
    }
  }

  public TupleId getTupleId() {
    return tupleId;
  }

  @Override
  public void getMaterializedTupleIds(ArrayList<TupleId> tupleIdList) {
    tupleIdList.add(tupleId);
  }

  @Override
  public String toSql() {
    StringBuilder strBuilder = new StringBuilder();
    Preconditions.checkState(operands.size() > 0);

    if (withClause != null) {
      strBuilder.append(withClause.toSql());
      strBuilder.append(" ");
    }

    strBuilder.append(operands.get(0).getQueryStmt().toSql());
    for (int i = 1; i < operands.size() - i; ++i) {
      strBuilder.append(" UNION " +
          ((operands.get(i).getQualifier() == Qualifier.ALL) ? "ALL " : ""));
      if (operands.get(i).getQueryStmt() instanceof UnionStmt) {
        strBuilder.append("(");
      }
      strBuilder.append(operands.get(i).getQueryStmt().toSql());
      if (operands.get(i).getQueryStmt() instanceof UnionStmt) {
        strBuilder.append(")");
      }
    }
    // Determine whether we need parenthesis around the last union operand.
    UnionOperand lastOperand = operands.get(operands.size() - 1);
    QueryStmt lastQueryStmt = lastOperand.getQueryStmt();
    strBuilder.append(" UNION " +
        ((lastOperand.getQualifier() == Qualifier.ALL) ? "ALL " : ""));
    if (lastQueryStmt instanceof UnionStmt ||
        ((hasOrderByClause() || hasLimitClause()) && !lastQueryStmt.hasLimitClause() &&
            !lastQueryStmt.hasOrderByClause())) {
      strBuilder.append("(");
      strBuilder.append(lastQueryStmt.toSql());
      strBuilder.append(")");
    } else {
      strBuilder.append(lastQueryStmt.toSql());
    }
    // Order By clause
    if (hasOrderByClause()) {
      strBuilder.append(" ORDER BY ");
      for (int i = 0; i < orderByElements.size(); ++i) {
        strBuilder.append(orderByElements.get(i).getExpr().toSql());
        strBuilder.append((sortInfo.getIsAscOrder().get(i)) ? " ASC" : " DESC");
        strBuilder.append((i+1 != orderByElements.size()) ? ", " : "");
      }
    }
    // Limit clause.
    if (hasLimitClause()) {
      strBuilder.append(" LIMIT ");
      strBuilder.append(limit);
    }
    return strBuilder.toString();
  }

  @Override
  public ArrayList<String> getColLabels() {
    Preconditions.checkState(operands.size() > 0);
    return operands.get(0).getQueryStmt().getColLabels();
  }

  @Override
  public QueryStmt clone() {
    List<UnionOperand> operandClones = Lists.newArrayList();
    for (UnionOperand operand: operands) {
      operandClones.add(operand.clone());
    }
    UnionStmt unionClone = new UnionStmt(operandClones, cloneOrderByElements(), limit);
    unionClone.setWithClause(cloneWithClause());
    return unionClone;
  }
}
