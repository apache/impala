package com.cloudera.impala.analysis;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;
import com.google.common.base.Preconditions;

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

    public UnionOperand(QueryStmt queryStmt, Qualifier qualifier) {
      this.queryStmt = queryStmt;
      this.qualifier = qualifier;
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
  }

  private final List<UnionOperand> operands;

  // Single tuple materialized by the union. Set in analyze().
  private TupleId tupleId;

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
  public void analyze(Analyzer analyzer)
      throws AnalysisException, InternalException {
    Preconditions.checkState(operands.size() > 0);

    // Propagates DISTINCT from left to right,
    propagateDistinct();

    // Make sure all operands return an equal number of exprs.
    QueryStmt firstQuery = operands.get(0).getQueryStmt();
    firstQuery.analyze(new Analyzer(analyzer));
    List<Expr> firstQueryExprs = firstQuery.getResultExprs();
    for (int i = 1; i < operands.size(); ++i) {
      QueryStmt query = operands.get(i).getQueryStmt();
      query.analyze(new Analyzer(analyzer));
      List<Expr> exprs = query.getResultExprs();
      if (firstQueryExprs.size() != exprs.size()) {
        throw new AnalysisException("Select blocks have unequal number of columns:\n" +
            "'" + firstQuery.toSql() + "' has " +
            firstQueryExprs.size() + " column(s)\n" +
            "'" + query.toSql() + "' has " + exprs.size() + " column(s)");
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
   *
   * @param analyzer
   * @throws AnalysisException
   */
  private void createTupleAndResultExprs(Analyzer analyzer) throws AnalysisException {
    // Create tuple descriptor for materialized tuple created by the union.
    TupleDescriptor tupleDesc = analyzer.getDescTbl().createTupleDescriptor();
    tupleDesc.setIsMaterialized(true);
    tupleId = tupleDesc.getId();
    // One slot per expr in the select blocks. Use first select block as representative.
    List<Expr> firstSelectExprs = operands.get(0).getQueryStmt().getResultExprs();
    for (int i = 0; i < firstSelectExprs.size(); ++i) {
      SlotDescriptor slotDesc = new SlotDescriptor(i, tupleDesc);
      slotDesc.setType(firstSelectExprs.get(i).getType());
      tupleDesc.addSlot(slotDesc);
      SlotRef slotRef = new SlotRef(slotDesc);
      resultExprs.add(slotRef);
      // Add to the substitution map so that column refs in "order by" can be resolved.
      if (orderByElements != null) {
        aliasSMap.lhs.add(new SlotRef(null, getColLabels().get(i)));
        aliasSMap.rhs.add(slotRef);
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
      i.set(resultExprs.get((int)pos - 1).clone());
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
}
