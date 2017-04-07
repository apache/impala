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

import org.apache.impala.catalog.ColumnStats;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.rewrite.ExprRewriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Representation of a union with its list of operands, and optional order by and limit.
 * A union materializes its results, and its resultExprs are SlotRefs into a new
 * materialized tuple.
 * During analysis, the operands are normalized (separated into a single sequence of
 * DISTINCT followed by a single sequence of ALL operands) and unnested to the extent
 * possible. This also creates the AggregationInfo for DISTINCT operands.
 *
 * Use of resultExprs vs. baseTblResultExprs:
 * We consistently use/cast the resultExprs of union operands because the final expr
 * substitution happens during planning. The only place where baseTblResultExprs are
 * used is in materializeRequiredSlots() because that is called before plan generation
 * and we need to mark the slots of resolved exprs as materialized.
 */
public class UnionStmt extends QueryStmt {
  private final static Logger LOG = LoggerFactory.getLogger(UnionStmt.class);

  public static enum Qualifier {
    ALL,
    DISTINCT
  }

  /**
   * Represents an operand to a union. It consists of a query statement and its left
   * all/distinct qualifier (null for the first operand).
   */
  public static class UnionOperand {
    // Effective qualifier. Should not be reset() to preserve changes made during
    // distinct propagation and unnesting that are needed after rewriting Subqueries.
    private Qualifier qualifier_;

    /////////////////////////////////////////
    // BEGIN: Members that need to be reset()

    private final QueryStmt queryStmt_;

    // Analyzer used for this operand. Set in analyze().
    // We must preserve the conjuncts registered in the analyzer for partition pruning.
    private Analyzer analyzer_;

    // Map from UnionStmt's result slots to our resultExprs. Used during plan generation.
    private final ExprSubstitutionMap smap_;

    // END: Members that need to be reset()
    /////////////////////////////////////////

    public UnionOperand(QueryStmt queryStmt, Qualifier qualifier) {
      queryStmt_ = queryStmt;
      qualifier_ = qualifier;
      smap_ = new ExprSubstitutionMap();
    }

    public void analyze(Analyzer parent) throws AnalysisException {
      if (isAnalyzed()) return;
      analyzer_ = new Analyzer(parent);
      queryStmt_.analyze(analyzer_);
    }

    public boolean isAnalyzed() { return analyzer_ != null; }
    public QueryStmt getQueryStmt() { return queryStmt_; }
    public Qualifier getQualifier() { return qualifier_; }
    // Used for propagating DISTINCT.
    public void setQualifier(Qualifier qualifier) { qualifier_ = qualifier; }
    public Analyzer getAnalyzer() { return analyzer_; }
    public ExprSubstitutionMap getSmap() { return smap_; }

    public boolean hasAnalyticExprs() {
      if (queryStmt_ instanceof SelectStmt) {
        return ((SelectStmt) queryStmt_).hasAnalyticInfo();
      } else {
        Preconditions.checkState(queryStmt_ instanceof UnionStmt);
        return ((UnionStmt) queryStmt_).hasAnalyticExprs();
      }
    }

    /**
     * C'tor for cloning.
     */
    private UnionOperand(UnionOperand other) {
      queryStmt_ = other.queryStmt_.clone();
      qualifier_ = other.qualifier_;
      analyzer_ = other.analyzer_;
      smap_ = other.smap_.clone();
    }

    public void reset() {
      queryStmt_.reset();
      analyzer_ = null;
      smap_.clear();
    }

    @Override
    public UnionOperand clone() { return new UnionOperand(this); }
  }

  /////////////////////////////////////////
  // BEGIN: Members that need to be reset()

  // before analysis, this contains the list of union operands derived verbatim
  // from the query;
  // after analysis, this contains all of distinctOperands followed by allOperands
  protected final List<UnionOperand> operands_;

  // filled during analyze(); contains all operands that need to go through
  // distinct aggregation
  protected final List<UnionOperand> distinctOperands_ = Lists.newArrayList();

  // filled during analyze(); contains all operands that can be aggregated with
  // a simple merge without duplicate elimination (also needs to merge the output
  // of the DISTINCT operands)
  protected final List<UnionOperand> allOperands_ = Lists.newArrayList();

  protected AggregateInfo distinctAggInfo_;  // only set if we have DISTINCT ops

 // Single tuple materialized by the union. Set in analyze().
  protected TupleId tupleId_;

  // set prior to unnesting
  protected String toSqlString_ = null;

  // true if any of the operands_ references an AnalyticExpr
  private boolean hasAnalyticExprs_ = false;

  // List of output expressions produced by the union without the ORDER BY portion
  // (if any). Same as resultExprs_ if there is no ORDER BY.
  private List<Expr> unionResultExprs_ = Lists.newArrayList();

  // END: Members that need to be reset()
  /////////////////////////////////////////

  public UnionStmt(List<UnionOperand> operands,
      ArrayList<OrderByElement> orderByElements, LimitElement limitElement) {
    super(orderByElements, limitElement);
    Preconditions.checkNotNull(operands);
    Preconditions.checkState(operands.size() > 0);
    operands_ = operands;
  }

  /**
   * C'tor for cloning.
   */
  protected UnionStmt(UnionStmt other) {
    super(other.cloneOrderByElements(),
        (other.limitElement_ == null) ? null : other.limitElement_.clone());
    operands_ = Lists.newArrayList();
    if (analyzer_ != null) {
      for (UnionOperand o: other.distinctOperands_) distinctOperands_.add(o.clone());
      for (UnionOperand o: other.allOperands_) allOperands_.add(o.clone());
      operands_.addAll(distinctOperands_);
      operands_.addAll(allOperands_);
    } else {
      for (UnionOperand operand: other.operands_) operands_.add(operand.clone());
    }
    analyzer_ = other.analyzer_;
    distinctAggInfo_ =
        (other.distinctAggInfo_ != null) ? other.distinctAggInfo_.clone() : null;
    tupleId_ = other.tupleId_;
    toSqlString_ = (other.toSqlString_ != null) ? new String(other.toSqlString_) : null;
    hasAnalyticExprs_ = other.hasAnalyticExprs_;
    withClause_ = (other.withClause_ != null) ? other.withClause_.clone() : null;
    unionResultExprs_ = Expr.cloneList(other.unionResultExprs_);
  }

  public List<UnionOperand> getOperands() { return operands_; }
  public List<UnionOperand> getDistinctOperands() { return distinctOperands_; }
  public boolean hasDistinctOps() { return !distinctOperands_.isEmpty(); }
  public List<UnionOperand> getAllOperands() { return allOperands_; }
  public boolean hasAllOps() { return !allOperands_.isEmpty(); }
  public AggregateInfo getDistinctAggInfo() { return distinctAggInfo_; }
  public boolean hasAnalyticExprs() { return hasAnalyticExprs_; }
  public TupleId getTupleId() { return tupleId_; }

  public void removeAllOperands() {
    operands_.removeAll(allOperands_);
    allOperands_.clear();
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed()) return;
    super.analyze(analyzer);

    // Propagates DISTINCT from right to left.
    propagateDistinct();

    // Analyze all operands and make sure they return an equal number of exprs.
    analyzeOperands(analyzer);

    // Remember the SQL string before unnesting operands.
    toSqlString_ = toSql();

    // Unnest the operands before casting the result exprs. Unnesting may add
    // additional entries to operands_ and the result exprs of those unnested
    // operands must also be cast properly.
    unnestOperands(analyzer);

    // Compute hasAnalyticExprs_
    hasAnalyticExprs_ = false;
    for (UnionOperand op: operands_) {
      if (op.hasAnalyticExprs()) {
        hasAnalyticExprs_ = true;
        break;
      }
    }

    // Collect all result expr lists and cast the exprs as necessary.
    List<List<Expr>> resultExprLists = Lists.newArrayList();
    for (UnionOperand op: operands_) {
      resultExprLists.add(op.getQueryStmt().getResultExprs());
    }
    analyzer.castToUnionCompatibleTypes(resultExprLists);

    // Create tuple descriptor materialized by this UnionStmt, its resultExprs, and
    // its sortInfo if necessary.
    createMetadata(analyzer);
    createSortInfo(analyzer);

    // Create unnested operands' smaps.
    for (UnionOperand operand: operands_) setOperandSmap(operand, analyzer);

    // Create distinctAggInfo, if necessary.
    if (!distinctOperands_.isEmpty()) {
      // Aggregate produces exactly the same tuple as the original union stmt.
      ArrayList<Expr> groupingExprs = Expr.cloneList(resultExprs_);
      try {
        distinctAggInfo_ =
            AggregateInfo.create(groupingExprs, null,
              analyzer.getDescTbl().getTupleDesc(tupleId_), analyzer);
      } catch (AnalysisException e) {
        // Should never happen.
        throw new IllegalStateException(
            "Error creating agg info in UnionStmt.analyze()", e);
      }
    }

    unionResultExprs_ = Expr.cloneList(resultExprs_);
    if (evaluateOrderBy_) createSortTupleInfo(analyzer);
    baseTblResultExprs_ = resultExprs_;
  }

  /**
   * Analyzes all operands and checks that they return an equal number of exprs.
   * Throws an AnalysisException if that is not the case, or if analyzing
   * an operand fails.
   */
  private void analyzeOperands(Analyzer analyzer) throws AnalysisException {
    for (int i = 0; i < operands_.size(); ++i) {
      operands_.get(i).analyze(analyzer);
      QueryStmt firstQuery = operands_.get(0).getQueryStmt();
      List<Expr> firstExprs = operands_.get(0).getQueryStmt().getResultExprs();
      QueryStmt query = operands_.get(i).getQueryStmt();
      List<Expr> exprs = query.getResultExprs();
      if (firstExprs.size() != exprs.size()) {
        throw new AnalysisException("Operands have unequal number of columns:\n" +
            "'" + queryStmtToSql(firstQuery) + "' has " +
            firstExprs.size() + " column(s)\n" +
            "'" + queryStmtToSql(query) + "' has " + exprs.size() + " column(s)");
      }
    }
  }

  /**
   * Marks the baseTblResultExprs of its operands as materialized, based on
   * which of the output slots have been marked.
   * Calls materializeRequiredSlots() on the operands themselves.
   */
  @Override
  public void materializeRequiredSlots(Analyzer analyzer) {
    TupleDescriptor tupleDesc = analyzer.getDescTbl().getTupleDesc(tupleId_);
    // to keep things simple we materialize all grouping exprs = output slots,
    // regardless of what's being referenced externally
    if (!distinctOperands_.isEmpty()) tupleDesc.materializeSlots();

    if (evaluateOrderBy_) sortInfo_.materializeRequiredSlots(analyzer, null);

    // collect operands' result exprs
    List<SlotDescriptor> outputSlots = tupleDesc.getSlots();
    List<Expr> exprs = Lists.newArrayList();
    for (int i = 0; i < outputSlots.size(); ++i) {
      SlotDescriptor slotDesc = outputSlots.get(i);
      if (!slotDesc.isMaterialized()) continue;
      for (UnionOperand op: operands_) {
        exprs.add(op.getQueryStmt().getBaseTblResultExprs().get(i));
      }
      if (distinctAggInfo_ != null) {
        // also mark the corresponding slot in the distinct agg tuple as being
        // materialized
        distinctAggInfo_.getOutputTupleDesc().getSlots().get(i).setIsMaterialized(true);
      }
    }
    materializeSlots(analyzer, exprs);

    for (UnionOperand op: operands_) {
      op.getQueryStmt().materializeRequiredSlots(analyzer);
    }
  }

  /**
   * Fill distinct-/allOperands and performs possible unnesting of UnionStmt
   * operands in the process.
   */
  private void unnestOperands(Analyzer analyzer) throws AnalysisException {
    if (operands_.size() == 1) {
      // ValuesStmt for a single row.
      allOperands_.add(operands_.get(0));
      return;
    }

    // find index of first ALL operand
    int firstUnionAllIdx = operands_.size();
    for (int i = 1; i < operands_.size(); ++i) {
      UnionOperand operand = operands_.get(i);
      if (operand.getQualifier() == Qualifier.ALL) {
        firstUnionAllIdx = (i == 1 ? 0 : i);
        break;
      }
    }
    // operands[0] is always implicitly ALL, so operands[1] can't be the
    // first one
    Preconditions.checkState(firstUnionAllIdx != 1);

    // unnest DISTINCT operands
    Preconditions.checkState(distinctOperands_.isEmpty());
    for (int i = 0; i < firstUnionAllIdx; ++i) {
      unnestOperand(distinctOperands_, Qualifier.DISTINCT, operands_.get(i));
    }

    // unnest ALL operands
    Preconditions.checkState(allOperands_.isEmpty());
    for (int i = firstUnionAllIdx; i < operands_.size(); ++i) {
      unnestOperand(allOperands_, Qualifier.ALL, operands_.get(i));
    }

    for (UnionOperand op: distinctOperands_) op.setQualifier(Qualifier.DISTINCT);
    for (UnionOperand op: allOperands_) op.setQualifier(Qualifier.ALL);

    operands_.clear();
    operands_.addAll(distinctOperands_);
    operands_.addAll(allOperands_);
  }

  /**
   * Sets the smap for the given operand. It maps from the output slots this union's
   * tuple to the corresponding result exprs of the operand.
   */
  private void setOperandSmap(UnionOperand operand, Analyzer analyzer) {
    TupleDescriptor tupleDesc = analyzer.getDescTbl().getTupleDesc(tupleId_);
    // operands' smaps were already set in the operands' analyze()
    operand.getSmap().clear();
    List<Expr> resultExprs = operand.getQueryStmt().getResultExprs();
    Preconditions.checkState(resultExprs.size() == tupleDesc.getSlots().size());
    for (int i = 0; i < tupleDesc.getSlots().size(); ++i) {
      SlotDescriptor outputSlot = tupleDesc.getSlots().get(i);
      // Map to the original (uncast) result expr of the operand.
      Expr origExpr = resultExprs.get(i).unwrapExpr(true).clone();
      operand.getSmap().put(new SlotRef(outputSlot), origExpr);
    }
  }

  /**
   * Add a single operand to the target list; if the operand itself is a UnionStmt, apply
   * unnesting to the extent possible (possibly modifying 'operand' in the process).
   */
  private void unnestOperand(
      List<UnionOperand> target, Qualifier targetQualifier, UnionOperand operand) {
    Preconditions.checkState(operand.isAnalyzed());
    QueryStmt queryStmt = operand.getQueryStmt();
    if (queryStmt instanceof SelectStmt) {
      target.add(operand);
      return;
    }

    Preconditions.checkState(queryStmt instanceof UnionStmt);
    UnionStmt unionStmt = (UnionStmt) queryStmt;
    if (unionStmt.hasLimit() || unionStmt.hasOffset()) {
      // we must preserve the nested Union
      target.add(operand);
    } else if (targetQualifier == Qualifier.DISTINCT || !unionStmt.hasDistinctOps()) {
      // there is no limit in the nested Union and we can absorb all of its
      // operands as-is
      target.addAll(unionStmt.getDistinctOperands());
      target.addAll(unionStmt.getAllOperands());
    } else {
      // the nested Union contains some Distinct ops and we're accumulating
      // into our All ops; unnest only the All ops and leave the rest in place
      target.addAll(unionStmt.getAllOperands());
      unionStmt.removeAllOperands();
      target.add(operand);
    }
  }

  /**
   * String representation of queryStmt used in reporting errors.
   * Allow subclasses to override this.
   */
  protected String queryStmtToSql(QueryStmt queryStmt) {
    return queryStmt.toSql();
  }

  /**
   * Propagates DISTINCT (if present) from right to left.
   * Implied associativity:
   * A UNION ALL B UNION DISTINCT C = (A UNION ALL B) UNION DISTINCT C
   * = A UNION DISTINCT B UNION DISTINCT C
   */
  private void propagateDistinct() {
    int lastDistinctPos = -1;
    for (int i = operands_.size() - 1; i > 0; --i) {
      UnionOperand operand = operands_.get(i);
      if (lastDistinctPos != -1) {
        // There is a DISTINCT somewhere to the right.
        operand.setQualifier(Qualifier.DISTINCT);
      } else if (operand.getQualifier() == Qualifier.DISTINCT) {
        lastDistinctPos = i;
      }
    }
  }

  /**
   * Create a descriptor for the tuple materialized by the union.
   * Set resultExprs to be slot refs into that tuple.
   * Also fills the substitution map, such that "order by" can properly resolve
   * column references from the result of the union.
   */
  private void createMetadata(Analyzer analyzer) throws AnalysisException {
    // Create tuple descriptor for materialized tuple created by the union.
    TupleDescriptor tupleDesc = analyzer.getDescTbl().createTupleDescriptor("union");
    tupleDesc.setIsMaterialized(true);
    tupleId_ = tupleDesc.getId();
    if (LOG.isTraceEnabled()) {
      LOG.trace("UnionStmt.createMetadata: tupleId=" + tupleId_.toString());
    }

    // One slot per expr in the select blocks. Use first select block as representative.
    List<Expr> firstSelectExprs = operands_.get(0).getQueryStmt().getResultExprs();

    // Compute column stats for the materialized slots from the source exprs.
    List<ColumnStats> columnStats = Lists.newArrayList();
    for (int i = 0; i < operands_.size(); ++i) {
      List<Expr> selectExprs = operands_.get(i).getQueryStmt().getResultExprs();
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
      SlotDescriptor slotDesc = analyzer.addSlotDescriptor(tupleDesc);
      slotDesc.setLabel(getColLabels().get(i));
      slotDesc.setType(expr.getType());
      slotDesc.setStats(columnStats.get(i));
      SlotRef outputSlotRef = new SlotRef(slotDesc);
      resultExprs_.add(outputSlotRef);

      // Add to aliasSMap so that column refs in "order by" can be resolved.
      if (orderByElements_ != null) {
        SlotRef aliasRef = new SlotRef(getColLabels().get(i));
        if (aliasSmap_.containsMappingFor(aliasRef)) {
          ambiguousAliasList_.add(aliasRef);
        } else {
          aliasSmap_.put(aliasRef, outputSlotRef);
        }
      }

      boolean isNullable = false;
      // register single-directional value transfers from output slot
      // to operands' result exprs (if those happen to be slotrefs);
      // don't do that if the operand computes analytic exprs
      // (see Planner.createInlineViewPlan() for the reasoning)
      for (UnionOperand op: operands_) {
        Expr resultExpr = op.getQueryStmt().getResultExprs().get(i);
        slotDesc.addSourceExpr(resultExpr);
        SlotRef slotRef = resultExpr.unwrapSlotRef(false);
        if (slotRef == null || slotRef.getDesc().getIsNullable()) isNullable = true;
        if (op.hasAnalyticExprs()) continue;
        slotRef = resultExpr.unwrapSlotRef(true);
        if (slotRef == null) continue;
        analyzer.registerValueTransfer(outputSlotRef.getSlotId(), slotRef.getSlotId());
      }
      // If all the child slots are not nullable, then the union output slot should not
      // be nullable as well.
      slotDesc.setIsNullable(isNullable);
    }
    baseTblResultExprs_ = resultExprs_;
  }

  @Override
  public void rewriteExprs(ExprRewriter rewriter) throws AnalysisException {
    for (UnionOperand op: operands_) op.getQueryStmt().rewriteExprs(rewriter);
    if (orderByElements_ != null) {
      for (OrderByElement orderByElem: orderByElements_) {
        orderByElem.setExpr(rewriter.rewrite(orderByElem.getExpr(), analyzer_));
      }
    }
  }

  @Override
  public void getMaterializedTupleIds(ArrayList<TupleId> tupleIdList) {
    // Return the sort tuple if there is an evaluated order by.
    if (evaluateOrderBy_) {
      tupleIdList.add(sortInfo_.getSortTupleDescriptor().getId());
    } else {
      tupleIdList.add(tupleId_);
    }
  }

  @Override
  public void collectTableRefs(List<TableRef> tblRefs, boolean fromClauseOnly) {
    super.collectTableRefs(tblRefs, fromClauseOnly);
    for (UnionOperand op: operands_) {
      op.getQueryStmt().collectTableRefs(tblRefs, fromClauseOnly);
    }
  }

  @Override
  public String toSql() {
    if (toSqlString_ != null) return toSqlString_;
    StringBuilder strBuilder = new StringBuilder();
    Preconditions.checkState(operands_.size() > 0);

    if (withClause_ != null) {
      strBuilder.append(withClause_.toSql());
      strBuilder.append(" ");
    }

    strBuilder.append(operands_.get(0).getQueryStmt().toSql());
    for (int i = 1; i < operands_.size() - 1; ++i) {
      strBuilder.append(" UNION " +
          ((operands_.get(i).getQualifier() == Qualifier.ALL) ? "ALL " : ""));
      if (operands_.get(i).getQueryStmt() instanceof UnionStmt) {
        strBuilder.append("(");
      }
      strBuilder.append(operands_.get(i).getQueryStmt().toSql());
      if (operands_.get(i).getQueryStmt() instanceof UnionStmt) {
        strBuilder.append(")");
      }
    }
    // Determine whether we need parenthesis around the last union operand.
    UnionOperand lastOperand = operands_.get(operands_.size() - 1);
    QueryStmt lastQueryStmt = lastOperand.getQueryStmt();
    strBuilder.append(" UNION " +
        ((lastOperand.getQualifier() == Qualifier.ALL) ? "ALL " : ""));
    if (lastQueryStmt instanceof UnionStmt ||
        ((hasOrderByClause() || hasLimit() || hasOffset()) &&
            !lastQueryStmt.hasLimit() && !lastQueryStmt.hasOffset() &&
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
      for (int i = 0; i < orderByElements_.size(); ++i) {
        strBuilder.append(orderByElements_.get(i).toSql());
        strBuilder.append((i+1 != orderByElements_.size()) ? ", " : "");
      }
    }
    // Limit clause.
    strBuilder.append(limitElement_.toSql());
    return strBuilder.toString();
  }

  @Override
  public List<String> getColLabels() {
    Preconditions.checkState(operands_.size() > 0);
    return operands_.get(0).getQueryStmt().getColLabels();
  }

  public List<Expr> getUnionResultExprs() { return unionResultExprs_; }

  @Override
  public UnionStmt clone() { return new UnionStmt(this); }

  /**
   * Undoes all changes made by analyze() except distinct propagation and unnesting.
   * After analysis, operands_ contains the list of unnested operands with qualifiers
   * adjusted to reflect distinct propagation. Every operand in that list is reset().
   * The distinctOperands_ and allOperands_ are cleared because they are redundant
   * with operands_.
   */
  @Override
  public void reset() {
    super.reset();
    for (UnionOperand op: operands_) op.reset();
    distinctOperands_.clear();
    allOperands_.clear();
    distinctAggInfo_ = null;
    tupleId_ = null;
    toSqlString_ = null;
    hasAnalyticExprs_ = false;
    unionResultExprs_.clear();
  }
}
