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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.ColumnStats;
import org.apache.impala.catalog.FeView;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.rewrite.ExprRewriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Representation of a set operation with its list of operands, and optional order by and
 * limit. A child class UnionStmt encapsulates instances that contain only Union operands.
 * EXCEPT, INTERSECT, and any combination of the previous with UNION is rewritten in
 * StmtRewriter to use joins in place of native operators.
 *
 * TODO: This class contains many methods and members that should be pushed down to
 * UnionStmt.
 *
 * A union materializes its results, and its resultExprs are SlotRefs into a new
 * materialized tuple. During analysis, the operands are normalized (separated into a
 * single sequence of DISTINCT followed by a single sequence of ALL operands) and unnested
 * to the extent possible. This also creates the AggregationInfo for UNION DISTINCT
 * operands. EXCEPT and INTERSECT are unnested during analysis, then rewritten discarding
 * the original instance of SetOperationStmt.
 *
 * Use of resultExprs vs. baseTblResultExprs: We consistently use/cast the resultExprs of
 * union operands because the final expr substitution happens during planning. The only
 * place where baseTblResultExprs are used is in materializeRequiredSlots() because that
 * is called before plan generation and we need to mark the slots of resolved exprs as
 * materialized.
 */
public class SetOperationStmt extends QueryStmt {
  private final static Logger LOG = LoggerFactory.getLogger(SetOperationStmt.class);

  public static enum Qualifier { ALL, DISTINCT }

  public static enum SetOperator { EXCEPT, INTERSECT, UNION }

  /**
   * Represents an operand to a set operation. It consists of a query statement, the set
   * operation to perform (except/intersect/union) and its left all/distinct qualifier
   * (null for the first operand).
   */
  public static class SetOperand {
    // Effective qualifier. Should not be reset() to preserve changes made during
    // distinct propagation and unnesting that are needed after rewriting Subqueries.
    private Qualifier qualifier_;

    private SetOperator operator_;
    /////////////////////////////////////////
    // BEGIN: Members that need to be reset()

    private QueryStmt queryStmt_;

    // Analyzer used for this operand. Set in analyze().
    // We must preserve the conjuncts registered in the analyzer for partition pruning.
    private Analyzer analyzer_;

    // Map from SetOperationStmt's result slots to our resultExprs. Used during plan
    // generation.
    private final ExprSubstitutionMap smap_;

    // END: Members that need to be reset()
    /////////////////////////////////////////

    public SetOperand(QueryStmt queryStmt, SetOperator operator, Qualifier qualifier) {
      queryStmt_ = queryStmt;
      operator_ = operator;
      qualifier_ = qualifier;
      smap_ = new ExprSubstitutionMap();
    }

    public void analyze(Analyzer parent) throws AnalysisException {
      if (isAnalyzed()) return;
      // Used to trigger a rewrite in StmtRewriter
      if (operator_ == SetOperator.INTERSECT || operator_ == SetOperator.EXCEPT) {
        parent.setSetOpNeedsRewrite();
      }
      analyzer_ = new Analyzer(parent);
      queryStmt_.analyze(analyzer_);
    }

    public boolean isAnalyzed() { return analyzer_ != null; }
    public QueryStmt getQueryStmt() { return queryStmt_; }
    public Qualifier getQualifier() { return qualifier_; }
    public SetOperator getSetOperator() { return operator_; }
    // Used for propagating DISTINCT.
    public void setQualifier(Qualifier qualifier) { qualifier_ = qualifier; }
    public void setSetOperator(SetOperator operator) { operator_ = operator; }
    public void setQueryStmt(QueryStmt stmt) { queryStmt_ = stmt; }
    public Analyzer getAnalyzer() { return analyzer_; }
    public ExprSubstitutionMap getSmap() { return smap_; }

    public boolean hasAnalyticExprs() {
      if (queryStmt_ instanceof SelectStmt) {
        return ((SelectStmt) queryStmt_).hasAnalyticInfo();
      } else {
        Preconditions.checkState(queryStmt_ instanceof SetOperationStmt);
        return ((SetOperationStmt) queryStmt_).hasAnalyticExprs();
      }
    }

    /**
     * C'tor for cloning.
     */
    private SetOperand(SetOperand other) {
      queryStmt_ = other.queryStmt_.clone();
      qualifier_ = other.qualifier_;
      operator_ = other.operator_;
      analyzer_ = other.analyzer_;
      smap_ = other.smap_.clone();
    }

    public void reset() {
      queryStmt_.reset();
      analyzer_ = null;
      smap_.clear();
    }

    @Override
    public SetOperand clone() {
      return new SetOperand(this);
    }
  }

  /////////////////////////////////////////
  // BEGIN: Members that need to be reset()

  // before analysis, this contains the list of operands derived verbatim from the query;
  // after analysis, this contains all of distinctOperands followed by allOperands
  protected final List<SetOperand> operands_;

  // filled during analyze(); contains all operands that need to go through
  // distinct aggregation
  protected final List<SetOperand> unionDistinctOperands_ = new ArrayList<>();

  // filled during analyze(); contains all operands that can be aggregated with
  // a simple merge without duplicate elimination (also needs to merge the output
  // of the DISTINCT operands)
  protected final List<SetOperand> unionAllOperands_ = new ArrayList<>();

  // filled during analyze(); contains all intersect distinct operands
  protected final List<SetOperand> intersectDistinctOperands_ = new ArrayList<>();

  // filled during analyze(); contains all except distinct operands
  protected final List<SetOperand> exceptDistinctOperands_ = new ArrayList<>();

  protected MultiAggregateInfo distinctAggInfo_; // only set if we have UNION DISTINCT ops

  // Single tuple materialized by the union. Set in analyze().
  protected TupleId tupleId_;

  // set prior to unnesting
  protected String toSqlString_ = null;

  // true if any of the operands_ references an AnalyticExpr
  private boolean hasAnalyticExprs_ = false;

  // List of output expressions produced by the union without the ORDER BY portion
  // (if any). Same as resultExprs_ if there is no ORDER BY.
  protected List<Expr> setOperationResultExprs_ = new ArrayList<>();

  // List of expressions produced by analyzer.castToSetOpCompatibleTypes().
  // Contains a list of exprs such that for every i-th expr in that list, it is the first
  // widest compatible expression encountered among all i-th exprs in every result expr
  // list of the operands.
  protected List<Expr> widestExprs_ = new ArrayList<>();

  // Holds the SelectStmt as a result of rewriting EXCEPT and INTERSECT. For cases where
  // all the operands are UNION this will remain null.
  protected QueryStmt rewrittenStmt_ = null;

  // END: Members that need to be reset()
  /////////////////////////////////////////

  public SetOperationStmt(List<SetOperand> operands, List<OrderByElement> orderByElements,
      LimitElement limitElement) {
    super(orderByElements, limitElement);
    Preconditions.checkNotNull(operands);
    Preconditions.checkState(operands.size() > 0);
    operands_ = operands;
  }

  /**
   * C'tor for cloning.
   */
  protected SetOperationStmt(SetOperationStmt other) {
    super(other.cloneOrderByElements(),
        (other.limitElement_ == null) ? null : other.limitElement_.clone());
    operands_ = new ArrayList<>();
    if (analyzer_ != null) {
      for (SetOperand o : other.unionDistinctOperands_) {
        unionDistinctOperands_.add(o.clone());
      }
      for (SetOperand o : other.unionAllOperands_) {
        unionAllOperands_.add(o.clone());
      }
      for (SetOperand o : other.exceptDistinctOperands_) {
        exceptDistinctOperands_.add(o.clone());
      }
      for (SetOperand o : other.intersectDistinctOperands_) {
        intersectDistinctOperands_.add(o.clone());
      }
    }
    for (SetOperand operand : other.operands_) operands_.add(operand.clone());
    analyzer_ = other.analyzer_;
    distinctAggInfo_ =
        (other.distinctAggInfo_ != null) ? other.distinctAggInfo_.clone() : null;
    tupleId_ = other.tupleId_;
    toSqlString_ = (other.toSqlString_ != null) ? new String(other.toSqlString_) : null;
    hasAnalyticExprs_ = other.hasAnalyticExprs_;
    withClause_ = (other.withClause_ != null) ? other.withClause_.clone() : null;
    setOperationResultExprs_ = Expr.cloneList(other.setOperationResultExprs_);
    widestExprs_ = other.widestExprs_;
    rewrittenStmt_ = other.rewrittenStmt_;
  }

  public static QueryStmt createUnionOrSetOperation(List<SetOperand> operands,
      List<OrderByElement> orderByElements, LimitElement limitElement) {
      boolean unionOnly = true;
      for (SetOperand op : operands) {
        if (op.getSetOperator() != null && op.getSetOperator() != SetOperator.UNION) {
          unionOnly = false;
          break;
        }
      }
      if (unionOnly) {
        return new UnionStmt(operands, orderByElements, limitElement);
      } else {
        return new SetOperationStmt(operands, orderByElements, limitElement);
      }
  }

  public List<SetOperand> getOperands() { return operands_; }
  public List<SetOperand> getUnionDistinctOperands() { return unionDistinctOperands_; }
  public boolean hasUnionDistinctOps() { return !unionDistinctOperands_.isEmpty(); }
  public List<SetOperand> getUnionAllOperands() { return unionAllOperands_; }
  public boolean hasUnionAllOps() { return !unionAllOperands_.isEmpty(); }
  public List<SetOperand> getExceptDistinctOperands() { return exceptDistinctOperands_; }
  public boolean hasExceptDistinctOps() { return !exceptDistinctOperands_.isEmpty(); }
  public List<SetOperand> getIntersectDistinctOperands() {
    return intersectDistinctOperands_;
  }
  public boolean hasIntersectDistinctOps() {
    return !intersectDistinctOperands_.isEmpty();
  }
  public boolean hasOnlyUnionOps() {
    return (hasUnionDistinctOps() || hasUnionAllOps()) && !hasIntersectDistinctOps()
        && !hasExceptDistinctOps();
  }
  public boolean hasOnlyUnionDistinctOps() {
    return hasUnionDistinctOps() && !hasUnionAllOps() && !hasIntersectDistinctOps()
        && !hasExceptDistinctOps();
  }
  public boolean hasOnlyUnionAllOps() {
    return hasUnionAllOps() && !hasUnionDistinctOps() && !hasIntersectDistinctOps()
        && !hasExceptDistinctOps();
  }
  public boolean hasOnlyIntersectDistinctOps() {
    return hasIntersectDistinctOps() && !hasUnionDistinctOps() && !hasUnionAllOps()
        && !hasExceptDistinctOps();
  }
  public boolean hasOnlyExceptDistinctOps() {
    return hasExceptDistinctOps() && !hasUnionDistinctOps() && !hasUnionAllOps()
        && !hasIntersectDistinctOps();
  }

  public MultiAggregateInfo getDistinctAggInfo() { return distinctAggInfo_; }
  public boolean hasAnalyticExprs() { return hasAnalyticExprs_; }
  public TupleId getTupleId() { return tupleId_; }
  public boolean hasRewrittenStmt() { return rewrittenStmt_ != null; }
  public QueryStmt getRewrittenStmt() { return rewrittenStmt_; }

  public void removeUnionAllOperands() {
    operands_.removeAll(unionAllOperands_);
    unionAllOperands_.clear();
  }

  @Override
  public boolean resolveTableMask(Analyzer analyzer) throws AnalysisException {
    boolean hasChanges = false;
    for (SetOperand op : operands_) {
      hasChanges |= op.getQueryStmt().resolveTableMask(analyzer);
    }
    return hasChanges;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed()) return;
    super.analyze(analyzer);

    final org.apache.impala.thrift.TQueryOptions query_options =
        analyzer.getQueryCtx().client_request.query_options;
    if (query_options.values_stmt_avoid_lossy_char_padding
        && query_options.allow_unsafe_casts) {
      throw new AnalysisException("Query options ALLOW_UNSAFE_CASTS and " +
          "VALUES_STMT_AVOID_LOSSY_CHAR_PADDING are not allowed to be set at the same " +
          "time if the query contains set operation(s).");
    }

    // Propagates DISTINCT from right to left.
    propagateDistinct();

    analyzeOperands(analyzer);

    // Remember the SQL string before unnesting operands.
    toSqlString_ = toSql();
    if (origSqlString_ == null) origSqlString_ = toSqlString_;

    // Unnest the operands before casting the result exprs. Unnesting may add
    // additional entries to operands_ and the result exprs of those unnested
    // operands must also be cast properly.
    unnestOperands(analyzer);

    // Compute hasAnalyticExprs_
    hasAnalyticExprs_ = false;
    for (SetOperand op : operands_) {
      if (op.hasAnalyticExprs()) {
        hasAnalyticExprs_ = true;
        break;
      }
    }

    // Collect all result expr lists and cast the exprs as necessary.
    List<List<Expr>> resultExprLists = new ArrayList<>();
    for (SetOperand op : operands_) {
      resultExprLists.add(op.getQueryStmt().getResultExprs());
    }
    widestExprs_ = analyzer.castToSetOpCompatibleTypes(resultExprLists,
        shouldAvoidLossyCharPadding(analyzer));
    // TODO (IMPALA-11018): Currently only UNION ALL is supported for collection types
    //       due to missing handling in BE.
    if (!hasOnlyUnionAllOps()) {
      for (Expr expr: widestExprs_) {
        Preconditions.checkState(!expr.getType().isCollectionType(),
            "UNION, EXCEPT and INTERSECT are not supported for collection types");
      }
    }

    // Create tuple descriptor materialized by this UnionStmt, its resultExprs, and
    // its sortInfo if necessary.
    createMetadata(analyzer);
    createSortInfo(analyzer);

    // Create unnested operands' smaps.
    for (SetOperand operand : operands_) setOperandSmap(operand, analyzer);

    // Create distinctAggInfo, if necessary.
    if (!unionDistinctOperands_.isEmpty()) {
      // Aggregate produces exactly the same tuple as the original union stmt.
      List<Expr> groupingExprs = Expr.cloneList(resultExprs_);
      try {
        distinctAggInfo_ = MultiAggregateInfo.createDistinct(
            groupingExprs, analyzer.getTupleDesc(tupleId_), analyzer);
      } catch (AnalysisException e) {
        // Should never happen.
        throw new IllegalStateException(
            "Error creating agg info in SetOperationStmt.analyze()", e);
      }
    }

    setOperationResultExprs_ = Expr.cloneList(resultExprs_);
    if (evaluateOrderBy_) createSortTupleInfo(analyzer);
    baseTblResultExprs_ = resultExprs_;
  }

  /**
   * If all values in a column are CHARs but they have different lengths, the common type
   * will normally be the CHAR type of the greatest length, in which case other CHAR
   * values are padded; this function decides whether this should be avoided by using
   * VARCHAR as the common type. See IMPALA-10753.
   *
   * The default behaviour is returning false, subclasses can override it.
   */
  protected boolean shouldAvoidLossyCharPadding(Analyzer analyzer) {
    return false;
  }

  /**
   * Analyzes all operands and checks that they return an equal number of exprs.
   * Throws an AnalysisException if that is not the case, or if analyzing
   * an operand fails.
   */
  protected void analyzeOperands(Analyzer analyzer) throws AnalysisException {
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
    if (!unionDistinctOperands_.isEmpty()) tupleDesc.materializeSlots();

    if (evaluateOrderBy_) sortInfo_.materializeRequiredSlots(analyzer, null);

    // collect operands' result exprs
    List<SlotDescriptor> outputSlots = tupleDesc.getSlots();
    List<Expr> exprs = new ArrayList<>();
    for (int i = 0; i < outputSlots.size(); ++i) {
      SlotDescriptor slotDesc = outputSlots.get(i);
      if (!slotDesc.isMaterialized()) continue;
      for (SetOperand op : operands_) {
        exprs.add(op.getQueryStmt().getBaseTblResultExprs().get(i));
      }
    }
    if (distinctAggInfo_ != null) {
      distinctAggInfo_.materializeRequiredSlots(analyzer, null);
    }
    materializeSlots(analyzer, exprs);

    for (SetOperand op : operands_) {
      op.getQueryStmt().materializeRequiredSlots(analyzer);
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
  protected void propagateDistinct() {
    int lastDistinctPos = -1;
    for (int i = operands_.size() - 1; i > 0; --i) {
      SetOperand operand = operands_.get(i);
      if (lastDistinctPos != -1) {
        // There is a DISTINCT somewhere to the right.
        operand.setQualifier(Qualifier.DISTINCT);
      } else if (operand.getQualifier() == Qualifier.DISTINCT) {
        lastDistinctPos = i;
      }
    }
  }

  @Override
  public void rewriteExprs(ExprRewriter rewriter) throws AnalysisException {
    // No rewrites are needed for distinctAggInfo_, resultExprs_, or baseTblResultExprs_
    // as those Exprs are simply SlotRefs.
    for (SetOperand op : operands_) op.getQueryStmt().rewriteExprs(rewriter);
    if (orderByElements_ != null) {
      for (OrderByElement orderByElem : orderByElements_) {
        orderByElem.setExpr(rewriter.rewrite(orderByElem.getExpr(), analyzer_));
      }
    }
  }

  @Override
  public void getMaterializedTupleIds(List<TupleId> tupleIdList) {
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
    for (SetOperand op : operands_) {
      op.getQueryStmt().collectTableRefs(tblRefs, fromClauseOnly);
    }
  }

  @Override
  public void collectInlineViews(Set<FeView> inlineViews) {
    super.collectInlineViews(inlineViews);
    for (SetOperand operand : operands_) {
      operand.getQueryStmt().collectInlineViews(inlineViews);
    }
  }

  @Override
  public String toSql(ToSqlOptions options) {
    if (!options.showRewritten() && toSqlString_ != null) return toSqlString_;

    StringBuilder strBuilder = new StringBuilder();
    Preconditions.checkState(operands_.size() > 0);

    if (withClause_ != null) {
      strBuilder.append(withClause_.toSql(options));
      strBuilder.append(" ");
    }

    operandsToSql(options, strBuilder);

    // Order By clause
    if (hasOrderByClause()) {
      strBuilder.append(" ORDER BY ");
      for (int i = 0; i < orderByElements_.size(); ++i) {
        strBuilder.append(orderByElements_.get(i).toSql(options));
        strBuilder.append((i + 1 != orderByElements_.size()) ? ", " : "");
      }
    }
    // Limit clause.
    strBuilder.append(limitElement_.toSql(options));
    return strBuilder.toString();
  }

  private void operandsToSql(ToSqlOptions options, StringBuilder strBuilder) {
    strBuilder.append(operands_.get(0).getQueryStmt().toSql(options));

    // If there is only one operand we simply print it without any mention of a set
    // operator. It is only possible in a 'ValuesStmt' - otherwise it is syntactically
    // impossible in SQL.
    if (operands_.size() == 1) return;

    for (int i = 1; i < operands_.size() - 1; ++i) {
      String opName = operands_.get(i).getSetOperator() != null ?
          operands_.get(i).getSetOperator().name() :
          "UNION";
      strBuilder.append(" " + opName + " "
          + ((operands_.get(i).getQualifier() == Qualifier.ALL) ? "ALL " : ""));
      if (operands_.get(i).getQueryStmt() instanceof SetOperationStmt) {
        strBuilder.append("(");
      }
      strBuilder.append(operands_.get(i).getQueryStmt().toSql(options));
      if (operands_.get(i).getQueryStmt() instanceof SetOperationStmt) {
        strBuilder.append(")");
      }
    }

    // Determine whether we need parentheses around the last union operand.
    SetOperand lastOperand = operands_.get(operands_.size() - 1);
    QueryStmt lastQueryStmt = lastOperand.getQueryStmt();
    strBuilder.append(" " + lastOperand.getSetOperator().name() + " "
        + ((lastOperand.getQualifier() == Qualifier.ALL) ? "ALL " : ""));
    if (lastQueryStmt instanceof SetOperationStmt
        || ((hasOrderByClause() || hasLimit() || hasOffset()) && !lastQueryStmt.hasLimit()
               && !lastQueryStmt.hasOffset() && !lastQueryStmt.hasOrderByClause())) {
      strBuilder.append("(");
      strBuilder.append(lastQueryStmt.toSql(options));
      strBuilder.append(")");
    } else {
      strBuilder.append(lastQueryStmt.toSql(options));
    }
  }

  @Override
  public List<String> getColLabels() {
    Preconditions.checkState(operands_.size() > 0);
    return operands_.get(0).getQueryStmt().getColLabels();
  }

  public List<Expr> getSetOperationResultExprs() { return setOperationResultExprs_; }

  public List<Expr> getWidestExprs() { return widestExprs_; }

  @Override
  public SetOperationStmt clone() { return new SetOperationStmt(this); }

  /**
   * Undoes all changes made by analyze() except distinct propagation and unnesting. After
   * analysis, operands_ contains the list of unnested operands with qualifiers adjusted
   * to reflect distinct propagation. Every operand in that list is reset(). The
   * unionDistinctOperands_ and unionAllOperands_ are cleared because they are redundant
   * with operands_.
   */
  @Override
  public void reset() {
    super.reset();
    for (SetOperand op : operands_) op.reset();
    unionDistinctOperands_.clear();
    unionAllOperands_.clear();
    intersectDistinctOperands_.clear();
    exceptDistinctOperands_.clear();
    distinctAggInfo_ = null;
    tupleId_ = null;
    toSqlString_ = null;
    hasAnalyticExprs_ = false;
    setOperationResultExprs_.clear();
    widestExprs_ = null;
    rewrittenStmt_ = null;
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
      LOG.trace("SetOperationStmt.createMetadata: tupleId=" + tupleId_.toString());
    }

    // One slot per expr in the select blocks. Use first select block as representative.
    List<Expr> firstSelectExprs = operands_.get(0).getQueryStmt().getResultExprs();

    // Compute column stats for the materialized slots from the source exprs.
    List<ColumnStats> columnStats = new ArrayList<>();
    List<Set<Column>> sourceColumns = new ArrayList<>();
    for (int i = 0; i < operands_.size(); ++i) {
      List<Expr> selectExprs = operands_.get(i).getQueryStmt().getResultExprs();
      for (int j = 0; j < selectExprs.size(); ++j) {
        if (i == 0) {
          ColumnStats statsToAdd = ColumnStats.fromExpr(selectExprs.get(j));
          columnStats.add(statsToAdd);
          sourceColumns.add(new HashSet<>());
        } else {
          ColumnStats statsToAdd = columnStats.get(j).hasNumDistinctValues() ?
              ColumnStats.fromExpr(selectExprs.get(j), sourceColumns.get(j)) :
              ColumnStats.fromExpr(selectExprs.get(j));
          columnStats.get(j).add(statsToAdd);
        }

        if (columnStats.get(j).hasNumDistinctValues()) {
          // Collect expr columns to keep ndv low in later stats addition.
          SlotRef slotRef = selectExprs.get(j).unwrapSlotRef(false);
          if (slotRef != null && slotRef.hasDesc()) {
            slotRef.getDesc().collectColumns(sourceColumns.get(j));
          }
        }
      }
    }

    // Create tuple descriptor and slots.
    for (int i = 0; i < firstSelectExprs.size(); ++i) {
      Expr expr = firstSelectExprs.get(i);
      SlotDescriptor slotDesc = analyzer.addSlotDescriptor(tupleDesc);
      slotDesc.setLabel(getColLabels().get(i));
      slotDesc.setType(expr.getType());
      if (expr.getType().isCollectionType()) {
        slotDesc.setItemTupleDesc(((SlotRef)expr).getDesc().getItemTupleDesc());
      }
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
      for (SetOperand op : operands_) {
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

  /**
   * Fill distinct-/all Operands and performs possible unnesting of SetOperationStmt
   * operands in the process.
   */
  private void unnestOperands(Analyzer analyzer) throws AnalysisException {
    if (operands_.size() == 1) {
      // ValuesStmt for a single row.
      unionAllOperands_.add(operands_.get(0));
      return;
    }

    // find index of first ALL operand
    int firstUnionAllIdx = operands_.size();
    for (int i = 1; i < operands_.size(); ++i) {
      SetOperand operand = operands_.get(i);
      if (operand.getQualifier() == Qualifier.ALL) {
        firstUnionAllIdx = (i == 1 ? 0 : i);
        break;
      }
    }

    List<SetOperand> localOps = new ArrayList<>();
    List<SetOperand> tempOps = new ArrayList<>();
    // operands[0] is always implicitly ALL, so operands[1] can't be the
    // first one
    Preconditions.checkState(firstUnionAllIdx != 1);

    // unnest DISTINCT operands
    for (int i = 0; i < firstUnionAllIdx; ++i) {
      tempOps.clear();
      SetOperator op = operands_.get(i).getSetOperator();
      if (op == null || op == SetOperator.UNION) {
        // It is safe to handle the first operand here as we know the first ALL index
        // isn't 1 therefore any union can be absorbed.
        unnestOperand(tempOps, Qualifier.DISTINCT, operands_.get(i));
        localOps.addAll(tempOps);
        // If the first operand isn't unnested we treat it as distinct
        unionDistinctOperands_.addAll(tempOps);
      } else if (op == SetOperator.EXCEPT) {
        unnestOperand(tempOps, Qualifier.DISTINCT, operands_.get(i));
        localOps.addAll(tempOps);
        exceptDistinctOperands_.addAll(tempOps);
      } else if (op == SetOperator.INTERSECT) {
        unnestOperand(tempOps, Qualifier.DISTINCT, operands_.get(i));
        localOps.addAll(tempOps);
        intersectDistinctOperands_.addAll(tempOps);
      } else {
          throw new AnalysisException("Invalid operand in SetOperationStmt: " +
              queryStmtToSql(this)); }
    }

    // unnest ALL operands
    for (int i = firstUnionAllIdx; i < operands_.size(); ++i) {
      tempOps.clear();
      unnestOperand(tempOps, Qualifier.ALL, operands_.get(i));
      localOps.addAll(tempOps);
      unionAllOperands_.addAll(tempOps);
    }

    for (SetOperand op : unionDistinctOperands_) {
      op.setSetOperator(SetOperator.UNION);
      op.setQualifier(Qualifier.DISTINCT);
    }
    for (SetOperand op : intersectDistinctOperands_) {
      op.setSetOperator(SetOperator.INTERSECT);
      op.setQualifier(Qualifier.DISTINCT);
    }
    for (SetOperand op : exceptDistinctOperands_) {
      op.setSetOperator(SetOperator.EXCEPT);
      op.setQualifier(Qualifier.DISTINCT);
    }
    for (SetOperand op : unionAllOperands_) {
      op.setSetOperator(SetOperator.UNION);
      op.setQualifier(Qualifier.ALL);
    }

    operands_.clear();
    operands_.addAll(localOps);
  }

  /**
   * Add a single operand to the target list; if the operand itself is a SetOperationStmt,
   * apply unnesting to the extent possible (possibly modifying 'operand' in the process).
   *
   * Absorb means convert qualifier into target type, this applies to ALL -> DISTINCT,
   * currently only done with UNIQUE propagateDistinct() ensures ALL operands are always
   * the rightmost, therefore they can be pulled out and absorbed into DISTINCT or just
   * added to the outer ALL.
   *
   * EXCEPT is never unnested.
   * (E E) E -> (E E) E
   * INTERSECT is unnested unless first
   * (I) I I -> (I) I I
   * I (I) I -> I I I
   * I (I (I I)) -> I I I I
   * UNION ALL plucks UNION ALL out
   * (UD UD UA) UA UA -> (UD UD) UA UA UA
   * UNION DISTINCT absorbs other unions.
   * (UD UD UA) UD UA -> UD UD UD UD UA
   * (UA UA UA) UA UA -> UA UA UA UA UA
   * (UA UA UA) UD UD -> UD UD UD UD UD
   *
   */
  private void unnestOperand(
      List<SetOperand> target, Qualifier targetQualifier, SetOperand operand) {
    Preconditions.checkState(operand.isAnalyzed());
    QueryStmt queryStmt = operand.getQueryStmt();
    if (queryStmt instanceof SelectStmt) {
      target.add(operand);
      return;
    }

    Preconditions.checkState(queryStmt instanceof SetOperationStmt);
    SetOperationStmt stmt = (SetOperationStmt) queryStmt;
    if (stmt.hasLimit() || stmt.hasOffset()) {
      // we must preserve the nested Set Operation
      target.add(operand);
      return;
    }

    // 1. Unnest INTERSECT only if the nested statement contains only INTERSECTs.
    // 2. Union distinct will absorb UNION DISTNCT and UNION ALL if no INTERSECT / EXCEPT
    // are present
    // 3. Union ALL will always be to the right of any DISTINCT, so we can unnest ALL if
    // the target is ALL
    // 4. For the first operand with a distinct target, we unnest and absorb only when
    // UNIONs are the only operator. All INTERSECT in the first operand aren't unnested,
    // this doesn't affect correctness, it's just a simplification.
    SetOperator targetOperator = operand.getSetOperator();
    if (targetOperator == SetOperator.INTERSECT && stmt.hasOnlyIntersectDistinctOps()) {
      target.addAll(stmt.getIntersectDistinctOperands());
    } else if (targetOperator == SetOperator.EXCEPT && stmt.hasOnlyExceptDistinctOps()) {
      // EXCEPT should not be unnested
      target.add(operand);
    } else if (targetOperator == SetOperator.UNION && stmt.hasOnlyUnionOps()) {
      if (targetQualifier == Qualifier.DISTINCT || !stmt.hasUnionDistinctOps()) {
        target.addAll(stmt.getUnionDistinctOperands());
        target.addAll(stmt.getUnionAllOperands());
      } else {
        target.addAll(stmt.getUnionAllOperands());
        stmt.removeUnionAllOperands();
        target.add(operand);
      }
    // Special cases for the first operand.
    } else if (targetOperator == null && targetQualifier == Qualifier.ALL) {
      // Case 3
      target.addAll(stmt.getUnionAllOperands());
      if (stmt.hasUnionDistinctOps() || stmt.hasExceptDistinctOps()
          || stmt.hasIntersectDistinctOps()) {
        stmt.removeUnionAllOperands();
        target.add(operand);
      }
    } else if (targetOperator == null && targetQualifier == Qualifier.DISTINCT) {
      // Case 4
      if (stmt.hasOnlyUnionOps()) {
        target.addAll(stmt.getUnionDistinctOperands());
        target.addAll(stmt.getUnionAllOperands());
      } else {
        target.add(operand);
      }
    } else {
      // Mixed operators are not safe to unnest
      target.add(operand);
    }
  }

  /**
   * Sets the smap for the given operand. It maps from the output slots this union's
   * tuple to the corresponding result exprs of the operand.
   */
  private void setOperandSmap(SetOperand operand, Analyzer analyzer) {
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
}
