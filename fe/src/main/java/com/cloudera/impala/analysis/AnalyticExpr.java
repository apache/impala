// Copyright 2014 Cloudera Inc.
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

import java.util.List;

import com.cloudera.impala.catalog.AggregateFunction;
import com.cloudera.impala.catalog.Function;
import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.TreeNode;
import com.cloudera.impala.service.FeSupport;
import com.cloudera.impala.thrift.TColumnValue;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.util.TColumnValueUtil;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Representation of an analytic function with OVER clause.
 * All "subexpressions" (such as the actual function call as well as the
 * partition/ordering exprs, etc.) are embedded as children in order to allow expr
 * substitution:
 *   function call: child 0
 *   partition exprs: children 1 .. numPartitionExprs_
 *   ordering exprs:
 *     children numPartitionExprs_ + 1 .. numPartitionExprs_ + orderByElements_.size()
 *   exprs in windowing clause: remaining children
 *
 * TODO: standardize on canonical format:
 * - map lead() to lag() by reversing ordering
 * - ...
 */
public class AnalyticExpr extends Expr {
  // These elements are modified to point to the corresponding child exprs to keep them
  // in sync through expr substitutions.
  private final List<OrderByElement> orderByElements_ = Lists.newArrayList();
  private final int numPartitionExprs_;
  private final AnalyticWindow window_;

  private static String LEAD = "lead";
  private static String LAG = "lag";
  private static String RANK = "rank";
  private static String DENSERANK = "dense_rank";
  private static String ROWNUMBER = "row_number";

  public AnalyticExpr(FunctionCallExpr fnCall, List<Expr> partitionExprs,
      List<OrderByElement> orderByElements, AnalyticWindow window) {
    Preconditions.checkNotNull(fnCall);
    addChild(fnCall);
    numPartitionExprs_ = partitionExprs != null ? partitionExprs.size() : 0;
    if (numPartitionExprs_ > 0) addChildren(partitionExprs);
    if (orderByElements != null) {
      for (OrderByElement e: orderByElements) {
        addChild(e.getExpr());
      }
      orderByElements_.addAll(orderByElements);
      // Point the order-by exprs to our children to avoid analyzing them separately.
      setOrderByExprs();
    }
    window_ = window;
  }

  /**
   * clone() c'tor
   */
  protected AnalyticExpr(AnalyticExpr other) {
    super(other);
    for (OrderByElement e: other.orderByElements_) {
      orderByElements_.add(e.clone());
    }
    numPartitionExprs_ = other.numPartitionExprs_;
    window_ = (other.window_ != null ? other.window_.clone() : null);
  }

  public FunctionCallExpr getFnCall() { return (FunctionCallExpr) getChild(0); }
  public List<Expr> getPartitionExprs() {
    return children_.subList(1, numPartitionExprs_ + 1);
  }
  public List<OrderByElement> getOrderByElements() { return orderByElements_; }
  public List<Expr> getOrderByExprs() {
    List<Expr> result = Lists.newArrayList();
    for (OrderByElement e: orderByElements_) {
      result.add(e.getExpr());
    }
    return result;
  }

  /**
  * Returns the explicit or implicit window of this AnalyticExpr. Returns the default
  * window if an order by was specified without a window clause. A null return value
  * implies that no order by was specified (and hence, no window).
  */
  public AnalyticWindow getWindow() {
    if (!orderByElements_.isEmpty() && window_ == null) {
      return AnalyticWindow.DEFAULT_WINDOW;
    }
    return window_;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) return false;
    AnalyticExpr o = (AnalyticExpr)obj;
    if ((window_ == null) != (o.window_ == null)) return false;
    if (window_ != null) {
      if (!window_.equals(o.window_)) return false;
    }
    return orderByElements_.equals(o.orderByElements_);
  }

  /**
   * Analytic exprs cannot be constant.
   */
  @Override
  public boolean isConstant() { return false; }

  @Override
  public Expr clone() { return new AnalyticExpr(this); }

  @Override
  public String toSqlImpl() {
    StringBuilder sb = new StringBuilder();
    sb.append(getFnCall().toSql()).append(" OVER (");
    boolean needsSpace = false;
    if (numPartitionExprs_ > 0) {
      sb.append("PARTITION BY ").append(Expr.toSql(getPartitionExprs()));
      needsSpace = true;
    }
    if (!orderByElements_.isEmpty()) {
      List<String> orderByStrings = Lists.newArrayList();
      for (OrderByElement e: orderByElements_) {
        orderByStrings.add(e.toSql());
      }
      if (needsSpace) sb.append(" ");
      sb.append("ORDER BY ").append(Joiner.on(", ").join(orderByStrings));
      needsSpace = true;
    }
    if (window_ != null) {
      if (needsSpace) sb.append(" ");
      sb.append(window_.toSql());
    }
    sb.append(")");
    return sb.toString();
  }

  @Override
  public String debugString() {
    return Objects.toStringHelper(this)
        .add("fn", getFnCall())
        .add("window", window_)
        .addValue(super.debugString())
        .toString();
  }

  @Override
  protected void toThrift(TExprNode msg) {
  }

  public static boolean isAnalyticFn(Function fn) {
    return fn instanceof AggregateFunction
        && ((AggregateFunction) fn).needsAnalyticExpr();
  }

  static private boolean isOffsetFn(Function fn) {
    if (!isAnalyticFn(fn)) return false;
    return fn.functionName().equals(LEAD) || fn.functionName().equals(LAG);
  }

  static private boolean isRankingFn(Function fn) {
    if (!isAnalyticFn(fn)) return false;
    return fn.functionName().equals(RANK)
        || fn.functionName().equals(DENSERANK)
        || fn.functionName().equals(ROWNUMBER);
  }

  /**
   * Checks that the value expr of an offset boundary of a RANGE window is compatible
   * with orderingExprs (and that there's only a single ordering expr).
   */
  private void checkRangeOffsetBoundaryExpr(AnalyticWindow.Boundary boundary)
      throws AnalysisException {
    Preconditions.checkState(boundary.getType().isOffset());
    if (orderByElements_.size() > 1) {
      throw new AnalysisException("Only one ORDER BY expression allowed if used with "
          + "a RANGE window with PRECEDING/FOLLOWING: " + toSql());
    }
    Expr rangeExpr = boundary.getExpr();
    if (!Type.isImplicitlyCastable(
        rangeExpr.getType(), orderByElements_.get(0).getExpr().getType())) {
      throw new AnalysisException(
          "The value expression of a PRECEDING/FOLLOWING clause of a RANGE window must "
            + "be implicitly convertable to the ORDER BY expression's type: "
            + rangeExpr.toSql() + " cannot be implicitly converted to "
            + orderByElements_.get(0).getExpr().getType().toSql());
    }
  }

  /**
   * Checks offset of lag()/lead().
   */
  void checkOffset(Analyzer analyzer) throws AnalysisException {
    Preconditions.checkState(isOffsetFn(getFnCall().getFn()));
    Preconditions.checkState(getFnCall().getChildren().size() > 1);
    Expr offset = getFnCall().getChild(1);
    Preconditions.checkState(offset.getType().isIntegerType());
    boolean isPosConstant = true;
    if (!offset.isConstant()) {
      isPosConstant = false;
    } else {
      try {
        TColumnValue val = FeSupport.EvalConstExpr(offset, analyzer.getQueryCtx());
        if (TColumnValueUtil.getNumericVal(val) <= 0) isPosConstant = false;
      } catch (InternalException exc) {
        throw new AnalysisException(
            "Couldn't evaluate LEAD/LAG offset: " + exc.getMessage());
      }
    }
    if (!isPosConstant) {
      throw new AnalysisException(
          "The offset parameter of LEAD/LAG must be a constant positive integer: "
            + getFnCall().toSql());
    }
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed_) return;
    super.analyze(analyzer);
    type_ = getFnCall().getType();

    if (getFnCall().getParams().isDistinct()) {
      throw new AnalysisException(
          "DISTINCT not allowed in analytic function: " + getFnCall().toSql());
    }

    // check for correct composition of analytic expr
    Function fn = getFnCall().getFn();
    if (!(fn instanceof AggregateFunction)) {
        throw new AnalysisException(
            "OVER clause requires aggregate or analytic function: "
              + getFnCall().toSql());
    }

    if (isAnalyticFn(fn)) {
      if (orderByElements_.isEmpty()) {
        throw new AnalysisException(
            "'" + getFnCall().toSql() + "' requires an ORDER BY clause");
      }
      if ((isRankingFn(fn) || isOffsetFn(fn)) && window_ != null) {
        throw new AnalysisException(
            "Windowing clause not allowed with '" + getFnCall().toSql() + "'");
      }
      if (isOffsetFn(fn) && getFnCall().getChildren().size() > 1) checkOffset(analyzer);
    }

    if (window_ != null) {
      if (orderByElements_.isEmpty()) {
        throw new AnalysisException("Windowing clause requires ORDER BY clause: "
            + toSql());
      }
      window_.analyze(analyzer);

      // update children
      Preconditions.checkNotNull(window_.getLeftBoundary());
      if (window_.getLeftBoundary().getExpr() != null) {
        addChild(window_.getLeftBoundary().getExpr());
      }
      if (window_.getRightBoundary() != null
          && window_.getRightBoundary().getExpr() != null) {
        addChild(window_.getRightBoundary().getExpr());
      }

      if (!orderByElements_.isEmpty() &&
          window_.getType() == AnalyticWindow.Type.RANGE) {
        // check that preceding/following ranges match ordering
        if (window_.getLeftBoundary().getType().isOffset()) {
          checkRangeOffsetBoundaryExpr(window_.getLeftBoundary());
        }
        if (window_.getRightBoundary() != null
            && window_.getRightBoundary().getType().isOffset()) {
          checkRangeOffsetBoundaryExpr(window_.getRightBoundary());
        }
      }
    }

    // check nesting
    if (TreeNode.contains(getChildren(), AnalyticExpr.class)) {
      throw new AnalysisException(
          "Nesting of analytic expressions is not allowed: " + toSql());
    }
  }

  /**
   * Point the order by elements to the corresponding children because they
   * may have been substituted and/or analyzed.
   */
  private void setOrderByExprs() {
    for (int i = 0; i < orderByElements_.size(); ++i) {
      Expr childExpr = getChild(numPartitionExprs_ + 1 + i);
      orderByElements_.get(i).setExpr(childExpr);
    }
  }

  @Override
  protected void resetAnalysisState() {
    super.resetAnalysisState();
    isAnalyzed_ = false;
    // remove window clause exprs added as children in analyze()
    children_.subList(
        1 + numPartitionExprs_ + orderByElements_.size(), children_.size()).clear();
  }

  @Override
  protected Expr substituteImpl(ExprSubstitutionMap smap, Analyzer analyzer)
      throws AnalysisException {
    Expr e = super.substituteImpl(smap, analyzer);
    if (!(e instanceof AnalyticExpr)) return e;
    // Keep the order-by elements in sync with the possibly substituted child exprs.
    setOrderByExprs();
    return e;
  }
}
