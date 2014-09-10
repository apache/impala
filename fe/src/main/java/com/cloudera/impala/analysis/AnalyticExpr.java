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

import java.math.BigDecimal;
import java.util.List;

import com.cloudera.impala.analysis.AnalyticWindow.Boundary;
import com.cloudera.impala.analysis.AnalyticWindow.BoundaryType;
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
 */
public class AnalyticExpr extends Expr {
  // These elements are modified to point to the corresponding child exprs to keep them
  // in sync through expr substitutions.
  private List<OrderByElement> orderByElements_ = Lists.newArrayList();
  private final int numPartitionExprs_;
  private AnalyticWindow window_;

  // If set, requires the window to be set to null in resetAnalysisState(). Required for
  // proper substitution/cloning because standardization may set a window that is illegal
  // in SQL, and hence, will fail analysis().
  private boolean resetWindow_ = false;

  // SQL string of this AnalyticExpr before standardization. Returned in toSqlImpl().
  private String sqlString_;

  private static String LEAD = "lead";
  private static String LAG = "lag";
  private static String FIRSTVALUE = "first_value";
  private static String LASTVALUE = "last_value";
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
    resetWindow_ = other.resetWindow_;
    sqlString_ = other.sqlString_;
  }

  public FunctionCallExpr getFnCall() { return (FunctionCallExpr) getChild(0); }
  public List<Expr> getPartitionExprs() {
    return children_.subList(1, numPartitionExprs_ + 1);
  }
  public List<OrderByElement> getOrderByElements() { return orderByElements_; }
  public AnalyticWindow getWindow() { return window_; }

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
    if (sqlString_ != null) return sqlString_;
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
        && ((AggregateFunction) fn).isAnalyticFn();
  }

  public static boolean isAggregateFn(Function fn) {
    return fn instanceof AggregateFunction
        && ((AggregateFunction) fn).isAggregateFn();
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

    // check for non-analytic aggregate functions
    if (!isAnalyticFn(fn)) {
      throw new AnalysisException(
          String.format("Aggregate function '%s' not supported with OVER clause.",
              getFnCall().toSql()));
    }

    if (isAnalyticFn(fn) && !isAggregateFn(fn)) {
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
      setWindowChildren();

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
    sqlString_ = toSql();

    standardize(analyzer);
  }

  /**
   * If necessary, rewrites the analytic function, window, and/or order-by elements into
   * a standard format for the purpose of simpler backend execution, as follows:
   * 1. row_number():
   *    Set a window from UNBOUNDED PRECEDING to CURRENT_ROW.
   * 2. lead()/lag():
   *    Explicitly set the default arguments to for BE simplicity.
   *    Set a window for lead(): UNBOUNDED PRECEDING to OFFSET FOLLOWING.
   *    Set a window for lag(): UNBOUNDED PRECEDING to OFFSET PRECEDING.
   * 3. UNBOUNDED FOLLOWING windows:
   *    Reverse the ordering and window if the start bound is not UNBOUNDED PRECEDING.
   *    Flip first_value() and last_value().
   * 4. first_value():
   *    Set the upper boundary to CURRENT_ROW if the lower boundary is
   *    UNBOUNDED_PRECEDING.
   * 5. Explicitly set the default window if no window was given but there
   *    are order-by elements.
   */
  private void standardize(Analyzer analyzer) {
    FunctionName analyticFnName = getFnCall().getFnName();

    // Set a window from UNBOUNDED PRECEDING to CURRENT_ROW for row_number().
    if (analyticFnName.getFunction().equals(ROWNUMBER)) {
      Preconditions.checkState(window_ == null, "Unexpected window set for row_numer()");
      window_ = new AnalyticWindow(AnalyticWindow.Type.ROWS,
          new Boundary(BoundaryType.UNBOUNDED_PRECEDING, null),
          new Boundary(BoundaryType.CURRENT_ROW, null));
      resetWindow_ = true;
      return;
    }

    // Explicitly set the default arguments to lead()/lag() for BE simplicity.
    // Set a window for lead(): UNBOUNDED PRECEDING to OFFSET FOLLOWING,
    // Set a window for lag(): UNBOUNDED PRECEDING to OFFSET PRECEDING.
    if (isOffsetFn(getFnCall().getFn())) {
      Preconditions.checkState(window_ == null);

      // If necessary, create a new fn call with the default args explicitly set.
      List<Expr> newExprParams = null;
      if (getFnCall().getChildren().size() == 1) {
        newExprParams = Lists.newArrayListWithExpectedSize(3);
        newExprParams.addAll(getFnCall().getChildren());
        // Default offset is 1.
        newExprParams.add(new NumericLiteral(BigDecimal.valueOf(1)));
        // Default default value is NULL.
        newExprParams.add(new NullLiteral());
      } else if (getFnCall().getChildren().size() == 2) {
        newExprParams = Lists.newArrayListWithExpectedSize(3);
        newExprParams.addAll(getFnCall().getChildren());
        // Default default value is NULL.
        newExprParams.add(new NullLiteral());
      } else  {
        Preconditions.checkState(getFnCall().getChildren().size() == 3);
      }
      if (newExprParams != null) {
        FunctionCallExpr newFnCall = new FunctionCallExpr(getFnCall().getFnName(),
            new FunctionParams(newExprParams));
        newFnCall.setIsAnalyticFnCall(true);
        newFnCall.analyzeNoThrow(analyzer);
        setChild(0, newFnCall);
      }

      // Set the window.
      BoundaryType rightBoundaryType = BoundaryType.FOLLOWING;
      if (analyticFnName.getFunction().equals(LAG)) {
        rightBoundaryType = BoundaryType.PRECEDING;
      }
      window_ = new AnalyticWindow(AnalyticWindow.Type.ROWS,
          new Boundary(BoundaryType.UNBOUNDED_PRECEDING, null),
          new Boundary(rightBoundaryType, getOffsetExpr(getFnCall())));
      try {
        window_.analyze(analyzer);
      } catch (AnalysisException e) {
        throw new IllegalStateException(e);
      }
      setWindowChildren();
      resetWindow_ = true;
      return;
    }

    // Reverse the ordering and window for windows ending with UNBOUNDED FOLLOWING,
    // and and not starting with UNBOUNDED PRECEDING.
    if (window_ != null
        && window_.getRightBoundary().getType() == BoundaryType.UNBOUNDED_FOLLOWING
        && window_.getLeftBoundary().getType() != BoundaryType.UNBOUNDED_PRECEDING) {
      orderByElements_ = OrderByElement.reverse(orderByElements_);
      window_ = window_.reverse();

      // Replace existing children.
      for (int i = 0; i < orderByElements_.size(); ++i) {
        setChild(numPartitionExprs_ + i + 1, orderByElements_.get(i).getExpr());
      }
      setWindowChildren();

      // Also flip first_value()/last_value(). For other analytic functions there is no
      // need to also change the function.
      FunctionName reversedFnName = null;
      if (analyticFnName.getFunction().equals(FIRSTVALUE)) {
        reversedFnName = new FunctionName(LASTVALUE);
      } else if (analyticFnName.getFunction().equals(LASTVALUE)) {
        reversedFnName = new FunctionName(FIRSTVALUE);
      }
      if (reversedFnName != null) {
        FunctionCallExpr reversedFn =
            new FunctionCallExpr(reversedFnName, getFnCall().getParams());
        reversedFn.setIsAnalyticFnCall(true);
        reversedFn.analyzeNoThrow(analyzer);
        setChild(0, reversedFn);
      }
      analyticFnName = getFnCall().getFnName();
    }

    // Set the upper boundary to CURRENT_ROW for first_value() if the lower boundary
    // is UNBOUNDED_PRECEDING.
    if (window_ != null
        && window_.getLeftBoundary().getType() == BoundaryType.UNBOUNDED_PRECEDING
        && analyticFnName.getFunction().equals(FIRSTVALUE)) {
      window_.setRightBoundary(new Boundary(BoundaryType.CURRENT_ROW, null));
    }

    // Set the default window.
    if (!orderByElements_.isEmpty() && window_ == null) {
      window_ = AnalyticWindow.DEFAULT_WINDOW;
      resetWindow_ = true;
    }
  }

  /**
   * Sets or adds the corresponding children to the exprs of window_.
   */
  private void setWindowChildren() {
    if (window_.getLeftBoundary().getExpr() != null) {
      if (numPartitionExprs_ + 1 + orderByElements_.size() >= children_.size()) {
        addChild(window_.getLeftBoundary().getExpr());
      } else {
        setChild(numPartitionExprs_ + 1 + orderByElements_.size(),
            window_.getLeftBoundary().getExpr());
      }
    }
    if (window_.getRightBoundary() != null
        && window_.getRightBoundary().getExpr() != null) {
      if (numPartitionExprs_ + 2 + orderByElements_.size() >= children_.size()) {
        addChild(window_.getRightBoundary().getExpr());
      } else {
        setChild(numPartitionExprs_ + 2 + orderByElements_.size(),
            window_.getRightBoundary().getExpr());
      }
    }
  }

  /**
   * Returns the explicit or implicit offset of an analytic function call.
   */
  private Expr getOffsetExpr(FunctionCallExpr offsetFnCall) {
    Preconditions.checkState(isOffsetFn(getFnCall().getFn()));
    if (offsetFnCall.getChild(1) != null) return offsetFnCall.getChild(1);
    // The default offset is 1.
    return new NumericLiteral(BigDecimal.valueOf(1));
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
    if (resetWindow_) window_ = null;
    resetWindow_ = false;
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
    ((AnalyticExpr) e).setOrderByExprs();
    return e;
  }
}
