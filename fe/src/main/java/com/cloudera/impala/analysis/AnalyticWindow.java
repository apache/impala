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

import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.service.FeSupport;
import com.cloudera.impala.thrift.TAnalyticWindow;
import com.cloudera.impala.thrift.TAnalyticWindowBoundary;
import com.cloudera.impala.thrift.TAnalyticWindowBoundaryType;
import com.cloudera.impala.thrift.TAnalyticWindowType;
import com.cloudera.impala.thrift.TColumnValue;
import com.cloudera.impala.util.TColumnValueUtil;
import com.google.common.base.Preconditions;


/**
 * Windowing clause of an analytic expr
 */
public class AnalyticWindow {
  public static final AnalyticWindow DEFAULT_WINDOW = new AnalyticWindow(Type.RANGE,
      new Boundary(BoundaryType.UNBOUNDED_PRECEDING, null),
      new Boundary(BoundaryType.CURRENT_ROW, null));

  enum Type {
    ROWS("ROWS"),
    RANGE("RANGE");

    private final String description_;

    private Type(String d) {
      description_ = d;
    }

    @Override
    public String toString() { return description_; }
    public TAnalyticWindowType toThrift() {
      return this == ROWS ? TAnalyticWindowType.ROWS : TAnalyticWindowType.RANGE;
    }
  }

  enum BoundaryType {
    UNBOUNDED_PRECEDING("UNBOUNDED PRECEDING"),
    UNBOUNDED_FOLLOWING("UNBOUNDED FOLLOWING"),
    CURRENT_ROW("CURRENT ROW"),
    PRECEDING("PRECEDING"),
    FOLLOWING("FOLLOWING");

    private final String description_;

    private BoundaryType(String d) {
      description_ = d;
    }

    @Override
    public String toString() { return description_; }
    public TAnalyticWindowBoundaryType toThrift() {
      Preconditions.checkState(!isAbsolutePos());
      if (this == CURRENT_ROW) {
        return TAnalyticWindowBoundaryType.CURRENT_ROW;
      } else if (this == PRECEDING) {
        return TAnalyticWindowBoundaryType.PRECEDING;
      } else if (this == FOLLOWING) {
        return TAnalyticWindowBoundaryType.FOLLOWING;
      }
      return null;
    }

    public boolean isAbsolutePos() {
      return this == UNBOUNDED_PRECEDING || this == UNBOUNDED_FOLLOWING;
    }

    public boolean isOffset() {
      return this == PRECEDING || this == FOLLOWING;
    }

    public boolean isPreceding() {
      return this == UNBOUNDED_PRECEDING || this == PRECEDING;
    }

    public boolean isFollowing() {
      return this == UNBOUNDED_FOLLOWING || this == FOLLOWING;
    }
  }

  public static class Boundary {
    private final BoundaryType type_;
    private final Expr expr_;  // only set for PRECEDING/FOLLOWING

    // set during analysis if the boundary is for a ROWS window
    private Long rowsOffset_;

    public BoundaryType getType() { return type_; }
    public Expr getExpr() { return expr_; }

    public Boundary(BoundaryType type, Expr e) {
      this(type, e, null);
    }

    // c'tor used by clone()
    private Boundary(BoundaryType type, Expr e, Long rowsOffset) {
      Preconditions.checkState(
        (type.isOffset() && e != null)
        || (!type.isOffset() && e == null));
      type_ = type;
      expr_ = e;
      rowsOffset_ = rowsOffset;
    }

    public String toSql() {
      StringBuilder sb = new StringBuilder();
      if (expr_ != null) sb.append(expr_.toSql()).append(" ");
      sb.append(type_.toString());
      return sb.toString();
    }

    public TAnalyticWindowBoundary toThrift() {
      TAnalyticWindowBoundary result = new TAnalyticWindowBoundary(type_.toThrift());
      if (type_.isOffset()) {
        if (rowsOffset_ == null) {
          result.setRange_offset_expr(expr_.treeToThrift());
        } else {
          long relativeOffset = rowsOffset_;
          if (type_ == BoundaryType.PRECEDING) relativeOffset *= -1;
          result.setRows_offset_idx(relativeOffset);
        }
      } else if (type_ == BoundaryType.CURRENT_ROW) {
        result.setRows_offset_idx(0);
      }
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) return false;
      if (obj.getClass() != this.getClass()) return false;
      Boundary o = (Boundary)obj;
      boolean exprEqual = (expr_ == null) == (o.expr_ == null);
      if (exprEqual && expr_ != null) exprEqual = expr_.equals(o.expr_);
      return type_ == o.type_ && exprEqual;
    }

    @Override
    public Boundary clone() {
      return new Boundary(type_, expr_ != null ? expr_.clone() : null, rowsOffset_);
    }

    public void analyze(Analyzer analyzer) throws AnalysisException {
      if (expr_ != null) expr_.analyze(analyzer);
    }
  }

  private final Type type_;
  private final Boundary leftBoundary_;
  private final Boundary rightBoundary_;  // may be null

  public Type getType() { return type_; }
  public Boundary getLeftBoundary() { return leftBoundary_; }
  public Boundary getRightBoundary() { return rightBoundary_; }

  public AnalyticWindow(Type type, Boundary b) {
    type_ = type;
    Preconditions.checkNotNull(b);
    leftBoundary_ = b;
    rightBoundary_ = null;
  }

  public AnalyticWindow(Type type, Boundary l, Boundary r) {
    type_ = type;
    Preconditions.checkNotNull(l);
    leftBoundary_ = l;
    Preconditions.checkNotNull(r);
    rightBoundary_ = r;
  }

  public String toSql() {
    StringBuilder sb = new StringBuilder();
    sb.append(type_.toString()).append(" ");
    if (rightBoundary_ == null) {
      sb.append(leftBoundary_.toSql());
    } else {
      sb.append("BETWEEN ").append(leftBoundary_.toSql()).append(" AND ");
      sb.append(rightBoundary_.toSql());
    }
    return sb.toString();
  }

  public TAnalyticWindow toThrift() {
    TAnalyticWindow result = new TAnalyticWindow(type_.toThrift());
    if (leftBoundary_.getType() != BoundaryType.UNBOUNDED_PRECEDING) {
      result.setWindow_start(leftBoundary_.toThrift());
    }
    if (rightBoundary_.getType() != BoundaryType.UNBOUNDED_FOLLOWING) {
      result.setWindow_end(rightBoundary_.toThrift());
    }
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) return false;
    if (obj.getClass() != this.getClass()) return false;
    AnalyticWindow o = (AnalyticWindow)obj;
    boolean rightBoundaryEqual =
        (rightBoundary_ == null) == (o.rightBoundary_ == null);
    if (rightBoundaryEqual && rightBoundary_ != null) {
      rightBoundaryEqual = rightBoundary_.equals(o.rightBoundary_);
    }
    return type_ == o.type_
        && leftBoundary_.equals(o.leftBoundary_)
        && rightBoundaryEqual;
  }

  @Override
  public AnalyticWindow clone() {
    return new AnalyticWindow(type_, leftBoundary_.clone(), rightBoundary_.clone());
  }

  /**
   * Semantic analysis for expr of a PRECEDING/FOLLOWING clause.
   */
  private void checkOffsetExpr(Analyzer analyzer, Boundary boundary)
      throws AnalysisException {
    Preconditions.checkState(boundary.getType().isOffset());
    Expr e = boundary.getExpr();
    Preconditions.checkNotNull(e);
    boolean isPos = true;
    Double val = null;
    if (e.isConstant() && e.getType().isNumericType()) {
      try {
        val = TColumnValueUtil.getNumericVal(
            FeSupport.EvalConstExpr(e, analyzer.getQueryCtx()));
        if (val <= 0) isPos = false;
      } catch (InternalException exc) {
        throw new AnalysisException(
            "Couldn't evaluate PRECEDING/FOLLOWING expression: " + exc.getMessage());
      }
    }

    if (type_ == Type.ROWS) {
      if (!e.isConstant() || !e.getType().isIntegerType() || !isPos) {
        throw new AnalysisException(
            "For ROWS window, the value of a PRECEDING/FOLLOWING offset must be a "
              + "constant positive integer: " + boundary.toSql());
      }
      Preconditions.checkNotNull(val);
      boundary.rowsOffset_ = val.longValue();
    } else {
      if (!e.isConstant() || !e.getType().isNumericType() || !isPos) {
        throw new AnalysisException(
            "For RANGE window, the value of a PRECEDING/FOLLOWING offset must be a "
              + "constant positive number: " + boundary.toSql());
      }
    }
  }

  /**
   * Check that b1 <= b2.
   */
  private void checkOffsetBoundaries(Analyzer analyzer, Boundary b1, Boundary b2)
      throws AnalysisException {
    Preconditions.checkState(b1.getType().isOffset());
    Preconditions.checkState(b2.getType().isOffset());
    Expr e1 = b1.getExpr();
    Preconditions.checkState(
        e1 != null && e1.isConstant() && e1.getType().isNumericType());
    Expr e2 = b2.getExpr();
    Preconditions.checkState(
        e2 != null && e2.isConstant() && e2.getType().isNumericType());

    try {
      TColumnValue val1 = FeSupport.EvalConstExpr(e1, analyzer.getQueryCtx());
      TColumnValue val2 = FeSupport.EvalConstExpr(e2, analyzer.getQueryCtx());
      double left = TColumnValueUtil.getNumericVal(val1);
      double right = TColumnValueUtil.getNumericVal(val2);
      if (left > right) {
        throw new AnalysisException(
            "Offset boundaries are in the wrong order: " + toSql());
      }
    } catch (InternalException exc) {
      throw new AnalysisException(
          "Couldn't evaluate PRECEDING/FOLLOWING expression: " + exc.getMessage());
    }

  }

  public void analyze(Analyzer analyzer) throws AnalysisException {
    leftBoundary_.analyze(analyzer);
    if (rightBoundary_ != null) rightBoundary_.analyze(analyzer);

    if (leftBoundary_.getType() == BoundaryType.UNBOUNDED_FOLLOWING) {
      throw new AnalysisException(
          leftBoundary_.getType().toString() + " is only allowed for upper bound of "
            + "BETWEEN");

    }
    if (rightBoundary_ != null
        && rightBoundary_.getType() == BoundaryType.UNBOUNDED_PRECEDING) {
      throw new AnalysisException(
          rightBoundary_.getType().toString() + " is only allowed for lower bound of "
            + "BETWEEN");

    }
    if (rightBoundary_ == null && leftBoundary_.getType() == BoundaryType.FOLLOWING) {
      throw new AnalysisException(
          leftBoundary_.getType().toString() + " requires a BETWEEN clause");
    }

    if (leftBoundary_.getType().isOffset()) checkOffsetExpr(analyzer, leftBoundary_);
    if (rightBoundary_ == null) return;
    if (rightBoundary_.getType().isOffset()) checkOffsetExpr(analyzer, rightBoundary_);

    if (leftBoundary_.getType() == BoundaryType.FOLLOWING) {
      if (rightBoundary_.getType() != BoundaryType.FOLLOWING
          && rightBoundary_.getType() != BoundaryType.UNBOUNDED_FOLLOWING) {
        throw new AnalysisException(
            "A lower window bound of " + BoundaryType.FOLLOWING.toString()
              + " requires that the upper bound also be "
              + BoundaryType.FOLLOWING.toString());
      }
      if (rightBoundary_.getType() != BoundaryType.UNBOUNDED_FOLLOWING) {
        checkOffsetBoundaries(analyzer, leftBoundary_, rightBoundary_);
      }
    }

    if (rightBoundary_.getType() == BoundaryType.PRECEDING) {
      if (leftBoundary_.getType() != BoundaryType.PRECEDING
          && leftBoundary_.getType() != BoundaryType.UNBOUNDED_PRECEDING) {
        throw new AnalysisException(
            "An upper window bound of " + BoundaryType.PRECEDING.toString()
              + " requires that the lower bound also be "
              + BoundaryType.PRECEDING.toString());
      }
      if (leftBoundary_.getType() != BoundaryType.UNBOUNDED_PRECEDING) {
        checkOffsetBoundaries(analyzer, rightBoundary_, leftBoundary_);
      }
    }
  }
}
