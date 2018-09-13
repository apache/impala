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

import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.InternalException;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TColumnValue;

import com.google.common.base.Preconditions;

/**
 * Combination of limit and offset expressions.
 */
class LimitElement {
  /////////////////////////////////////////
  // BEGIN: Members that need to be reset()

  private final Expr limitExpr_;
  private final Expr offsetExpr_;
  private long limit_;
  private long offset_;
  private boolean isAnalyzed_;

  // END: Members that need to be reset()
  /////////////////////////////////////////

  /**
   * Constructs the LimitElement.
   * @param limitExpr The limit expression. May be null if there is no LIMIT clause.
   * @param offsetExpr The offset expression. May be null if there is no OFFSET clause.
   */
  public LimitElement(Expr limitExpr, Expr offsetExpr) {
    this.limitExpr_ = limitExpr;
    this.offsetExpr_ = offsetExpr;
    isAnalyzed_ = false;
    limit_ = -1;
    offset_ = 0;
  }

  /**
   * Copy c'tor used in clone().
   */
  protected LimitElement(LimitElement other) {
    limitExpr_ = (other.limitExpr_ != null) ? other.limitExpr_.clone() : null;
    offsetExpr_ = (other.offsetExpr_ != null) ? other.offsetExpr_.clone() : null;
    limit_ = other.limit_;
    offset_ = other.offset_;
    isAnalyzed_ = other.isAnalyzed_;
  }

  public Expr getLimitExpr() { return limitExpr_; }
  public Expr getOffsetExpr() { return offsetExpr_; }

  /**
   * Returns the integer limit, evaluated from the limit expression. Must call analyze()
   * first. If no limit was set, then -1 is returned.
   */
  public long getLimit() {
    Preconditions.checkState(isAnalyzed_);
    return limit_;
  }

  /**
   * Returns the integer offset, evaluated from the offset expression. Must call
   * analyze() first. If no offsetExpr exists, then 0 (the default offset) is returned.
   */
  public long getOffset() {
    Preconditions.checkState(isAnalyzed_);
    return offset_;
  }

  public String toSql() {
    StringBuilder sb = new StringBuilder();
    if (limitExpr_ != null) {
      sb.append(" LIMIT ");
      sb.append(limitExpr_.toSql());
    }
    // Don't add the offset if it is the default value. However, we do print it if it
    // hasn't been analyzed yet because we need to output the expression used in errors.
    if (offsetExpr_ != null && (offset_ != 0 || !isAnalyzed_)) {
      sb.append(" OFFSET ");
      sb.append(offsetExpr_.toSql());
    }
    return sb.toString();
  }

  public void analyze(Analyzer analyzer) throws AnalysisException {
    isAnalyzed_ = true;
    if (limitExpr_ != null) {
      limit_ = evalIntegerExpr(analyzer, limitExpr_, "LIMIT");
    }
    if (limit_ == 0) analyzer.setHasEmptyResultSet();
    if (offsetExpr_ != null) {
      offset_ = evalIntegerExpr(analyzer, offsetExpr_, "OFFSET");
    }
  }

  /**
   * Analyzes and evaluates expression to a non-zero integral value, returned as a long.
   * Throws if the expression cannot be evaluated, if the value evaluates to null, or if
   * the result is negative. The 'name' parameter is used in exception messages, e.g.
   * "LIMIT expression evaluates to NULL".
   */
  private static long evalIntegerExpr(Analyzer analyzer, Expr expr, String name)
      throws AnalysisException {
    // Check for slotrefs and subqueries before analysis so we can provide a more
    // helpful error message.
    if (expr.contains(SlotRef.class) || expr.contains(Subquery.class)) {
      throw new AnalysisException(name + " expression must be a constant expression: " +
          expr.toSql());
    }
    expr.analyze(analyzer);
    if (!expr.isConstant()) {
      throw new AnalysisException(name + " expression must be a constant expression: " +
          expr.toSql());
    }
    if (!expr.getType().isIntegerType()) {
      throw new AnalysisException(name + " expression must be an integer type but is '" +
          expr.getType() + "': " + expr.toSql());
    }
    TColumnValue val = null;
    try {
      val = FeSupport.EvalExprWithoutRow(expr, analyzer.getQueryCtx());
    } catch (InternalException e) {
      throw new AnalysisException("Failed to evaluate expr: " + expr.toSql(), e);
    }
    long value;
    if (val.isSetLong_val()) {
      value = val.getLong_val();
    } else if (val.isSetInt_val()) {
      value = val.getInt_val();
    } else if (val.isSetShort_val()) {
      value = val.getShort_val();
    } else if (val.isSetByte_val()) {
      value = val.getByte_val();
    } else {
      throw new AnalysisException(name + " expression evaluates to NULL: " +
          expr.toSql());
    }
    if (value < 0) {
      throw new AnalysisException(name + " must be a non-negative integer: " +
          expr.toSql() + " = " + value);
    }
    return value;
  }

  @Override
  public LimitElement clone() { return new LimitElement(this); }

  public void reset() {
    isAnalyzed_ = false;
    limit_ = -1;
    offset_ = 0;
    if (limitExpr_ != null) limitExpr_.reset();
    if (offsetExpr_ != null) offsetExpr_.reset();
  }
}
