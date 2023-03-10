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

import java.util.HashMap;
import java.util.Map;

import org.apache.impala.analysis.ArithmeticExpr.Operator;
import org.apache.impala.catalog.Function.CompareMode;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TExprNode;
import org.apache.impala.thrift.TExprNodeType;
import com.google.common.base.Preconditions;

/**
 * Describes the addition and subtraction of time units from timestamps/dates.
 * Arithmetic expressions on timestamps/dates are syntactic sugar.
 * They are executed as function call exprs in the BE.
 * TimestampArithmeticExpr is used for both TIMESTAMP and DATE values.
 */
public class TimestampArithmeticExpr extends Expr {

  // Time units supported in timestamp/date arithmetic.
  public static enum TimeUnit {
    YEAR("YEAR"),
    MONTH("MONTH"),
    WEEK("WEEK"),
    DAY("DAY"),
    HOUR("HOUR"),
    MINUTE("MINUTE"),
    SECOND("SECOND"),
    MILLISECOND("MILLISECOND"),
    MICROSECOND("MICROSECOND"),
    NANOSECOND("NANOSECOND");

    private final String description_;

    private TimeUnit(String description) {
      this.description_ = description;
    }

    @Override
    public String toString() {
      return description_;
    }
  }

  private static Map<String, TimeUnit> TIME_UNITS_MAP = new HashMap<>();
  static {
    for (TimeUnit timeUnit : TimeUnit.values()) {
      TIME_UNITS_MAP.put(timeUnit.toString(), timeUnit);
      TIME_UNITS_MAP.put(timeUnit.toString() + "S", timeUnit);
    }
  }

  // Set for function call-like arithmetic.
  private final String funcName_;
  private ArithmeticExpr.Operator op_;

  // Keep the original string passed in the c'tor to resolve
  // ambiguities with other uses of IDENT during query parsing.
  private final String timeUnitIdent_;
  private TimeUnit timeUnit_;

  // Indicates an expr where the interval comes first, e.g., 'interval b year + a'.
  private final boolean intervalFirst_;

  // C'tor for function-call like arithmetic, e.g., 'date_add(a, interval b year)'.
  public TimestampArithmeticExpr(String funcName, Expr e1, Expr e2,
      String timeUnitIdent) {
    this.funcName_ = funcName.toLowerCase();
    this.timeUnitIdent_ = timeUnitIdent;
    this.intervalFirst_ = false;
    children_.add(e1);
    children_.add(e2);
  }

  // C'tor for non-function-call like arithmetic, e.g., 'a + interval b year'.
  // e1 always refers to the timestamp/date to be added/subtracted from, and e2
  // to the time value (even in the interval-first case).
  public TimestampArithmeticExpr(ArithmeticExpr.Operator op, Expr e1, Expr e2,
      String timeUnitIdent, boolean intervalFirst) {
    Preconditions.checkState(op == Operator.ADD || op == Operator.SUBTRACT);
    this.funcName_ = null;
    this.op_ = op;
    this.timeUnitIdent_ = timeUnitIdent;
    this.intervalFirst_ = intervalFirst;
    children_.add(e1);
    children_.add(e2);
  }

  /**
   * Copy c'tor used in clone().
   */
  protected TimestampArithmeticExpr(TimestampArithmeticExpr other) {
    super(other);
    funcName_ = other.funcName_;
    op_ = other.op_;
    timeUnitIdent_ = other.timeUnitIdent_;
    timeUnit_ = other.timeUnit_;
    intervalFirst_ = other.intervalFirst_;
  }

  @Override
  protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    if (funcName_ != null) {
      // Set op based on funcName for function-call like version.
      if (funcName_.equals("date_add")) {
        op_ = ArithmeticExpr.Operator.ADD;
      } else if (funcName_.equals("date_sub")) {
        op_ = ArithmeticExpr.Operator.SUBTRACT;
      } else {
        throw new AnalysisException("Encountered function name '" + funcName_ +
            "' in timestamp/date arithmetic expression '" + toSql() + "'. " +
            "Expected function name 'DATE_ADD' or 'DATE_SUB'.");
      }
    }

    timeUnit_ = TIME_UNITS_MAP.get(timeUnitIdent_.toUpperCase());
    if (timeUnit_ == null) {
      throw new AnalysisException("Invalid time unit '" + timeUnitIdent_ +
          "' in timestamp/date arithmetic expression '" + toSql() + "'.");
    }

    // The first child must return a timestamp or date or null.
    if (!getChild(0).getType().isTimestamp() && !getChild(0).getType().isDate()
        && !getChild(0).getType().isNull()) {
      throw new AnalysisException("Operand '" + getChild(0).toSql() +
          "' of timestamp/date arithmetic expression '" + toSql() + "' returns type '" +
          getChild(0).getType().toSql() + "'. Expected type 'TIMESTAMP' or 'DATE'.");
    }

    // If first child returns a date, time unit must be YEAR/MONTH/WEEK/DAY.
    if (getChild(0).getType().isDate() && timeUnit_ != TimeUnit.YEAR
        && timeUnit_ != TimeUnit.MONTH && timeUnit_ != TimeUnit.WEEK
        && timeUnit_ != TimeUnit.DAY) {
      throw new AnalysisException("'" + timeUnit_ + "' intervals are not allowed in " +
          "date arithmetic expressions");
    }

    // The second child must be an integer type.
    if (!getChild(1).getType().isIntegerType() &&
        !getChild(1).getType().isNull()) {
      throw new AnalysisException("Operand '" + getChild(1).toSql() +
          "' of timestamp/date arithmetic expression '" + toSql() + "' returns type '" +
          getChild(1).getType().toSql() + "'. Expected an integer type.");
    }

    String funcOpName = String.format("%sS_%s",  timeUnit_,
        (op_ == ArithmeticExpr.Operator.ADD) ? "ADD" : "SUB");
    // For the month interval, use the invisible special-case implementation.
    // "ADD_MONTHS(t, m)" by definition is different from "t + INTERVAL m MONTHS".
    if (timeUnit_ == TimeUnit.MONTH) funcOpName += "_INTERVAL";

    fn_ = getBuiltinFunction(analyzer, funcOpName.toLowerCase(),
         collectChildReturnTypes(), CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
    castForFunctionCall(false, analyzer.getRegularCompatibilityLevel());

    Preconditions.checkNotNull(fn_);
    Preconditions.checkState(fn_.getReturnType().isTimestamp()
        || fn_.getReturnType().isDate());
    type_ = fn_.getReturnType();
  }

  @Override
  protected float computeEvalCost() {
    return hasChildCosts() ? getChildCosts() + TIMESTAMP_ARITHMETIC_COST : UNKNOWN_COST;
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.FUNCTION_CALL;
  }

  public String getTimeUnitIdent() { return timeUnitIdent_; }
  public TimeUnit getTimeUnit() { return timeUnit_; }
  public ArithmeticExpr.Operator getOp() { return op_; }

  @Override
  public String toSqlImpl(ToSqlOptions options) {
    StringBuilder strBuilder = new StringBuilder();
    if (funcName_ != null) {
      // Function-call like version.
      strBuilder.append(funcName_.toUpperCase() + "(");
      strBuilder.append(getChild(0).toSql(options) + ", ");
      strBuilder.append("INTERVAL ");
      strBuilder.append(getChild(1).toSql(options));
      strBuilder.append(" " + timeUnitIdent_);
      strBuilder.append(")");
      return strBuilder.toString();
    }
    if (intervalFirst_) {
      // Non-function-call like version with interval as first operand.
      strBuilder.append("INTERVAL ");
      strBuilder.append(getChild(1).toSql(options) + " ");
      strBuilder.append(timeUnitIdent_);
      strBuilder.append(" " + op_.toString() + " ");
      strBuilder.append(getChild(0).toSql(options));
    } else {
      // Non-function-call like version with interval as second operand.
      strBuilder.append(getChild(0).toSql(options));
      strBuilder.append(" " + op_.toString() + " ");
      strBuilder.append("INTERVAL ");
      strBuilder.append(getChild(1).toSql(options) + " ");
      strBuilder.append(timeUnitIdent_);
    }
    return strBuilder.toString();
  }

  @Override
  public Expr clone() { return new TimestampArithmeticExpr(this); }
}
