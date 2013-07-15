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

import java.util.HashMap;
import java.util.Map;

import com.cloudera.impala.analysis.ArithmeticExpr.Operator;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.opcode.FunctionOperator;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.google.common.base.Preconditions;

/**
 * Describes the addition and subtraction of time units from timestamps.
 * Arithmetic expressions on timestamps are syntactic sugar.
 * They are executed as function call exprs in the BE.
 */
public class TimestampArithmeticExpr extends Expr {

  // Time units supported in timestamp arithmetic.
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

    private final String description;

    private TimeUnit(String description) {
      this.description = description;
    }

    @Override
    public String toString() {
      return description;
    }
  }

  private static Map<String, TimeUnit> TIME_UNITS_MAP = new HashMap<String, TimeUnit>();
  static {
    for (TimeUnit timeUnit : TimeUnit.values()) {
      TIME_UNITS_MAP.put(timeUnit.toString(), timeUnit);
      TIME_UNITS_MAP.put(timeUnit.toString() + "S", timeUnit);
    }
  }

  // Set for function call-like arithmetic.
  private final String funcName;
  private ArithmeticExpr.Operator op;

  // Keep the original string passed in the c'tor to resolve
  // ambiguities with other uses of IDENT during query parsing.
  private final String timeUnitIdent;
  private TimeUnit timeUnit;

  // Indicates an expr where the interval comes first, e.g., 'interval b year + a'.
  private final boolean intervalFirst;

  // C'tor for function-call like arithmetic, e.g., 'date_add(a, interval b year)'.
  public TimestampArithmeticExpr(String funcName, Expr e1, Expr e2,
      String timeUnitIdent) {
    this.funcName = funcName;
    this.timeUnitIdent = timeUnitIdent;
    this.intervalFirst = false;
    children.add(e1);
    children.add(e2);
  }

  // C'tor for non-function-call like arithmetic, e.g., 'a + interval b year'.
  // e1 always refers to the timestamp to be added/subtracted from, and e2
  // to the time value (even in the interval-first case).
  public TimestampArithmeticExpr(ArithmeticExpr.Operator op, Expr e1, Expr e2,
      String timeUnitIdent, boolean intervalFirst) {
    Preconditions.checkState(op == Operator.ADD || op == Operator.SUBTRACT);
    this.funcName = null;
    this.op = op;
    this.timeUnitIdent = timeUnitIdent;
    this.intervalFirst = intervalFirst;
    children.add(e1);
    children.add(e2);
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);

    if (funcName != null) {
      // Set op based on funcName for function-call like version.
      if (funcName.toUpperCase().equals("DATE_ADD")) {
        op = ArithmeticExpr.Operator.ADD;
      } else if (funcName.toUpperCase().equals("DATE_SUB")) {
        op = ArithmeticExpr.Operator.SUBTRACT;
      } else {
        throw new AnalysisException("Encountered function name '" + funcName +
            "' in timestamp arithmetic expression '" + toSql() + "'. " +
            "Expected function name 'DATE_ADD' or 'DATE_SUB'.");
      }
    }

    timeUnit = TIME_UNITS_MAP.get(timeUnitIdent.toUpperCase());
    if (timeUnit == null) {
      throw new AnalysisException("Invalid time unit '" + timeUnitIdent +
          "' in timestamp arithmetic expression '" + toSql() + "'.");
    }

    // The first child must return a timestamp or null.
    if (getChild(0).getType() != PrimitiveType.TIMESTAMP &&
        !getChild(0).getType().isNull()) {
      throw new AnalysisException("Operand '" + getChild(0).toSql() +
          "' of timestamp arithmetic expression '" + toSql() + "' returns type '" +
          getChild(0).getType() + "'. Expected type 'TIMESTAMP'.");
    }

    // The second child must be an integer type.
    if (!getChild(1).getType().isIntegerType() &&
        !getChild(1).getType().isNull()) {
      throw new AnalysisException("Operand '" + getChild(1).toSql() +
          "' of timestamp arithmetic expression '" + toSql() + "' returns type '" +
          getChild(1).getType() + "'. Expected an integer type.");
    }

    PrimitiveType[] argTypes = new PrimitiveType[this.children.size()];
    for (int i = 0; i < this.children.size(); ++i) {
      this.children.get(i).analyze(analyzer);
      argTypes[i] = this.children.get(i).getType();
    }
    String funcOpName = String.format("%sS_%s", timeUnit.toString(),
        (op == ArithmeticExpr.Operator.ADD) ? "ADD" : "SUB");
    FunctionOperator funcOp = OpcodeRegistry.instance().getFunctionOperator(funcOpName);
    OpcodeRegistry.BuiltinFunction match =
        OpcodeRegistry.instance().getFunctionInfo(funcOp, true, argTypes);
    // We have already done type checking to ensure the function will resolve.
    Preconditions.checkNotNull(match);
    Preconditions.checkState(match.getDesc().getReturnType() == PrimitiveType.TIMESTAMP);
    opcode = match.opcode;
    type = match.getDesc().getReturnType();
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.FUNCTION_CALL;
    msg.setOpcode(opcode);
  }

  public String getTimeUnitIdent() { return timeUnitIdent; }
  public TimeUnit getTimeUnit() { return timeUnit; }
  public ArithmeticExpr.Operator getOp() { return op; }

  @Override
  public String toSqlImpl() {
    StringBuilder strBuilder = new StringBuilder();
    if (funcName != null) {
      // Function-call like version.
      strBuilder.append(funcName.toUpperCase() + "(");
      strBuilder.append(getChild(0).toSql() + ", ");
      strBuilder.append("INTERVAL ");
      strBuilder.append(getChild(1).toSql());
      strBuilder.append(" " + timeUnitIdent);
      strBuilder.append(")");
      return strBuilder.toString();
    }
    if (intervalFirst) {
      // Non-function-call like version with interval as first operand.
      strBuilder.append("INTERVAL ");
      strBuilder.append(getChild(1).toSql() + " ");
      strBuilder.append(timeUnitIdent);
      strBuilder.append(" " + op.toString() + " ");
      strBuilder.append(getChild(0).toSql());
    } else {
      // Non-function-call like version with interval as second operand.
      strBuilder.append(getChild(0).toSql());
      strBuilder.append(" " + op.toString() + " ");
      strBuilder.append("INTERVAL ");
      strBuilder.append(getChild(1).toSql() + " ");
      strBuilder.append(timeUnitIdent);
    }
    return strBuilder.toString();
  }
}
