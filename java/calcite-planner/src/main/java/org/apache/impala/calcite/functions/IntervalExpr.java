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

package org.apache.impala.calcite.functions;

import com.google.common.base.Preconditions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.ToSqlOptions;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TExprNode;

/**
 * IntervalExpr is a temporary expression class. It is needed because of
 * the way Calcite handles Interval expressions and how the Calcite RexNode  to
 * Impala translation works.
 * A calcite timestamp add expression might look like this:
 * TIMESTAMP +( TIMESTAMP, INTERVAL YEAR *( INT, INTERVAL_YEAR)
 * The main "plus" operation returns a timestamp.
 * The two parameters are a timestamp and the interval to add.
 * The inner "multiply" is an interval expression.  The first parameter
 * of the interval expression in this case is the number of years to
 * multiply. The second parameter is a default "1" of INTERVAL_YEAR.
 *
 * To be concrete about this,  if we were adding 12 years to our timestamp
 * expression, the inner interval expression looks like this:
 * mult(12, 1: INTERVAL_YEAR)
 *
 * This inner interval expression doesn't have any Impala equivalent, so that's
 * why this class is necessary. This object will only exist as a placeholder.
 * When the + expression is created as a ArithTimestampExpr, the information of
 * the IntervalExpr will be used and then the object will be discarded.
 */
public class IntervalExpr extends Expr {

  private final RexCall intervalExpr_;

  private final Expr literal_;

  private final String timeUnit_;

  public IntervalExpr(RexCall intervalExpr, Expr literalExpr) {
    Preconditions.checkState(intervalExpr.getOperator().getKind().equals(SqlKind.TIMES));
    Preconditions.checkState(SqlTypeName.INTERVAL_TYPES.contains(
        intervalExpr.getOperands().get(1).getType().getSqlTypeName()));
    intervalExpr_ = intervalExpr;
    literal_ = literalExpr;
    timeUnit_ = calcTimeIntervalUnit(intervalExpr);

  }

  public Expr getLiteral() {
    return literal_;
  }

  public String getTimeUnit() {
    return timeUnit_;
  }

  private String calcTimeIntervalUnit(RexCall call) {
    // hack.  Not sure the best way to get week out.  Also need to figure out
    // fracitional seconds
    if (call.getOperands().get(1).getType().toString().contains("WEEK")) {
      return "WEEK";
    }
    if (call.getOperands().get(1).getType().toString().contains("MILLISECOND")) {
      return "MILLISECOND";
    }
    if (call.getOperands().get(1).getType().toString().contains("MICROSECOND")) {
      return "MICROSECOND";
    }
    if (call.getOperands().get(1).getType().toString().contains("NANOSECOND")) {
      return "NANOSECOND";
    }
    return call.getOperands().get(1).getType().getSqlTypeName().getStartUnit().toString();
  }

  @Override
  protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    throw new RuntimeException("not implemented");
  }

  @Override
  protected float computeEvalCost() {
    throw new RuntimeException("not implemented");
  }

  @Override
  protected String toSqlImpl(ToSqlOptions options) {
    throw new RuntimeException("not implemented");
  }

  @Override
  protected void toThrift(TExprNode msg) {
    throw new RuntimeException("not implemented");
  }

  @Override
  public Expr clone() {
    throw new RuntimeException("not implemented");
  }
}
