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

package org.apache.impala.calcite.rel.util;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.impala.analysis.Expr;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;

import java.util.List;

/**
 * CreateExprVisitor will generate Impala expressions for function calls and literals.
 * TODO: In this iteration, it only handles input refs.
 */
public class CreateExprVisitor extends RexVisitorImpl<Expr> {

  private final List<Expr> inputExprs_;

  public CreateExprVisitor(List<Expr> inputExprs) {
    super(false);
    this.inputExprs_ = inputExprs;
  }

  @Override
  public Expr visitInputRef(RexInputRef rexInputRef) {
    return inputExprs_.get(rexInputRef.getIndex());
  }

  @Override
  public Expr visitCall(RexCall rexCall) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public Expr visitLiteral(RexLiteral rexLiteral) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public Expr visitLocalRef(RexLocalRef localRef) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public Expr visitOver(RexOver over) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public Expr visitCorrelVariable(RexCorrelVariable correlVariable) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public Expr visitDynamicParam(RexDynamicParam dynamicParam) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public Expr visitRangeRef(RexRangeRef rangeRef) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public Expr visitFieldAccess(RexFieldAccess fieldAccess) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public Expr visitSubQuery(RexSubQuery subQuery) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public Expr visitTableInputRef(RexTableInputRef fieldRef) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public Expr visitPatternFieldRef(RexPatternFieldRef fieldRef) {
    throw new RuntimeException("Not supported");
  }

  /**
   * Wrapper around visitor which catches the unchecked RuntimeException and throws
   * an ImpalaException.
   */
  public static Expr getExpr(CreateExprVisitor visitor, RexNode operand)
      throws ImpalaException {
    try {
      return operand.accept(visitor);
    } catch (Exception e) {
      throw new AnalysisException(e);
    }
  }
}
