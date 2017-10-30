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

package org.apache.impala.rewrite;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.Expr;

/**
 * Normalizes binary predicates of the form <expr> <op> <slot> so that the slot is
 * on the left hand side. Predicates where <slot> is wrapped in a cast (implicit or
 * explicit) are normalized, too. Predicates of the form <constant> <op> <expr>
 * are also normalized so that <constant> is always on the right hand side.
 *
 * Examples:
 * 5 > id -> id < 5
 * cast(0 as double) = id -> id = cast(0 as double)
 * 5 = id + 2 -> id + 2 = 5
 *
 */
public class NormalizeBinaryPredicatesRule implements ExprRewriteRule {
  public static ExprRewriteRule INSTANCE = new NormalizeBinaryPredicatesRule();

  @Override
  public Expr apply(Expr expr, Analyzer analyzer) {
    if (!(expr instanceof BinaryPredicate)) return expr;

    if (isExprOpSlotRef(expr) || isConstantOpExpr(expr)) {
      BinaryPredicate.Operator op = ((BinaryPredicate) expr).getOp();
      return new BinaryPredicate(op.converse(), expr.getChild(1), expr.getChild(0));
    }
    return expr;
  }

  boolean isConstantOpExpr(Expr expr) {
    return expr.getChild(0).isConstant() && !expr.getChild(1).isConstant();
  }

  boolean isExprOpSlotRef(Expr expr) {
    return expr.getChild(0).unwrapSlotRef(false) == null
        && expr.getChild(1).unwrapSlotRef(false) != null;
  }
}
