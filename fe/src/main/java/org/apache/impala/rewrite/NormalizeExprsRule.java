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
import org.apache.impala.analysis.BoolLiteral;
import org.apache.impala.analysis.CompoundPredicate;
import org.apache.impala.analysis.Expr;

/**
 * Normalizes CompoundPredicates by ensuring that if either child of AND or OR is a
 * BoolLiteral, then the left (i.e. first) child is a BoolLiteral.
 *
 * Examples:
 * id = 0 && true -> true && id = 0
 */
public class NormalizeExprsRule implements ExprRewriteRule {
  public static ExprRewriteRule INSTANCE = new NormalizeExprsRule();

  @Override
  public Expr apply(Expr expr, Analyzer analyzer) {
    if (!expr.isAnalyzed()) return expr;

    // TODO: add normalization for other expr types.
    if (expr instanceof CompoundPredicate) {
      return normalizeCompoundPredicate((CompoundPredicate) expr, analyzer);
    }
    return expr;
  }

  private Expr normalizeCompoundPredicate(CompoundPredicate expr, Analyzer analyzer) {
    if (expr.getOp() == CompoundPredicate.Operator.NOT) return expr;

    if (!(expr.getChild(0) instanceof BoolLiteral)
        && expr.getChild(1) instanceof BoolLiteral) {
      CompoundPredicate newExpr = new CompoundPredicate(expr.getOp(), expr.getChild(1),
          expr.getChild(0));
      newExpr.analyzeNoThrow(analyzer);
      return newExpr;
    }
    return expr;
  }
}
