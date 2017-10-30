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
import org.apache.impala.analysis.BoolLiteral;
import org.apache.impala.analysis.Expr;

/**
 * Simplifies DISTINCT FROM and NOT DISTINCT FROM predicates
 * where the arguments are identical expressions.
 *
 * x IS DISTINCT FROM x -> false
 * x IS NOT DISTINCT FROM x -> true
 *
 * Note that "IS NOT DISTINCT FROM" and the "<=>" are the same.
 */
public class SimplifyDistinctFromRule implements ExprRewriteRule {
  public static ExprRewriteRule INSTANCE = new SimplifyDistinctFromRule();

  @Override
  public Expr apply(Expr expr, Analyzer analyzer) {
    if (!expr.isAnalyzed()) return expr;

    if (expr instanceof BinaryPredicate) {
      BinaryPredicate pred = (BinaryPredicate) expr;
      if (pred.getOp() == BinaryPredicate.Operator.NOT_DISTINCT) {
        if (pred.getChild(0).equals(pred.getChild(1))) {
          return new BoolLiteral(true);
        }
      }
      if (pred.getOp() == BinaryPredicate.Operator.DISTINCT_FROM) {
        if (pred.getChild(0).equals(pred.getChild(1))) {
          return new BoolLiteral(false);
        }
      }
    }
    return expr;
  }
}
