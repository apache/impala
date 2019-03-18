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
import org.apache.impala.analysis.CastExpr;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.analysis.StringLiteral;
import org.apache.impala.analysis.TypeDef;
import org.apache.impala.common.AnalysisException;

/**
 * Removes redundant explicit string casts (includes String, Char and Varchar
 * types) from binary predicates of the form
 * "cast(<non-const expr> to <string type>) <eq/ne op> <string literal>"
 * because casting and comparing as string is expensive. Redundancy is
 * established by making sure that the equivalence relationship will be same
 * even if the constant is cast to the non-const expression type, that is, check
 * if the following is true: cast(cast(<string literal> as typeOf(<non-const expr>))
 * as typeOf(<original cast expr>)) == <string literal>
 *
 * It relies on NormalizeBinaryPredicatesRule and FoldConstantsRule so that
 * <string literal> is always on the right hand side and all constant Exprs have been
 * evaluated and converted to LiteralExprs.
 *
 * Examples:
 * Few cases that are successfully rewritten:
 * 1. cast(int_col as string) = '123456' -> int_col = 123456
 * 2. cast(timestamp_col as string) = '2009-01-01 00:01:00' -> timestamp_col =
 *    TIMESTAMP '2009-01-01 00:01:00'
 *
 * Few cases that are not rewritten:
 * 1. cast(int_col as string) = '0123456' -> no change as equivalence relationship
 *    is not the same if '0123456' is cast to int.
 * 2. cast(tinyint_col as char(2)) = '100' -> no change as casting '100' to char(2) would
 *    give '10' and hence failing the redundancy test.
 * 3. cast(smallint_col as string) = '1000000000' -> no change due to limitations of this
 *    rule as during the intermediate step of the the redundancy check, converting
 *    '1000000000' to smallint will overflow and give wrong result.
 */
public class RemoveRedundantStringCast implements ExprRewriteRule {
  public static ExprRewriteRule INSTANCE = new RemoveRedundantStringCast();

  @Override
  public Expr apply(Expr expr, Analyzer analyzer) throws AnalysisException {
    if(!(expr instanceof BinaryPredicate)) return expr;
    BinaryPredicate.Operator op = ((BinaryPredicate) expr).getOp();
    boolean isPotentiallyRedundantCast =
        (op == BinaryPredicate.Operator.EQ || op == BinaryPredicate.Operator.NE) &&
        (expr.getChild(0).ignoreImplicitCast() instanceof CastExpr) &&
        expr.getChild(0).ignoreImplicitCast().getType().isStringType() &&
        expr.getChild(1).getType().isStringType() &&
        Expr.IS_LITERAL.apply(expr.getChild(1));

    if (!isPotentiallyRedundantCast) return expr;
    // Ignore the implicit casts added during parsing.
    Expr castExpr = expr.getChild(0).ignoreImplicitCast();
    Expr castExprChild = castExpr.getChild(0).ignoreImplicitCast();
    LiteralExpr literalExpr = (LiteralExpr) expr.getChild(1);
    // Now check for redundancy.
    Expr castForRedundancyCheck = new CastExpr(new TypeDef(castExpr.getType()),
        new CastExpr(new TypeDef(castExprChild.getType()), literalExpr));
    castForRedundancyCheck.analyze(analyzer);
    LiteralExpr resultOfReverseCast = LiteralExpr.createBounded(castForRedundancyCheck,
        analyzer.getQueryCtx(), StringLiteral.MAX_STRING_LEN);
    // Need to trim() while comparing char(n) types as conversion might add trailing
    // spaces to the 'resultOfReverseCast'.
    if (resultOfReverseCast != null &&
        !Expr.IS_NULL_VALUE.apply(resultOfReverseCast) &&
        resultOfReverseCast.getStringValue().trim()
            .equals(literalExpr.getStringValue().trim())) {
      return new BinaryPredicate(op, castExprChild,
          castForRedundancyCheck.getChild(0).ignoreImplicitCast());
    }
    return expr;
  }
}
