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
import org.apache.impala.analysis.CompoundVerticalBarExpr;

/**
 * Replaces || predicates
 * with OR if the given arguments are BOOLEAN
 * or with concat function call if the given arguments are STRING
 */
public class ExtractCompoundVerticalBarExprRule implements ExprRewriteRule {
  public static ExprRewriteRule INSTANCE = new ExtractCompoundVerticalBarExprRule();

  @Override
  public Expr apply(Expr expr, Analyzer analyzer) {
    if (expr instanceof CompoundVerticalBarExpr) {
      CompoundVerticalBarExpr pred = (CompoundVerticalBarExpr) expr;
      if (!expr.isAnalyzed()) {
        pred = (CompoundVerticalBarExpr) expr.clone();
        pred.analyzeNoThrow(analyzer);
      }
      return pred.getEncapsulatedExpr();
    }
    return expr;
  }
}
