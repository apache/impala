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
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.analysis.FunctionName;
import org.apache.impala.analysis.FunctionParams;

/**
 * Replaces count(<literal>) with an equivalent count{*}.
 *
 * Examples:
 * count(1)    --> count(*)
 * count(2017) --> count(*)
 * count(null) --> count(null)
 */
public class NormalizeCountStarRule implements ExprRewriteRule {
  public static ExprRewriteRule INSTANCE = new NormalizeCountStarRule();

  @Override
  public Expr apply(Expr expr, Analyzer analyzer) {
    if (!(expr instanceof FunctionCallExpr)) return expr;
    FunctionCallExpr origExpr = (FunctionCallExpr) expr;
    if (!origExpr.getFnName().getFunction().equalsIgnoreCase("count")) return expr;
    if (origExpr.getParams().isStar()) return expr;
    if (origExpr.getParams().isDistinct()) return expr;
    if (origExpr.getParams().exprs().size() != 1) return expr;
    Expr child = origExpr.getChild(0);
    if (!Expr.IS_LITERAL.apply(child)) return expr;
    if (Expr.IS_NULL_VALUE.apply(child)) return expr;
    FunctionCallExpr result = new FunctionCallExpr(new FunctionName("count"),
        FunctionParams.createStarParam());
    return result;
  }

  private NormalizeCountStarRule() {}
}
