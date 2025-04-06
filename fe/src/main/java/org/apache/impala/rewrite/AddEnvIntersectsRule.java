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

import java.util.Arrays;
import java.util.List;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.CompoundPredicate;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.analysis.FunctionName;
import org.apache.impala.common.AnalysisException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Add st_envintersects() to conjuncts with a geospatial relation that can be true only
 * if the bounding rects (envelopes) intersect.
 *
 * This is mainly beneficial because st_envintersects has a native implementation
 * (IMPALA-14573) but other relations are not yet implemented in c++.
 *
 * Examples:
 *  st_intersects(geom1, geom2)
 *    ->
 *  st_envintersects(geom1, geom2) AND
 *  st_intersects(geom1, geom2)
 *
 */
public class AddEnvIntersectsRule implements ExprRewriteRule {
  public static ExprRewriteRule INSTANCE = new AddEnvIntersectsRule();

  static final List<String> relations = Arrays.asList("st_contains", "st_crosses",
      "st_equals", "st_intersects", "st_overlaps", "st_touches", "st_within");

  // Common subexpressions are computed multiple times (IMPALA-7737), so this rule is only
  // beneficial if children are cheap. The limit is above JAVA_FUNCTION_CALL_COST to allow
  // an St_Point() and a few other expressions (needed for PointEnvIntersectsRule).
  // TODO: rethink cost limit once st_point() get c++ implementation (IMPALA-14629)
  static final float CHILD_COST_LIMIT = Expr.JAVA_FUNCTION_CALL_COST * 2;

  @Override
  public Expr apply(Expr expr, Analyzer analyzer) throws AnalysisException {
    if (!(expr instanceof FunctionCallExpr)) return expr;
    FunctionCallExpr fn = (FunctionCallExpr) expr;
    FunctionName fnName = fn.getFnName();
    if (!fnName.isBuiltin()) return expr;
    if (!relations.contains(fnName.getFunction())) return expr;

    Expr arg1 = expr.getChild(0).clone();
    Expr arg2 = expr.getChild(1).clone();
    if (!arg1.hasCost() || !arg2.hasCost()
        || arg1.getCost() + arg2.getCost() > CHILD_COST_LIMIT) {
      return expr;
    }
    Expr newPred = new FunctionCallExpr("st_envintersects", Arrays.asList(arg1, arg2));
    return new CompoundPredicate(CompoundPredicate.Operator.AND, newPred, expr.clone());
  }
}
