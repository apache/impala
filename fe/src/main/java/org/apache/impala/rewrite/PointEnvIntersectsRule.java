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
 * Turns st_envintersects() on points from x/y pairs to applying bounding rect
 * intersection directly on the input coordinates.
 *
 * Examples:
 *  st_envintersects(CONST_GEOM, st_point(x, y))
 *    ->
 *  x >= st_minx(CONST_GEOM) AND y >= st_miny(CONST_GEOM) AND
 *  x <= st_maxx(CONST_GEOM) AND y <= st_maxy(CONST_GEOM)
 *
 *  st_maxx(CONST_GEOM) and others will be rewritten to constants by FoldConstantsRule, so
 *  the final conjuncts will be like x <= CONST_DOUBLE, which can be often pushed down
 *  efficiently (e.g. to Parquet or Iceberg).
 *
 *  Warning:
 *    this rule is only valid in planar geometry, in the spherical case
 *    shapes that intersect the antimeridian would need special handling
 */
public class PointEnvIntersectsRule implements ExprRewriteRule {
  public static ExprRewriteRule INSTANCE = new PointEnvIntersectsRule();

  // Common subexpressions are computed multiple times (IMPALA-7737), so this rule is only
  // beneficial if children are very cheap.
  static final float CHILD_COST_LIMIT = Expr.FUNCTION_CALL_COST * 2;

  @Override
  public Expr apply(Expr expr, Analyzer analyzer) throws AnalysisException {
    if (!(expr instanceof FunctionCallExpr)) return expr;
    FunctionCallExpr fn = (FunctionCallExpr) expr;
    FunctionName fnName = fn.getFnName();
    if (!fnName.isBuiltin()) return expr;
    if (!fnName.getFunction().equals("st_envintersects")) return expr;

    // first args must be const due to NormalizeGeospatialRelationsRule
    Expr constChild = expr.getChild(0);
    if (!constChild.isConstant()) return expr;
    Expr pointChild = expr.getChild(1);

    Preconditions.checkState(constChild.getType().isBinary());
    if (!(pointChild instanceof FunctionCallExpr)) return expr;
    FunctionCallExpr pointFn = (FunctionCallExpr) pointChild;
    if (!pointFn.getFnName().getFunction().equalsIgnoreCase("st_point")) return expr;
    // TODO: maybe do this only if child expressions are simple?
    if (pointFn.getChildCount() != 2
        || !pointFn.getChild(0).getType().isFloatingPointType()
        || !pointFn.getChild(1).getType().isFloatingPointType()) {
      return expr;
    }
    Expr x = pointFn.getChild(0).clone();
    Expr y = pointFn.getChild(1).clone();
    if (!x.hasCost() || !y.hasCost() || x.getCost() + y.getCost() > CHILD_COST_LIMIT) {
      return expr;
    }
    Expr minx = new FunctionCallExpr("st_minx",  Arrays.asList(constChild.clone()));
    Expr miny = new FunctionCallExpr("st_miny",  Arrays.asList(constChild.clone()));
    Expr maxx = new FunctionCallExpr("st_maxx",  Arrays.asList(constChild.clone()));
    Expr maxy = new FunctionCallExpr("st_maxy",  Arrays.asList(constChild.clone()));
    List<Expr> predicates = Arrays.asList(
      new BinaryPredicate(BinaryPredicate.Operator.LE, minx, x),
      new BinaryPredicate(BinaryPredicate.Operator.LE, miny, y),
      new BinaryPredicate(BinaryPredicate.Operator.GE, maxx, x),
      new BinaryPredicate(BinaryPredicate.Operator.GE, maxy, y)
    );
    return CompoundPredicate.createConjunctivePredicate(predicates);
  }

}
