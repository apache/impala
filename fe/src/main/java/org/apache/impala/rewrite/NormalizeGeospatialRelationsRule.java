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
import org.apache.impala.analysis.BoolLiteral;
import org.apache.impala.analysis.CompoundPredicate;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.analysis.FunctionName;

import com.google.common.base.Preconditions;

/**
 * Normalizes symmetric st_ relations by ensuring that if one child is constant then
 * it is always the first child.
 * This would highly benefit Hive ESRI relations if constness would be propageted to
 * Java UDFs as those are optimized for const 1st argument(see IMPALA-14575 for details).
 *
 * Examples:
 * ST_Intersects(geom_col, ST_Polygon(1,1, 1,4, 4,4, 4,1))
 * ->
 * ST_Intersects(ST_Polygon(1,1, 1,4, 4,4, 4,1), geom_col)
 */
public class NormalizeGeospatialRelationsRule implements ExprRewriteRule {
  public static ExprRewriteRule INSTANCE = new NormalizeGeospatialRelationsRule();
  static List<String> symmetric_rels = Arrays.asList(
      "st_crosses", "st_disjoint", "st_equals", "st_envintersects",
      "st_intersects", "st_overlaps", "st_touches");

  @Override
  public Expr apply(Expr expr, Analyzer analyzer) {
    if (!(expr instanceof FunctionCallExpr)) return expr;
    FunctionCallExpr fn = (FunctionCallExpr) expr;
    FunctionName fnName = fn.getFnName();
    if (!fnName.isBuiltin()) return expr;
    String name = fnName.getFunction();
    if (!symmetric_rels.contains(name)) return expr;
    Expr arg1 = fn.getChild(0);
    Expr arg2 = fn.getChild(1);
    if (arg1.isConstant() || !arg2.isConstant()) return expr; // no need to swap
    return new FunctionCallExpr(name,  Arrays.asList(arg2, arg1));
  }
}
