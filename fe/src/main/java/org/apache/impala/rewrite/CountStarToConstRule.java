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
import org.apache.impala.analysis.ArithmeticExpr;
import org.apache.impala.analysis.ArithmeticExpr.Operator;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.analysis.NumericLiteral;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;

/**
 * Rewrite rule to replace plain count star function call expr to const expr.
 * Examples:
 * 1. Iceberg V1 Table
 * 1.1 "SELECT COUNT(*) FROM ice_tbl" -> "SELECT `CONST`"
 * 1.2 "SELECT COUNT(*),MIN(col_a),MAX(col_b) FROM ice_tbl" -> "SELECT `CONST`,MIN(col_a),
 * MAX(col_b) FROM ice_tbl"
 *
 * 2. Iceberg V2 Table
 *
 *     AGGREGATE
 *     COUNT(*)
 *         |
 *     UNION ALL
 *    /        \
 *   /          \
 *  /            \
 * SCAN all  ANTI JOIN
 * datafiles  /      \
 * without   /        \
 * deletes  SCAN      SCAN
 *          datafiles deletes
 *
 *          ||
 *        rewrite
 *          ||
 *          \/
 *
 *    ArithmeticExpr(ADD)
 *    /             \
 *   /               \
 *  /                 \
 * record_count  AGGREGATE
 * of all        COUNT(*)
 * datafiles         |
 * without       ANTI JOIN
 * deletes      /         \
 *             /           \
 *             SCAN        SCAN
 *             datafiles   deletes
 */
public enum CountStarToConstRule implements ExprRewriteRule {

  INSTANCE,
  ;

  @Override
  public Expr apply(Expr expr, Analyzer analyzer) throws AnalysisException {
    if (expr.isRewritten()) return expr;
    if (!FunctionCallExpr.isCountStarFunctionCallExpr(expr)) return expr;
    if (analyzer.canRewriteCountStarForV1()) {
      return LiteralExpr.createFromUnescapedStr(String.valueOf(
          analyzer.getTotalRecordsNumV1()), Type.BIGINT);
    } else if (analyzer.canRewriteCountStartForV2()) {
      expr.setRewritten(true);
      return new ArithmeticExpr(Operator.ADD, expr, NumericLiteral.create(
          analyzer.getTotalRecordsNumV2()));
    } else {
      return expr;
    }
  }
}
