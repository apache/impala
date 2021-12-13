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

import com.google.common.base.Preconditions;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.CastExpr;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.NullLiteral;
import org.apache.impala.catalog.Type;

/**
 * This rewrite rule simplifies cast expr when cast target data type is the same as inner
 * expr data type.
 * Examples:
 * CAST(str_col AS STRING) ==> str_col
 * SUM(CAST(int_col AS INT)) ==> SUM(int_col)
 * CAST(int_col+int_col AS INT) ==> int_col+int_col
 * CAST(CAST(col as BIGINT) as BIGINT) ==> CAST(col as BIGINT)
 *
 * This rule is also valid for complex type.
 * Examples:
 * CAST(int_array.item as INT) ==> int_array.item
 * CAST(string_map.value as STRING) ==> string_map.value
 * CAST(CAST(array.item as INT) AS INT) ==> CAST(array.item as INT)
 * CAST(CAST(map.value as STRING) AS STRING) ==> CAST(map.value as STRING)
 *
 * Pay attention:
 * For varchar type, we remove cast expr only when lengths are the same.
 * For decimal type, we remove cast expr only when precisions and scales are the same.
 */
public class SimplifyCastExprRule implements ExprRewriteRule {
  public static ExprRewriteRule INSTANCE = new SimplifyCastExprRule();

  @Override
  public Expr apply(Expr expr, Analyzer analyzer) {
    if (!(expr instanceof CastExpr)) return expr;

    Preconditions.checkState(expr.getChildren().size() == 1);
    CastExpr castExpr = (CastExpr) expr;
    Expr child = castExpr.getChild(0);
    //In analysis phase, CaseExpr will convert child to target type, we cannot rewrite in
    //this situation, such as cast(cast(NULL as string) as timestamp FORMAT 'YYYY-MM-DD').
    //If we replace inner 'cast(NULL as string)' as 'NULL', query will throw exception.
    if (child instanceof NullLiteral) return castExpr;

    //Expr must analyzed before rewrite. Without analyze, expr type is 'INVALID'
    if (!castExpr.isAnalyzed()) castExpr.analyzeNoThrow(analyzer);
    if (!child.isAnalyzed()) child.analyzeNoThrow(analyzer);
    Preconditions.checkState(castExpr.getType() != Type.INVALID &&
        child.getType() != Type.INVALID, "'castExpr' must be analyzed before rewrite");
    return castExpr.getType().matchesType(child.getType())? child : castExpr;
  }
}
