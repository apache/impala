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
import org.apache.impala.analysis.NumericLiteral;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;

import java.math.BigDecimal;
import java.util.List;

/**
 * Rewrite rule to set NDV call's default scale.
 *
 * If DEFAULT_NDV_SCALE query option is set. This rule will rewrite the NDV calls
 * to Scaled NDV calls. Note this rule works with APPX_COUNT_DISTINCT option. If
 * APPX_COUNT_DISTINCT is set. Any count(distinct) call will be firstly converted
 * to NDV, and then converted to NDV(scale).
 */
public class DefaultNdvScaleRule implements ExprRewriteRule{
    // Singleton
    public static DefaultNdvScaleRule INSTANCE = new DefaultNdvScaleRule();

    /**
     * Applies this rewrite rule to the given analyzed Expr. Returns the transformed and
     * analyzed Expr or the original unmodified Expr if no changes were made. If any
     * changes were made, the new Expr is guaranteed to be a different Expr object,
     * so callers can rely on object reference comparison for change detection.
     *
     * @param expr expr to be checked
     * @param analyzer default analyzer
     */
    @Override
    public Expr apply(Expr expr, Analyzer analyzer) throws AnalysisException {
        // Short-circuit rules
        if (!(expr instanceof FunctionCallExpr) || !expr.isAnalyzed()) return expr;
        int scale = analyzer.getQueryOptions().getDefault_ndv_scale();
        if (scale > 10 || scale < 1 || scale == 2) return expr;

        // Returns if expr is not ndv, or it is scaled ndv already.
        FunctionCallExpr oldFunctionCallExpr = (FunctionCallExpr) expr;
        if (!oldFunctionCallExpr.getFnName().getFunction().equals("ndv")
                || oldFunctionCallExpr.getParams().exprs().size() > 1) {
            return expr;
        }

        // Do substitution
        List<Expr> params = oldFunctionCallExpr.getParams().exprs();
        params.add(new NumericLiteral(BigDecimal.valueOf(scale), Type.INT));
        FunctionCallExpr functionCall =  new FunctionCallExpr("ndv", params);
        functionCall.analyze(analyzer);

        return functionCall;
    }
}
