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

import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.analysis.Analyzer;

/*
 * Rewrite rule to replace count distinct to ndv
 *
 */
public class CountDistinctToNdvRule implements ExprRewriteRule{

    // Singleton
    public static CountDistinctToNdvRule INSTANCE = new CountDistinctToNdvRule();

    /*
     * This is an implementation of IMPALA-110.
     * Replace count distinct operators to NDVs if APPX_COUNT_DISTINCT is set.
     * Will traverse the expression tree and return modified expr.
     *
     * @param expr expr to be checked
     * @param analyzer query analyzer
     */
    @Override
    public Expr apply(Expr expr, Analyzer analyzer) throws AnalysisException {
        // Short-circuit rules
        if (!analyzer.getQueryCtx().client_request.query_options.appx_count_distinct) {
            return expr;
        }
        if (!(expr instanceof FunctionCallExpr) || !expr.isAnalyzed()) return expr;

        // Returns if expr is not count(distinct), or it has more 1 param.
        FunctionCallExpr oldFunctionCallExpr = (FunctionCallExpr) expr;
        if (!oldFunctionCallExpr.getFnName().getFunction().equals("count")
                || !oldFunctionCallExpr.isDistinct()
                || oldFunctionCallExpr.getParams().exprs().size() != 1 ) {
            return expr;
        }

        // Create a new function `ndv(<expr>)` to substitute the `count(distinct <expr>)`
        FunctionCallExpr ndvFunc = new FunctionCallExpr("ndv",
                oldFunctionCallExpr.getParams().exprs());

        // Analyze the newly added FunctionCall, otherwise follow-up rules won't fire.
        ndvFunc.analyze(analyzer);
        return ndvFunc;
    }
}
