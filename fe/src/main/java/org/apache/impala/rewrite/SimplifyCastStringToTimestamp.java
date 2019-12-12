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
import org.apache.impala.analysis.CastExpr;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.analysis.FunctionName;
import org.apache.impala.analysis.TypeDef;
import org.apache.impala.common.AnalysisException;
import com.google.common.collect.Lists;


/**
 ** Case 1:
 ** Simplify 'string -> bigint -> timestamp' TO 'string -> timestamp':
 ** cast(unix_timestamp('timestr') as timestamp) -> cast('timestr' as timestamp)
 **
 ** Case 2:
 ** Simplify 'string[fmt] -> bigint -> timestamp' TO 'string -> timestamp':
 ** cast(unix_timestamp('timestr', 'fmt') as timestamp) -> to_timestamp('timestr', 'fmt')
 **/

public class SimplifyCastStringToTimestamp implements ExprRewriteRule {
  public static ExprRewriteRule INSTANCE = new SimplifyCastStringToTimestamp();

  @Override
  public Expr apply(Expr expr, Analyzer analyzer) throws AnalysisException {
    if (expr instanceof CastExpr &&
        !((CastExpr)expr).isImplicit() &&
        expr.getChild(0) instanceof FunctionCallExpr) {
      if (!expr.isAnalyzed())
        expr.analyze(analyzer);

      FunctionCallExpr fce = (FunctionCallExpr)expr.getChild(0);
      if (!expr.getType().isTimestamp() ||
          !fce.getFnName().getFunction().equalsIgnoreCase("unix_timestamp"))
        return expr;

      Expr simplifiedExpr = null;
      if (fce.getChildren().size() == 1 &&
          fce.getChild(0).getType().isStringType()) {
        // Handle Case 1
        simplifiedExpr = new CastExpr(new TypeDef(expr.getType()), fce.getChild(0));
      } else if (fce.getChildren().size() == 2 &&
                 fce.getChild(0).getType().isStringType() &&
                 fce.getChild(1).getType().isStringType()) {
        // Handle Case 2
        simplifiedExpr = new FunctionCallExpr(new FunctionName("to_timestamp"),
            Lists.newArrayList(fce.getChildren()));
      }

      if (simplifiedExpr != null) {
        simplifiedExpr.analyze(analyzer);
        return simplifiedExpr;
      }
    }

    return expr;
  }
}
