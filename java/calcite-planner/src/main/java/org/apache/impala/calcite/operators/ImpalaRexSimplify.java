/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.impala.calcite.operators;

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.type.SqlTypeUtil;


/**
 * ImpalaRexSimplify extends the RexSimplify class in order to 'simplify' expressions.
 * The extension of the "simplify" method catches the double and float types used in
 * a binary predicate and avoids the simplification.  This is needed because Calcite
 * will simplify various cases of NaN the wrong way.  For example, NOT(my_col >= 3)
 * should include NaN in its calculation. But this gets simplified to my_col < 3
 * which does not include NaN in its calculation.
 */
public class ImpalaRexSimplify extends RexSimplify {
  private final RexExecutor rexExecutor_;

  public ImpalaRexSimplify(RexBuilder rexBuilder, RexExecutor rexExecutor) {
    super(rexBuilder, RelOptPredicateList.EMPTY, rexExecutor);
    rexExecutor_ = rexExecutor;
  }

  public RexExecutor getRexExecutor() {
    return rexExecutor_;
  }

  @Override
  public RexNode simplify(RexNode rexNode) {
    return hasApproximateTypeIssues(rexNode)
        ? rexNode
        : super.simplifyUnknownAs(rexNode, RexUnknownAs.FALSE);
  }

  private boolean hasApproximateTypeIssues(RexNode rexNode) {
    RexHasApproximateIssuesShuttle shuttle = new RexHasApproximateIssuesShuttle();
    shuttle.apply(rexNode);
    return shuttle.hasApproximateIssues_;
  }

  private static class RexHasApproximateIssuesShuttle extends RexShuttle {
    public boolean hasApproximateIssues_ = false;

    // Sets hasApproximateIssues to true if there is a binary predicate with a double
    // or float type in it.
    @Override
    public RexNode visitCall(RexCall rexCall) {
      super.visitCall(rexCall);
      if (rexCall.getOperator() instanceof SqlBinaryOperator) {
        if (SqlTypeUtil.isApproximateNumeric(rexCall.getOperands().get(0).getType()) ||
            SqlTypeUtil.isApproximateNumeric(rexCall.getOperands().get(1).getType())) {
          hasApproximateIssues_ = true;
        }
      }
      return rexCall;
    }
  }
}
