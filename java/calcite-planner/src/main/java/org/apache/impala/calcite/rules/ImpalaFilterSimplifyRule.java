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
package org.apache.impala.calcite.rules;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.impala.calcite.operators.ImpalaRexSimplify;
import org.apache.impala.calcite.operators.ImpalaRexUtil;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * ImpalaFilterSimplifyRule calls the given ImpalaRexSimplify.simplify()
 * method (derived from Calcite's RexSimplify) for the filter condition.
 */
public class ImpalaFilterSimplifyRule extends RelOptRule {

  private final ImpalaRexSimplify simplifier_;

  public ImpalaFilterSimplifyRule(ImpalaRexSimplify simplifier) {
    super(operand(Filter.class, none()));
    this.simplifier_ = simplifier;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Filter filter = call.rel(0);
    RelOptCluster cluster = filter.getCluster();
    RexBuilder rexBuilder = cluster.getRexBuilder();
    RexNode condition = filter.getCondition();

    RexNode newCondition = simplifier_.simplify(condition);

    if (newCondition.equals(condition)) {
      return;
    }

    Filter newFilter =
        filter.copy(filter.getTraitSet(), filter.getInput(0), newCondition);
    call.transformTo(newFilter);
  }
}
