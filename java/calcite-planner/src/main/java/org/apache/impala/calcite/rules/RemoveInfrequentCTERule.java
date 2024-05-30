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

import java.util.List;

import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.impala.calcite.rel.node.ImpalaCTEConsumer;

/**
 * Rule that removes CTEs from the plan by expanding the respective TableScans.
 * The rule assumes that all materializations registered in the planner refer to CTEs.
 */
public class RemoveInfrequentCTERule extends RelRule<CTERuleConfig> {

  public RemoveInfrequentCTERule(CTERuleConfig config) {
    super(config);
    if (config.referenceThreshold() <= 0) {
      throw new IllegalArgumentException(
          "Invalid reference threshold:" + config.referenceThreshold());
    }
  }

  @Override
  public boolean matches(final RelOptRuleCall call) {
    ImpalaCTEConsumer consumer = call.rel(0);
    return config.tableOccurrences().getOrDefault(consumer.getQualifiedName(), 0)
        <= config.referenceThreshold();
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    ImpalaCTEConsumer consumer = call.rel(0);
    List<String> cteName = consumer.getQualifiedName();
    for (RelOptMaterialization cte : call.getPlanner().getMaterializations()) {
      if (cteName.equals(cte.qualifiedTableName)) {
        call.transformTo(cte.queryRel);
      }
    }
  }
}
