/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.impala.calcite.schema;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.BuiltInMetadata.CumulativeCost;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.impala.calcite.rel.node.ImpalaPlanRel;
import org.apache.impala.calcite.rules.ImpalaMQContext;

import java.util.List;

/**
 * ImpalaRelMdNonCumulativeCost is used to give a cumulative cost value for
 * a RelNode tree. This is overridden from the current Calcite version. This
 * version takes inputRefs into account. At the TableScan level, the cost
 * should only be calculated using the inputRefs that are used rather than
 * taking the cost with all the columns. The Calcite Context (ImpalaMQContext)
 * is used to pass the information from parent RelNode to child RelNode.
 */
public class ImpalaRelMdCumulativeCost implements CumulativeCost.Handler {

  public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider
      .reflectiveSource(BuiltInMethod.CUMULATIVE_COST.method,
          new ImpalaRelMdCumulativeCost());

  @Override
  public RelOptCost getCumulativeCost(RelNode rel, RelMetadataQuery mq) {
    ImpalaMQContext mqContext =
        rel.getCluster().getPlanner().getContext().unwrap(ImpalaMQContext.class);

    ImpalaRelMdNonCumulativeCost nonCumulativeCostHandler =
        new ImpalaRelMdNonCumulativeCost();
    // calculate cost for this RelNode.
    RelOptCost cost = nonCumulativeCostHandler.getNonCumulativeCost(rel, mq);
    if (cost == null) {
      return null;
    }
    // recursively get cumulative costs for childrean and add them to this cost.
    List<RelNode> inputs = rel.getInputs();

    ImmutableBitSet savedInputRefs = mqContext.getInputRefs();
    for (int i = 0; i < inputs.size(); ++i) {
      mqContext.setInputRefs(getInputRefsForContext(rel, mqContext.getInputRefs(), i));
      RelOptCost inputCost = mq.getCumulativeCost(inputs.get(i));
      if (inputCost == null) {
        mqContext.setInputRefs(savedInputRefs);
        return null;
      }
      cost = cost.plus(inputCost);
    }
    mqContext.setInputRefs(savedInputRefs);
    return cost;
  }

  /**
   * Return the bitset of the inputrefs used for the given RelNode and its parent's
   * inputref bitset. If the RelNode passed in is a Join RelNode, only return the inputs
   * of the ith input (where i is 0 or 1).
   */
  private static ImmutableBitSet getInputRefsForContext(RelNode rel,
      ImmutableBitSet currentSet, int i) {
    switch (ImpalaPlanRel.getRelNodeType(rel)) {
      case AGGREGATE:
      case SORT:
      case UNION:
        // these RelNodes use all of its inputs, so we return the range from 0..size of
        // its input.
        return ImmutableBitSet.range(
            rel.getInputs().get(0).getRowType().getFieldList().size());
      case JOIN:
        int leftSize = rel.getInputs().get(0).getRowType().getFieldList().size();
        // only return the inputs on the ith side.
        return (i == 0)
            ? ImmutableBitSet.range(leftSize)
            : ImmutableBitSet.range(rel.getRowType().getFieldList().size() - leftSize);
      case PROJECT:
        // a Project will only use inputrefs found in the project.
        return RelOptUtil.InputFinder.bits(((Project) rel).getProjects(), null);
      case FILTER:
        // a filter will use all the inputs from its parent, but will also use any
        // inputrefs found in the condition. Note, we have to look at the parent of the
        // filter because if the parent is a project, we only want to use the input fields
        // from the parent project, not all the fields passed into the filter.
        return currentSet.union(
            RelOptUtil.InputFinder.bits(((Filter) rel).getCondition()));
      default:
        throw new RuntimeException("Unknown RelNodeType: " +
            ImpalaPlanRel.getRelNodeType(rel));
    }
  }
}
