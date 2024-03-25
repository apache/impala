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

package org.apache.impala.calcite.rel.node;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.impala.common.ImpalaException;
import org.apache.calcite.rel.core.Filter;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ImpalaFilterRel:  There is no Impala PlanNode that maps directly
 * from this RelNode. When this RelNode gets hit in the tree, it passes
 * its filter condition down into a RelNode that can handle the filter.
 */
public class ImpalaFilterRel extends Filter
    implements ImpalaPlanRel {

  protected static final Logger LOG =
      LoggerFactory.getLogger(ImpalaFilterRel.class.getName());

  public ImpalaFilterRel(Filter filter) {
    super(filter.getCluster(), filter.getTraitSet(), filter.getInput(),
        filter.getCondition());
  }

  private ImpalaFilterRel(RelOptCluster cluster, RelTraitSet traits,
      RelNode child, RexNode condition) {
    super(cluster, traits, child, condition);
  }

  @Override
  public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
    return new ImpalaFilterRel(getCluster(), traitSet, input, condition);
  }

  @Override
  public NodeWithExprs getPlanNode(ParentPlanRelContext context) throws ImpalaException {
    Preconditions.checkState(getInputs().size() == 1);
    return getChildPlanNode(context);
  }

  private NodeWithExprs getChildPlanNode(ParentPlanRelContext context)
      throws ImpalaException {
    ImpalaPlanRel relInput = (ImpalaPlanRel) getInput(0);
    ParentPlanRelContext.Builder builder =
        new ParentPlanRelContext.Builder(context, this);
    RexNode newFilterCondition =
        createNewCondition(context.filterCondition_, getCondition());
    builder.setFilterCondition(newFilterCondition);

    // need to set the inputRefs.  The HdfsScan RelNode needs to know which columns are
    // needed from the table in order to implement the filter condition. The input ref
    // used here may nor may not be projected out. So a union needs to be done with
    // potential existing projected input refs from a parent RelNode.
    // Note that if the parent RelNode hasn't set any input refs, it is assumed that all
    // input refs are needed (the default case when inputRefs_ is null).
    if (context.inputRefs_ != null) {
      ImmutableBitSet inputRefs =
          RelOptUtil.InputFinder.bits(Lists.newArrayList(getCondition()), null);
      builder.setInputRefs(inputRefs.union(context.inputRefs_));
    }
    return relInput.getPlanNode(builder.build());
  }

  private RexNode createNewCondition(RexNode previousCondition, RexNode newCondition) {
    if (previousCondition == null) {
      return newCondition;
    }

    List<RexNode> conditions = ImmutableList.of(previousCondition, newCondition);
    return RexUtil.composeConjunction(getCluster().getRexBuilder(), conditions);
  }
}
