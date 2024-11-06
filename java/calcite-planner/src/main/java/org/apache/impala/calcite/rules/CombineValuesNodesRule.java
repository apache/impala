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

package org.apache.impala.calcite.rules;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * CombineValuesNodesRule is a rule to combine multiple Values RelNodes
 * into a single Values RelNode with multiple tuples.
 *
 * This is needed when there are many literals in an IN clause. Calcite creates
 * a RelNode for each literal this which becomes incredibly slow at execution time.
 *
 * This rule only kicks in if there is a Union RelNode on top of multiple values
 * RelNodes.
 */
public class CombineValuesNodesRule extends RelOptRule {

  public CombineValuesNodesRule() {
      super(operand(LogicalUnion.class, none()));
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalUnion union = call.rel(0);

    List<RelNode> newRelNodes = new ArrayList<>();
    RelDataType rowType = union.getRowType();
    ImmutableList.Builder<ImmutableList<RexLiteral>> rowBuilder =
        new ImmutableList.Builder();
    int numTuples = 0;
    for (RelNode input : union.getInputs()) {
      // Calcite creates the HepRelVertex as an intermediary when doing optimizations, so
      // the Values RelNode needs to be retrieved off of this.
      RelNode realInput = input instanceof HepRelVertex
          ? ((HepRelVertex) input).getCurrentRel()
          : input;
      if (realInput instanceof LogicalValues) {
        rowBuilder.addAll(((LogicalValues) realInput).getTuples());
        numTuples++;
      } else {
        // If it's something other than a Values RelNode, the input will not be combined
        // with the Values RelNode and will be kept as/is.
        newRelNodes.add(input);
      }
    }
    if (numTuples > 1) {
      LogicalValues newValues =
          LogicalValues.create(union.getCluster(), rowType, rowBuilder.build());
      newRelNodes.add(newValues);
      LogicalUnion newUnion = union.copy(union.getTraitSet(), newRelNodes, union.all);
      call.transformTo(newUnion);
    }
  }
}
