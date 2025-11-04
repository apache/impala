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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.impala.calcite.operators.ImpalaRexSimplify;
import org.apache.impala.calcite.operators.ImpalaRexUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * ImpalaProjectSimplifyRule calls the given ImpalaRexSimplify.simplify()
 * method (derived from Calcite's RexSimplify) for all the columns in the
 * Project RelNode.
 */
public class ImpalaProjectSimplifyRule extends RelOptRule {

  private final ImpalaRexSimplify simplifier_;

  public ImpalaProjectSimplifyRule(ImpalaRexSimplify simplifier) {
    super(operand(Project.class, none()));
    this.simplifier_ = simplifier;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Project projectRel = call.rel(0);
    RelOptCluster cluster = projectRel.getCluster();
    RexBuilder rexBuilder = cluster.getRexBuilder();

    List<RexNode> newProjects = new ArrayList<>();
    boolean projectChanged = false;
    for (RexNode project : projectRel.getProjects()) {
      RexNode newProject = simplifier_.simplify(project);

      // Need to make sure the nullables match for projects so the parent row
      // type gets the nullability it expects.
      if (newProject.getType().isNullable() != project.getType().isNullable()) {
        newProject = ImpalaRexUtil.makeNullable(rexBuilder, newProject,
            project.getType().isNullable());
      }
      if (!newProject.equals(project)) {
        projectChanged = true;
      }
      newProjects.add(newProject);
    }

    if (!projectChanged) {
      return;
    }

    // only create the new project if any projects changed.
    Project newProjectRel = projectRel.copy(projectRel.getTraitSet(),
        projectRel.getInput(0), newProjects, projectRel.getRowType());
    call.transformTo(newProjectRel);
  }
}
