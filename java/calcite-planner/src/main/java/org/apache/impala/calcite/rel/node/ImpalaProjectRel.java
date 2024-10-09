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

package org.apache.impala.calcite.rel.node;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.calcite.rel.util.CreateExprVisitor;
import org.apache.impala.common.ImpalaException;

import java.util.List;


/**
 * ImpalaProjectRel is the Impala specific RelNode corresponding to
 * Project.
 *
 * There is no PlanNode equivalent for ProjectRel in Impala. The output expressions
 * are generated and passed to its parent PlanNode (or the final output exprs if the
 * Project is at the top of the tree). The input references are passed into the child
 * PlanNode for pruning purposes (HdfsScan would only need to select out the columns
 * that are being used).
 */
public class ImpalaProjectRel extends Project
    implements ImpalaPlanRel {

  public ImpalaProjectRel(Project project) {
    super(project.getCluster(), project.getTraitSet(), project.getInput(),
        project.getProjects(), project.getRowType());
  }

  // Needed for Calcite framework
  private ImpalaProjectRel(RelOptCluster cluster, RelTraitSet traits,
      RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
    super(cluster, traits, input, projects, rowType);
  }

  // Needed for Calcite framework
  @Override
  public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects,
      RelDataType rowType) {
    return new ImpalaProjectRel(getCluster(), traitSet, input, projects, rowType);
  }

  @Override
  public NodeWithExprs getPlanNode(ParentPlanRelContext context) throws ImpalaException {

    // see comment in isCoercedProjectForValues method
    boolean isCoercedProjectForValues = isCoercedProjectForValues(context);
    NodeWithExprs inputWithExprs = getChildPlanNode(context, isCoercedProjectForValues);

    // If this Project is a coercedProjectForValues, then this Project has been taken
    // care of in the underlying Values RelNode so the output of that node is passed
    // directly up to the parent.
    if (isCoercedProjectForValues) {
      return inputWithExprs;
    }

    // get the output exprs for this node that are needed by the parent node.
    List<Expr> outputExprs =
        createProjectExprs(context.ctx_.getRootAnalyzer(), inputWithExprs);

    // There is no Impala Plan Node mapped to Project, so we just return the child
    // PlanNode. However, the outputExprs change with the Project.
    return new NodeWithExprs(inputWithExprs.planNode_, outputExprs, inputWithExprs);
  }

  /**
   * Translate the RexNode expressions in the Project to Impala Exprs.
   */
  private List<Expr> createProjectExprs(Analyzer basicAnalyzer,
      NodeWithExprs inputNodeWithExprs)
      throws ImpalaException {
    ImpalaPlanRel inputRel = (ImpalaPlanRel) getInput(0);

    CreateExprVisitor visitor = new CreateExprVisitor(getCluster().getRexBuilder(),
        inputNodeWithExprs.outputExprs_, basicAnalyzer);

    ImmutableList.Builder<Expr> builder = new ImmutableList.Builder();
    for (RexNode rexNode : getProjects()) {
      Expr projectExpr = CreateExprVisitor.getExpr(visitor, rexNode);
      Preconditions.checkNotNull(projectExpr,
          "Visitor returned null Impala expr for RexNode %s", rexNode);
      builder.add(projectExpr);
    }
    return builder.build();
  }

  private NodeWithExprs getChildPlanNode(ParentPlanRelContext context,
      boolean isCoercedProjectForValues) throws ImpalaException {
    Preconditions.checkState(context.filterCondition_ == null,
        "Failure, Filter RelNode needs to be passed through the Project Rel Node.");
    ImpalaPlanRel relInput = (ImpalaPlanRel) getInput(0);
    ParentPlanRelContext.Builder builder =
        new ParentPlanRelContext.Builder(context, this);
    // see "isCoercedProjectForValues" method
    if (isCoercedProjectForValues) {
      // RelNode type of parent of project is set for the child
      builder.setParentType(context.parentType_);
      // For a coerced project, the rowtype of the project is passed in. This is because
      // a "cast(inputref)" may change the row type for the underlying Values RelNode.
      builder.setParentRowType(getRowType());
    }
    builder.setInputRefs(RelOptUtil.InputFinder.bits(getProjects(), null));
    return relInput.getPlanNode(builder.build());
  }

  // isCoercedProjectForValues returns true if this Project RelNode was
  // explicitly created by the CoerceNodes module for the purpose of handling
  // a RelDataType that had to be modified. This is a hack to get around a
  // limitation of Calcite. Calcite only allows string literals to be created
  // as "char(x)". So the CoerceNodes module injects a Project above the Values
  // RelNode, casts the column to string, and propagates the string data type
  // to all the RelNodes above the Project.
  //
  // The signs that show this is a coerced nodes created project is that all
  // the columns are either passthrough (just an inputref to the same column
  // number) or a cast of an inputref of the same column number.
  private boolean isCoercedProjectForValues(ParentPlanRelContext context) {

    // only care about simple projects if the underlying input is a Values
    if (!(getInput() instanceof Values)) {
      return false;
    }

    List<RexNode> projects = getProjects();
    for (int i = 0; i < projects.size(); ++i) {
      RexNode project = projects.get(i);
      if (project instanceof RexInputRef) {
        RexInputRef inputRef = (RexInputRef) project;
        if (inputRef.getIndex() == i) {
          continue;
        }
      }

      if (project instanceof RexCall) {
        RexCall call = (RexCall) project;
        if (call.getKind().equals(SqlKind.CAST) &&
            call.getOperands().get(0) instanceof RexInputRef) {
          RexInputRef inputRef = (RexInputRef) call.getOperands().get(0);
          if (inputRef.getIndex() == i) {
            continue;
          }
        }
      }
      return false;
    }
    return true;
  }

  @Override
  public RelNodeType relNodeType() {
    return RelNodeType.PROJECT;
  }
}
