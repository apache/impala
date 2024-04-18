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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.impala.planner.PlannerContext;

/**
 * ParentPlanRelContext is passed into each layer of the Impala
 * RelNodes so the child can make certain decisions based on its
 * parent.
 */
public class ParentPlanRelContext {

  // ctx:  This doesn't change throughout the tree
  public final PlannerContext ctx_;

  // filterCondition: A filter which can be used by the current node.
  public final RexNode filterCondition_;

  // The input refs used by the parent PlanRel Node
  public final ImmutableBitSet inputRefs_;

  // type of parent RelNode
  public final ImpalaPlanRel.RelNodeType parentType_;

  public final RelDataType parentRowType_;

  /**
   * Constructor meant for root node.
   */
  private ParentPlanRelContext(PlannerContext plannerContext) {
    this.ctx_ = plannerContext;
    this.filterCondition_ = null;
    this.inputRefs_ = null;
    this.parentType_ = null;
    this.parentRowType_ = null;
  }

  private ParentPlanRelContext(Builder builder) {
    this.ctx_ = builder.context_;
    this.filterCondition_ = builder.filterCondition_;
    this.inputRefs_ = builder.inputRefs_;
    this.parentType_ = builder.parentType_;
    this.parentRowType_ = builder.parentRowType_;
  }

  public static class Builder {
    private PlannerContext context_;
    private RexNode filterCondition_;
    private ImmutableBitSet inputRefs_;
    private ImpalaPlanRel.RelNodeType parentType_;
    private RelDataType  parentRowType_;

    /**
     * Should only be called from root level.
     */
    public Builder(PlannerContext plannerContext) {
      this.context_ = plannerContext;
      this.parentType_ = null;
    }

    public Builder(ParentPlanRelContext planRelContext, ImpalaPlanRel planRel) {
      this.context_ = planRelContext.ctx_;
      this.filterCondition_ = planRelContext.filterCondition_;
      this.parentType_ = planRel.relNodeType();
    }

    public void setFilterCondition(RexNode filterCondition) {
      this.filterCondition_ = filterCondition;
    }

    public void setInputRefs(ImmutableBitSet inputRefs) {
      this.inputRefs_ = inputRefs;
    }

    public void setParentRowType(RelDataType parentRowType) {
      this.parentRowType_ = parentRowType;
    }

    public void setParentType(ImpalaPlanRel.RelNodeType parentType) {
      this.parentType_ = parentType;
    }

    public ParentPlanRelContext build() {
      return new ParentPlanRelContext(this);
    }
  }

  public static ParentPlanRelContext createRootContext(PlannerContext context) {
    return new ParentPlanRelContext(context);
  }
}
