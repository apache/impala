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

  // The input refs that are only used in a filter and are not used by a projection
  // above the filter. This is for the partition pruning optimization. For the query
  // "select id + 5 from alltypes where year = 2010", if the year column is partitioned,
  // the partition pruning removes all files under the 2010 "year" directory on the
  // file system. Since the "year" column is not in the "select" clause, there is no
  // reason to create memory for the year column in the scan node.
  // The variable filterOnlyInputRefs_ will contain the columns that apply for this
  // optimization. This will be passed down to the scan node and the field will not
  // be materialized.
  public final ImmutableBitSet filterOnlyInputRefs_;

  public ImpalaAggRel parentAggregate_;

  /**
   * Constructor meant for root node.
   */
  private ParentPlanRelContext(PlannerContext plannerContext) {
    this.ctx_ = plannerContext;
    this.filterCondition_ = null;
    this.inputRefs_ = null;
    this.filterOnlyInputRefs_ = ImmutableBitSet.of();
    this.parentAggregate_ = null;
  }

  private ParentPlanRelContext(Builder builder) {
    this.ctx_ = builder.context_;
    this.filterCondition_ = builder.filterCondition_;
    this.inputRefs_ = builder.inputRefs_;
    this.filterOnlyInputRefs_ = builder.filterOnlyInputRefs_;
    this.parentAggregate_ = builder.parentAggregate_;
  }

  public static class Builder {
    private PlannerContext context_;
    private RexNode filterCondition_;
    private ImmutableBitSet inputRefs_;
    private ImmutableBitSet filterOnlyInputRefs_;
    private ImpalaAggRel parentAggregate_;

    /**
     * Should only be called from root level.
     */
    public Builder(PlannerContext plannerContext) {
      this.context_ = plannerContext;
      this.filterOnlyInputRefs_ = ImmutableBitSet.of();
    }

    public Builder(ParentPlanRelContext planRelContext, ImpalaPlanRel planRel) {
      this.context_ = planRelContext.ctx_;
      this.filterCondition_ = planRelContext.filterCondition_;
      this.filterOnlyInputRefs_ = planRelContext.filterOnlyInputRefs_;
      this.parentAggregate_ = ImpalaPlanRel.canPassThroughParentAggregate(planRel)
          ? planRelContext.parentAggregate_
          : null;
    }

    public void setFilterCondition(RexNode filterCondition) {
      this.filterCondition_ = filterCondition;
      if (filterCondition == null) {
        // convenience setting. If there is no filter condition, the fields here
        // can be cleared out.
        // Note: if there is a filterCondition, it is up to the caller to set which
        // input refs are filterOnlyInputRefs
        this.filterOnlyInputRefs_ = ImmutableBitSet.of();
      }
    }

    public void setInputRefs(ImmutableBitSet inputRefs) {
      this.inputRefs_ = inputRefs;
    }

    public void setFilterOnlyInputRefs(ImmutableBitSet filterOnlyInputRefs) {
      this.filterOnlyInputRefs_ = filterOnlyInputRefs;
    }

    public void setParentAggregate(ImpalaAggRel parentAggregate) {
      this.parentAggregate_ = parentAggregate;
    }

    public ParentPlanRelContext build() {
      return new ParentPlanRelContext(this);
    }
  }

  public static ParentPlanRelContext createRootContext(PlannerContext context) {
    return new ParentPlanRelContext(context);
  }
}
