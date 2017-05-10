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

package org.apache.impala.analysis;

import java.util.List;

import org.apache.impala.planner.PlanNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Dummy table ref that is used in plan generation for adding a SingularRowSrcNode
 * inside a SubplanNode's plan tree (second child).
 */
public class SingularRowSrcTableRef extends TableRef {
  private final List<TupleId> tblRefIds_;
  private final List<TupleId> tupleIds_;

  public SingularRowSrcTableRef(PlanNode subplanInput) {
    super(null, "singular-row-src-tblref");
    Preconditions.checkNotNull(subplanInput);
    Preconditions.checkState(sampleParams_ == null);
    desc_ = null;
    isAnalyzed_ = true;
    tblRefIds_ = Lists.newArrayList(subplanInput.getTblRefIds());
    tupleIds_ = Lists.newArrayList(subplanInput.getTupleIds());
  }

  /**
   * This override is needed to support join inversion where the original lhs
   * is a SingularRowSrcTableRef.
   */
  @Override
  public void setLeftTblRef(TableRef leftTblRef) {
    super.setLeftTblRef(leftTblRef);
    tblRefIds_.clear();
    tupleIds_.clear();
    tblRefIds_.addAll(leftTblRef_.getAllTableRefIds());
    tupleIds_.addAll(leftTblRef_.getAllMaterializedTupleIds());
  }

  @Override
  public TupleId getId() { return tblRefIds_.get(tblRefIds_.size() - 1); }

  @Override
  public List<TupleId> getAllTableRefIds() { return tblRefIds_; }

  @Override
  public List<TupleId> getAllMaterializedTupleIds() { return tupleIds_; }
}
