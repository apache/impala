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

package org.apache.impala.planner;

import com.google.common.base.Preconditions;

/**
 * A basic implementation of {@link ProcessingCost} that takes account expression cost
 * and average row size as per-row costing weight.
 */
public class BaseProcessingCost extends ProcessingCost {
  private final long cardinality_;
  private final float exprsCost_;
  private final float materializationCost_;
  private final double totalCost_;

  public BaseProcessingCost(
      long cardinality, float exprsCost, float materializationCost) {
    // TODO: materializationCost accommodates ProcessingCost where row width should
    // factor in. Currently, ProcessingCost of base class ScanNode (not HdfsScanNode),
    // has row width factored in through materialization parameter here.
    // If we convert the remaining non-Hdfs scan node types to use benchmark-based
    // cost coefficients we should remove or simplify this constructor at that time.
    cardinality_ = cardinality;
    exprsCost_ = exprsCost;
    materializationCost_ = materializationCost;
    totalCost_ =
        Math.ceil(Math.max(cardinality_, 0) * (exprsCost_ + materializationCost_));
  }

  public BaseProcessingCost(double totalCost) {
    Preconditions.checkArgument(!Double.isNaN(totalCost), "totalCost must not be a NaN!");
    Preconditions.checkArgument(
        Double.isFinite(totalCost), "totalCost must be a finite double!");
    Preconditions.checkArgument(totalCost >= 0, "totalCost must not be a negative!");
    cardinality_ = 0L;
    exprsCost_ = 0.0F;
    materializationCost_ = 0.0F;
    totalCost_ = totalCost;
  }

  // private float costFactor() { return exprsCost_ + materializationCost_; }

  @Override
  public long getTotalCost() {
    // Total cost must be non-negative.
    return (long) totalCost_;
  }

  @Override
  public boolean isValid() {
    return totalCost_ >= 0.0;
  }

  @Override
  public ProcessingCost clone() {
    if (cardinality_ != 0L) {
      return new BaseProcessingCost(cardinality_, exprsCost_, materializationCost_);
    } else {
      return new BaseProcessingCost(totalCost_);
    }
  }

  @Override
  public String getDetails() {
    StringBuilder output = new StringBuilder();
    output.append(super.getDetails());
    output.append(" raw-cost=").append(totalCost_);
    if (cardinality_ != 0L) { output.append(" cardinality=").append(cardinality_); }
    return output.toString();
  }
}
