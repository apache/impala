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

/**
 * A basic implementation of {@link ProcessingCost} that takes account expression cost
 * and average row size as per-row costing weight.
 */
public class BaseProcessingCost extends ProcessingCost {
  private final long cardinality_;
  private final float exprsCost_;
  private final float materializationCost_;

  public BaseProcessingCost(
      long cardinality, float exprsCost, float materializationCost) {
    // TODO: materializationCost accommodate ProcessingCost where row width should be
    // factor in. Currently, ProcessingCost of ScanNode, ExchangeNode, and DataStreamSink
    // has row width factored in through materialization parameter here. Investigate if
    // other operator need to have its row width factored in as well and whether we should
    // have specific 'rowWidth' parameter here.
    cardinality_ = cardinality;
    exprsCost_ = exprsCost;
    materializationCost_ = materializationCost;
  }

  private float costFactor() { return exprsCost_ + materializationCost_; }

  @Override
  public long getTotalCost() {
    // Total cost must be non-negative.
    return (long) Math.ceil(Math.max(cardinality_, 0) * costFactor());
  }

  @Override
  public boolean isValid() {
    return cardinality_ >= 0;
  }

  @Override
  public ProcessingCost clone() {
    return new BaseProcessingCost(cardinality_, exprsCost_, materializationCost_);
  }

  @Override
  public String getDetails() {
    StringBuilder output = new StringBuilder();
    output.append(super.getDetails());
    output.append(" cardinality=")
        .append(cardinality_)
        .append(" cost-factor=")
        .append(costFactor());
    return output.toString();
  }
}
