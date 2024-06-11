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

import org.apache.impala.util.MathUtil;

public class ScaledProcessingCost extends ProcessingCost {
  private final ProcessingCost cost_;
  private final long multiplier_;

  protected ScaledProcessingCost(ProcessingCost cost, long multiplier) {
    Preconditions.checkArgument(
        cost.isValid(), "ScaledProcessingCost: cost is invalid! %s", cost);
    Preconditions.checkArgument(
        multiplier >= 0, "ScaledProcessingCost: multiplier must be non-negative!");
    cost_ = cost;
    multiplier_ = multiplier;
  }

  @Override
  public long getTotalCost() {
    return MathUtil.saturatingMultiply(cost_.getTotalCost(), multiplier_);
  }

  @Override
  public boolean isValid() {
    return true;
  }

  @Override
  public ProcessingCost clone() {
    return new ScaledProcessingCost(cost_, multiplier_);
  }

  @Override
  public String getExplainString(String detailPrefix, boolean fullExplain) {
    StringBuilder sb = new StringBuilder();
    sb.append(detailPrefix);
    sb.append("ScaledCost(");
    sb.append(multiplier_);
    sb.append("): ");
    sb.append(getDetails());
    if (fullExplain) {
      sb.append("\n");
      sb.append(cost_.getExplainString(detailPrefix + "  ", true));
    }
    return sb.toString();
  }
}
