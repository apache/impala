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

import java.util.function.Supplier;

/**
 * Similar as {@link ScaledProcessingCost}, but the multiple (countSupplier) represent
 * fragment instance count associated with this ProcessingCost and may change after the
 * object construction.
 * <p>
 * countSupplier must always return positive value.
 */
public class BroadcastProcessingCost extends ProcessingCost {
  private final ProcessingCost childProcessingCost_;

  protected BroadcastProcessingCost(
      ProcessingCost cost, Supplier<Integer> countSupplier) {
    Preconditions.checkArgument(
        cost.isValid(), "BroadcastProcessingCost: cost is invalid! %s", cost);
    childProcessingCost_ = cost;
    setNumInstanceExpected(countSupplier);
  }

  @Override
  public long getTotalCost() {
    return MathUtil.saturatingMultiply(
        childProcessingCost_.getTotalCost(), getNumInstancesExpected());
  }

  @Override
  public boolean isValid() {
    return getNumInstancesExpected() > 0;
  }

  @Override
  public ProcessingCost clone() {
    return new BroadcastProcessingCost(childProcessingCost_, numInstanceSupplier_);
  }

  @Override
  public String getExplainString(String detailPrefix, boolean fullExplain) {
    StringBuilder sb = new StringBuilder();
    sb.append(detailPrefix);
    sb.append("BroadcastCost(");
    sb.append(getNumInstancesExpected());
    sb.append("): ");
    sb.append(getDetails());
    if (fullExplain) {
      sb.append("\n");
      sb.append(childProcessingCost_.getExplainString(detailPrefix + "  ", true));
    }
    return sb.toString();
  }
}
