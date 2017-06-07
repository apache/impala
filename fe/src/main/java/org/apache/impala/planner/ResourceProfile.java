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

import org.apache.impala.common.PrintUtils;
import org.apache.impala.util.MathUtil;

/**
 * The resources that will be consumed by some part of a plan, e.g. a plan node or
 * plan fragment.
 */
public class ResourceProfile {
  // If the computed values are valid.
  private final boolean isValid_;

  // Estimated memory consumption in bytes.
  // TODO: IMPALA-5013: currently we are inconsistent about how these estimates are
  // derived or what they mean. Re-evaluate what they mean and either deprecate or
  // fix them.
  private final long memEstimateBytes_;

  // Minimum buffer reservation required to execute in bytes.
  private final long minReservationBytes_;

  private ResourceProfile(boolean isValid, long memEstimateBytes, long minReservationBytes) {
    isValid_ = isValid;
    memEstimateBytes_ = memEstimateBytes;
    minReservationBytes_ = minReservationBytes;
  }

  public ResourceProfile(long memEstimateBytes, long minReservationBytes) {
    this(true, memEstimateBytes, minReservationBytes);
  }

  public static ResourceProfile invalid() {
    return new ResourceProfile(false, -1, -1);
  }

  public boolean isValid() { return isValid_; }
  public long getMemEstimateBytes() { return memEstimateBytes_; }
  public long getMinReservationBytes() { return minReservationBytes_; }

  // Return a string with the resource profile information suitable for display in an
  // explain plan in a format like: "resource1=value resource2=value"
  public String getExplainString() {
    StringBuilder output = new StringBuilder();
    output.append("mem-estimate=");
    output.append(isValid_ ? PrintUtils.printBytes(memEstimateBytes_) : "invalid");
    output.append(" mem-reservation=");
    output.append(isValid_ ? PrintUtils.printBytes(minReservationBytes_) : "invalid");
    return output.toString();
  }

  // Returns a profile with the max of each value in 'this' and 'other'.
  public ResourceProfile max(ResourceProfile other) {
    if (!isValid()) return other;
    if (!other.isValid()) return this;
    return new ResourceProfile(
        Math.max(getMemEstimateBytes(), other.getMemEstimateBytes()),
        Math.max(getMinReservationBytes(), other.getMinReservationBytes()));
  }

  // Returns a profile with the sum of each value in 'this' and 'other'.
  public ResourceProfile sum(ResourceProfile other) {
    if (!isValid()) return other;
    if (!other.isValid()) return this;
    return new ResourceProfile(
        MathUtil.saturatingAdd(getMemEstimateBytes(), other.getMemEstimateBytes()),
        MathUtil.saturatingAdd(getMinReservationBytes(), other.getMinReservationBytes()));
  }

  // Returns a profile with all values multiplied by 'factor'.
  public ResourceProfile multiply(int factor) {
    if (!isValid()) return this;
    return new ResourceProfile(
        MathUtil.saturatingMultiply(memEstimateBytes_, factor),
        MathUtil.saturatingMultiply(minReservationBytes_, factor));
  }
}
