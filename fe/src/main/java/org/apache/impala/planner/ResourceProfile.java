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
import org.apache.impala.thrift.TBackendResourceProfile;
import org.apache.impala.util.MathUtil;

import com.google.common.base.Preconditions;
import com.google.common.math.LongMath;

/**
 * The resources that will be consumed by some part of a plan, e.g. a plan node or
 * plan fragment.
 */
public class ResourceProfile {
  // If the computed values are valid.
  private final boolean isValid_;

  // Estimated memory consumption in bytes. Guaranteed to be >= minReservationBytes_ if
  // both are set (the constructor ensures this).
  // TODO: IMPALA-5013: currently we are inconsistent about how these estimates are
  // derived or what they mean. Re-evaluate what they mean and either deprecate or
  // fix them.
  private final long memEstimateBytes_;

  // Minimum buffer reservation required to execute in bytes.
  // The valid range is [0, maxReservationBytes_].
  private final long minReservationBytes_;

  // Maximum buffer reservation allowed for this plan node.
  // The valid range is [minReservationBytes_, Long.MAX_VALUE].
  private final long maxReservationBytes_;

  // The default spillable buffer size to use in a plan node. Only valid for resource
  // profiles for spilling PlanNodes. Operations like sum(), max(), etc., produce
  // profiles without valid spillableBufferBytes_ values. -1 means invalid.
  private final long spillableBufferBytes_;

  // The buffer size to use for max-sized row in a plan node. -1 means invalid.
  // Must be set to a valid power-of-two value if spillableBufferBytes_ is set.
  private final long maxRowBufferBytes_;

  ResourceProfile(boolean isValid, long memEstimateBytes,
      long minReservationBytes, long maxReservationBytes, long spillableBufferBytes,
      long maxRowBufferBytes) {
    Preconditions.checkArgument(spillableBufferBytes == -1 || maxRowBufferBytes != -1);
    Preconditions.checkArgument(spillableBufferBytes == -1
        || LongMath.isPowerOfTwo(spillableBufferBytes));
    Preconditions.checkArgument(maxRowBufferBytes == -1
        || LongMath.isPowerOfTwo(maxRowBufferBytes));
    isValid_ = isValid;
    memEstimateBytes_ = (minReservationBytes != -1) ?
        Math.max(memEstimateBytes, minReservationBytes) : memEstimateBytes;
    minReservationBytes_ = minReservationBytes;
    maxReservationBytes_ = maxReservationBytes;
    spillableBufferBytes_ = spillableBufferBytes;
    maxRowBufferBytes_ = maxRowBufferBytes;
  }

  // Create a resource profile with zero min or max reservation.
  public static ResourceProfile noReservation(long memEstimateBytes) {
    return new ResourceProfile(true, memEstimateBytes, 0, 0, -1, -1);
  }

  public static ResourceProfile invalid() {
    return new ResourceProfile(false, -1, -1, -1, -1, -1);
  }

  public boolean isValid() { return isValid_; }
  public long getMemEstimateBytes() { return memEstimateBytes_; }
  public long getMinReservationBytes() { return minReservationBytes_; }
  public long getMaxReservationBytes() { return maxReservationBytes_; }
  public long getSpillableBufferBytes() { return spillableBufferBytes_; }
  public long getMaxRowBufferBytes() { return maxRowBufferBytes_; }

  // Return a string with the resource profile information suitable for display in an
  // explain plan in a format like: "resource1=value resource2=value"
  public String getExplainString() {
    StringBuilder output = new StringBuilder();
    output.append("mem-estimate=");
    output.append(isValid_ ? PrintUtils.printBytes(memEstimateBytes_) : "invalid");
    output.append(" mem-reservation=");
    output.append(isValid_ ? PrintUtils.printBytes(minReservationBytes_) : "invalid");
    // TODO: output maxReservation_ here if the planner becomes more sophisticated in
    // choosing it (beyond 0/unlimited).
    if (isValid_ && spillableBufferBytes_ != -1) {
      output.append(" spill-buffer=");
      output.append(PrintUtils.printBytes(spillableBufferBytes_));
    }
    return output.toString();
  }

  // Returns a profile with the max of each value in 'this' and 'other'.
  public ResourceProfile max(ResourceProfile other) {
    if (!isValid()) return other;
    if (!other.isValid()) return this;
    return new ResourceProfile(true,
        Math.max(getMemEstimateBytes(), other.getMemEstimateBytes()),
        Math.max(getMinReservationBytes(), other.getMinReservationBytes()),
        Math.max(getMaxReservationBytes(), other.getMaxReservationBytes()), -1, -1);
  }

  // Returns a profile with the sum of each value in 'this' and 'other'.
  public ResourceProfile sum(ResourceProfile other) {
    if (!isValid()) return other;
    if (!other.isValid()) return this;
    return new ResourceProfile(true,
        MathUtil.saturatingAdd(getMemEstimateBytes(), other.getMemEstimateBytes()),
        MathUtil.saturatingAdd(getMinReservationBytes(),other.getMinReservationBytes()),
        MathUtil.saturatingAdd(getMaxReservationBytes(), other.getMaxReservationBytes()),
        -1, -1);
  }

  // Returns a profile with all values multiplied by 'factor'.
  public ResourceProfile multiply(int factor) {
    if (!isValid()) return this;
    return new ResourceProfile(true,
        MathUtil.saturatingMultiply(memEstimateBytes_, factor),
        MathUtil.saturatingMultiply(minReservationBytes_, factor),
        MathUtil.saturatingMultiply(maxReservationBytes_, factor), -1, -1);
  }

  public TBackendResourceProfile toThrift() {
    TBackendResourceProfile result = new TBackendResourceProfile();
    result.setMin_reservation(minReservationBytes_);
    result.setMax_reservation(maxReservationBytes_);
    if (spillableBufferBytes_ != -1) {
      result.setSpillable_buffer_size(spillableBufferBytes_);
    }
    if (maxRowBufferBytes_ != -1) {
      result.setMax_row_buffer_size(maxRowBufferBytes_);
    }
    return result;
  }
}
