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
  // both are set and guaranteed to be <= maxMemReservationBytes_ if both are set (the
  // constructor ensures both of these conditions).
  private final long memEstimateBytes_;

  // Minimum memory reservation required to execute in bytes.
  // The valid range is [0, maxReservationBytes_].
  private final long minMemReservationBytes_;

  // Maximum memory reservation allowed for this plan node.
  // The valid range is [minMemReservationBytes_, Long.MAX_VALUE].
  private final long maxMemReservationBytes_;

  // The default spillable buffer size to use in a plan node. Only valid for resource
  // profiles for spilling PlanNodes. Operations like sum(), max(), etc., produce
  // profiles without valid spillableBufferBytes_ values. -1 means invalid.
  private final long spillableBufferBytes_;

  // The buffer size to use for max-sized row in a plan node. -1 means invalid.
  // Must be set to a valid power-of-two value if spillableBufferBytes_ is set.
  private final long maxRowBufferBytes_;

  // The number of threads required to execute the plan node or plan tree. Does not
  // include any optional threads that may be dynamically created or any threads outside
  // of plan execution threads (e.g. system threads or threads used in the RPC stack).
  // -1 if the profile is invalid (i.e. isValid_ is false).
  private final long threadReservation_;

  ResourceProfile(boolean isValid, long memEstimateBytes, long minMemReservationBytes,
      long maxMemReservationBytes, long spillableBufferBytes,
      long maxRowBufferBytes, long threadReservation) {
    Preconditions.checkArgument(spillableBufferBytes == -1 || maxRowBufferBytes != -1);
    Preconditions.checkArgument(spillableBufferBytes == -1
        || LongMath.isPowerOfTwo(spillableBufferBytes));
    Preconditions.checkArgument(maxRowBufferBytes == -1
        || LongMath.isPowerOfTwo(maxRowBufferBytes));
    Preconditions.checkArgument(!isValid || threadReservation >= 0, threadReservation);
    Preconditions.checkArgument(maxMemReservationBytes >= minMemReservationBytes);
    isValid_ = isValid;
    memEstimateBytes_ = (minMemReservationBytes != -1) ?
        Math.max(memEstimateBytes, minMemReservationBytes) : memEstimateBytes;
    minMemReservationBytes_ = minMemReservationBytes;
    maxMemReservationBytes_ = maxMemReservationBytes;
    spillableBufferBytes_ = spillableBufferBytes;
    maxRowBufferBytes_ = maxRowBufferBytes;
    threadReservation_ = threadReservation;
  }

  // Create a resource profile with zero min or max reservation and zero required
  // threads.
  public static ResourceProfile noReservation(long memEstimateBytes) {
    return new ResourceProfile(true, memEstimateBytes, 0, 0, -1, -1, 0);
  }

  public static ResourceProfile invalid() {
    return new ResourceProfile(false, -1, -1, -1, -1, -1, -1);
  }

  public boolean isValid() { return isValid_; }
  public long getMemEstimateBytes() { return memEstimateBytes_; }
  public long getMinMemReservationBytes() { return minMemReservationBytes_; }
  public long getMaxMemReservationBytes() { return maxMemReservationBytes_; }
  public long getSpillableBufferBytes() { return spillableBufferBytes_; }
  public long getMaxRowBufferBytes() { return maxRowBufferBytes_; }
  public long getThreadReservation() { return threadReservation_; }

  /**
   * Returns true if this uses some resources. Must only call on valid profiles.
   */
  public boolean isNonZero() {
    Preconditions.checkState(isValid_);
    return memEstimateBytes_ > 0 || minMemReservationBytes_ > 0 || threadReservation_ > 0;
  }

  // Return a string with the resource profile information suitable for display in an
  // explain plan in a format like: "resource1=value resource2=value"
  public String getExplainString() {
    StringBuilder output = new StringBuilder();
    output.append("mem-estimate=");
    output.append(isValid_ ? PrintUtils.printBytes(memEstimateBytes_) : "invalid");
    output.append(" mem-reservation=");
    output.append(isValid_ ? PrintUtils.printBytes(minMemReservationBytes_) : "invalid");
    // TODO: output maxReservation_ here if the planner becomes more sophisticated in
    // choosing it (beyond 0/unlimited).
    if (isValid_ && spillableBufferBytes_ != -1) {
      output.append(" spill-buffer=");
      output.append(PrintUtils.printBytes(spillableBufferBytes_));
    }
    output.append(" thread-reservation=");
    output.append(isValid_ ? threadReservation_ : "invalid");
    return output.toString();
  }

  // Returns a profile with the max of each aggregate value in 'this' and 'other'.
  // Values which don't aggregate (like buffer sizes) are invalid in the result.
  public ResourceProfile max(ResourceProfile other) {
    if (!isValid()) return other;
    if (!other.isValid()) return this;
    return new ResourceProfile(true,
        Math.max(getMemEstimateBytes(), other.getMemEstimateBytes()),
        Math.max(getMinMemReservationBytes(), other.getMinMemReservationBytes()),
        Math.max(getMaxMemReservationBytes(), other.getMaxMemReservationBytes()),
        -1, -1,
        Math.max(getThreadReservation(), other.getThreadReservation()));
  }

  // Returns a profile with the sum of each aggregate value in 'this' and 'other'.
  // Values which don't aggregate (like buffer sizes) are invalid in the result.
  public ResourceProfile sum(ResourceProfile other) {
    if (!isValid()) return other;
    if (!other.isValid()) return this;
    return new ResourceProfile(true,
        MathUtil.saturatingAdd(getMemEstimateBytes(), other.getMemEstimateBytes()),
        MathUtil.saturatingAdd(
            getMinMemReservationBytes(),other.getMinMemReservationBytes()),
        MathUtil.saturatingAdd(
            getMaxMemReservationBytes(), other.getMaxMemReservationBytes()), -1, -1,
        MathUtil.saturatingAdd(getThreadReservation(), other.getThreadReservation()));
  }

  // Returns a profile with the sum of each aggregate value in 'this' and 'other'.
  // For buffer sizes, where summing the values doesn't make sense, returns the max.
  public ResourceProfile combine(ResourceProfile other) {
    if (!isValid()) return other;
    if (!other.isValid()) return this;
    return new ResourceProfile(true,
        MathUtil.saturatingAdd(getMemEstimateBytes(), other.getMemEstimateBytes()),
        MathUtil.saturatingAdd(
            getMinMemReservationBytes(),other.getMinMemReservationBytes()),
        MathUtil.saturatingAdd(
            getMaxMemReservationBytes(), other.getMaxMemReservationBytes()),
        Math.max(getSpillableBufferBytes(), other.getSpillableBufferBytes()),
        Math.max(getMaxRowBufferBytes(), other.getMaxRowBufferBytes()),
        MathUtil.saturatingAdd(getThreadReservation(), other.getThreadReservation()));
  }

  // Returns a profile with each aggregate value multiplied by 'factor'.
  // For buffer sizes, where multiplying the values doesn't make sense, invalid values
  // are present in the returned profile.
  public ResourceProfile multiply(int factor) {
    if (!isValid()) return this;
    return new ResourceProfile(true,
        MathUtil.saturatingMultiply(memEstimateBytes_, factor),
        MathUtil.saturatingMultiply(minMemReservationBytes_, factor),
        MathUtil.saturatingMultiply(maxMemReservationBytes_, factor), -1, -1,
        MathUtil.saturatingMultiply(threadReservation_, factor));
  }

  public TBackendResourceProfile toThrift() {
    TBackendResourceProfile result = new TBackendResourceProfile();
    result.setMin_reservation(minMemReservationBytes_);
    result.setMax_reservation(maxMemReservationBytes_);
    if (spillableBufferBytes_ != -1) {
      result.setSpillable_buffer_size(spillableBufferBytes_);
    }
    if (maxRowBufferBytes_ != -1) {
      result.setMax_row_buffer_size(maxRowBufferBytes_);
    }
    return result;
  }
}
