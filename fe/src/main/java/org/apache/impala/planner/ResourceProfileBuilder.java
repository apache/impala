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
 * Utility class to help set up the various parameters of a ResourceProfile.
 */
public class ResourceProfileBuilder {

  // Must be set by caller.
  private long memEstimateBytes_ = -1;

  // Assume no reservation is used unless the caller explicitly sets it.
  private long minReservationBytes_ = 0;
  private long maxReservationBytes_ = 0;

  // The spillable buffer size is only set by plan nodes that use it.
  private long spillableBufferBytes_= -1;

  // Must be set if spillableBufferBytes_ is set.
  private long maxRowBufferBytes_= -1;

  public ResourceProfileBuilder setMemEstimateBytes(long memEstimateBytes) {
    memEstimateBytes_ = memEstimateBytes;
    return this;
  }

  /**
   * Sets the minimum reservation and an unbounded maximum reservation.
   */
  public ResourceProfileBuilder setMinReservationBytes(long minReservationBytes) {
    minReservationBytes_ = minReservationBytes;
    maxReservationBytes_ = Long.MAX_VALUE;
    return this;
  }

  public ResourceProfileBuilder setSpillableBufferBytes(long spillableBufferBytes) {
    spillableBufferBytes_ = spillableBufferBytes;
    return this;
  }

  public ResourceProfileBuilder setMaxRowBufferBytes(long maxRowBufferBytes) {
    maxRowBufferBytes_ = maxRowBufferBytes;
    return this;
  }

  ResourceProfile build() {
    Preconditions.checkState(memEstimateBytes_ >= 0, "Mem estimate must be set");
    return new ResourceProfile(true, memEstimateBytes_, minReservationBytes_,
        maxReservationBytes_, spillableBufferBytes_, maxRowBufferBytes_);
  }
}
