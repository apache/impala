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

package org.apache.impala.util;

import java.util.List;

import org.apache.impala.thrift.TEventSequence;

import com.google.common.collect.Lists;

/**
 * Wrapper around TEventSequence so that we can mark events with a single method call.
 * Events are 'marked' as they happen (so in order, with no time-travel backwards).
 */
public class EventSequence {
  private final List<Long> timestamps_ = Lists.newArrayList();
  private final List<String> labels_ = Lists.newArrayList();

  private final long startTime_;
  private final String name_;
  private long lastTime_;

  public EventSequence(String name) {
    name_ = name;
    startTime_ = System.nanoTime();
    lastTime_ = startTime_;
  }

  /**
   * Returns a new EventSequence instance that won't be used. Some code paths
   * (e.g. event-processor) don't have catalog profiles so don't provide a timeline to
   * update. Use this to avoid passing in a null value.
   */
  public static EventSequence getUnusedTimeline() {
    return NoOpEventSequence.INSTANCE;
  }

  /**
   * Saves an event at the current time with the given label.
   * It returns the duration in nano seconds between the last and the current event.
   */
  public long markEvent(String label) {
    // Timestamps should be in ns resolution
    long currentTime = System.nanoTime();
    long durationNs = currentTime - lastTime_;
    lastTime_ = currentTime;
    timestamps_.add(currentTime - startTime_);
    labels_.add(label);
    return durationNs;
  }

  // For testing
  public int getNumEvents() { return labels_.size(); }

  public TEventSequence toThrift() {
    TEventSequence ret = new TEventSequence();
    ret.timestamps = timestamps_;
    ret.labels = labels_;
    ret.name = name_;
    return ret;
  }
}
