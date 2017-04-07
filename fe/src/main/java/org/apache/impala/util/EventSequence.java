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

  public EventSequence(String name) {
    name_ = name;
    startTime_ = System.nanoTime();
  }

  /**
   * Saves an event at the current time with the given label.
   */
  public void markEvent(String label) {
    // Timestamps should be in ns resolution
    timestamps_.add(System.nanoTime() - startTime_);
    labels_.add(label);
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
