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

package org.apache.impala.service;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * Type-safe wrapper for historical statistics values stored in the cache.
 * Encapsulates a list of historical runs for a specific stats type.
 *
 * @param <T> The type of historical run data (e.g., TPlanNodeRun)
 */
public class HistoricalStatsValue<T> implements Serializable {
  private static final long serialVersionUID = 1L;
  private final List<T> runs;

  /**
   * Create a historical stats value with an initial run.
   *
   * @param initialRun The first historical run to store
   */
  public HistoricalStatsValue(T initialRun) {
    this.runs = new LinkedList<>();
    this.runs.add(initialRun);
  }

  /**
   * Create a historical stats value from an existing list of runs. Used when
   * reconstructing the value from a serialized form. The runs are copied so later
   * mutations of the argument do not affect this value.
   */
  public HistoricalStatsValue(List<T> runs) {
    this.runs = new LinkedList<>(runs);
  }

  public List<T> getRuns() {
    return runs;
  }

  public int size() {
    return runs.size();
  }
}
