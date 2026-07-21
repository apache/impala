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
 * Interface for DataSinks that determine the number of fragment instances and nodes
 * they should run on. When a PlanFragment's sink implements this interface, the
 * fragment delegates instance/node count determination to the sink rather than using
 * the default derived from the plan root.
 *
 * Implementors include sinks that cap parallelism based on query options (e.g.
 * MAX_FS_WRITERS) and sinks whose instance count is dictated by their consumer
 * (e.g. join build sinks co-located with their destination join node).
 */
public interface HasQuantityLimit {
  /**
   * Returns the number of parallel instances this sink should run on within its
   * fragment. The returned value overrides the default instance count that would
   * otherwise be derived from the fragment's plan root.
   */
  int getNumInstances();

  /**
   * Returns the number of nodes this sink's fragment should run on. The returned
   * value overrides the default node count that would otherwise be derived from the
   * fragment's plan root.
   */
  int getNumNodes();
}
