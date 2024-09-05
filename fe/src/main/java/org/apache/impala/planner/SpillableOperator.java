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

import org.apache.impala.thrift.TQueryOptions;

/**
 * An interface for {@link PlanNode} that support spilling to disk.
 * TODO: It is probably better to merge this functionality into PlanNode itself,
 * but that will require major refactoring.
 */
public interface SpillableOperator {
  /**
   * Alternative for {@link PlanNode#computeNodeResourceProfile(TQueryOptions)} and {@link
   * DataSink#computeResourceProfile(TQueryOptions)} that assumes memory estimation of
   * fragment instance can be capped at maxMemoryEstimatePerInstance because query can
   * spill to disk.
   * @param queryOptions the query options to run this query.
   * @param maxMemoryEstimatePerInstance maximum memory estimation per fragment instance
   * in bytes. Determined by Frontend.
   */
  void computeResourceProfileIfSpill(
      TQueryOptions queryOptions, long maxMemoryEstimatePerInstance);
}
