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

import org.apache.impala.thrift.TExecNodePhase;
import org.apache.impala.thrift.TPipelineMembership;

/**
 * Represents that a plan node executes as part of a query pipeline in one of its
 * execution phases (GetNext() or Open()). A pipeline is a set of plan nodes and
 * data sinks that execute concurrently, streaming rows through from the bottom
 * to the top of the pipeline. There will be some amount of buffering at different
 * parts of the pipeline, but the general idea is that the top of the pipeline may
 * start processing rows while all other nodes in the pipeline are producing rows.
 * A pipeline can span fragments. Often a pipeline is a linear sequence of
 * nodes, but subplans are an exception because the right tree of the subplan is
 * repeatedly executes for each row streamed from the left tree of the subplan.
 *
 * If a plan node's Open() phase is part of a pipeline, that means that it is a blocking
 * node at the top of the pipeline, accumulating rows from the pipeline. If a plan node's
 * GetNext() phase is part of a pipeline, that means that it is below the top of the
 * pipeline.
 */
public class PipelineMembership {
  // The id of the bottom-most node in the pipeline. Used to identify the pipeline.
  private final PlanNodeId id;

  // The height of this node in the pipeline. Starts at 0.
  private final int height;

  // The phase of the PlanNode that executes as part of the pipeline.
  private final TExecNodePhase phase;


  public PipelineMembership(PlanNodeId id, int height, TExecNodePhase phase) {
    this.id = id;
    this.height = height;
    this.phase = phase;
  }

  public PlanNodeId getId() {
    return id;
  }

  public int getHeight() {
    return height;
  }

  public TExecNodePhase getPhase() {
    return phase;
  }

  public TPipelineMembership toThrift() {
    return new TPipelineMembership(getId().asInt(), height, getPhase());
  }

  public String getExplainString() {
    return getId().toString() + "(" + getPhase().toString() + ")";
  }
}
