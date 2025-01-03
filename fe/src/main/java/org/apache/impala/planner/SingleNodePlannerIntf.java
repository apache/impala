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

import org.apache.impala.analysis.ExprSubstitutionMap;
import org.apache.impala.analysis.ParsedStatement;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.thrift.TResultSetMetadata;

import java.util.List;

/**
 * SingleNodePlanner interface that creates the physical non-distributed
 * plan.
 *
 * This is currently implemented by the original Impala planner and the
 * Calcite planner.
 */
public interface SingleNodePlannerIntf {

  // Main method: Create the PlanNode which is the step after analysis
  // in the compilation process.
  public PlanNode createSingleNodePlan() throws ImpalaException;

  // Create the top level DataSink.
  public DataSink createDataSink(ExprSubstitutionMap rootNodeSmap);

  // Create the column labels for the output expressions
  public List<String> getColLabels();

  // Create the TResultSetMetadata which contains the output expressions
  public TResultSetMetadata getTResultSetMetadata(ParsedStatement stmt);

}
