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

package org.apache.impala.analysis;

import org.apache.impala.analysis.AnalysisContext.AnalysisResult;

/**
 * Interface for the analyze method which returns an AnalysisResult.
 *
 * The AnalysisDriver interface allows the existence of multiple planners to
 * implement how each one does analysis. Namely, the Calcite Planner analyzes
 * the statement differently from the way the original Impala planner does.
 *
 * The "analyze" method here does not throw an exception. The exception handling
 * is handled within the analyze method and the exception should be placed within the
 * AnalysisResult. The authorization checker needs the analysis information whether
 * it succeeds or fails. This is because an authorization exception would supercede
 * an analysis exception, as the end user should not be able to see messages
 * like 'column <x> not found' if they do not have authorization rights to view
 * the table.
 */
public interface AnalysisDriver {
  public AnalysisResult analyze();
}
