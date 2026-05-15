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

import java.util.List;

import com.google.common.base.Preconditions;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.SlotDescriptor;

/**
 * Default {@link ScanNodeHelper} for the classic Impala planner.
 */
public final class ScanNodeHelperImpl implements ScanNodeHelper {

  /**
   * Returns the count star optimization descriptor for the given scan node if
   * the count star optimization can be applied.
   */
  @Override
  public SlotDescriptor getCountStarOptimizationDescriptor(ScanNode scanNode,
      Analyzer analyzer, List<Expr> conjuncts) {
    if (!scanNode.canApplyCountStarOptimization(analyzer)) {
      return null;
    }
    Preconditions.checkState(scanNode.getTupleDesc().getPath().destTable() != null);
    Preconditions.checkState(conjuncts.isEmpty());
    return scanNode.applyCountStarOptimization(analyzer);
  }
}
