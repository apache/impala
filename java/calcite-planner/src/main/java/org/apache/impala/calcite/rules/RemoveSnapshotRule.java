/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.impala.calcite.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Snapshot;
import org.apache.calcite.rel.core.TableScan;
import org.apache.impala.calcite.schema.CalciteIcebergTable;

/**
 * RemoveSnapshotRule removes the Snapshot RelNode. This is created by the
 * SqlToRelNodeConverter because the SqlSnapshot SqlNode is created for
 * Iceberg. However, the RelNode is not needed because the Iceberg snapshot
 * table is resolved at validation time (as an IcebergTimeTravelTable for
 * Iceberg) and exists in the LogicalTableScan.
 */
public class RemoveSnapshotRule extends RelOptRule {
  public static final RemoveSnapshotRule INSTANCE =
      new RemoveSnapshotRule();

  private RemoveSnapshotRule() {
    super(operand(Snapshot.class, operand(TableScan.class, none())));
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Snapshot snapshot = call.rel(0);
    final TableScan scan = call.rel(1);
    // Only remove snapshot classes with an underlying CalciteIcebergTable
    // which are handled in the analysis validation phase.
    if (!(scan.getTable() instanceof CalciteIcebergTable)) {
      return;
    }

    final RelNode input = snapshot.getInput();
    call.transformTo(input);
  }
}

