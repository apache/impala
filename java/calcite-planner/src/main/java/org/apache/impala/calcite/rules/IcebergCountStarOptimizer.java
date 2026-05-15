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

package org.apache.impala.calcite.rules;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.impala.calcite.operators.ImpalaCustomOperatorTable;
import org.apache.impala.calcite.schema.CalciteIcebergTable;
import org.apache.impala.calcite.type.ImpalaTypeConverter;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.ImpalaException;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * IcebergCountStarOptimizer is a RelShuttle class that changes the RelShuttle
 * tree in situations where a count(*) optimization can be applied to an Iceberg
 * table.
 *
 * The only RelNode patterns where this can occur is either
 * Aggregate <- TableScan
 * Aggregate <- Project <- TableScan
 *
 * The count(*) optimization can be applied when a count(*) is retrieved for the
 * whole table without any filters or group bys.
 *
 * Count optimization has to be handled different based on whether:
 * a) the Iceberg table does or does not have delete files
 * b) there are other expressions that need to be calculated besides the count(*),
 *    e.g. "select sum(x), count(*) from my_iceberg_tbl"
 *
 * Case 1) The table does not have delete files and the only expression is count():
 * In this case, we convert the agg/tablescan to a LogicValues clause containing
 * the precalulated number of records
 *
 * Case 2) The table does not have delete files and there multiple expressions:
 * The agg/tablescan still needs to exist for the other expression. Since the
 * count(*) is known, it can be taken out of the agg and put on top of the Agg
 * in a LogicalProject as a separate column with a RexLiteral representing the
 * precalculated count.

 * Case 3) The table has delete files:
 * A first pass calculation is done, but an extra add needs to be done to handle
 * the delete files. The agg/tablescan hierarchy remains as is because of the extra
 * sum that is needed, but a Project on top adds the precalculated count to the
 * count needed to handle the delete files.  The output in the project RelNode for
 * the count(*) will be "InputRef($1) + <precalculated count(*) constant>".
 *
 * For case 3), a little bit of extra work has to be done, but cannot be done within
 * this code. The IcebergScanPlanner needs the TableRef of the Iceberg table to
 * contain the flag optimizeCountStarForIcebergV2 set to true. At this phase, there
 * are only Calcite objects. The TableRef object will not exist until the Logical to
 * physical conversion is done. The code to handle this part exists in
 * the class ImpalaHdfsScanRel.
 */
public class IcebergCountStarOptimizer extends RelShuttleImpl {
  protected static final Logger LOG =
      LoggerFactory.getLogger(IcebergCountStarOptimizer.class.getName());

  @Override
  public RelNode visit(LogicalAggregate agg) {

    // The only patterns allowed for the optimization are agg <- ts
    // and agg <- proj <- ts.
    TableScan ts = getAggregatedTableScan(agg);
    if (ts == null) {
      // if pattern not found, continue traversing the tree.
      return super.visit(agg);
    }

    if (!(ts.getTable() instanceof CalciteIcebergTable)) {
      return super.visit(agg);
    }
    CalciteIcebergTable calciteTable = (CalciteIcebergTable) ts.getTable();

    if (!hasACountStar(agg)) {
      return agg;
    }

    try {
      if (calciteTable.hasDeleteFiles() && !hasCountStarOnly(agg)) {
        return agg;
      }

      return calciteTable.hasDeleteFiles()
          ? transformForV2CountStarOptimization(agg, calciteTable)
          : transformForV1CountStarOptimization(agg, calciteTable);
    } catch (Exception e) {
      // On exception, do not do the optimization
      LOG.info("Exception caught while performing count star optimization for Iceberg:" +
          e);
      return agg;
    }
  }

  private RelNode transformForV2CountStarOptimization(LogicalAggregate agg,
      CalciteIcebergTable calciteTable) throws ImpalaException {
    long count = calciteTable.getRecordCount();
    if (count <= 0) {
      return agg;
    }

    RelOptCluster cluster = agg.getCluster();
    RexBuilder rexBuilder = cluster.getRexBuilder();

    RexLiteral countLiteral =
        rexBuilder.makeLiteral(count, ImpalaTypeConverter.getRelDataType(Type.BIGINT));
    List<RexNode> projects = new ArrayList<>();
    int i = 0;
    // Iterate through all the agg calls.
    for (AggregateCall aggCall : agg.getAggCallList()) {
      RexNode aggInput = rexBuilder.makeInputRef(aggCall.getType(), i++);
      Preconditions.checkState(aggCall.getAggregation().getKind().equals(SqlKind.COUNT));
      Preconditions.checkState(aggCall.getArgList().size() == 0);
      // For the count(*), we add the input ref to the count literal constant from the
      // v2 count.
      projects.add(rexBuilder.makeCall(ImpalaCustomOperatorTable.PLUS, aggInput,
          countLiteral));
    }

    // return the new project on top of the agg
    return LogicalProject.create(agg, new ArrayList<>(), projects, agg.getRowType());
  }

  private RelNode transformForV1CountStarOptimization(LogicalAggregate agg,
      CalciteIcebergTable calciteTable) throws ImpalaException {

    long count = calciteTable.getRecordCount();
    if (count <= 0) {
      return agg;
    }

    RelOptCluster cluster = agg.getCluster();
    RexBuilder rexBuilder = cluster.getRexBuilder();

    RexLiteral countLiteral =
        rexBuilder.makeLiteral(count, ImpalaTypeConverter.getRelDataType(Type.BIGINT));

    if (hasCountStarOnly(agg)) {
      List<RexLiteral> literals = new ArrayList<>();
      // Use a for loop here in case sql has count(*) multiple times.
      for (AggregateCall aggCall : agg.getAggCallList()) {
        literals.add(countLiteral);
      }
      // Convert to a constant LogicalValues and return;
      return LogicalValues.create(cluster, agg.getRowType(),
          ImmutableList.of(ImmutableList.copyOf(literals)));
    } else {
      // case where there are other aggregates besides count(*)
      List<AggregateCall> nonCountStarAggregates = new ArrayList<>();
      for (AggregateCall aggCall : agg.getAggCallList()) {
        // keep the non count(*) aggs, put them in a list.
        if (!(aggCall.getAggregation().getKind().equals(SqlKind.COUNT) &&
            aggCall.getArgList().size() == 0)) {
          nonCountStarAggregates.add(aggCall);
        }
      }
      // re-create the new agg without the count(*) aggregates.
      LogicalAggregate newAgg = LogicalAggregate.create(agg.getInputs().get(0),
          agg.getHints(), agg.getGroupSet(), agg.getGroupSets(), nonCountStarAggregates);

      List<RexNode> projects = new ArrayList<>();
      int i = 0;
      for (AggregateCall aggCall : agg.getAggCallList()) {
        // Need to keep the output of the project on top of the agg the same as the
        // original agg. Everytime we encounter a non-count(*) we take the
        // current input of the agg and bump the input ref number. If the
        // original slot was a count(*), place the literal in the project.
        if (!(aggCall.getAggregation().getKind().equals(SqlKind.COUNT) &&
            aggCall.getArgList().size() == 0)) {
          projects.add(rexBuilder.makeInputRef(aggCall.getType(), i++));
        } else {
          projects.add(countLiteral);
        }
      }
      return LogicalProject.create(newAgg, new ArrayList<>(), projects, agg.getRowType());
    }
  }

  private boolean hasCountStarOnly(Aggregate agg) {
    if (!agg.getGroupSet().isEmpty()) {
      return false;
    }
    if (agg.getAggCallList().size() == 0) {
      return false;
    }
    for (AggregateCall aggCall : agg.getAggCallList()) {
      if (!aggCall.getAggregation().getKind().equals(SqlKind.COUNT)) {
        return false;
      }
      if (aggCall.getArgList().size() > 0) {
        return false;
      }
    }
    return true;
  }

  private boolean hasACountStar(Aggregate agg) {
    if (!agg.getGroupSet().isEmpty()) {
      return false;
    }
    if (agg.getAggCallList().size() == 0) {
      return false;
    }
    for (AggregateCall aggCall : agg.getAggCallList()) {
      if (aggCall.getAggregation().getKind().equals(SqlKind.COUNT) &&
          aggCall.getArgList().size() == 0) {
        return true;
      }
    }
    return false;
  }

  private TableScan getAggregatedTableScan(LogicalAggregate agg) {
    if (agg.getInputs().get(0) instanceof TableScan) {
      return (TableScan) agg.getInputs().get(0);
    }

    if (!(agg.getInputs().get(0) instanceof LogicalProject)) {
      return null;
    }

    LogicalProject proj = (LogicalProject) agg.getInputs().get(0);

    return proj.getInputs().get(0) instanceof TableScan
        ? (TableScan) proj.getInputs().get(0)
        : null;
  }
}
