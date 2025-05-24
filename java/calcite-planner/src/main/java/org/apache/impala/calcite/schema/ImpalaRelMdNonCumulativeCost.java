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
package org.apache.impala.calcite.schema;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.BuiltInMetadata.NonCumulativeCost;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdPercentageOriginalRows;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ImpalaRelMdNonCumulativeCost is used to give a cost value for various RelNodes.
 * The base code was copied from the Hive code base, but manipulated to make it
 * slightly cleaner.
 */
public class ImpalaRelMdNonCumulativeCost implements NonCumulativeCost.Handler {

  // TODO: Should we make this configurable?
  public static final double cpuUnitCost = 0.000001;
  public static final double netCost = cpuUnitCost * 150;
  public static final double localFSWrite = netCost * 4.0;
  public static final double localFSRead = netCost * 4.0;
  public static final double hdfsWrite = localFSWrite * 10.0;
  public static final double hdfsRead = localFSRead * 1.5;

  protected static final Logger LOG =
      LoggerFactory.getLogger(ImpalaRelMdNonCumulativeCost.class.getName());

  public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider
      .reflectiveSource(BuiltInMethod.NON_CUMULATIVE_COST.method,
          new ImpalaRelMdNonCumulativeCost());

  @Override
  public RelOptCost getNonCumulativeCost(RelNode rel, RelMetadataQuery mq) {
    if (rel instanceof Join) {
      return getJoinCost((Join) rel, mq);
    }

    if (rel instanceof TableScan) {
      return getScanCost((TableScan) rel, mq);
    }

    if (rel instanceof Aggregate) {
      return getAggregateCost((Aggregate) rel, mq);
    }

    return ImpalaCost.ZERO;
  }

  private RelOptCost getJoinCost(Join join, RelMetadataQuery mq) {
    // 1. Sum of input cardinalities
    final Double leftRCount = mq.getRowCount(join.getLeft());
    final Double rightRCount = mq.getRowCount(join.getRight());
    final Double leftRAverageSize = mq.getAverageRowSize(join.getLeft());
    final Double rightRAverageSize = mq.getAverageRowSize(join.getRight());

    if (leftRCount == null || rightRCount == null ||
        leftRAverageSize == null || rightRAverageSize == null) {
      return null;
    }
    final double rowCount = leftRCount + rightRCount;

    // CPU cost = HashTable  construction  cost  +
    //            join cost
    final double cpuCost = rowCount * cpuUnitCost;

    // IO cost = cost of transferring small tables to join node
    final double ioCost = rightRCount * rightRAverageSize * netCost;

    // Result
    RelOptCost finalCost = ImpalaCost.FACTORY.makeCost(1.0, cpuCost, ioCost);
    return finalCost;
  }

  private RelOptCost getScanCost(TableScan scan, RelMetadataQuery mq) {
    double cardinality = mq.getRowCount(scan);
    double avgTupleSize = mq.getAverageRowSize(scan);
    return new ImpalaCost(0, hdfsRead * cardinality * avgTupleSize);
  }

  private RelOptCost getAggregateCost(Aggregate agg, RelMetadataQuery mq) {

    if (mq.distribution(agg).getKeys().containsAll(agg.getGroupSet().asList())) {
      return ImpalaCost.ZERO;
    }

    final Double rCount = mq.getRowCount(agg.getInput());
    if (rCount == null) {
      return null;
    }

    // CPU cost = sorting cost
    final double cpuCost = rCount * Math.log(rCount) * cpuUnitCost;

    // IO cost = cost of writing intermediary results to local FS +
    //           cost of reading from local FS for transferring to GBy +
    //           cost of transferring map outputs to GBy operator
    final Double rAverageSize = mq.getAverageRowSize(agg.getInput());
    if (rAverageSize == null) {
      return null;
    }

    double ioCost = 0.0;
    // Write cost
    ioCost += rCount * rAverageSize * localFSWrite;
    // Read cost
    ioCost += rCount * rAverageSize * localFSRead;
    // Net transfer cost
    ioCost += rCount * rAverageSize * netCost;

    // Result
    return ImpalaCost.FACTORY.makeCost(1.0, cpuCost, ioCost);
  }
}
