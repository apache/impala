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

import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.impala.calcite.rel.util.PrunedPartitionHelper;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.calcite.schema.JoinRelationInfo.EqualityConjunction;

import com.google.common.base.Preconditions;

import static org.apache.calcite.util.NumberUtil.multiply;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ImpalaRelMdRowCount is an extension of the RelMdRowCount
 * Calcite class which returns row counts for a given logical RelNode
 */
public class ImpalaRelMdRowCount extends RelMdRowCount {

  protected static final Logger LOG =
      LoggerFactory.getLogger(ImpalaRelMdRowCount.class.getName());

  public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider
      .reflectiveSource(BuiltInMethod.ROW_COUNT.method, new ImpalaRelMdRowCount());

  protected ImpalaRelMdRowCount() {
    super();
  }

  @Override
  public Double getRowCount(Filter filter, RelMetadataQuery mq) {
    RelNode input = filter.getInput();
    CalciteTable table = getTable(input);
    RexNode condition = filter.getCondition();

    Double inputRowCount = mq.getRowCount(input);
    Preconditions.checkState(inputRowCount >= 0.0);

    FilterSelectivityEstimator estimator =
        new FilterSelectivityEstimator(input, mq);
    Double selectivity = condition != null
       ? estimator.estimateSelectivity(condition)
       : 1.0;
    return multiply(inputRowCount, selectivity);
  }

  @Override
  public Double getRowCount(Join join, RelMetadataQuery mq) {
    RexBuilder rexBuilder = join.getCluster().getRexBuilder();

    // Build information about the join, and get the row count from there
    // if available
    JoinRelationInfo info = new JoinRelationInfo(join, rexBuilder, mq);
    return (info.useDefaultRowCount())
        ? super.getRowCount(join, mq)
        : info.getRowCount();
  }

  @Override
  public Double getRowCount(TableScan ts, RelMetadataQuery mq) {
    return ts.getTable().getRowCount();
  }

  private CalciteTable getTable(RelNode input) {
    return (input instanceof HepRelVertex)
        ? (CalciteTable) ((HepRelVertex)input).getCurrentRel().getTable()
        : (CalciteTable) input.getTable();
  }
}
