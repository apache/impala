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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdDistinctRowCount;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ImpalaRelMdDistinctRowCount is an extension of the RelMdDistinctCount
 * Calcite class which returns distinct row counts for a given logical RelNode
 */
public class ImpalaRelMdDistinctRowCount extends RelMdDistinctRowCount {

  protected static final Logger LOG =
      LoggerFactory.getLogger(ImpalaRelMdDistinctRowCount.class.getName());

  public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider
      .reflectiveSource(BuiltInMethod.DISTINCT_ROW_COUNT.method,
      new ImpalaRelMdDistinctRowCount());

  @Override
  public Double getDistinctRowCount(TableScan scan, RelMetadataQuery mq,
      ImmutableBitSet groupKey, RexNode predicate) {
    double distinctRows = 1.0;
    CalciteTable table = (CalciteTable) scan.getTable();
    double totalRows = table.getRowCount();
    Preconditions.checkState(totalRows >= 0.0);
    for (Integer i : groupKey.asList()) {
      long distinctValues = table.getColumn(i).getStats().getNumDistinctValues();
      // if no distinct values stats, just assume all rows are distinct
      if (distinctValues < 0) {
        return totalRows;
      }
      distinctRows *= distinctValues;
      // number of distinct rows can never be more than number of total rows
      if (distinctRows >= totalRows) {
        return totalRows;
      }
    }
    return distinctRows;
  }

  @Override
  public Double getDistinctRowCount(Aggregate rel, RelMetadataQuery mq,
      ImmutableBitSet groupKey, RexNode predicate) {
    // Use Calcite distinct row calculation
    // number of distinct rows can never be more than number of total rows
    return Math.min(mq.getRowCount(rel),
        super.getDistinctRowCount(rel, mq, groupKey, predicate));
  }

  @Override
  public Double getDistinctRowCount(Filter rel, RelMetadataQuery mq,
      ImmutableBitSet groupKey, RexNode predicate) {
    // For the distinct row count, we take the number of distinct rows before the
    // filter and multiply it by the selectivity
    // TODO: We can do a little better if the "groupKey" is for the column being
    // selected. Specifically, if we find the selectivity for col1 and the condition
    // is "col1 is null", we know there is only one distinct row, but it will multiply
    // by the selectivity of all the distinct rows.
    Double rowCount = mq.getRowCount(rel);
    Preconditions.checkState(rowCount >= 0.0);
    Double childRowCount = mq.getRowCount(rel.getInput(0));
    Double distinctRowCount =
        mq.getDistinctRowCount(rel.getInput(0), groupKey, predicate);
    Preconditions.checkState(rowCount <= childRowCount);
    return Math.max(1.0, distinctRowCount * rowCount / childRowCount);
  }

  @Override
  public Double getDistinctRowCount(Join rel, RelMetadataQuery mq,
      ImmutableBitSet groupKey, RexNode predicate) {
    // Use Calcite distinct row calculation
    // number of distinct rows can never be more than number of total rows
    return Math.min(mq.getRowCount(rel),
        super.getDistinctRowCount(rel, mq, groupKey, predicate));
  }
}
