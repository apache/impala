/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.impala.calcite.rel.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.SlotId;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.calcite.schema.CalciteTable;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.Pair;
import org.apache.impala.planner.HdfsPartitionPruner;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * PrunedPartitionHelper is a wrapper around the Impala Planner's partition
 * pruner and holds the partitions remaining after pruning and the conjuncts
 * separated by the ones that pruned and the ones that did not prune.
 */
public class PrunedPartitionHelper {

  private final List<? extends FeFsPartition> prunedPartitions_;

  private final List<Expr> partitionedConjuncts_;

  private final List<Expr> nonPartitionedConjuncts_;

  public PrunedPartitionHelper(CalciteTable table,
      ExprConjunctsConverter converter, TupleDescriptor tupleDesc,
      RexBuilder rexBuilder,
      Analyzer analyzer) throws ImpalaException {

    HdfsPartitionPruner pruner = new HdfsPartitionPruner(tupleDesc);

    List<Expr> conjuncts = converter.getImpalaConjuncts();
    // IMPALA-13849: tblref is null.  Tablesampling is disabled.
    Pair<List<? extends FeFsPartition>, List<Expr>> impalaPair =
        pruner.prunePartitions(analyzer, new ArrayList<>(conjuncts), true, false, null);

    prunedPartitions_ = impalaPair.first;

    ImmutableList.Builder<Expr> partitionedConjBuilder =
        new ImmutableList.Builder();
    ImmutableList.Builder<Expr> nonPartitionedConjBuilder =
        new ImmutableList.Builder();
    List<SlotId> partitionSlots = tupleDesc.getPartitionSlots();

    for (Expr conjunct : conjuncts) {
      if (HdfsPartitionPruner.isPartitionPrunedFilterConjunct(partitionSlots, conjunct,
          false)) {
        partitionedConjBuilder.add(conjunct);
      } else {
        nonPartitionedConjBuilder.add(conjunct);
      }
    }

    partitionedConjuncts_ = partitionedConjBuilder.build();
    nonPartitionedConjuncts_ = nonPartitionedConjBuilder.build();
  }

  public List<? extends FeFsPartition> getPrunedPartitions() {
    return prunedPartitions_;
  }

  public List<Expr> getPartitionedConjuncts() {
    return partitionedConjuncts_;
  }

  public List<Expr> getNonPartitionedConjuncts() {
    return nonPartitionedConjuncts_;
  }
}
