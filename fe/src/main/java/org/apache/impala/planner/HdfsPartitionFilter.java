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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.ExprSubstitutionMap;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotId;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.PrunablePartition;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.NotImplementedException;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TColumnValue;
import org.apache.impala.thrift.TResultRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * An HdfsPartitionFilter represents a predicate on the partition columns (or a subset)
 * of a table. It can be evaluated at plan generation time against an HdfsPartition.
 */
public class HdfsPartitionFilter {
  private final static Logger LOG = LoggerFactory.getLogger(HdfsPartitionFilter.class);

  private final Expr predicate_;

  // lhs exprs of smap used in isMatch()
  private final List<SlotRef> lhsSlotRefs_ = new ArrayList<>();
  // indices into Table.getColumnNames()
  private final List<Integer> refdKeys_ = new ArrayList<>();

  public HdfsPartitionFilter(Expr predicate, FeFsTable tbl, Analyzer analyzer) {
    predicate_ = predicate;

    // populate lhsSlotRefs_ and refdKeys_
    List<SlotId> refdSlots = new ArrayList<>();
    predicate.getIds(null, refdSlots);
    Map<Column, SlotDescriptor> slotDescsByCol = new HashMap<>();
    for (SlotId refdSlot: refdSlots) {
      SlotDescriptor slotDesc = analyzer.getDescTbl().getSlotDesc(refdSlot);
      slotDescsByCol.put(slotDesc.getColumn(), slotDesc);
    }

    for (int i = 0; i < tbl.getNumClusteringCols(); ++i) {
      Column col = tbl.getColumns().get(i);
      SlotDescriptor slotDesc = slotDescsByCol.get(col);
      if (slotDesc != null) {
        lhsSlotRefs_.add(new SlotRef(slotDesc));
        refdKeys_.add(i);
      }
    }
    Preconditions.checkState(lhsSlotRefs_.size() == refdKeys_.size());
  }

  /**
   * Evaluate a filter against a batch of partitions and return the partition ids
   * that pass the filter.
   */
  public Set<Long> getMatchingPartitionIds(List<PrunablePartition> partitions,
      Analyzer analyzer) throws ImpalaException {
    Set<Long> result = new HashSet<>();
    // List of predicates to evaluate
    List<Expr> predicates = new ArrayList<>(partitions.size());
    long[] partitionIds = new long[partitions.size()];
    int indx = 0;
    for (PrunablePartition p: partitions) {
      predicates.add(buildPartitionPredicate(p, analyzer));
      partitionIds[indx++] = p.getId();
    }
    // Evaluate the predicates
    TResultRow results = FeSupport.EvalPredicateBatch(predicates,
        analyzer.getQueryCtx());
    Preconditions.checkState(results.getColValsSize() == partitions.size());
    indx = 0;
    for (TColumnValue val: results.getColVals()) {
      if (val.isBool_val()) result.add(partitionIds[indx]);
      ++indx;
    }
    return result;
  }

  /**
   * Construct a predicate for a given partition by substituting the SlotRefs
   * for the partition cols with the respective partition-key values.
   */
  private Expr buildPartitionPredicate(PrunablePartition p, Analyzer analyzer)
      throws ImpalaException {
    // construct smap
    ExprSubstitutionMap sMap = new ExprSubstitutionMap();
    for (int i = 0; i < refdKeys_.size(); ++i) {
      sMap.put(
          lhsSlotRefs_.get(i), p.getPartitionValues().get(refdKeys_.get(i)));
    }

    Expr literalPredicate = predicate_.substitute(sMap, analyzer, false);
    if (LOG.isTraceEnabled()) {
      LOG.trace("buildPartitionPredicate: " + literalPredicate.toSql() + " " +
          literalPredicate.debugString());
    }
    if (!literalPredicate.isConstant()) {
      throw new NotImplementedException(
          "Unsupported non-deterministic predicate: " + predicate_.toSql());
    }
    return literalPredicate;
  }
}
