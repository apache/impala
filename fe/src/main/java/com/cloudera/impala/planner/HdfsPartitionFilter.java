// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.planner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.ExprSubstitutionMap;
import com.cloudera.impala.analysis.SlotDescriptor;
import com.cloudera.impala.analysis.SlotId;
import com.cloudera.impala.analysis.SlotRef;
import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.HdfsPartition;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.service.FeSupport;
import com.cloudera.impala.thrift.TColumnValue;
import com.cloudera.impala.thrift.TResultRow;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * An HdfsPartitionFilter represents a predicate on the partition columns (or a subset)
 * of a table. It can be evaluated at plan generation time against an HdfsPartition.
 */
public class HdfsPartitionFilter {
  private final static Logger LOG = LoggerFactory.getLogger(HdfsPartitionFilter.class);

  private final Expr predicate_;

  // lhs exprs of smap used in isMatch()
  private final ArrayList<SlotRef> lhsSlotRefs_ = Lists.newArrayList();
  // indices into Table.getColumns()
  private final ArrayList<Integer> refdKeys_ = Lists.newArrayList();

  public HdfsPartitionFilter(Expr predicate, HdfsTable tbl, Analyzer analyzer) {
    predicate_ = predicate;

    // populate lhsSlotRefs_ and refdKeys_
    ArrayList<SlotId> refdSlots = Lists.newArrayList();
    predicate.getIds(null, refdSlots);
    HashMap<Column, SlotDescriptor> slotDescsByCol = Maps.newHashMap();
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
  public HashSet<Long> getMatchingPartitionIds(ArrayList<HdfsPartition> partitions,
      Analyzer analyzer) throws InternalException {
    HashSet<Long> result = new HashSet<Long>();
    // List of predicates to evaluate
    ArrayList<Expr> predicates = new ArrayList<Expr>(partitions.size());
    long[] partitionIds = new long[partitions.size()];
    int indx = 0;
    for (HdfsPartition p: partitions) {
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
  private Expr buildPartitionPredicate(HdfsPartition partition, Analyzer analyzer)
      throws InternalException {
    // construct smap
    ExprSubstitutionMap sMap = new ExprSubstitutionMap();
    for (int i = 0; i < refdKeys_.size(); ++i) {
      sMap.put(
          lhsSlotRefs_.get(i), partition.getPartitionValues().get(refdKeys_.get(i)));
    }

    Expr literalPredicate = predicate_.substitute(sMap, analyzer, false);
    LOG.trace("buildPartitionPredicate: " + literalPredicate.toSql() + " " +
        literalPredicate.debugString());
    Preconditions.checkState(literalPredicate.isConstant());
    return literalPredicate;
  }
}
