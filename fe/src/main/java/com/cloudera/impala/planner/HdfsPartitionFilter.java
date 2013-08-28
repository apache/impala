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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.SlotDescriptor;
import com.cloudera.impala.analysis.SlotId;
import com.cloudera.impala.analysis.SlotRef;
import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.HdfsPartition;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.service.FeSupport;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * An HdfsPartitionFilter represents a predicate on the partition columns (or a subset)
 * of a table. It can be evaluated at plan generation time against an HdfsPartition.
 */
public class HdfsPartitionFilter {
  private final static Logger LOG = LoggerFactory.getLogger(HdfsPartitionFilter.class);

  private final Expr predicate;

  // lhs exprs of smap used in isMatch()
  private final ArrayList<SlotRef> lhsSlotRefs = Lists.newArrayList();
  // indices into Table.getColumns()
  private final ArrayList<Integer> refdKeys = Lists.newArrayList();

  public HdfsPartitionFilter(Expr predicate, HdfsTable tbl, Analyzer analyzer) {
    this.predicate = predicate;

    // populate lhsSlotRefs and refdKeys
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
        lhsSlotRefs.add(new SlotRef(slotDesc));
        refdKeys.add(i);
      }
    }
    Preconditions.checkState(lhsSlotRefs.size() == refdKeys.size());
  }

  /**
   * Determines whether the filter predicate evaluates to 'true' for the given
   * partition.
   * Does this by substituting SlotRefs for the partition cols with the respective
   * key values.
   */
  public boolean isMatch(HdfsPartition partition, Analyzer analyzer)
      throws InternalException, AuthorizationException {
    // construct smap
    Expr.SubstitutionMap sMap = new Expr.SubstitutionMap();
    for (int i = 0; i < refdKeys.size(); ++i) {
      sMap.addMapping(
          lhsSlotRefs.get(i), partition.getPartitionValues().get(refdKeys.get(i)));
    }

    Expr literalPredicate = predicate.clone(sMap);
    LOG.trace(
        "isMatch: " + literalPredicate.toSql() + " " + literalPredicate.debugString());
    Preconditions.checkState(literalPredicate.isConstant());
    // analyze to insert casts, etc.
    try {
      literalPredicate.analyze(analyzer);
    } catch (AnalysisException e) {
      // this should never happen
      throw new InternalException(
          "couldn't analyze predicate " + literalPredicate.toSql(), e);
    }

    // call backend
    if (!FeSupport.EvalPredicate(literalPredicate, analyzer.getQueryGlobals())) {
      return false;
    }

    return true;
  }
}
