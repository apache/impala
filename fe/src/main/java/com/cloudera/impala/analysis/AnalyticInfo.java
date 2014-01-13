// Copyright 2014 Cloudera Inc.
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

package com.cloudera.impala.analysis;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.catalog.ColumnStats;
import com.cloudera.impala.catalog.Type;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Encapsulates the analytic functions found in a single select block plus
 * the corresponding analytic result tuple and its substitution map.
 */
public class AnalyticInfo {
  private final static Logger LOG = LoggerFactory.getLogger(AnalyticInfo.class);

  private final ArrayList<AnalyticExpr> analyticExprs_;

  // The tuple into which the output of analyticExprs_ is (logically)
  // materialized; contains one slot for every element of analyticExprs_
  private TupleDescriptor analyticTupleDesc_;

  // map from analyticExprs_ to their corresponding analytic tuple slotrefs
  private final ExprSubstitutionMap analyticTupleSmap_ = new ExprSubstitutionMap();

  // indices into analyticExprs_ for those exprs/slots that need to be materialized
  private final ArrayList<Integer> materializedSlots_ = Lists.newArrayList();

  // C'tor creates copies of groupingExprs and aggExprs.
  // Does *not* set aggTupleDesc, aggTupleSMap, mergeAggInfo, secondPhaseDistinctAggInfo.
  private AnalyticInfo(ArrayList<AnalyticExpr> analyticExprs) {
    analyticExprs_ = Expr.cloneList(analyticExprs);
  }

  public ArrayList<AnalyticExpr> getAnalyticExprs() { return analyticExprs_; }
  public ExprSubstitutionMap getSmap() { return analyticTupleSmap_; }
  public TupleId getTupleId() { return analyticTupleDesc_.getId(); }
  public TupleDescriptor getTupleDesc() { return analyticTupleDesc_; }

  /**
   * Creates complete AnalyticInfo for analyticExprs, including tuple descriptor and
   * smap.
   */
  static public AnalyticInfo create(
      ArrayList<AnalyticExpr> analyticExprs, Analyzer analyzer) {
    Preconditions.checkState(analyticExprs != null && !analyticExprs.isEmpty());
    Expr.removeDuplicates(analyticExprs);
    AnalyticInfo result = new AnalyticInfo(analyticExprs);
    result.createTupleDesc(analyzer);
    LOG.info("analytic info:\n" + result.debugString());
    return result;
  }


  /**
   * Append ids of all slots that are being referenced in the process
   * of performing the analytic computation described by this AnalyticInfo.
   */
  public void getRefdSlots(List<SlotId> ids) {
    Preconditions.checkState(analyticTupleDesc_ != null);
    Expr.getIds(analyticExprs_, null, ids);
    // The backend assumes that the entire analyticTupleDesc is materialized
    for (SlotDescriptor slotDesc: analyticTupleDesc_.getSlots()) {
      ids.add(slotDesc.getId());
    }
  }

  // TODO: implement this
  /**
   * Substitute all the expressions (grouping expr, aggregate expr) and update our
   * substitution map according to the given substitution map:
   * - smap typically maps from tuple t1 to tuple t2 (example: the smap of an
   *   inline view maps the virtual table ref t1 into a base table ref t2)
   * - our grouping and aggregate exprs need to be substituted with the given
   *   smap so that they also reference t2
   * - aggTupleSMap needs to be recomputed to map exprs based on t2
   *   onto our aggTupleDesc (ie, the left-hand side needs to be substituted with
   *   smap)
   * - mergeAggInfo: this is not affected, because
   *   * its grouping and aggregate exprs only reference aggTupleDesc_
   *   * its smap is identical to aggTupleSMap_
   * - 2ndPhaseDistinctAggInfo:
   *   * its grouping and aggregate exprs also only reference aggTupleDesc_
   *     and are therefore not affected
   *   * its smap needs to be recomputed to map exprs based on t2 to its own
   *     aggTupleDesc
  public void substitute(ExprSubstitutionMap smap, Analyzer analyzer)
      throws InternalException {
    groupingExprs_ = Expr.substituteList(groupingExprs_, smap, analyzer);
    LOG.trace("AggInfo: grouping_exprs=" + Expr.debugString(groupingExprs_));

    // The smap in this case should not substitute the aggs themselves, only
    // their subexpressions.
    List<Expr> substitutedAggs = Expr.substituteList(aggregateExprs_, smap, analyzer);
    aggregateExprs_.clear();
    for (Expr substitutedAgg: substitutedAggs) {
      aggregateExprs_.add((FunctionCallExpr) substitutedAgg);
    }

    LOG.trace("AggInfo: agg_exprs=" + Expr.debugString(aggregateExprs_));
    aggTupleSMap_.substituteLhs(smap, analyzer);
    if (secondPhaseDistinctAggInfo_ != null) {
      secondPhaseDistinctAggInfo_.substitute(smap, analyzer);
    }
  }
   */

  /**
   * Creates descriptor for output tuple for analytic exprs.
   */
  public void createTupleDesc(Analyzer analyzer) {
    analyticTupleDesc_ = analyzer.getDescTbl().createTupleDescriptor();
    for (Expr expr: analyticExprs_) {
      SlotDescriptor outputSlotDesc =
          analyzer.addSlotDescriptor(analyticTupleDesc_);
      outputSlotDesc.setLabel(expr.toSql());
      Preconditions.checkState(expr.getType().isValid());
      outputSlotDesc.setType(expr.getType());
      outputSlotDesc.setStats(ColumnStats.fromExpr(expr));
      LOG.info(outputSlotDesc.debugString());
      analyticTupleSmap_.put(expr.clone(), new SlotRef(outputSlotDesc));
    }
    LOG.info("analytictuple=" + analyticTupleDesc_.debugString());
    LOG.info("analytictuplesmap=" + analyticTupleSmap_.debugString());
  }

  public void materializeRequiredSlots(Analyzer analyzer, ExprSubstitutionMap smap) {
    materializedSlots_.clear();
    List<Expr> exprs = Lists.newArrayList();
    for (int i = 0; i < analyticExprs_.size(); ++i) {
      SlotDescriptor slotDesc = analyticTupleDesc_.getSlots().get(i);
      if (!slotDesc.isMaterialized()) continue;
      exprs.add(analyticExprs_.get(i));
      materializedSlots_.add(i);
    }
    List<Expr> resolvedExprs = Expr.substituteList(exprs, smap, analyzer);
    analyzer.materializeSlots(resolvedExprs);
  }

  /**
   * Validates internal state: Checks that the number of materialized slots of the
   * analytic tuple corresponds to the number of materialized analytic functions. Also
   * checks that the return types of the analytic exprs correspond to the slots in the
   * analytic tuple.
   */
  public void checkConsistency() {
    ArrayList<SlotDescriptor> slots = analyticTupleDesc_.getSlots();

    // Check materialized slots.
    int numMaterializedSlots = 0;
    for (SlotDescriptor slotDesc: slots) {
      if (slotDesc.isMaterialized()) ++numMaterializedSlots;
    }
    Preconditions.checkState(numMaterializedSlots ==
        materializedSlots_.size());

    // Check that analytic expr return types match the slot descriptors.
    int slotIdx = 0;
    for (int i = 0; i < analyticExprs_.size(); ++i) {
      Expr analyticExpr = analyticExprs_.get(i);
      Type slotType = slots.get(slotIdx).getType();
      Preconditions.checkState(analyticExpr.getType().equals(slotType),
          String.format("Analytic expr %s returns type %s but its analytic tuple " +
              "slot has type %s", analyticExpr.toSql(),
              analyticExpr.getType().toString(), slotType.toString()));
      ++slotIdx;
    }
  }

  public String debugString() {
    StringBuilder out = new StringBuilder();
    out.append(Objects.toStringHelper(this)
        .add("analytic_exprs", Expr.debugString(analyticExprs_))
        .add("analytic_tuple",
          (analyticTupleDesc_ == null ? "null" : analyticTupleDesc_.debugString()))
        .add("smap", analyticTupleSmap_.debugString())
        .toString());
    return out.toString();
  }
}
