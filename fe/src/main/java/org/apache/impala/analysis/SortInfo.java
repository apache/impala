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

package org.apache.impala.analysis;
import org.apache.impala.common.TreeNode;

import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Encapsulates all the information needed to compute ORDER BY
 * This doesn't contain aliases or positional exprs.
 * TODO: reorganize this completely, this doesn't really encapsulate anything; this
 * should move into planner/ and encapsulate the implementation of the sort of a
 * particular input row (materialize all row slots)
 */
public class SortInfo {
  private List<Expr> orderingExprs_;
  private final List<Boolean> isAscOrder_;
  // True if "NULLS FIRST", false if "NULLS LAST", null if not specified.
  private final List<Boolean> nullsFirstParams_;
  // The single tuple that is materialized, sorted, and output by a sort operator
  // (i.e. SortNode or TopNNode)
  private TupleDescriptor sortTupleDesc_;
  // Input expressions materialized into sortTupleDesc_. One expr per slot in
  // sortTupleDesc_.
  private List<Expr> sortTupleSlotExprs_;

  public SortInfo(List<Expr> orderingExprs, List<Boolean> isAscOrder,
      List<Boolean> nullsFirstParams) {
    Preconditions.checkArgument(orderingExprs.size() == isAscOrder.size());
    Preconditions.checkArgument(orderingExprs.size() == nullsFirstParams.size());
    orderingExprs_ = orderingExprs;
    isAscOrder_ = isAscOrder;
    nullsFirstParams_ = nullsFirstParams;
  }

  /**
   * C'tor for cloning.
   */
  private SortInfo(SortInfo other) {
    orderingExprs_ = Expr.cloneList(other.orderingExprs_);
    isAscOrder_ = Lists.newArrayList(other.isAscOrder_);
    nullsFirstParams_ = Lists.newArrayList(other.nullsFirstParams_);
    sortTupleDesc_ = other.sortTupleDesc_;
    if (other.sortTupleSlotExprs_ != null) {
      sortTupleSlotExprs_ = Expr.cloneList(other.sortTupleSlotExprs_);
    }
  }

  /**
   * Sets sortTupleDesc_, which is the internal row representation to be materialized and
   * sorted. The source exprs of the slots in sortTupleDesc_ are changed to those in
   * tupleSlotExprs.
   */
  public void setMaterializedTupleInfo(
      TupleDescriptor tupleDesc, List<Expr> tupleSlotExprs) {
    Preconditions.checkState(tupleDesc.getSlots().size() == tupleSlotExprs.size());
    sortTupleDesc_ = tupleDesc;
    sortTupleSlotExprs_ = tupleSlotExprs;
    for (int i = 0; i < sortTupleDesc_.getSlots().size(); ++i) {
      SlotDescriptor slotDesc = sortTupleDesc_.getSlots().get(i);
      slotDesc.setSourceExpr(sortTupleSlotExprs_.get(i));
    }
  }
  public List<Expr> getOrderingExprs() { return orderingExprs_; }
  public List<Boolean> getIsAscOrder() { return isAscOrder_; }
  public List<Boolean> getNullsFirstParams() { return nullsFirstParams_; }
  public List<Expr> getSortTupleSlotExprs() { return sortTupleSlotExprs_; }
  public TupleDescriptor getSortTupleDescriptor() { return sortTupleDesc_; }

  /**
   * Gets the list of booleans indicating whether nulls come first or last, independent
   * of asc/desc.
   */
  public List<Boolean> getNullsFirst() {
    List<Boolean> nullsFirst = Lists.newArrayList();
    for (int i = 0; i < orderingExprs_.size(); ++i) {
      nullsFirst.add(OrderByElement.nullsFirst(nullsFirstParams_.get(i),
          isAscOrder_.get(i)));
    }
    return nullsFirst;
  }

  /**
   * Materializes the slots in sortTupleDesc_ referenced in the ordering exprs.
   * Materializes the slots referenced by the corresponding sortTupleSlotExpr after
   * applying the 'smap'.
   */
  public void materializeRequiredSlots(Analyzer analyzer, ExprSubstitutionMap smap) {
    Preconditions.checkNotNull(sortTupleDesc_);
    Preconditions.checkNotNull(sortTupleSlotExprs_);
    Preconditions.checkState(sortTupleDesc_.isMaterialized());
    analyzer.materializeSlots(orderingExprs_);
    List<SlotDescriptor> sortTupleSlotDescs = sortTupleDesc_.getSlots();
    List<Expr> materializedExprs = Lists.newArrayList();
    for (int i = 0; i < sortTupleSlotDescs.size(); ++i) {
      if (sortTupleSlotDescs.get(i).isMaterialized()) {
        materializedExprs.add(sortTupleSlotExprs_.get(i));
      }
    }
    List<Expr> substMaterializedExprs =
        Expr.substituteList(materializedExprs, smap, analyzer, false);
    analyzer.materializeSlots(substMaterializedExprs);
  }

  /**
   * Replaces orderingExprs_ according to smap. This needs to be called to make sure that
   * the ordering exprs refer to the new tuple materialized by this sort instead of the
   * original input.
   */
  public void substituteOrderingExprs(ExprSubstitutionMap smap, Analyzer analyzer) {
    orderingExprs_ = Expr.substituteList(orderingExprs_, smap, analyzer, false);
  }

  /**
   * Asserts that all ordering exprs are bound by the sort tuple.
   */
  public void checkConsistency() {
    for (Expr orderingExpr: orderingExprs_) {
      Preconditions.checkState(orderingExpr.isBound(sortTupleDesc_.getId()));
    }
  }

  @Override
  public SortInfo clone() { return new SortInfo(this); }

  /**
   * Create a tuple descriptor for the single tuple that is materialized, sorted, and
   * output by the sort node. Done by materializing slot refs in the order-by and given
   * result expressions. Those slot refs in the ordering and result exprs are substituted
   * with slot refs into the new tuple. This simplifies the sorting logic for total and
   * top-n sorts. The substitution map is returned.
   * TODO: We could do something more sophisticated than simply copying input slot refs -
   * e.g. compute some order-by expressions.
   */
  public ExprSubstitutionMap createSortTupleInfo(
      List<Expr> resultExprs, Analyzer analyzer) {
    // sourceSlots contains the slots from the sort input to materialize.
    Set<SlotRef> sourceSlots = Sets.newHashSet();

    TreeNode.collect(resultExprs, Predicates.instanceOf(SlotRef.class), sourceSlots);
    TreeNode.collect(orderingExprs_, Predicates.instanceOf(SlotRef.class), sourceSlots);

    // The descriptor for the tuples on which the sort operates.
    TupleDescriptor sortTupleDesc = analyzer.getDescTbl().createTupleDescriptor("sort");
    sortTupleDesc.setIsMaterialized(true);

    List<Expr> sortTupleExprs = Lists.newArrayList();

    // substOrderBy is the mapping from slot refs in the sort node's input to slot refs in
    // the materialized sort tuple. Each slot ref in the input gets cloned and builds up
    // the tuple operated on and returned by the sort node.
    ExprSubstitutionMap substOrderBy = new ExprSubstitutionMap();
    for (SlotRef origSlotRef: sourceSlots) {
      SlotDescriptor origSlotDesc = origSlotRef.getDesc();
      SlotDescriptor materializedDesc =
          analyzer.copySlotDescriptor(origSlotDesc, sortTupleDesc);
      SlotRef cloneRef = new SlotRef(materializedDesc);
      substOrderBy.put(origSlotRef, cloneRef);
      sortTupleExprs.add(origSlotRef);
    }

    // The ordering exprs still point to the old slot refs and need to be replaced with
    // ones that point to the slot refs into the sort's output tuple.
    substituteOrderingExprs(substOrderBy, analyzer);

    // Update the tuple descriptor used to materialize the input of the sort.
    setMaterializedTupleInfo(sortTupleDesc, sortTupleExprs);

    return substOrderBy;
  }
}
