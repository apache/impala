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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.impala.catalog.ArrayType;
import org.apache.impala.catalog.MapType;
import org.apache.impala.catalog.PrimitiveType;
import org.apache.impala.catalog.StructField;
import org.apache.impala.catalog.StructType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.TreeNode;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.ProcessingCost;
import org.apache.impala.thrift.TSortingOrder;
import org.apache.impala.util.ExprUtil;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;

/**
 * Encapsulates all the information needed to compute ORDER BY
 * This doesn't contain aliases or positional exprs.
 * TODO: reorganize this completely, this doesn't really encapsulate anything; this
 * should move into planner/ and encapsulate the implementation of the sort of a
 * particular input row (materialize all row slots)
 */
public class SortInfo {
  // All sort exprs with cost greater than this will be materialized. Since we don't
  // currently have any information about actual function costs, this value is intended to
  // ensure that all expensive functions will be materialized while still leaving simple
  // operations unmaterialized, for example 'SlotRef + SlotRef' should have a cost below
  // this threshold.
  // TODO: rethink this when we have a better cost model.
  private static final float SORT_MATERIALIZATION_COST_THRESHOLD =
      Expr.FUNCTION_CALL_COST;

  private List<Expr> sortExprs_;
  // List of original sort exprs (bofore smap substitutions)
  private List<Expr> origSortExprs_;
  private final List<Boolean> isAscOrder_;
  // True if "NULLS FIRST", false if "NULLS LAST", null if not specified.
  private final List<Boolean> nullsFirstParams_;
  // The single tuple that is materialized, sorted, and output by a sort operator
  // (i.e. SortNode or TopNNode)
  private TupleDescriptor sortTupleDesc_;
  // List of exprs evaluated against the sort input and materialized into the sort tuple.
  // One expr per slot in 'sortTupleDesc_'.
  private final List<Expr> materializedExprs_;
  // Maps from exprs materialized into the sort tuple to their corresponding SlotRefs.
  private final ExprSubstitutionMap outputSmap_;
  private final TSortingOrder sortingOrder_;
  // Number of leading keys that should be sorted lexically in sortExprs_. Only used in
  // pre-insert sort node that uses Z-order. So the Z-order sort node can sort rows
  // lexically on partition keys, and sort the remaining keys in Z-order.
  private int numLexicalKeysInZOrder_ = 0;

  public SortInfo(List<Expr> sortExprs, List<Boolean> isAscOrder,
      List<Boolean> nullsFirstParams) {
    this(sortExprs, isAscOrder, nullsFirstParams, TSortingOrder.LEXICAL);
  }

  public SortInfo(List<Expr> sortExprs, List<Boolean> isAscOrder,
      List<Boolean> nullsFirstParams, TSortingOrder sortingOrder) {
    Preconditions.checkArgument(sortExprs.size() == isAscOrder.size());
    Preconditions.checkArgument(sortExprs.size() == nullsFirstParams.size());
    sortExprs_ = sortExprs;
    origSortExprs_ = Expr.cloneList(sortExprs_);
    isAscOrder_ = isAscOrder;
    nullsFirstParams_ = nullsFirstParams;
    materializedExprs_ = new ArrayList<>();
    outputSmap_ = new ExprSubstitutionMap();
    sortingOrder_ = sortingOrder;
  }

  /**
   * C'tor for cloning.
   */
  private SortInfo(SortInfo other) {
    sortExprs_ = Expr.cloneList(other.sortExprs_);
    isAscOrder_ = Lists.newArrayList(other.isAscOrder_);
    nullsFirstParams_ = Lists.newArrayList(other.nullsFirstParams_);
    materializedExprs_ = Expr.cloneList(other.materializedExprs_);
    sortTupleDesc_ = other.sortTupleDesc_;
    outputSmap_ = other.outputSmap_.clone();
    sortingOrder_ = other.sortingOrder_;
    numLexicalKeysInZOrder_ = other.numLexicalKeysInZOrder_;
  }

  public List<Expr> getSortExprs() { return sortExprs_; }
  public List<Expr> getOrigSortExprs() { return origSortExprs_; }
  public List<Boolean> getIsAscOrder() { return isAscOrder_; }
  public List<Boolean> getNullsFirstParams() { return nullsFirstParams_; }
  public List<Expr> getMaterializedExprs() { return materializedExprs_; }
  public TupleDescriptor getSortTupleDescriptor() { return sortTupleDesc_; }
  public ExprSubstitutionMap getOutputSmap() { return outputSmap_; }
  public TSortingOrder getSortingOrder() { return sortingOrder_; }
  public int getNumLexicalKeysInZOrder() { return numLexicalKeysInZOrder_; }
  public void setNumLexicalKeysInZOrder(int numLexicalKeysInZOrder) {
    numLexicalKeysInZOrder_ = numLexicalKeysInZOrder;
  }

  /**
   * Gets the list of booleans indicating whether nulls come first or last, independent
   * of asc/desc.
   */
  public List<Boolean> getNullsFirst() {
    Preconditions.checkState(sortExprs_.size() == nullsFirstParams_.size());
    List<Boolean> nullsFirst = new ArrayList<>();
    for (int i = 0; i < sortExprs_.size(); ++i) {
      nullsFirst.add(OrderByElement.nullsFirst(nullsFirstParams_.get(i),
          isAscOrder_.get(i)));
    }
    return nullsFirst;
  }

  /**
   * Materializes the slots in 'sortTupleDesc_' referenced in the sort exprs.
   * Materializes the slots referenced by the corresponding materialized expr after
   * applying the 'smap'. Valid to call after createSortTupleInfo().
   */
  public void materializeRequiredSlots(Analyzer analyzer, ExprSubstitutionMap smap) {
    Preconditions.checkNotNull(sortTupleDesc_);
    Preconditions.checkState(sortTupleDesc_.isMaterialized());
    analyzer.materializeSlots(sortExprs_);
    List<SlotDescriptor> sortTupleSlotDescs = sortTupleDesc_.getSlots();
    List<Expr> materializedExprs = new ArrayList<>();
    for (int i = 0; i < sortTupleSlotDescs.size(); ++i) {
      if (sortTupleSlotDescs.get(i).isMaterialized()) {
        materializedExprs.add(materializedExprs_.get(i));
      }
    }
    List<Expr> substMaterializedExprs =
        Expr.substituteList(materializedExprs, smap, analyzer, false);
    analyzer.materializeSlots(substMaterializedExprs);
  }

  /**
   * Replaces 'sortExprs_' according to smap. This needs to be called to make sure that
   * the sort exprs refer to the new tuple materialized by this sort instead of the
   * original input.
   */
  public void substituteSortExprs(ExprSubstitutionMap smap, Analyzer analyzer) {
    sortExprs_ = Expr.substituteList(sortExprs_, smap, analyzer, false);
  }

  /**
   * Validates internal state. Asserts that all sort exprs are bound by the sort tuple.
   */
  public void checkConsistency() {
    Preconditions.checkState(
        materializedExprs_.size() == sortTupleDesc_.getSlots().size());
    for (Expr sortExpr: sortExprs_) {
      Preconditions.checkState(sortExpr.isBound(sortTupleDesc_.getId()));
    }
  }

  @Override
  public SortInfo clone() { return new SortInfo(this); }

  /**
   * Matches SlotRef expressions that do not reference the sort tuple.
   */
  private class IsInputSlotRefPred implements com.google.common.base.Predicate<Expr> {
    private final TupleId sortTid_;
    public IsInputSlotRefPred(TupleId sortTid) {
      sortTid_ = sortTid;
    }

    @Override
    public boolean apply(Expr e) {
      return e instanceof SlotRef && !e.isBound(sortTid_);
    }
  }

  /**
   * Create a tuple descriptor for the single tuple that is materialized, sorted, and
   * output by the sort node. Materializes slots required by 'resultExprs' as well as
   * non-deterministic and expensive order by exprs. The materialized exprs are
   * substituted with slot refs into the new tuple. This simplifies the sorting logic for
   * total and top-n sorts.
   */
  public void createSortTupleInfo(List<Expr> resultExprs, Analyzer analyzer) {
    Preconditions.checkState(sortTupleDesc_ == null);
    Preconditions.checkState(outputSmap_.size() == 0);

    // The descriptor for the tuples on which the sort operates.
    sortTupleDesc_ = analyzer.getDescTbl().createTupleDescriptor("sort");
    sortTupleDesc_.setIsMaterialized(true);

    // The following exprs are materialized:
    // 1. Sort exprs that we chose to materialize
    // 2. SlotRefs against the sort input contained in the result and sort exprs
    //    after substituting the materialized sort exprs.
    // 3. TupleIsNullPredicates from 'resultExprs' which are not legal to evaluate after
    //    the sort because the tuples referenced by it are gone after the sort.

    // Case 1: Materialize chosen sort exprs.
    addMaterializedExprs(getMaterializedSortExprs(), analyzer);

    // Case 2: Materialize required input slots. Using a LinkedHashSet prevents the
    // slots getting reordered unnecessarily.
    Set<SlotRef> inputSlotRefs = new LinkedHashSet<>();
    IsInputSlotRefPred pred = new IsInputSlotRefPred(sortTupleDesc_.getId());
    TreeNode.collect(Expr.substituteList(resultExprs, outputSmap_, analyzer, false),
        pred, inputSlotRefs);
    TreeNode.collect(Expr.substituteList(sortExprs_, outputSmap_, analyzer, false),
        pred, inputSlotRefs);
    addMaterializedExprs(inputSlotRefs, analyzer);

    // Case 3: Materialize TupleIsNullPredicates.
    List<Expr> tupleIsNullPreds = new ArrayList<>();
    TreeNode.collect(resultExprs, Predicates.instanceOf(TupleIsNullPredicate.class),
        tupleIsNullPreds);
    Expr.removeDuplicates(tupleIsNullPreds);
    addMaterializedExprs(tupleIsNullPreds, analyzer);

    // The sort exprs are evaluated against the sort tuple, so they must reflect the
    // materialization decision above.
    substituteSortExprs(outputSmap_, analyzer);
    checkConsistency();
  }

  /**
   * Materializes each of the given exprs into 'sortTupleDesc' as follows:
   * - Adds a new slot in 'sortTupleDesc_'
   * - Adds an entry in 'outputSmap_' mapping from the expr to a SlotRef on the new slot
   * - Adds an entry in 'materializedExprs_'
   * Valid to call after createSortTupleInfo().
   */
  public <T extends Expr> void addMaterializedExprs(Collection<T> exprs,
      Analyzer analyzer) {
    Preconditions.checkNotNull(sortTupleDesc_);
    for (Expr srcExpr : exprs) {
      SlotDescriptor dstSlotDesc;
      if (srcExpr instanceof SlotRef) {
        SlotDescriptor srcSlotDesc = ((SlotRef) srcExpr).getDesc();
        dstSlotDesc = analyzer.copySlotDescriptor(srcSlotDesc, sortTupleDesc_);
      } else {
        dstSlotDesc = analyzer.addSlotDescriptor(sortTupleDesc_);
        dstSlotDesc.initFromExpr(srcExpr);
      }
      dstSlotDesc.setSourceExpr(srcExpr);
      SlotRef dstExpr = new SlotRef(dstSlotDesc);
      Type dstType = dstSlotDesc.getType();
      if (dstType.isStructType() &&
          dstSlotDesc.getItemTupleDesc() != null) {
        try {
          dstExpr.reExpandStruct(analyzer);
        } catch (AnalysisException ex) {
          // Adding SlotRefs shouldn't throw here as the source SlotRef had already been
          // analysed.
          Preconditions.checkNotNull(null);
        }
      } else if (dstType.isCollectionType()) {
        dstSlotDesc.setShouldMaterializeRecursively(true);
      }
      outputSmap_.put(srcExpr.clone(), dstExpr);
      materializedExprs_.add(srcExpr);
    }
  }

  /**
   * Estimates the size of the data materialized in memory by the TopN operator. The
   * method uses the formula <code>estimatedSize = estimated # of rows in memory *
   * average tuple serialized size</code>. 'cardinality' is the cardinality of the TopN
   * operator and 'offset' is the value in the 'OFFSET [x]' clause.
   */
  public long estimateTopNMaterializedSize(long cardinality, long offset) {
    long totalRows = PlanNode.checkedAdd(cardinality, offset);
    return estimateMaterializedSize(totalRows);
  }

  /**
   * Estimates the size of 'totalRows' rows for this sort materialized in memory.
   * The method uses the formula <code>estimatedSize = estimated # of rows in memory *
   * average tuple serialized size</code>.
   */
  public long estimateMaterializedSize(long totalRows) {
    getSortTupleDescriptor().computeMemLayout();
    return (long) Math.ceil(getSortTupleDescriptor().getAvgSerializedSize()
        * totalRows);
  }

  /**
   * Returns the subset of 'sortExprs_' that should be materialized. A sort expr is
   * is materialized if it:
   * - contains a non-deterministic expr
   * - contains a UDF (since we don't know if they're deterministic)
   * - is more expensive than a cost threshold
   * - does not have a cost set
   */
  private List<Expr> getMaterializedSortExprs() {
    List<Expr> result = new ArrayList<>();
    for (Expr sortExpr : sortExprs_) {
      if (!sortExpr.hasCost()
          || sortExpr.getCost() > SORT_MATERIALIZATION_COST_THRESHOLD
          || sortExpr.contains(Expr.IS_NONDETERMINISTIC_BUILTIN_FN_PREDICATE)
          || sortExpr.contains(Expr.IS_UDF_PREDICATE)) {
        result.add(sortExpr);
      }
    }
    return result;
  }

  public ProcessingCost computeProcessingCost(String label, long inputCardinality) {
    float weight = ExprUtil.computeExprsTotalCost(getSortExprs());

    return ProcessingCost.basicCost(label, inputCardinality, weight);
  }

  // Collections within structs (also if they are nested in another struct or collection)
  // are currently not allowed in the sorting tuple (see IMPALA-12160). This function
  // returns whether the given type is allowed in the sorting tuple.
  public static boolean isValidInSortingTuple(Type type) {
    if (type.isCollectionType()) {
      if (type instanceof ArrayType) {
        ArrayType arrayType = (ArrayType) type;
        return isValidInSortingTuple(arrayType.getItemType());
      } else {
        Preconditions.checkState(type instanceof MapType);
        MapType mapType = (MapType) type;

        if (!isValidInSortingTuple(mapType.getKeyType())) return false;

        return isValidInSortingTuple(mapType.getValueType());
      }
    } else if (type.isStructType()) {
      StructType structType = (StructType) type;
      return isValidStructInSortingTuple(structType);
    }

    return true;
  }

  // Helper for isValidInSortingTuple(), see more there.
  private static boolean isValidStructInSortingTuple(StructType structType) {
    for (StructField field : structType.getFields()) {
      Type fieldType = field.getType();
      if (fieldType.isStructType()) {
        if (!isValidStructInSortingTuple((StructType) fieldType)) return false;
      } else if (fieldType.isCollectionType()) {
        // TODO IMPALA-12160: Once we allow sorting collections in structs, test that
        // collections containing var-len types are handled correctly.
        return false;
      }
    }
    return true;
  }
}
