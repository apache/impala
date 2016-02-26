// Copyright 2016 Cloudera Inc.
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.BetweenPredicate;
import com.cloudera.impala.analysis.BinaryPredicate;
import com.cloudera.impala.analysis.BinaryPredicate.Operator;
import com.cloudera.impala.analysis.CompoundPredicate;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.InPredicate;
import com.cloudera.impala.analysis.IsNullPredicate;
import com.cloudera.impala.analysis.LiteralExpr;
import com.cloudera.impala.analysis.NullLiteral;
import com.cloudera.impala.analysis.SlotDescriptor;
import com.cloudera.impala.analysis.SlotId;
import com.cloudera.impala.analysis.SlotRef;
import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.catalog.HdfsPartition;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;


/**
 * HDFS partitions pruner provides a mechanism to filter out partitions of an HDFS
 * table based on the conjuncts provided by the caller.
 *
 * The pruner is initialized with a TupleDescriptor for the slots being materialized.
 * The prunePartitions() method is the external interface exposed by this class. It
 * takes a list of conjuncts, loops through all the partitions and prunes them based
 * on applicable conjuncts. It returns a list of partitions left after applying all
 * the conjuncts and also removes the conjuncts which have been fully evaluated with
 * the partition columns.
 */
public class HdfsPartitionPruner {

  private final static Logger LOG = LoggerFactory.getLogger(HdfsPartitionPruner.class);

  // Partition batch size used during partition pruning.
  private final static int PARTITION_PRUNING_BATCH_SIZE = 1024;

  private final HdfsTable tbl_;

  private List<SlotId> partitionSlots_ = Lists.newArrayList();

  public HdfsPartitionPruner(TupleDescriptor tupleDesc) {
    Preconditions.checkState(tupleDesc.getTable() instanceof HdfsTable);
    tbl_ = (HdfsTable)tupleDesc.getTable();

    // Collect all the partitioning columns from TupleDescriptor.
    for (SlotDescriptor slotDesc: tupleDesc.getSlots()) {
      if (slotDesc.getColumn() == null) continue;
      if (slotDesc.getColumn().getPosition() < tbl_.getNumClusteringCols()) {
        partitionSlots_.add(slotDesc.getId());
      }
    }
  }

  /**
   * Return a list of partitions left after applying the conjuncts. Please note
   * that conjunts used for filtering will be removed from the list 'conjuncts'.
   */
  public List<HdfsPartition> prunePartitions(Analyzer analyzer, List<Expr> conjuncts)
      throws InternalException {
    // Start with creating a collection of partition filters for the applicable conjuncts.
    List<HdfsPartitionFilter> partitionFilters = Lists.newArrayList();
    // Conjuncts that can be evaluated from the partition key values.
    List<Expr> simpleFilterConjuncts = Lists.newArrayList();

    // Simple predicates (e.g. binary predicates of the form
    // <SlotRef> <op> <LiteralExpr>) can be used to derive lists
    // of matching partition ids directly from the partition key values.
    // Split conjuncts among those that can be evaluated from partition
    // key values and those that need to be evaluated in the BE.
    Iterator<Expr> it = conjuncts.iterator();
    while (it.hasNext()) {
      Expr conjunct = it.next();
      if (conjunct.isBoundBySlotIds(partitionSlots_)) {
        // Check if the conjunct can be evaluated from the partition metadata.
        // canEvalUsingPartitionMd() operates on a cloned conjunct which may get
        // modified if it contains constant expressions. If the cloned conjunct
        // cannot be evaluated from the partition metadata, the original unmodified
        // conjuct is evaluated in the BE.
        Expr clonedConjunct = conjunct.clone();
        if (canEvalUsingPartitionMd(clonedConjunct, analyzer)) {
          simpleFilterConjuncts.add(Expr.pushNegationToOperands(clonedConjunct));
        } else {
          partitionFilters.add(new HdfsPartitionFilter(conjunct, tbl_, analyzer));
        }
        it.remove();
      }
    }

    // Set of matching partition ids, i.e. partitions that pass all filters
    HashSet<Long> matchingPartitionIds = null;

    // Evaluate the partition filters from the partition key values.
    // The result is the intersection of the associated partition id sets.
    for (Expr filter: simpleFilterConjuncts) {
      // Evaluate the filter
      HashSet<Long> matchingIds = evalSlotBindingFilter(filter);
      if (matchingPartitionIds == null) {
        matchingPartitionIds = matchingIds;
      } else {
        matchingPartitionIds.retainAll(matchingIds);
      }
    }

    // Check if we need to initialize the set of valid partition ids.
    if (simpleFilterConjuncts.size() == 0) {
      Preconditions.checkState(matchingPartitionIds == null);
      matchingPartitionIds = Sets.newHashSet(tbl_.getPartitionIds());
    }

    // Evaluate the 'complex' partition filters in the BE.
    evalPartitionFiltersInBe(partitionFilters, matchingPartitionIds, analyzer);

    // Populate the list of valid, non-empty partitions to process
    List<HdfsPartition> results = Lists.newArrayList();
    Map<Long, HdfsPartition> partitionMap = tbl_.getPartitionMap();
    for (Long id: matchingPartitionIds) {
      HdfsPartition partition = partitionMap.get(id);
      Preconditions.checkNotNull(partition);
      if (partition.hasFileDescriptors()) {
        results.add(partition);
        analyzer.getDescTbl().addReferencedPartition(tbl_, partition.getId());
      }
    }
    return results;
  }

  /**
   * Recursive function that checks if a given partition expr can be evaluated
   * directly from the partition key values. If 'expr' contains any constant expressions,
   * they are evaluated in the BE and are replaced by their corresponding results, as
   * LiteralExprs.
   */
  private boolean canEvalUsingPartitionMd(Expr expr, Analyzer analyzer) {
    Preconditions.checkNotNull(expr);
    if (expr instanceof BinaryPredicate) {
      // Evaluate any constant expression in the BE
      try {
        expr.foldConstantChildren(analyzer);
      } catch (AnalysisException e) {
        LOG.error("Error evaluating constant expressions in the BE: " + e.getMessage());
        return false;
      }
      BinaryPredicate bp = (BinaryPredicate)expr;
      SlotRef slot = bp.getBoundSlot();
      if (slot == null) return false;
      Expr bindingExpr = bp.getSlotBinding(slot.getSlotId());
      if (bindingExpr == null || !bindingExpr.isLiteral()) return false;
      return true;
    } else if (expr instanceof CompoundPredicate) {
      boolean res = canEvalUsingPartitionMd(expr.getChild(0), analyzer);
      if (expr.getChild(1) != null) {
        res &= canEvalUsingPartitionMd(expr.getChild(1), analyzer);
      }
      return res;
    } else if (expr instanceof IsNullPredicate) {
      // Check for SlotRef IS [NOT] NULL case
      IsNullPredicate nullPredicate = (IsNullPredicate)expr;
      return nullPredicate.getBoundSlot() != null;
    } else if (expr instanceof InPredicate) {
      // Evaluate any constant expressions in the BE
      try {
        expr.foldConstantChildren(analyzer);
      } catch (AnalysisException e) {
        LOG.error("Error evaluating constant expressions in the BE: " + e.getMessage());
        return false;
      }
      // Check for SlotRef [NOT] IN (Literal, ... Literal) case
      SlotRef slot = ((InPredicate)expr).getBoundSlot();
      if (slot == null) return false;
      for (int i = 1; i < expr.getChildren().size(); ++i) {
        if (!(expr.getChild(i).isLiteral())) return false;
      }
      return true;
    } else if (expr instanceof BetweenPredicate) {
      return canEvalUsingPartitionMd(((BetweenPredicate) expr).getRewrittenPredicate(),
          analyzer);
    }
    return false;
  }

  /**
   * Evaluate a BinaryPredicate filter on a partition column and return the
   * ids of the matching partitions. An empty set is returned if there
   * are no matching partitions.
   */
  private HashSet<Long> evalBinaryPredicate(Expr expr) {
    Preconditions.checkNotNull(expr);
    Preconditions.checkState(expr instanceof BinaryPredicate);
    boolean isSlotOnLeft = true;
    if (expr.getChild(0).isLiteral()) isSlotOnLeft = false;

    // Get the operands
    BinaryPredicate bp = (BinaryPredicate)expr;
    SlotRef slot = bp.getBoundSlot();
    Preconditions.checkNotNull(slot);
    Expr bindingExpr = bp.getSlotBinding(slot.getSlotId());
    Preconditions.checkNotNull(bindingExpr);
    Preconditions.checkState(bindingExpr.isLiteral());
    LiteralExpr literal = (LiteralExpr)bindingExpr;
    Operator op = bp.getOp();
    if ((literal instanceof NullLiteral) && (op != Operator.NOT_DISTINCT)
        && (op != Operator.DISTINCT_FROM)) {
      return Sets.newHashSet();
    }

    // Get the partition column position and retrieve the associated partition
    // value metadata.
    int partitionPos = slot.getDesc().getColumn().getPosition();
    TreeMap<LiteralExpr, HashSet<Long>> partitionValueMap =
        tbl_.getPartitionValueMap(partitionPos);
    if (partitionValueMap.isEmpty()) return Sets.newHashSet();

    HashSet<Long> matchingIds = Sets.newHashSet();
    // Compute the matching partition ids
    if (op == Operator.NOT_DISTINCT) {
      // Case: SlotRef <=> Literal
      if (literal instanceof NullLiteral) {
        Set<Long> ids = tbl_.getNullPartitionIds(partitionPos);
        if (ids != null) matchingIds.addAll(ids);
        return matchingIds;
      }
      // Punt to equality case:
      op = Operator.EQ;
    }
    if (op == Operator.EQ) {
      // Case: SlotRef = Literal
      HashSet<Long> ids = partitionValueMap.get(literal);
      if (ids != null) matchingIds.addAll(ids);
      return matchingIds;
    }
    if (op == Operator.DISTINCT_FROM) {
      // Case: SlotRef IS DISTINCT FROM Literal
      if (literal instanceof NullLiteral) {
        matchingIds.addAll(tbl_.getPartitionIds());
        Set<Long> nullIds = tbl_.getNullPartitionIds(partitionPos);
        matchingIds.removeAll(nullIds);
        return matchingIds;
      } else {
        matchingIds.addAll(tbl_.getPartitionIds());
        HashSet<Long> ids = partitionValueMap.get(literal);
        if (ids != null) matchingIds.removeAll(ids);
        return matchingIds;
      }
    }
    if (op == Operator.NE) {
      // Case: SlotRef != Literal
      matchingIds.addAll(tbl_.getPartitionIds());
      Set<Long> nullIds = tbl_.getNullPartitionIds(partitionPos);
      matchingIds.removeAll(nullIds);
      HashSet<Long> ids = partitionValueMap.get(literal);
      if (ids != null) matchingIds.removeAll(ids);
      return matchingIds;
    }

    // Determine the partition key value range of this predicate.
    NavigableMap<LiteralExpr, HashSet<Long>> rangeValueMap = null;
    LiteralExpr firstKey = partitionValueMap.firstKey();
    LiteralExpr lastKey = partitionValueMap.lastKey();
    boolean upperInclusive = false;
    boolean lowerInclusive = false;
    LiteralExpr upperBoundKey = null;
    LiteralExpr lowerBoundKey = null;

    if (((op == Operator.LE || op == Operator.LT) && isSlotOnLeft) ||
        ((op == Operator.GE || op == Operator.GT) && !isSlotOnLeft)) {
      // Case: SlotRef <[=] Literal
      if (literal.compareTo(firstKey) < 0) return Sets.newHashSet();
      if (op == Operator.LE || op == Operator.GE) upperInclusive = true;

      if (literal.compareTo(lastKey) <= 0) {
        upperBoundKey = literal;
      } else {
        upperBoundKey = lastKey;
        upperInclusive = true;
      }
      lowerBoundKey = firstKey;
      lowerInclusive = true;
    } else {
      // Cases: SlotRef >[=] Literal
      if (literal.compareTo(lastKey) > 0) return Sets.newHashSet();
      if (op == Operator.GE || op == Operator.LE) lowerInclusive = true;

      if (literal.compareTo(firstKey) >= 0) {
        lowerBoundKey = literal;
      } else {
        lowerBoundKey = firstKey;
        lowerInclusive = true;
      }
      upperBoundKey = lastKey;
      upperInclusive = true;
    }

    // Retrieve the submap that corresponds to the computed partition key
    // value range.
    rangeValueMap = partitionValueMap.subMap(lowerBoundKey, lowerInclusive,
        upperBoundKey, upperInclusive);
    // Compute the matching partition ids
    for (HashSet<Long> idSet: rangeValueMap.values()) {
      if (idSet != null) matchingIds.addAll(idSet);
    }
    return matchingIds;
  }

  /**
   * Evaluate an InPredicate filter on a partition column and return the ids of
   * the matching partitions.
   */
  private HashSet<Long> evalInPredicate(Expr expr) {
    Preconditions.checkNotNull(expr);
    Preconditions.checkState(expr instanceof InPredicate);
    InPredicate inPredicate = (InPredicate)expr;
    HashSet<Long> matchingIds = Sets.newHashSet();
    SlotRef slot = inPredicate.getBoundSlot();
    Preconditions.checkNotNull(slot);
    int partitionPos = slot.getDesc().getColumn().getPosition();
    TreeMap<LiteralExpr, HashSet<Long>> partitionValueMap =
        tbl_.getPartitionValueMap(partitionPos);

    if (inPredicate.isNotIn()) {
      // Case: SlotRef NOT IN (Literal, ..., Literal)
      // If there is a NullLiteral, return an empty set.
      List<Expr> nullLiterals = Lists.newArrayList();
      inPredicate.collectAll(Predicates.instanceOf(NullLiteral.class), nullLiterals);
      if (!nullLiterals.isEmpty()) return matchingIds;
      matchingIds.addAll(tbl_.getPartitionIds());
      // Exclude partitions with null partition column values
      Set<Long> nullIds = tbl_.getNullPartitionIds(partitionPos);
      matchingIds.removeAll(nullIds);
    }
    // Compute the matching partition ids
    for (int i = 1; i < inPredicate.getChildren().size(); ++i) {
      LiteralExpr literal = (LiteralExpr)inPredicate.getChild(i);
      HashSet<Long> idSet = partitionValueMap.get(literal);
      if (idSet != null) {
        if (inPredicate.isNotIn()) {
          matchingIds.removeAll(idSet);
        } else {
          matchingIds.addAll(idSet);
        }
      }
    }
    return matchingIds;
  }

  /**
   * Evaluate an IsNullPredicate on a partition column and return the ids of the
   * matching partitions.
   */
  private HashSet<Long> evalIsNullPredicate(Expr expr) {
    Preconditions.checkNotNull(expr);
    Preconditions.checkState(expr instanceof IsNullPredicate);
    HashSet<Long> matchingIds = Sets.newHashSet();
    IsNullPredicate nullPredicate = (IsNullPredicate)expr;
    SlotRef slot = nullPredicate.getBoundSlot();
    Preconditions.checkNotNull(slot);
    int partitionPos = slot.getDesc().getColumn().getPosition();
    Set<Long> nullPartitionIds = tbl_.getNullPartitionIds(partitionPos);

    if (nullPredicate.isNotNull()) {
      matchingIds.addAll(tbl_.getPartitionIds());
      matchingIds.removeAll(nullPartitionIds);
    } else {
      matchingIds.addAll(nullPartitionIds);
    }
    return matchingIds;
  }

  /**
   * Evaluate a slot binding predicate on a partition key using the partition
   * key values; return the matching partition ids. An empty set is returned
   * if there are no matching partitions. This function can evaluate the following
   * types of predicates: BinaryPredicate, CompoundPredicate, IsNullPredicate,
   * InPredicate, and BetweenPredicate.
   */
  private HashSet<Long> evalSlotBindingFilter(Expr expr) {
    Preconditions.checkNotNull(expr);
    if (expr instanceof BinaryPredicate) {
      return evalBinaryPredicate(expr);
    } else if (expr instanceof CompoundPredicate) {
      HashSet<Long> leftChildIds = evalSlotBindingFilter(expr.getChild(0));
      CompoundPredicate cp = (CompoundPredicate)expr;
      // NOT operators have been eliminated
      Preconditions.checkState(cp.getOp() != CompoundPredicate.Operator.NOT);
      if (cp.getOp() == CompoundPredicate.Operator.AND) {
        HashSet<Long> rightChildIds = evalSlotBindingFilter(expr.getChild(1));
        leftChildIds.retainAll(rightChildIds);
      } else if (cp.getOp() == CompoundPredicate.Operator.OR) {
        HashSet<Long> rightChildIds = evalSlotBindingFilter(expr.getChild(1));
        leftChildIds.addAll(rightChildIds);
      }
      return leftChildIds;
    } else if (expr instanceof InPredicate) {
      return evalInPredicate(expr);
    } else if (expr instanceof IsNullPredicate) {
      return evalIsNullPredicate(expr);
    } else if (expr instanceof BetweenPredicate) {
      return evalSlotBindingFilter(((BetweenPredicate) expr).getRewrittenPredicate());
    }
    return null;
  }

  /**
   * Evaluate a list of HdfsPartitionFilters in the BE. These are 'complex'
   * filters that could not be evaluated from the partition key values.
   */
  private void evalPartitionFiltersInBe(List<HdfsPartitionFilter> filters,
      HashSet<Long> matchingPartitionIds, Analyzer analyzer) throws InternalException {
    Map<Long, HdfsPartition> partitionMap = tbl_.getPartitionMap();
    // Set of partition ids that pass a filter
    HashSet<Long> matchingIds = Sets.newHashSet();
    // Batch of partitions
    ArrayList<HdfsPartition> partitionBatch = Lists.newArrayList();
    // Identify the partitions that pass all filters.
    for (HdfsPartitionFilter filter: filters) {
      // Iterate through the currently valid partitions
      for (Long id: matchingPartitionIds) {
        HdfsPartition p = partitionMap.get(id);
        Preconditions.checkState(
            p.getPartitionValues().size() == tbl_.getNumClusteringCols());
        // Add the partition to the current batch
        partitionBatch.add(partitionMap.get(id));
        if (partitionBatch.size() == PARTITION_PRUNING_BATCH_SIZE) {
          // Batch is full. Evaluate the predicates of this batch in the BE.
          matchingIds.addAll(filter.getMatchingPartitionIds(partitionBatch, analyzer));
          partitionBatch.clear();
        }
      }
      // Check if there are any unprocessed partitions.
      if (!partitionBatch.isEmpty()) {
        matchingIds.addAll(filter.getMatchingPartitionIds(partitionBatch, analyzer));
        partitionBatch.clear();
      }
      // Prune the partitions ids that didn't pass the filter
      matchingPartitionIds.retainAll(matchingIds);
      matchingIds.clear();
    }
  }
}
