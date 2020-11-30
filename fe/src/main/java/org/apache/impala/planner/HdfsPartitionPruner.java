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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BetweenPredicate;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.BinaryPredicate.Operator;
import org.apache.impala.analysis.CompoundPredicate;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.InPredicate;
import org.apache.impala.analysis.IsNullPredicate;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.analysis.NullLiteral;
import org.apache.impala.analysis.SlotId;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.TableRef;
import org.apache.impala.analysis.TableSampleClause;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.PrunablePartition;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.Pair;
import org.apache.impala.rewrite.BetweenToCompoundRule;
import org.apache.impala.rewrite.ExprRewriter;
import org.apache.impala.rewrite.FoldConstantsRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
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
 *
 * The pruner does not update referenced partitions in the DescriptorTable because
 * not all users of this class require the resulting partitions to be serialized, e.g.,
 * DDL commands.
 * It is up to the user of this class to mark referenced partitions as needed.
 */
public class HdfsPartitionPruner {

  private final static Logger LOG = LoggerFactory.getLogger(HdfsPartitionPruner.class);

  // Partition batch size used during partition pruning.
  private final static int PARTITION_PRUNING_BATCH_SIZE = 1024;

  private final FeFsTable tbl_;
  private final List<SlotId> partitionSlots_;

  // For converting BetweenPredicates to CompoundPredicates so they can be
  // executed in the BE.
  private final ExprRewriter exprRewriter_ =
      new ExprRewriter(new ArrayList<>(
          Arrays.asList(BetweenToCompoundRule.INSTANCE,
                        FoldConstantsRule.INSTANCE)));

  public HdfsPartitionPruner(TupleDescriptor tupleDesc) {
    Preconditions.checkState(tupleDesc.getTable() instanceof FeFsTable);
    tbl_ = (FeFsTable)tupleDesc.getTable();
    partitionSlots_ = tupleDesc.getPartitionSlots();

  }

  /**
   * Return a list of partitions left after applying the conjuncts.
   * Conjuncts used for filtering will be removed from the list 'conjuncts' and
   * returned as the second item in the returned Pair. These expressions
   * include planner rewrites such as view reference substitutions and can be
   * shown in the EXPLAIN output.
   *
   * If 'allowEmpty' is False, empty partitions are not returned.
   */
  public Pair<List<? extends FeFsPartition>, List<Expr>> prunePartitions(
      Analyzer analyzer, List<Expr> conjuncts, boolean allowEmpty,
      TableRef hdfsTblRef)
      throws ImpalaException {
    // Start with creating a collection of partition filters for the applicable conjuncts.
    List<HdfsPartitionFilter> partitionFilters = new ArrayList<>();
    // Conjuncts that can be evaluated from the partition key values.
    List<Expr> simpleFilterConjuncts = new ArrayList<>();
    List<Expr> partitionConjuncts = new ArrayList<>();

    // Simple predicates (e.g. binary predicates of the form
    // <SlotRef> <op> <LiteralExpr>) can be used to derive lists
    // of matching partition ids directly from the partition key values.
    // Split conjuncts among those that can be evaluated from partition
    // key values and those that need to be evaluated in the BE.
    Iterator<Expr> it = conjuncts.iterator();
    while (it.hasNext()) {
      Expr conjunct = it.next();
      if (conjunct.isBoundBySlotIds(partitionSlots_) &&
          !conjunct.contains(Expr.IS_NONDETERMINISTIC_BUILTIN_FN_PREDICATE)) {
        // Check if the conjunct can be evaluated from the partition metadata.
        // Use a cloned conjunct to rewrite BetweenPredicates and allow
        // canEvalUsingPartitionMd() to fold constant expressions without modifying
        // the original expr.
        Expr clonedConjunct = exprRewriter_.rewrite(conjunct.clone(), analyzer);
        if (canEvalUsingPartitionMd(clonedConjunct, analyzer)) {
          simpleFilterConjuncts.add(Expr.pushNegationToOperands(clonedConjunct));
        } else {
          partitionFilters.add(new HdfsPartitionFilter(clonedConjunct, tbl_, analyzer));
        }
        partitionConjuncts.add(clonedConjunct);
        it.remove();
      }
    }

    // Set of matching partition ids, i.e. partitions that pass all filters
    Set<Long> matchingPartitionIds = null;

    // Evaluate the partition filters from the partition key values.
    // The result is the intersection of the associated partition id sets.
    for (Expr filter: simpleFilterConjuncts) {
      // Evaluate the filter
      Set<Long> matchingIds = evalSlotBindingFilter(filter);
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
    List<? extends FeFsPartition> results = tbl_.loadPartitions(
        matchingPartitionIds);
    if (!allowEmpty) {
      results = Lists.newArrayList(Iterables.filter(results,
          new Predicate<FeFsPartition>() {
            @Override
            public boolean apply(FeFsPartition partition) {
              return partition.hasFileDescriptors();
            }
          }));
    }
    if (hdfsTblRef != null) {
      // check and apply pruning for simple limit
      results = pruneForSimpleLimit(hdfsTblRef, analyzer, results);
    }
    return new Pair<>(results, partitionConjuncts);
  }

  /**
   * Prune partitions based on eligibility of simple limit optimization:
   *  - Either the table ref should not already have a TABLESAMPLE clause
   *    or if it does then the simple_limit_to_sample hint must be set
   *  - OPTIMIZE_SIMPLE_LIMIT is enabled and the query block satisfies
   *    simple limit criteria
   */
  private List<? extends FeFsPartition> pruneForSimpleLimit(TableRef tblRef,
    Analyzer analyzer, List<? extends FeFsPartition> partitions) {
    if ((tblRef.getSampleParams() == null
          || tblRef.hasConvertLimitToSampleHint())
        && analyzer.getQueryCtx().client_request.getQuery_options()
        .isOptimize_simple_limit()
        && analyzer.getSimpleLimitStatus() != null
        && analyzer.getSimpleLimitStatus().first) {
      long limitValue = analyzer.getSimpleLimitStatus().second;
      if (tblRef.hasConvertLimitToSampleHint()) {
        tblRef.setTableSampleClause(new TableSampleClause(
            tblRef.getConvertLimitToSampleHintPercent(), /* seed */ 1L));
      } else {
        List<FeFsPartition> prunedPartitions = new ArrayList<>();
        long numRows = 0;
        // Instead of using the partitions num rows statistic which may be stale,
        // we use a conservative estimate of number of files within a partition and
        // 1 row per file
        for (FeFsPartition p : partitions) {
          numRows += p.getNumFileDescriptors();
          prunedPartitions.add(p);
          if (numRows >= limitValue) {
            // here we only limit the partitions; later in HdfsScanNode we will
            // limit the file descriptors within a partition
            break;
          }
        }
        return prunedPartitions;
      }
    }
    return partitions;
  }

  /**
   * Recursive function that checks if a given partition expr can be evaluated
   * directly from the partition key values. If 'expr' contains any constant expressions,
   * they are evaluated in the BE and are replaced by their corresponding results, as
   * LiteralExprs.
   */
  private boolean canEvalUsingPartitionMd(Expr expr, Analyzer analyzer) {
    Preconditions.checkNotNull(expr);
    Preconditions.checkState(!(expr instanceof BetweenPredicate));
    if (expr instanceof BinaryPredicate) {
      // Evaluate any constant expression in the BE
      try {
        // Constant folding and rewrite should already be done by this
        // point -- unless this is a copy of an expression taken
        // before analysis, which would introduce its own issues.
        expr = analyzer.getConstantFolder().rewrite(expr, analyzer);
        Preconditions.checkState(expr instanceof BinaryPredicate);
      } catch (AnalysisException e) {
        LOG.error("Error evaluating constant expressions in the BE: " + e.getMessage());
        return false;
      }
      BinaryPredicate bp = (BinaryPredicate)expr;
      if (bp.getChild(0).isImplicitCast()) return false;
      SlotRef slot = bp.getBoundSlot();
      if (slot == null) return false;
      Expr bindingExpr = bp.getSlotBinding(slot.getSlotId());
      if (bindingExpr == null || !Expr.IS_LITERAL.apply(bindingExpr)) return false;
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
        analyzer.getConstantFolder().rewrite(expr, analyzer);
      } catch (AnalysisException e) {
        LOG.error("Error evaluating constant expressions in the BE: " + e.getMessage());
        return false;
      }
      // Check for SlotRef [NOT] IN (Literal, ... Literal) case
      SlotRef slot = ((InPredicate)expr).getBoundSlot();
      if (slot == null) return false;
      for (int i = 1; i < expr.getChildren().size(); ++i) {
        if (!Expr.IS_LITERAL.apply(expr.getChild(i))) return false;
      }
      return true;
    }
    return false;
  }

  /**
   * Evaluate a BinaryPredicate filter on a partition column and return the
   * ids of the matching partitions. An empty set is returned if there
   * are no matching partitions.
   */
  private Set<Long> evalBinaryPredicate(Expr expr) {
    Preconditions.checkNotNull(expr);
    Preconditions.checkState(expr instanceof BinaryPredicate);
    // TODO: Note that rewrite rules should have ensured that the slot
    // is on the left.
    boolean isSlotOnLeft = true;
    if (Expr.IS_LITERAL.apply(expr.getChild(0))) isSlotOnLeft = false;

    // Get the operands
    BinaryPredicate bp = (BinaryPredicate)expr;
    SlotRef slot = bp.getBoundSlot();
    Preconditions.checkNotNull(slot);
    Expr bindingExpr = bp.getSlotBinding(slot.getSlotId());
    Preconditions.checkNotNull(bindingExpr);
    Preconditions.checkState(Expr.IS_LITERAL.apply(bindingExpr));
    LiteralExpr literal = (LiteralExpr)bindingExpr;
    Operator op = bp.getOp();
    if (Expr.IS_NULL_LITERAL.apply(literal) && (op != Operator.NOT_DISTINCT)
        && (op != Operator.DISTINCT_FROM)) {
      return new HashSet<>();
    }

    // Get the partition column position and retrieve the associated partition
    // value metadata.
    int partitionPos = slot.getDesc().getColumn().getPosition();
    TreeMap<LiteralExpr, Set<Long>> partitionValueMap =
        tbl_.getPartitionValueMap(partitionPos);
    if (partitionValueMap.isEmpty()) return new HashSet<>();

    Set<Long> matchingIds = new HashSet<>();
    // Compute the matching partition ids
    if (op == Operator.NOT_DISTINCT) {
      // Case: SlotRef <=> Literal
      if (Expr.IS_NULL_LITERAL.apply(literal)) {
        Set<Long> ids = tbl_.getNullPartitionIds(partitionPos);
        if (ids != null) matchingIds.addAll(ids);
        return matchingIds;
      }
      // Punt to equality case:
      op = Operator.EQ;
    }
    if (op == Operator.EQ) {
      // Case: SlotRef = Literal
      Set<Long> ids = partitionValueMap.get(literal);
      if (ids != null) matchingIds.addAll(ids);
      return matchingIds;
    }
    if (op == Operator.DISTINCT_FROM) {
      // Case: SlotRef IS DISTINCT FROM Literal
      matchingIds.addAll(tbl_.getPartitionIds());
      if (Expr.IS_NULL_LITERAL.apply(literal)) {
        Set<Long> nullIds = tbl_.getNullPartitionIds(partitionPos);
        matchingIds.removeAll(nullIds);
      } else {
        Set<Long> ids = partitionValueMap.get(literal);
        if (ids != null) matchingIds.removeAll(ids);
      }
      return matchingIds;
    }
    if (op == Operator.NE) {
      // Case: SlotRef != Literal
      matchingIds.addAll(tbl_.getPartitionIds());
      Set<Long> nullIds = tbl_.getNullPartitionIds(partitionPos);
      matchingIds.removeAll(nullIds);
      Set<Long> ids = partitionValueMap.get(literal);
      if (ids != null) matchingIds.removeAll(ids);
      return matchingIds;
    }

    // Determine the partition key value range of this predicate.
    NavigableMap<LiteralExpr, Set<Long>> rangeValueMap = null;
    LiteralExpr firstKey = partitionValueMap.firstKey();
    LiteralExpr lastKey = partitionValueMap.lastKey();
    boolean upperInclusive = false;
    boolean lowerInclusive = false;
    LiteralExpr upperBoundKey = null;
    LiteralExpr lowerBoundKey = null;

    if (((op == Operator.LE || op == Operator.LT) && isSlotOnLeft) ||
        ((op == Operator.GE || op == Operator.GT) && !isSlotOnLeft)) {
      // Case: SlotRef <[=] Literal
      if (literal.compareTo(firstKey) < 0) return new HashSet<>();
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
      if (literal.compareTo(lastKey) > 0) return new HashSet<>();
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
    for (Set<Long> idSet: rangeValueMap.values()) {
      if (idSet != null) matchingIds.addAll(idSet);
    }
    return matchingIds;
  }

  /**
   * Evaluate an InPredicate filter on a partition column and return the ids of
   * the matching partitions.
   */
  private Set<Long> evalInPredicate(Expr expr) {
    Preconditions.checkNotNull(expr);
    Preconditions.checkState(expr instanceof InPredicate);
    InPredicate inPredicate = (InPredicate)expr;
    Set<Long> matchingIds = new HashSet<>();
    SlotRef slot = inPredicate.getBoundSlot();
    Preconditions.checkNotNull(slot);
    int partitionPos = slot.getDesc().getColumn().getPosition();
    TreeMap<LiteralExpr, Set<Long>> partitionValueMap =
        tbl_.getPartitionValueMap(partitionPos);

    if (inPredicate.isNotIn()) {
      // Case: SlotRef NOT IN (Literal, ..., Literal)
      // If there is a NullLiteral, return an empty set.
      List<Expr> nullLiterals = new ArrayList<>();
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
      Set<Long> idSet = partitionValueMap.get(literal);
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
  private Set<Long> evalIsNullPredicate(Expr expr) {
    Preconditions.checkNotNull(expr);
    Preconditions.checkState(expr instanceof IsNullPredicate);
    Set<Long> matchingIds = new HashSet<>();
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
   * InPredicate.
   */
  private Set<Long> evalSlotBindingFilter(Expr expr) {
    Preconditions.checkNotNull(expr);
    Preconditions.checkState(!(expr instanceof BetweenPredicate));
    if (expr instanceof BinaryPredicate) {
      return evalBinaryPredicate(expr);
    } else if (expr instanceof CompoundPredicate) {
      Set<Long> leftChildIds = evalSlotBindingFilter(expr.getChild(0));
      CompoundPredicate cp = (CompoundPredicate)expr;
      // NOT operators have been eliminated
      Preconditions.checkState(cp.getOp() != CompoundPredicate.Operator.NOT);
      if (cp.getOp() == CompoundPredicate.Operator.AND) {
        Set<Long> rightChildIds = evalSlotBindingFilter(expr.getChild(1));
        leftChildIds.retainAll(rightChildIds);
      } else if (cp.getOp() == CompoundPredicate.Operator.OR) {
        Set<Long> rightChildIds = evalSlotBindingFilter(expr.getChild(1));
        leftChildIds.addAll(rightChildIds);
      }
      return leftChildIds;
    } else if (expr instanceof InPredicate) {
      return evalInPredicate(expr);
    } else if (expr instanceof IsNullPredicate) {
      return evalIsNullPredicate(expr);
    }
    return null;
  }

  /**
   * Evaluate a list of HdfsPartitionFilters in the BE. These are 'complex'
   * filters that could not be evaluated from the partition key values.
   */
  private void evalPartitionFiltersInBe(List<HdfsPartitionFilter> filters,
      Set<Long> matchingPartitionIds, Analyzer analyzer) throws ImpalaException {
    Map<Long, ? extends PrunablePartition> partitionMap = tbl_.getPartitionMap();
    // Set of partition ids that pass a filter
    Set<Long> matchingIds = new HashSet<>();
    // Batch of partitions
    List<PrunablePartition> partitionBatch = new ArrayList<>();
    // Identify the partitions that pass all filters.
    for (HdfsPartitionFilter filter: filters) {
      // Iterate through the currently valid partitions
      for (Long id: matchingPartitionIds) {
        PrunablePartition p = partitionMap.get(id);
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
