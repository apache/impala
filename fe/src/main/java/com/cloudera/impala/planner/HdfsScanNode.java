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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.BinaryPredicate;
import com.cloudera.impala.analysis.BinaryPredicate.Operator;
import com.cloudera.impala.analysis.CompoundPredicate;
import com.cloudera.impala.analysis.DescriptorTable;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.InPredicate;
import com.cloudera.impala.analysis.IsNullPredicate;
import com.cloudera.impala.analysis.LiteralExpr;
import com.cloudera.impala.analysis.NullLiteral;
import com.cloudera.impala.analysis.SlotDescriptor;
import com.cloudera.impala.analysis.SlotId;
import com.cloudera.impala.analysis.SlotRef;
import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.analysis.TupleId;
import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.HdfsFileFormat;
import com.cloudera.impala.catalog.HdfsPartition;
import com.cloudera.impala.catalog.HdfsPartition.FileBlock;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.NotImplementedException;
import com.cloudera.impala.common.PrintUtils;
import com.cloudera.impala.common.RuntimeEnv;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.TExpr;
import com.cloudera.impala.thrift.THdfsFileBlock;
import com.cloudera.impala.thrift.THdfsFileSplit;
import com.cloudera.impala.thrift.THdfsScanNode;
import com.cloudera.impala.thrift.TNetworkAddress;
import com.cloudera.impala.thrift.TPlanNode;
import com.cloudera.impala.thrift.TPlanNodeType;
import com.cloudera.impala.thrift.TQueryOptions;
import com.cloudera.impala.thrift.TScanRange;
import com.cloudera.impala.thrift.TScanRangeLocation;
import com.cloudera.impala.thrift.TScanRangeLocations;
import com.cloudera.impala.util.MembershipSnapshot;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Scan of a single single table. Currently limited to full-table scans.
 * TODO: pass in range restrictions.
 */
public class HdfsScanNode extends ScanNode {
  private final static Logger LOG = LoggerFactory.getLogger(HdfsScanNode.class);

  // Read size of the backend I/O manager. Used in computeCosts().
  private final static long IO_MGR_BUFFER_SIZE = 8L * 1024L * 1024L;

  // Maximum number of I/O buffers per thread executing this scan.
  private final static long MAX_IO_BUFFERS_PER_THREAD = 10;

  // Number of scanner threads per core executing this scan.
  private final static int THREADS_PER_CORE = 3;

  // Factor capturing the worst-case deviation from a uniform distribution of scan ranges
  // among nodes. The factor of 1.2 means that a particular node may have 20% more
  // scan ranges than would have been estimated assuming a uniform distribution.
  private final static double SCAN_RANGE_SKEW_FACTOR = 1.2;

  // Partition batch size used during partition pruning
  private final static int PARTITION_PRUNING_BATCH_SIZE = 1024;

  private final HdfsTable tbl_;

  // Partitions that are filtered in for scanning by the key ranges
  private final ArrayList<HdfsPartition> partitions_ = Lists.newArrayList();

  // Total number of files from partitions_
  private long totalFiles_ = 0;

  // Total number of bytes from partitions_
  private long totalBytes_ = 0;

  // Conjuncts that can be evaluated while materializing the items (tuples) of
  // collection-typed slots. Maps from tuple descriptor to the conjuncts bound by that
  // tuple. Uses a linked hash map for consistent display in explain.
  private final Map<TupleDescriptor, List<Expr>> collectionConjuncts_ =
      Maps.newLinkedHashMap();

  // Indicates corrupt table stats based on the number of non-empty scan ranges and
  // numRows set to 0. Set in computeStats().
  private boolean hasCorruptTableStats_;

  /**
   * Constructs node to scan given data files of table 'tbl_'.
   */
  public HdfsScanNode(PlanNodeId id, TupleDescriptor desc, HdfsTable tbl) {
    super(id, desc, "SCAN HDFS");
    tbl_ = tbl;
  }

  @Override
  protected String debugString() {
    ToStringHelper helper = Objects.toStringHelper(this);
    for (HdfsPartition partition: partitions_) {
      helper.add("Partition " + partition.getId() + ":", partition.toString());
    }
    return helper.addValue(super.debugString()).toString();
  }

  /**
   * Populate conjuncts_, collectionConjuncts_, partitions_, and scanRanges_.
   */
  @Override
  public void init(Analyzer analyzer) throws ImpalaException {
    ArrayList<Expr> bindingPredicates = analyzer.getBoundPredicates(tupleIds_.get(0));
    conjuncts_.addAll(bindingPredicates);

    // also add remaining unassigned conjuncts
    assignConjuncts(analyzer);

    analyzer.createEquivConjuncts(tupleIds_.get(0), conjuncts_);

    // do partition pruning before deciding which slots to materialize,
    // we might end up removing some predicates
    prunePartitions(analyzer);
    checkForSupportedFileFormats();

    // mark all slots referenced by the remaining conjuncts as materialized
    markSlotsMaterialized(analyzer, conjuncts_);

    assignCollectionConjuncts(analyzer);

    computeMemLayout(analyzer);

    // compute scan range locations
    computeScanRangeLocations(analyzer);

    // do this at the end so it can take all conjuncts and scan ranges into account
    computeStats(analyzer);

    // TODO: do we need this?
    assignedConjuncts_ = analyzer.getAssignedConjuncts();
  }

  /**
   * Throws if the table schema contains a complex type and we need to scan
   * a partition that has a format for which we do not support complex types,
   * regardless of whether a complex-typed column is actually referenced
   * in the query.
   */
  @Override
  protected void checkForSupportedFileFormats() throws NotImplementedException {
    Preconditions.checkNotNull(desc_);
    Preconditions.checkNotNull(desc_.getTable());
    Column firstComplexTypedCol = null;
    for (Column col: desc_.getTable().getColumns()) {
      if (col.getType().isComplexType()) {
        firstComplexTypedCol = col;
        break;
      }
    }
    if (firstComplexTypedCol == null) return;

    for (HdfsPartition part: partitions_) {
      HdfsFileFormat format = part.getInputFormatDescriptor().getFileFormat();
      if (format.isComplexTypesSupported()) continue;
      String errSuffix = String.format(
          "Complex types are supported for these file formats: %s",
          Joiner.on(", ").join(HdfsFileFormat.complexTypesFormats()));
      if (desc_.getTable().getNumClusteringCols() == 0) {
        throw new NotImplementedException(String.format(
            "Scan of table '%s' in format '%s' is not supported because the table " +
            "has a column '%s' with a complex type '%s'.\n%s.",
            desc_.getAlias(), format, firstComplexTypedCol.getName(),
            firstComplexTypedCol.getType().toSql(), errSuffix));
      }
      throw new NotImplementedException(String.format(
          "Scan of partition '%s' in format '%s' of table '%s' is not supported " +
          "because the table has a column '%s' with a complex type '%s'.\n%s.",
          part.getPartitionName(), format, desc_.getAlias(),
          firstComplexTypedCol.getName(), firstComplexTypedCol.getType().toSql(),
          errSuffix));
    }
  }

  /**
   * Populates the collection conjuncts, materializes their required slots, and marks
   * the conjuncts as assigned, if it is correct to do so. Some conjuncts may have to
   * also be evaluated at a subsequent semi or outer join.
   */
  private void assignCollectionConjuncts(Analyzer analyzer) {
    collectionConjuncts_.clear();
    assignCollectionConjuncts(desc_, analyzer);
  }

  /**
   * Recursively collects and assigns conjuncts bound by tuples materialized in a
   * collection-typed slot.
   *
   * Limitation: Conjuncts that must first be migrated into inline views and that cannot
   * be captured by slot binding will not be assigned here, but in an UnnestNode.
   * This limitation applies to conjuncts bound by inline-view slots that are backed by
   * non-SlotRef exprs in the inline-view's select list. We only capture value transfers
   * between slots, and not between arbitrary exprs.
   *
   * TODO for 2.3: The logic for gathering conjuncts and deciding which ones should be
   * marked as assigned needs to be clarified and consolidated in one place. The code
   * below is rather different from the code for assigning the top-level conjuncts in
   * init() although the performed tasks is conceptually identical. Refactoring the
   * assignment code is tricky/risky for now.
   */
  private void assignCollectionConjuncts(TupleDescriptor tupleDesc, Analyzer analyzer) {
    for (SlotDescriptor slotDesc: tupleDesc.getSlots()) {
      if (!slotDesc.getType().isCollectionType()) continue;
      Preconditions.checkNotNull(slotDesc.getItemTupleDesc());
      TupleDescriptor itemTupleDesc = slotDesc.getItemTupleDesc();
      TupleId itemTid = itemTupleDesc.getId();
      // First collect unassigned and binding predicates. Then remove redundant
      // predicates based on slot equivalences and enforce slot equivalences by
      // generating new predicates.
      List<Expr> collectionConjuncts =
          analyzer.getUnassignedConjuncts(Lists.newArrayList(itemTid), false);
      ArrayList<Expr> bindingPredicates = analyzer.getBoundPredicates(itemTid);
      for (Expr boundPred: bindingPredicates) {
        if (!collectionConjuncts.contains(boundPred)) collectionConjuncts.add(boundPred);
      }
      analyzer.createEquivConjuncts(itemTid, collectionConjuncts);
      // Mark those conjuncts as assigned that do not also need to be evaluated by a
      // subsequent semi or outer join.
      for (Expr conjunct: collectionConjuncts) {
        if (!analyzer.evalByJoin(conjunct)) analyzer.markConjunctAssigned(conjunct);
      }
      if (!collectionConjuncts.isEmpty()) {
        markSlotsMaterialized(analyzer, collectionConjuncts);
        collectionConjuncts_.put(itemTupleDesc, collectionConjuncts);
      }
      // Recursively look for collection-typed slots in nested tuple descriptors.
      assignCollectionConjuncts(itemTupleDesc, analyzer);
    }
  }

  /**
   * Computes scan ranges (hdfs splits) plus their storage locations, including volume
   * ids, based on the given maximum number of bytes each scan range should scan.
   */
  private void computeScanRangeLocations(Analyzer analyzer) {
    long maxScanRangeLength = analyzer.getQueryCtx().getRequest().getQuery_options()
        .getMax_scan_range_length();
    scanRanges_ = Lists.newArrayList();
    for (HdfsPartition partition: partitions_) {
      Preconditions.checkState(partition.getId() >= 0);
      for (HdfsPartition.FileDescriptor fileDesc: partition.getFileDescriptors()) {
        for (THdfsFileBlock thriftBlock: fileDesc.getFileBlocks()) {
          HdfsPartition.FileBlock block = FileBlock.fromThrift(thriftBlock);
          List<Integer> replicaHostIdxs = block.getReplicaHostIdxs();
          if (replicaHostIdxs.size() == 0) {
            // we didn't get locations for this block; for now, just ignore the block
            // TODO: do something meaningful with that
            continue;
          }
          // Collect the network address and volume ID of all replicas of this block.
          List<TScanRangeLocation> locations = Lists.newArrayList();
          for (int i = 0; i < replicaHostIdxs.size(); ++i) {
            TScanRangeLocation location = new TScanRangeLocation();
            // Translate from the host index (local to the HdfsTable) to network address.
            Integer tableHostIdx = replicaHostIdxs.get(i);
            TNetworkAddress networkAddress =
                partition.getTable().getHostIndex().getEntry(tableHostIdx);
            Preconditions.checkNotNull(networkAddress);
            // Translate from network address to the global (to this request) host index.
            Integer globalHostIdx = analyzer.getHostIndex().getIndex(networkAddress);
            location.setHost_idx(globalHostIdx);
            location.setVolume_id(block.getDiskId(i));
            location.setIs_cached(block.isCached(i));
            locations.add(location);
          }
          // create scan ranges, taking into account maxScanRangeLength
          long currentOffset = block.getOffset();
          long remainingLength = block.getLength();
          while (remainingLength > 0) {
            long currentLength = remainingLength;
            if (maxScanRangeLength > 0 && remainingLength > maxScanRangeLength) {
              currentLength = maxScanRangeLength;
            }
            TScanRange scanRange = new TScanRange();
            scanRange.setHdfs_file_split(new THdfsFileSplit(
                fileDesc.getFileName(), currentOffset, currentLength, partition.getId(),
                fileDesc.getFileLength(), fileDesc.getFileCompression(),
                fileDesc.getModificationTime()));
            TScanRangeLocations scanRangeLocations = new TScanRangeLocations();
            scanRangeLocations.scan_range = scanRange;
            scanRangeLocations.locations = locations;
            scanRanges_.add(scanRangeLocations);
            remainingLength -= currentLength;
            currentOffset += currentLength;
          }
        }
      }
    }
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
        HashSet<Long> ids = tbl_.getNullPartitionIds(partitionPos);
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
        HashSet<Long> nullIds = tbl_.getNullPartitionIds(partitionPos);
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
      HashSet<Long> nullIds = tbl_.getNullPartitionIds(partitionPos);
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
      HashSet<Long> nullIds = tbl_.getNullPartitionIds(partitionPos);
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
    HashSet<Long> nullPartitionIds = tbl_.getNullPartitionIds(partitionPos);

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
   * types of predicates: BinaryPredicate, CompoundPredicate, IsNullPredicate, and
   * InPredicate.
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
    }
    return null;
  }

  /**
   * Populate partitions_ based on all applicable conjuncts and remove
   * conjuncts used for filtering from conjuncts_.
   */
  private void prunePartitions(Analyzer analyzer) throws InternalException {
    DescriptorTable descTbl = analyzer.getDescTbl();
    // loop through all partitions and prune based on applicable conjuncts;
    // start with creating a collection of partition filters for the applicable conjuncts
    List<SlotId> partitionSlots = Lists.newArrayList();
    for (SlotDescriptor slotDesc: descTbl.getTupleDesc(tupleIds_.get(0)).getSlots()) {
      if (slotDesc.getColumn() == null) continue;
      if (slotDesc.getColumn().getPosition() < tbl_.getNumClusteringCols()) {
        partitionSlots.add(slotDesc.getId());
      }
    }
    List<HdfsPartitionFilter> partitionFilters = Lists.newArrayList();
    // Conjuncts that can be evaluated from the partition key values.
    List<Expr> simpleFilterConjuncts = Lists.newArrayList();

    // Simple predicates (e.g. binary predicates of the form
    // <SlotRef> <op> <LiteralExpr>) can be used to derive lists
    // of matching partition ids directly from the partition key values.
    // Split conjuncts among those that can be evaluated from partition
    // key values and those that need to be evaluated in the BE.
    Iterator<Expr> it = conjuncts_.iterator();
    while (it.hasNext()) {
      Expr conjunct = it.next();
      if (conjunct.isBoundBySlotIds(partitionSlots)) {
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
    HashMap<Long, HdfsPartition> partitionMap = tbl_.getPartitionMap();
    for (Long id: matchingPartitionIds) {
      HdfsPartition partition = partitionMap.get(id);
      Preconditions.checkNotNull(partition);
      if (partition.hasFileDescriptors()) {
        partitions_.add(partition);
        descTbl.addReferencedPartition(tbl_, partition.getId());
      }
    }
  }

  /**
   * Evaluate a list of HdfsPartitionFilters in the BE. These are 'complex'
   * filters that could not be evaluated from the partition key values.
   */
  private void evalPartitionFiltersInBe(List<HdfsPartitionFilter> filters,
      HashSet<Long> matchingPartitionIds, Analyzer analyzer) throws InternalException {
    HashMap<Long, HdfsPartition> partitionMap = tbl_.getPartitionMap();
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

  /**
   * Also computes totalBytes_
   */
  @Override
  public void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);
    LOG.debug("collecting partitions for table " + tbl_.getName());
    numPartitionsMissingStats_ = 0;
    if (tbl_.getPartitions().isEmpty()) {
      cardinality_ = tbl_.getNumRows();
      if ((cardinality_ < -1 || cardinality_ == 0) && tbl_.getTotalHdfsBytes() > 0) {
        hasCorruptTableStats_ = true;
      }
    } else {
      cardinality_ = 0;
      totalFiles_ = 0;
      totalBytes_ = 0;
      boolean hasValidPartitionCardinality = false;
      for (HdfsPartition p: partitions_) {
        // Check for corrupt table stats
        if ((p.getNumRows() == 0 || p.getNumRows() < -1) && p.getSize() > 0)  {
          hasCorruptTableStats_ = true;
        }
        // ignore partitions with missing stats in the hope they don't matter
        // enough to change the planning outcome
        if (p.getNumRows() > -1) {
          cardinality_ = addCardinalities(cardinality_, p.getNumRows());
          hasValidPartitionCardinality = true;
        } else {
          ++numPartitionsMissingStats_;
        }
        totalFiles_ += p.getFileDescriptors().size();
        totalBytes_ += p.getSize();
      }

      if (!partitions_.isEmpty() && !hasValidPartitionCardinality) {
        // if none of the partitions knew its number of rows, we fall back on
        // the table stats
        cardinality_ = tbl_.getNumRows();
      }
    }
    // Adjust cardinality for all collections referenced along the tuple's path.
    if (cardinality_ != -1) {
      for (Type t: desc_.getPath().getMatchedTypes()) {
        if (t.isCollectionType()) cardinality_ *= PlannerContext.AVG_COLLECTION_SIZE;
      }
    }
    inputCardinality_ = cardinality_;
    Preconditions.checkState(cardinality_ >= 0 || cardinality_ == -1,
        "Internal error: invalid scan node cardinality: " + cardinality_);
    if (cardinality_ > 0) {
      LOG.debug("cardinality_=" + Long.toString(cardinality_) +
                " sel=" + Double.toString(computeSelectivity()));
      cardinality_ = Math.round(cardinality_ * computeSelectivity());
      // IMPALA-2165: Avoid setting the cardinality to 0 after rounding.
      cardinality_ = Math.max(cardinality_, 1);
    }
    cardinality_ = capAtLimit(cardinality_);
    LOG.debug("computeStats HdfsScan: cardinality_=" + Long.toString(cardinality_));

    computeNumNodes(analyzer, cardinality_);
    LOG.debug("computeStats HdfsScan: #nodes=" + Integer.toString(numNodes_));
  }

  /**
   * Estimate the number of impalad nodes that this scan node will execute on (which is
   * ultimately determined by the scheduling done by the backend's SimpleScheduler).
   * Assume that scan ranges that can be scheduled locally will be, and that scan
   * ranges that cannot will be round-robined across the cluster.
   */
  protected void computeNumNodes(Analyzer analyzer, long cardinality) {
    Preconditions.checkNotNull(scanRanges_);
    MembershipSnapshot cluster = MembershipSnapshot.getCluster();
    HashSet<TNetworkAddress> localHostSet = Sets.newHashSet();
    int totalNodes = 0;
    int numLocalRanges = 0;
    int numRemoteRanges = 0;
    for (TScanRangeLocations range: scanRanges_) {
      boolean anyLocal = false;
      for (TScanRangeLocation loc: range.locations) {
        TNetworkAddress dataNode = analyzer.getHostIndex().getEntry(loc.getHost_idx());
        if (cluster.contains(dataNode)) {
          anyLocal = true;
          // Use the full datanode address (including port) to account for the test
          // minicluster where there are multiple datanodes and impalads on a single
          // host.  This assumes that when an impalad is colocated with a datanode,
          // there are the same number of impalads as datanodes on this host in this
          // cluster.
          localHostSet.add(dataNode);
        }
      }
      // This range has at least one replica with a colocated impalad, so assume it
      // will be scheduled on one of those nodes.
      if (anyLocal) {
        ++numLocalRanges;
      } else {
        ++numRemoteRanges;
      }
      // Approximate the number of nodes that will execute locally assigned ranges to
      // be the smaller of the number of locally assigned ranges and the number of
      // hosts that hold block replica for those ranges.
      int numLocalNodes = Math.min(numLocalRanges, localHostSet.size());
      // The remote ranges are round-robined across all the impalads.
      int numRemoteNodes = Math.min(numRemoteRanges, cluster.numNodes());
      // The local and remote assignments may overlap, but we don't know by how much so
      // conservatively assume no overlap.
      totalNodes = Math.min(numLocalNodes + numRemoteNodes, cluster.numNodes());
      // Exit early if all hosts have a scan range assignment, to avoid extraneous work
      // in case the number of scan ranges dominates the number of nodes.
      if (totalNodes == cluster.numNodes()) break;
    }
    // Tables can reside on 0 nodes (empty table), but a plan node must always be
    // executed on at least one node.
    numNodes_ = (cardinality == 0 || totalNodes == 0) ? 1 : totalNodes;
    LOG.debug("computeNumNodes totalRanges=" + scanRanges_.size() +
        " localRanges=" + numLocalRanges + " remoteRanges=" + numRemoteRanges +
        " localHostSet.size=" + localHostSet.size() +
        " clusterNodes=" + cluster.numNodes());
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.hdfs_scan_node = new THdfsScanNode(desc_.getId().asInt());
    msg.node_type = TPlanNodeType.HDFS_SCAN_NODE;
    if (!collectionConjuncts_.isEmpty()) {
      Map<Integer, List<TExpr>> tcollectionConjuncts = Maps.newLinkedHashMap();
      for (Map.Entry<TupleDescriptor, List<Expr>> entry:
        collectionConjuncts_.entrySet()) {
        tcollectionConjuncts.put(entry.getKey().getId().asInt(),
            Expr.treesToThrift(entry.getValue()));
      }
      msg.hdfs_scan_node.setCollection_conjuncts(tcollectionConjuncts);
    }
  }

  @Override
  protected String getDisplayLabelDetail() {
    HdfsTable table = (HdfsTable) desc_.getTable();
    List<String> path = Lists.newArrayList();
    path.add(table.getDb().getName());
    path.add(table.getName());
    Preconditions.checkNotNull(desc_.getPath());
    if (desc_.hasExplicitAlias()) {
      return desc_.getPath().toString() + " " + desc_.getAlias();
    } else {
      return desc_.getPath().toString();
    }
  }

  @Override
  protected String getNodeExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    HdfsTable table = (HdfsTable) desc_.getTable();
    output.append(String.format("%s%s [%s", prefix, getDisplayLabel(),
        getDisplayLabelDetail()));
    if (detailLevel.ordinal() >= TExplainLevel.EXTENDED.ordinal() &&
        fragment_.isPartitioned()) {
      output.append(", " + fragment_.getDataPartition().getExplainString());
    }
    output.append("]\n");
    if (detailLevel.ordinal() >= TExplainLevel.STANDARD.ordinal()) {
      int numPartitions = partitions_.size();
      if (tbl_.getNumClusteringCols() == 0) numPartitions = 1;
      output.append(String.format("%spartitions=%s/%s files=%s size=%s", detailPrefix,
          numPartitions, table.getPartitions().size() - 1, totalFiles_,
          PrintUtils.printBytes(totalBytes_)));
      output.append("\n");
      if (!conjuncts_.isEmpty()) {
        output.append(
            detailPrefix + "predicates: " + getExplainString(conjuncts_) + "\n");
      }
      if (!collectionConjuncts_.isEmpty()) {
        for (Map.Entry<TupleDescriptor, List<Expr>> entry:
          collectionConjuncts_.entrySet()) {
          String alias = entry.getKey().getAlias();
          output.append(String.format("%spredicates on %s: %s\n",
              detailPrefix, alias, getExplainString(entry.getValue())));
        }
      }
    }
    if (detailLevel.ordinal() >= TExplainLevel.EXTENDED.ordinal()) {
      output.append(getStatsExplainString(detailPrefix, detailLevel));
      output.append("\n");
    }
    return output.toString();
  }

  @Override
  public void computeCosts(TQueryOptions queryOptions) {
    Preconditions.checkNotNull(scanRanges_, "Cost estimation requires scan ranges.");
    if (scanRanges_.isEmpty()) {
      perHostMemCost_ = 0;
      return;
    }
    Preconditions.checkState(0 < numNodes_ && numNodes_ <= scanRanges_.size());
    Preconditions.checkNotNull(desc_);
    Preconditions.checkNotNull(desc_.getTable() instanceof HdfsTable);
    HdfsTable table = (HdfsTable) desc_.getTable();
    int perHostScanRanges;
    if (table.getMajorityFormat() == HdfsFileFormat.PARQUET) {
      // For the purpose of this estimation, the number of per-host scan ranges for
      // Parquet files are equal to the number of non-partition columns scanned.
      perHostScanRanges = 0;
      for (SlotDescriptor slot: desc_.getSlots()) {
        if (slot.getColumn() == null ||
            slot.getColumn().getPosition() >= table.getNumClusteringCols()) {
          ++perHostScanRanges;
        }
      }
    } else {
      perHostScanRanges = (int) Math.ceil((
          (double) scanRanges_.size() / (double) numNodes_) * SCAN_RANGE_SKEW_FACTOR);
    }

    // TODO: The total memory consumption for a particular query depends on the number
    // of *available* cores, i.e., it depends the resource consumption of other
    // concurrent queries. Figure out how to account for that.
    int maxScannerThreads = Math.min(perHostScanRanges,
        RuntimeEnv.INSTANCE.getNumCores() * THREADS_PER_CORE);
    // Account for the max scanner threads query option.
    if (queryOptions.isSetNum_scanner_threads() &&
        queryOptions.getNum_scanner_threads() > 0) {
      maxScannerThreads =
          Math.min(maxScannerThreads, queryOptions.getNum_scanner_threads());
    }

    long avgScanRangeBytes = (long) Math.ceil(totalBytes_ / (double) scanRanges_.size());
    // The +1 accounts for an extra I/O buffer to read past the scan range due to a
    // trailing record spanning Hdfs blocks.
    long perThreadIoBuffers =
        Math.min((long) Math.ceil(avgScanRangeBytes / (double) IO_MGR_BUFFER_SIZE),
            MAX_IO_BUFFERS_PER_THREAD) + 1;
    perHostMemCost_ = maxScannerThreads * perThreadIoBuffers * IO_MGR_BUFFER_SIZE;

    // Sanity check: the tighter estimation should not exceed the per-host maximum.
    long perHostUpperBound = getPerHostMemUpperBound();
    if (perHostMemCost_ > perHostUpperBound) {
      LOG.warn(String.format("Per-host mem cost %s exceeded per-host upper bound %s.",
          PrintUtils.printBytes(perHostMemCost_),
          PrintUtils.printBytes(perHostUpperBound)));
      perHostMemCost_ = perHostUpperBound;
    }
  }

  /**
   * Hdfs scans use a shared pool of buffers managed by the I/O manager. Intuitively,
   * the maximum number of I/O buffers is limited by the total disk bandwidth of a node.
   * Therefore, this upper bound is independent of the number of concurrent scans and
   * queries and helps to derive a tighter per-host memory estimate for queries with
   * multiple concurrent scans.
   */
  public static long getPerHostMemUpperBound() {
    // THREADS_PER_CORE each using a default of
    // MAX_IO_BUFFERS_PER_THREAD * IO_MGR_BUFFER_SIZE bytes.
    return (long) RuntimeEnv.INSTANCE.getNumCores() * (long) THREADS_PER_CORE *
        MAX_IO_BUFFERS_PER_THREAD * IO_MGR_BUFFER_SIZE;
  }

  @Override
  public boolean hasCorruptTableStats() { return hasCorruptTableStats_; }
}
