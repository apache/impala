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
import com.cloudera.impala.analysis.SlotDescriptor;
import com.cloudera.impala.analysis.SlotId;
import com.cloudera.impala.analysis.SlotRef;
import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.HdfsFileFormat;
import com.cloudera.impala.catalog.HdfsPartition;
import com.cloudera.impala.catalog.HdfsPartition.FileBlock;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.PrintUtils;
import com.cloudera.impala.common.RuntimeEnv;
import com.cloudera.impala.thrift.TExplainLevel;
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
import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
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

  // Total number of bytes from partitions_
  private long totalBytes_ = 0;

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
   * Populate conjuncts_, partitions_, and scanRanges_.
   */
  @Override
  public void init(Analyzer analyzer) throws InternalException {
    ArrayList<Expr> bindingPredicates = analyzer.getBoundPredicates(tupleIds_.get(0));
    conjuncts_.addAll(bindingPredicates);

    // also add remaining unassigned conjuncts
    assignConjuncts(analyzer);

    analyzer.enforceSlotEquivalences(tupleIds_.get(0), conjuncts_);

    // do partition pruning before deciding which slots to materialize,
    // we might end up removing some predicates
    prunePartitions(analyzer);

    // mark all slots referenced by the remaining conjuncts as materialized
    markSlotsMaterialized(analyzer, conjuncts_);
    computeMemLayout(analyzer);

    // do this at the end so it can take all conjuncts into account
    computeStats(analyzer);

    // compute scan range locations
    computeScanRangeLocations(analyzer);

    // TODO: do we need this?
    assignedConjuncts_ = analyzer.getAssignedConjuncts();
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
                fileDesc.getFileLength(), fileDesc.getFileCompression()));
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
   * Check if the PrimitiveType of an Expr is the same as the
   * PrimitiveType of a SlotRef's column.
   */
  private boolean hasIdenticalType(SlotRef slot, Expr literal) {
    Preconditions.checkNotNull(slot);
    Preconditions.checkNotNull(literal);
    Column slotCol = slot.getDesc().getColumn();
    PrimitiveType slotType = slotCol.getType().getPrimitiveType();
    PrimitiveType literalType = literal.getType().getPrimitiveType();
    return slotType == literalType ? true : false;
  }

  /**
   * Recursive function that checks if a given partition expr can be evaluated
   * directly from the partition key values.
   */
  private boolean canEvalUsingPartitionMd(Expr expr) {
    Preconditions.checkNotNull(expr);
    if (expr instanceof BinaryPredicate) {
      BinaryPredicate bp = (BinaryPredicate)expr;
      SlotRef slot = bp.getBoundSlot();
      if (slot == null) return false;
      Expr bindingExpr = bp.getSlotBinding(slot.getSlotId());
      if (bindingExpr == null || !(bindingExpr.isLiteral())) return false;
      // Make sure the SlotRef column and the LiteralExpr have the same
      // PrimitiveType. If not, the expr needs to be evaluated in the BE.
      //
      // TODO: If a cast is required to do the map lookup with a
      // literal, execute the cast expr in the BE and then do the lookup instead
      // of disabling this predicate altogether.
      return hasIdenticalType(slot, bindingExpr);
    } else if (expr instanceof CompoundPredicate) {
      boolean res = canEvalUsingPartitionMd(expr.getChild(0));
      if (expr.getChild(1) != null) {
        res &= canEvalUsingPartitionMd(expr.getChild(1));
      }
      return res;
    } else if (expr instanceof IsNullPredicate) {
      // Check for SlotRef IS [NOT] NULL case
      IsNullPredicate nullPredicate = (IsNullPredicate)expr;
      return nullPredicate.getBoundSlot() != null;
    } else if (expr instanceof InPredicate) {
      // Check for SlotRef [NOT] IN (Literal, ... Literal) case
      SlotRef slot = ((InPredicate)expr).getBoundSlot();
      if (slot == null) return false;

      for (int i = 1; i < expr.getChildren().size(); ++i) {
        Expr rhs = expr.getChild(i);
        if (!(rhs.isLiteral())) {
          return false;
        } else {
          // Make sure the SlotRef column and the LiteralExpr have the same
          // PrimitiveType. If not, the expr needs to be evaluated in the BE.
          if (!hasIdenticalType(slot, rhs)) return false;
        }
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

    // Get the partition column position and retrieve the associated partition
    // value metadata.
    int partitionPos = slot.getDesc().getColumn().getPosition();
    TreeMap<LiteralExpr, HashSet<Long>> partitionValueMap =
        tbl_.getPartitionValueMap(partitionPos);
    if (partitionValueMap.isEmpty()) return Sets.newHashSet();

    HashSet<Long> matchingIds = Sets.newHashSet();
    // Compute the matching partition ids
    Operator op = bp.getOp();
    if (op == Operator.EQ) {
      // Case: SlotRef = Literal
      HashSet<Long> ids = partitionValueMap.get(literal);
      if (ids != null) matchingIds.addAll(ids);
      return matchingIds;
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
      Preconditions.checkState(slotDesc.getColumn() != null);
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
        if (canEvalUsingPartitionMd(conjunct)) {
          simpleFilterConjuncts.add(Expr.pushNegationToOperands(conjunct));
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
    if (tbl_.getPartitions().isEmpty()) {
      cardinality_ = tbl_.getNumRows();
    } else {
      cardinality_ = 0;
      totalBytes_ = 0;
      boolean hasValidPartitionCardinality = false;
      for (HdfsPartition p: partitions_) {
        // ignore partitions with missing stats in the hope they don't matter
        // enough to change the planning outcome
        if (p.getNumRows() > 0) {
          cardinality_ = addCardinalities(cardinality_, p.getNumRows());
          hasValidPartitionCardinality = true;
        }
        totalBytes_ += p.getSize();
      }

      if (!partitions_.isEmpty() && !hasValidPartitionCardinality) {
        // if none of the partitions knew its number of rows, we fall back on
        // the table stats
        cardinality_ = tbl_.getNumRows();
      }
    }

    Preconditions.checkState(cardinality_ >= 0 || cardinality_ == -1,
        "Internal error: invalid scan node cardinality: " + cardinality_);
    if (cardinality_ > 0) {
      LOG.debug("cardinality_=" + Long.toString(cardinality_) +
                " sel=" + Double.toString(computeSelectivity()));
      cardinality_ = Math.round((double) cardinality_ * computeSelectivity());
    }
    cardinality_ = capAtLimit(cardinality_);
    LOG.debug("computeStats HdfsScan: cardinality_=" + Long.toString(cardinality_));

    // TODO: take actual partitions into account
    numNodes_ = cardinality_ == 0 ? 1 : tbl_.getNumNodes();
    LOG.debug("computeStats HdfsScan: #nodes=" + Integer.toString(numNodes_));
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    // TODO: retire this once the migration to the new plan is complete
    msg.hdfs_scan_node = new THdfsScanNode(desc_.getId().asInt());
    msg.node_type = TPlanNodeType.HDFS_SCAN_NODE;
  }

  @Override
  protected String getDisplayLabelDetail() {
    HdfsTable table = (HdfsTable) desc_.getTable();
    String result = table.getFullName();
    if (!table.getFullName().equalsIgnoreCase(desc_.getAlias()) &&
        !table.getName().equalsIgnoreCase(desc_.getAlias())) {
      result = result + " " + desc_.getAlias();
    }
    return result;
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
      output.append(String.format("%spartitions=%s/%s size=%s", detailPrefix,
          numPartitions, table.getPartitions().size() - 1,
          PrintUtils.printBytes(totalBytes_)));
      output.append("\n");
      if (!conjuncts_.isEmpty()) {
        output.append(
            detailPrefix + "predicates: " + getExplainString(conjuncts_) + "\n");
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

    // Number of nodes for the purpose of resource estimation adjusted
    // for the special cases listed below.
    long adjNumNodes = numNodes_;
    if (numNodes_ <= 0) {
      adjNumNodes = 1;
    } else if (scanRanges_.size() < numNodes_) {
      // TODO: Empirically evaluate whether there is more Hdfs block skew for relatively
      // small files, i.e., whether this estimate is too optimistic.
      adjNumNodes = scanRanges_.size();
    }

    Preconditions.checkNotNull(desc_);
    Preconditions.checkNotNull(desc_.getTable() instanceof HdfsTable);
    HdfsTable table = (HdfsTable) desc_.getTable();
    int perHostScanRanges;
    if (table.getMajorityFormat() == HdfsFileFormat.PARQUET) {
      // For the purpose of this estimation, the number of per-host scan ranges for
      // Parquet files are equal to the number of non-partition columns scanned.
      perHostScanRanges = 0;
      for (SlotDescriptor slot: desc_.getSlots()) {
        if (slot.getColumn().getPosition() >= table.getNumClusteringCols()) {
          ++perHostScanRanges;
        }
      }
    } else {
      perHostScanRanges = (int) Math.ceil((
          (double) scanRanges_.size() / (double) adjNumNodes) * SCAN_RANGE_SKEW_FACTOR);
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
        (long) MAX_IO_BUFFERS_PER_THREAD * IO_MGR_BUFFER_SIZE;
  }
}
