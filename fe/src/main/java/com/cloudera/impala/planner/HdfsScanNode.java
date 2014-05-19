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
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.SlotDescriptor;
import com.cloudera.impala.analysis.SlotId;
import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.HdfsFileFormat;
import com.cloudera.impala.catalog.HdfsPartition;
import com.cloudera.impala.catalog.HdfsPartition.FileBlock;
import com.cloudera.impala.catalog.HdfsTable;
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

  // List of scan-range locations. Populated in getScanRangeLocations().
  private List<TScanRangeLocations> scanRanges_;

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
   * Populate conjuncts_ and partitions_.
   */
  @Override
  public void init(Analyzer analyzer)
      throws InternalException, AuthorizationException {
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

    // TODO: do we need this?
    assignedConjuncts_ = analyzer.getAssignedConjuncts();
  }

 /**
   * Populate partitions_ based on all applicable conjuncts and remove
   * conjuncts used for filtering from conjuncts_.
   */
  private void prunePartitions(Analyzer analyzer)
      throws InternalException, AuthorizationException {
    // loop through all partitions and prune based on applicable conjuncts;
    // start with creating a collection of partition filters for the applicable conjuncts
    List<SlotId> partitionSlots = Lists.newArrayList();
    for (SlotDescriptor slotDesc:
        analyzer.getDescTbl().getTupleDesc(tupleIds_.get(0)).getSlots()) {
      Preconditions.checkState(slotDesc.getColumn() != null);
      if (slotDesc.getColumn().getPosition() < tbl_.getNumClusteringCols()) {
        partitionSlots.add(slotDesc.getId());
      }
    }
    List<HdfsPartitionFilter> partitionFilters = Lists.newArrayList();
    List<Expr> filterConjuncts = Lists.newArrayList();
    for (Expr conjunct: conjuncts_) {
      if (conjunct.isBoundBySlotIds(partitionSlots)) {
        partitionFilters.add(new HdfsPartitionFilter(conjunct, tbl_, analyzer));
        filterConjuncts.add(conjunct);
      }
    }
    // filterConjuncts are applied implicitly via partition pruning
    conjuncts_.removeAll(filterConjuncts);

    int partitionsCount = tbl_.getPartitions().size();
    // Map of partition ids to partition objects
    HashMap<Long, HdfsPartition> partitionMap =
        new HashMap<Long, HdfsPartition>(partitionsCount);
    // Set of valid partition ids. A partition is considered 'valid' if it
    // passes all the partition filters. Initially, all the partitions are
    // considered 'valid'.
    HashSet<Long> validPartitionIds = new HashSet<Long>();
    // TODO: To avoid generating too many objects, the first filter should
    // iterate directly over the tbl_.getPartitions() and then store the ids that
    // pass in validPartitionIds.
    for (HdfsPartition p: tbl_.getPartitions()) {
      // ignore partitions without data
      if (p.getFileDescriptors().size() == 0) continue;

      Preconditions.checkState(
          p.getPartitionValues().size() == tbl_.getNumClusteringCols());

      partitionMap.put(p.getId(), p);
      validPartitionIds.add(p.getId());
    }

    // Set of partition ids that pass a filter.
    HashSet<Long> matchingIds = new HashSet<Long>();
    // Batch of partitions
    ArrayList<HdfsPartition> partitionBatch = new ArrayList<HdfsPartition>();
    // Identify the partitions that pass all filters.
    for (HdfsPartitionFilter filter: partitionFilters) {
      // Iterate through the currently valid partitions
      for (Long id: validPartitionIds) {
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
      validPartitionIds.retainAll(matchingIds);
      matchingIds.clear();
    }

    // Populate the list of valid partitions to process
    for (Long id: validPartitionIds) {
      partitions_.add(partitionMap.get(id));
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
          cardinality_ += p.getNumRows();
          hasValidPartitionCardinality = true;
        }
        totalBytes_ += p.getSize();
      }
      // if none of the partitions knew its number of rows, we fall back on
      // the table stats
      if (!hasValidPartitionCardinality) cardinality_ = tbl_.getNumRows();
    }

    Preconditions.checkState(cardinality_ >= 0 || cardinality_ == -1);
    if (cardinality_ > 0) {
      LOG.debug("cardinality_=" + Long.toString(cardinality_) +
                " sel=" + Double.toString(computeSelectivity()));
      cardinality_ = Math.round((double) cardinality_ * computeSelectivity());
    }
    cardinality_ = capAtLimit(cardinality_);
    LOG.debug("computeStats HdfsScan: cardinality_=" + Long.toString(cardinality_));

    // TODO: take actual partitions into account
    numNodes_ = tbl_.getNumNodes();
    LOG.debug("computeStats HdfsScan: #nodes=" + Integer.toString(numNodes_));
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    // TODO: retire this once the migration to the new plan is complete
    msg.hdfs_scan_node = new THdfsScanNode(desc_.getId().asInt());
    msg.node_type = TPlanNodeType.HDFS_SCAN_NODE;
  }

  /**
   * Return scan ranges (hdfs splits) plus their storage locations, including volume
   * ids.
   */
  @Override
  public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
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

          // Look up the network addresses of all hosts (datanodes) that contain
          // replicas of this block.
          List<TNetworkAddress> blockNetworkAddresses =
              new ArrayList<TNetworkAddress>(replicaHostIdxs.size());
          for (Integer replicaHostId: replicaHostIdxs) {
            TNetworkAddress blockNetworkAddress =
                partition.getTable().getNetworkAddressByIdx(replicaHostId);
            Preconditions.checkNotNull(blockNetworkAddress);
            blockNetworkAddresses.add(blockNetworkAddress);
          }

          // record host/ports and volume ids
          Preconditions.checkState(blockNetworkAddresses.size() > 0);
          List<TScanRangeLocation> locations = Lists.newArrayList();
          for (int i = 0; i < blockNetworkAddresses.size(); ++i) {
            TScanRangeLocation location = new TScanRangeLocation();
            location.setServer(blockNetworkAddresses.get(i));
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
                new Path(partition.getLocation(), fileDesc.getFileName()).toString(),
                currentOffset, currentLength, partition.getId(),
                fileDesc.getFileLength()));
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
    return scanRanges_;
  }

  @Override
  protected String getNodeExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    HdfsTable table = (HdfsTable) desc_.getTable();
    String aliasStr = "";
    if (!table.getFullName().equalsIgnoreCase(desc_.getAlias()) &&
        !table.getName().equalsIgnoreCase(desc_.getAlias())) {
      aliasStr = " " + desc_.getAlias();
    }
    output.append(String.format("%s%s:%s [%s%s", prefix, id_.toString(),
        displayName_, table.getFullName(), aliasStr));
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
      if (compactData_) {
        output.append(" compact\n");
      } else {
        output.append("\n");
      }
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
