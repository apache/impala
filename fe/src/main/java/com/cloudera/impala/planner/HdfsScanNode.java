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
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.catalog.HdfsPartition;
import com.cloudera.impala.catalog.HdfsPartition.FileBlock;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.NotImplementedException;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.THdfsFileBlock;
import com.cloudera.impala.thrift.THdfsFileSplit;
import com.cloudera.impala.thrift.THdfsScanNode;
import com.cloudera.impala.thrift.TPlanNode;
import com.cloudera.impala.thrift.TPlanNodeType;
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

  private final HdfsTable tbl;

  // Partitions that are filtered in for scanning by the key ranges
  private final ArrayList<HdfsPartition> partitions = Lists.newArrayList();

  private List<SingleColumnFilter> keyFilters;

  // Total number of bytes from partitions
  private long totalBytes = 0;

  /**
   * Constructs node to scan given data files of table 'tbl'.
   */
  public HdfsScanNode(PlanNodeId id, TupleDescriptor desc, HdfsTable tbl) {
    super(id, desc, "SCAN HDFS");
    this.tbl = tbl;
  }

  public void setKeyFilters(List<SingleColumnFilter> filters) {
    Preconditions.checkNotNull(filters);
    this.keyFilters = filters;
  }

  @Override
  protected String debugString() {
    ToStringHelper helper = Objects.toStringHelper(this);
    for (HdfsPartition partition: partitions) {
      helper.add("Partition " + partition.getId() + ":", partition.toString());
    }
    return helper.addValue(super.debugString()).toString();
  }

  /**
   * Compute file paths and key values based on key ranges.
   * This finalize() implementation also includes the computeStats() logic
   * (and there is no computeStats()), because it's easier to do that during
   * ValueRange construction.
   */
  @Override
  public void finalize(Analyzer analyzer) throws InternalException {
    Preconditions.checkNotNull(keyFilters);
    super.finalize(analyzer);

    LOG.info("collecting partitions for table " + tbl.getName());
    if (tbl.getPartitions().isEmpty()) {
      cardinality = tbl.getNumRows();
    } else {
      cardinality = 0;
      boolean hasValidPartitionCardinality = false;
      for (HdfsPartition p: tbl.getPartitions()) {
        if (p.getFileDescriptors().size() == 0) {
          // No point scanning partitions that have no data
          continue;
        }

        Preconditions.checkState(
            p.getPartitionValues().size() == tbl.getNumClusteringCols());
        // check partition key values against key ranges, if set
        Preconditions.checkState(keyFilters.size() == p.getPartitionValues().size());
        boolean matchingPartition = true;
        for (int i = 0; i < keyFilters.size(); ++i) {
          SingleColumnFilter keyFilter = keyFilters.get(i);
          if (keyFilter != null
              && !keyFilter.isTrue(analyzer, p.getPartitionValues().get(i))) {
            matchingPartition = false;
            break;
          }
        }
        if (!matchingPartition) {
          // skip this partition, it's outside the key filters
          continue;
        }
        // HdfsPartition is immutable, so it's ok to copy by reference
        partitions.add(p);

        // ignore partitions with missing stats in the hope they don't matter
        // enough to change the planning outcome
        if (p.getNumRows() > 0) {
          cardinality += p.getNumRows();
          hasValidPartitionCardinality = true;
        }
        totalBytes += p.getSize();
      }
      // if none of the partitions knew its number of rows, we fall back on
      // the table stats
      if (!hasValidPartitionCardinality) cardinality = tbl.getNumRows();
    }

    Preconditions.checkState(cardinality >= 0 || cardinality == -1);
    if (cardinality > 0) {
      LOG.info("cardinality=" + Long.toString(cardinality) + " sel=" + Double.toString(computeSelectivity()));
      cardinality = Math.round((double) cardinality * computeSelectivity());
    }
    LOG.info("finalize HdfsScan: cardinality=" + Long.toString(cardinality));

    // TODO: take actual partitions into account
    numNodes = tbl.getNumNodes();
    LOG.info("finalize HdfsScan: #nodes=" + Integer.toString(numNodes));
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    // TODO: retire this once the migration to the new plan is complete
    msg.hdfs_scan_node = new THdfsScanNode(desc.getId().asInt());
    msg.node_type = TPlanNodeType.HDFS_SCAN_NODE;
  }

  /**
   * Return scan ranges (hdfs splits) plus their storage locations, including volume
   * ids.
   */
  @Override
  public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
    List<TScanRangeLocations> result = Lists.newArrayList();
    for (HdfsPartition partition: partitions) {
      Preconditions.checkState(partition.getId() >= 0);
      for (HdfsPartition.FileDescriptor fileDesc: partition.getFileDescriptors()) {
        for (THdfsFileBlock thriftBlock: fileDesc.getFileBlocks()) {
          HdfsPartition.FileBlock block = FileBlock.fromThrift(thriftBlock);
          List<String> blockHostPorts = block.getHostPorts();
          if (blockHostPorts.size() == 0) {
            // we didn't get locations for this block; for now, just ignore the block
            // TODO: do something meaningful with that
            continue;
          }

          // record host/ports and volume ids
          Preconditions.checkState(blockHostPorts.size() > 0);
          List<TScanRangeLocation> locations = Lists.newArrayList();
          for (int i = 0; i < blockHostPorts.size(); ++i) {
            TScanRangeLocation location = new TScanRangeLocation();
            String hostPort = blockHostPorts.get(i);
            location.setServer(addressToTNetworkAddress(hostPort));
            location.setVolume_id(block.getDiskId(i));
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
            scanRange.setHdfs_file_split(
                new THdfsFileSplit(block.getFileName(), currentOffset,
                  currentLength, partition.getId(), block.getFileSize()));
            TScanRangeLocations scanRangeLocations = new TScanRangeLocations();
            scanRangeLocations.scan_range = scanRange;
            scanRangeLocations.locations = locations;
            result.add(scanRangeLocations);
            remainingLength -= currentLength;
            currentOffset += currentLength;
          }
        }
      }
    }
    return result;
  }

  @Override
  protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    output.append(prefix + "table=" + desc.getTable().getFullName());
    output.append(" #partitions=" + partitions.size());
    output.append(" size=" + printBytes(totalBytes));
    if (compactData) {
      output.append(" compact\n");
    } else {
      output.append("\n");
    }
    if (!conjuncts.isEmpty()) {
      output.append(prefix + "predicates: " + getExplainString(conjuncts) + "\n");
    }
    return output.toString();
  }

  /**
   * Return the number in TB, GB, MB, KB with 2 decimal points. For example 5000 will be
   * returned as 4.88KB.
   * @param num
   * @return
   */
  static private String printBytes(long value) {
    long kb = 1024;
    long mb = kb * 1024;
    long gb = mb * 1024;
    long tb = gb * 1024;

    double result = value;
    if (value > tb) {
      return String.format("%.2f", result / tb) + "TB";
    }
    if (value > gb) {
      return String.format("%.2f", result / gb) + "GB";
    }
    if (value > mb) {
      return String.format("%.2f", result / mb) + "MB";
    }
    if (value > kb) {
      return String.format("%.2f", result / kb) + "KB";
    }
    return value + "B";
  }

  /**
   * Raises NotImplementedException if any of the partitions uses an unsupported file
   * format.  This is useful for experimental formats, which we currently don't have.
   * Can only be called after finalize().
   */
  public void validateFileFormat() throws NotImplementedException {
  }

  @Override
  public void computeCosts() {
    // TODO: The total memory consumption for a particular query depends on the number
    // of *available* cores, i.e., it depends the resource consumption of other
    // concurrent queries. Figure out how to account for that.
    // TODO: A better way to estimate the costs might be to ask the BE IoMgr
    // to return an estimate.
    // TODO: The calculation below is a worst-case estimate that is temporarily
    // acceptable for testing resource management with hard memory limit enforcement.
    long numCores = Runtime.getRuntime().availableProcessors();
    // Impala starts up 3 scan ranges per core each using a default of 5 * 8MB buffers.
    perHostMemCost = numCores * 3L * 5L * 8L * 1024L * 1024L;
  }
}
