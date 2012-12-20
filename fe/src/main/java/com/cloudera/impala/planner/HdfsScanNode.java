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
import com.cloudera.impala.catalog.HdfsFileFormat;
import com.cloudera.impala.catalog.HdfsPartition;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.NotImplementedException;
import com.cloudera.impala.thrift.TExplainLevel;
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

  // Total number of bytes from partitions
  private long totalBytes = 0;

  /**
   * Constructs node to scan given data files of table 'tbl'.
   */
  public HdfsScanNode(PlanNodeId id, TupleDescriptor desc, HdfsTable tbl) {
    super(id, desc);
    this.tbl = tbl;
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
   */
  @Override
  public void finalize(Analyzer analyzer) throws InternalException {
    Preconditions.checkNotNull(keyRanges);

    LOG.info("collecting partitions for table " + tbl.getName());
    for (HdfsPartition p: tbl.getPartitions()) {
      if (p.getFileDescriptors().size() == 0) {
        // No point scanning partitions that have no data
        continue;
      }

      Preconditions.checkState(p.getPartitionValues().size() ==
        tbl.getNumClusteringCols());
      // check partition key values against key ranges, if set
      Preconditions.checkState(keyRanges.size() == p.getPartitionValues().size());
      boolean matchingPartition = true;
      for (int i = 0; i < keyRanges.size(); ++i) {
        ValueRange keyRange = keyRanges.get(i);
        if (keyRange != null &&
            !keyRange.isInRange(analyzer, p.getPartitionValues().get(i))) {
          matchingPartition = false;
          break;
        }
      }
      if (!matchingPartition) {
        // skip this partition, it's outside the key ranges
        continue;
      }
      // HdfsPartition is immutable, so it's ok to copy by reference
      partitions.add(p);

      totalBytes += p.getSize();
    }
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
    List<HdfsTable.PartitionBlockMetadata> partitionBlockMd =
        HdfsTable.getBlockMetadata(partitions);
    for (HdfsTable.PartitionBlockMetadata partition: partitionBlockMd) {
      for (HdfsTable.BlockMetadata block: partition.getBlockMetadata()) {
        String[] blockHostPorts = block.getHostPorts();
        if (blockHostPorts.length == 0) {
          // we didn't get locations for this block; for now, just ignore the block
          // TODO: do something meaningful with that
          continue;
        }

        // record host/ports and volume ids
        Preconditions.checkState(blockHostPorts.length > 0);
        List<TScanRangeLocation> locations = Lists.newArrayList();
        for (int i = 0; i < blockHostPorts.length; ++i) {
          TScanRangeLocation location = new TScanRangeLocation();
          String hostPort = blockHostPorts[i];
          location.setServer(addressToTHostPort(hostPort));
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
                currentLength, partition.getPartition().getId()));
          TScanRangeLocations scanRangeLocations = new TScanRangeLocations();
          scanRangeLocations.scan_range = scanRange;
          scanRangeLocations.locations = locations;
          result.add(scanRangeLocations);
          remainingLength -= currentLength;
          currentOffset += currentLength;
        }
      }
    }
    return result;
  }

  @Override
  protected String getExplainString(String prefix, TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    output.append(prefix + "SCAN HDFS table=" + desc.getTable().getFullName());
    output.append(" #partitions=" + partitions.size());
    output.append(" size=" + printBytes(totalBytes));
    output.append(" (" + id + ")");
    if (compactData) {
      output.append(" compact\n");
    } else {
      output.append("\n");
    }
    if (!conjuncts.isEmpty()) {
      output.append(prefix + "  PREDICATES: " + getExplainString(conjuncts) + "\n");
    }
    output.append(super.getExplainString(prefix + "  ", detailLevel));
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
   * Raise NotImplementedException if any of the partitions has unsupported file format
   * (RC or Trevni).
   * Can only be called after finalize().
   */
  public void validateFileFormat() throws NotImplementedException {
    for (HdfsPartition partition :partitions) {
      HdfsFileFormat format = partition.getInputFormatDescriptor().getFileFormat();
      if (format == HdfsFileFormat.TREVNI) {
        StringBuilder error = new StringBuilder();
        error.append("Table ").append(desc.getTable().getFullName())
          .append(" has unsupported format ").append(format.name());
        throw new NotImplementedException(error.toString());
      }
    }
  }
}
