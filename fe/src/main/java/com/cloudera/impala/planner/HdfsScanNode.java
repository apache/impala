// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import org.apache.hadoop.fs.BlockLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.catalog.HdfsPartition;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.thrift.Constants;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.THdfsFileSplit;
import com.cloudera.impala.thrift.THdfsFileSplit2;
import com.cloudera.impala.thrift.THdfsScanNode;
import com.cloudera.impala.thrift.THostPort;
import com.cloudera.impala.thrift.TPlanNode;
import com.cloudera.impala.thrift.TPlanNodeType;
import com.cloudera.impala.thrift.TScanRange;
import com.cloudera.impala.thrift.TScanRange2;
import com.cloudera.impala.thrift.TScanRangeLocation;
import com.cloudera.impala.thrift.TScanRangeLocations;
import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Scan of a single single table. Currently limited to full-table scans.
 * TODO: pass in range restrictions.
 */
public class HdfsScanNode extends ScanNode {
  private final static Logger LOG = LoggerFactory.getLogger(HdfsScanNode.class);

  private final HdfsTable tbl;

  // Partitions that are filtered in for scanning by the key ranges
  private final ArrayList<HdfsPartition> partitions = Lists.newArrayList();

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
    for (HdfsPartition p: tbl.getPartitions()) {
      if (p.getFileDescriptors().size() == 0) {
        // No point scanning partitions that have no data
        continue;
      }

      Preconditions.checkState(p.getPartitionValues().size() ==
        tbl.getNumClusteringCols());
      if (keyRanges != null) {
        // check partition key values against key ranges, if set
        Preconditions.checkState(keyRanges.size() <= p.getPartitionValues().size());
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
      }
      // HdfsPartition is immutable, so it's ok to copy by reference
      partitions.add(p);
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
    List<HdfsTable.BlockMetadata> blockMetadata = HdfsTable.getBlockMetadata(partitions);
    for (HdfsTable.BlockMetadata block: blockMetadata) {
      // collect all locations for block
      String[] blockHostPorts = null;
      try {
        // Use getNames() to get port number as well
        blockHostPorts = block.getLocation().getNames();
        // uncomment if you need to see detailed block locations
        //LOG.info(Arrays.toString(blockHostPorts));
      } catch (IOException e) {
        // this shouldn't happen, getHosts() doesn't throw anything
        String errorMsg = "BlockLocation.getHosts() failed:\n" + e.getMessage();
        LOG.error(errorMsg);
        throw new IllegalStateException(errorMsg);
      }

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
        location.setVolume_id(block.getVolumeId(i));
        locations.add(location);
      }

      // create scan ranges, taking into account maxScanRangeLength
      BlockLocation blockLocation = block.getLocation();
      long currentOffset = blockLocation.getOffset();
      long remainingLength = blockLocation.getLength();
      while (remainingLength > 0) {
        long currentLength = remainingLength;
        if (maxScanRangeLength > 0 && remainingLength > maxScanRangeLength) {
          currentLength = maxScanRangeLength;
        }
        TScanRange2 scanRange = new TScanRange2();
        scanRange.hdfsFileSplit =
            new THdfsFileSplit2(block.getFileName(), currentOffset,
              currentLength, block.getPartition().getId());
        TScanRangeLocations scanRangeLocations = new TScanRangeLocations();
        scanRangeLocations.scan_range = scanRange;
        scanRangeLocations.locations = locations;
        result.add(scanRangeLocations);
        remainingLength -= currentLength;
        currentOffset += currentLength;
      }
    }
    return result;
  }

  /**
   * Block assignment data, including the total number of assigned bytes, for a single
   * host / port.
   */
  static private class HostBlockAssignment {
    private final THostPort address;
    private long assignedBytes;

    // list of (file path, block location)
    private final List<HdfsTable.BlockMetadata> blockMetadata;

    // We've chosen a particular datanode. blockHostPortIndex[i] is the index to the
    // chosen in blockMetadata[i].getLocation().getName().
    private final List<Integer> blockHostPortIndex;

    public long getAssignedBytes() { return assignedBytes; }

    public THostPort getAddress() {
      return address;
    }

    HostBlockAssignment(THostPort address) {
      this.address = address;
      this.assignedBytes = 0;
      this.blockMetadata = Lists.newArrayList();
      this.blockHostPortIndex = Lists.newArrayList();
    }

    public void addBlock(HdfsTable.BlockMetadata block, int hostPortIndex) {
      blockMetadata.add(block);
      blockHostPortIndex.add(new Integer(hostPortIndex));
      assignedBytes += block.getLocation().getLength();
    }

    public void add(HostBlockAssignment info) {
      blockMetadata.addAll(info.blockMetadata);
      blockHostPortIndex.addAll(info.blockHostPortIndex);
      assignedBytes += info.assignedBytes;
    }
  }

  /**
   * Given a target number of nodes, assigns all blocks in all active partitions to nodes
   * and returns a block assignment for each host.
   */
  private List<HostBlockAssignment> computeHostBlockAssignments(int numNodes) {
    // map from host to list of blocks assigned to that host
    Map<String, HostBlockAssignment> assignmentMap = Maps.newHashMap();

    List<HdfsTable.BlockMetadata> blockMetadata =
        HdfsTable.getBlockMetadata(partitions);

    for (HdfsTable.BlockMetadata block: blockMetadata) {
      String[] blockHostPorts = null;
      try {
        // Use getNames() to get port number as well
        blockHostPorts = block.getLocation().getNames();
        // uncomment if you need to see detailed block locations
        //LOG.info(Arrays.toString(blockHostPorts));
      } catch (IOException e) {
        // this shouldn't happen, getHosts() doesn't throw anything
        String errorMsg = "BlockLocation.getHosts() failed:\n" + e.getMessage();
        LOG.error(errorMsg);
        throw new IllegalStateException(errorMsg);
      }

      if (blockHostPorts.length == 0) {
        // TODO: can we log more diagnostic info to help the user fix this?
        LOG.error("skipping block with missing BlockLocation: " + block.toString());
        continue;
      }

      // greedy block assignment: find host with fewest assigned bytes
      Preconditions.checkState(blockHostPorts.length > 0);
      int chosenHostPortIndex = 0;
      HostBlockAssignment minHost = assignmentMap.get(blockHostPorts[0]);
      for (int i = 0; i < blockHostPorts.length; ++i) {
        String hostPort = blockHostPorts[i];
        if (assignmentMap.containsKey(hostPort)) {
          HostBlockAssignment info = assignmentMap.get(hostPort);
          if (minHost.getAssignedBytes() > info.getAssignedBytes()) {
            minHost = info;
            chosenHostPortIndex = i;
          }
        } else {
          // new host with 0 bytes so far
          THostPort addr = addressToTHostPort(hostPort);
          minHost = new HostBlockAssignment(addr);
          chosenHostPortIndex = i;
          assignmentMap.put(hostPort, minHost);
          break;
        }
      }
      minHost.addBlock(block, chosenHostPortIndex);
    }

    if (numNodes != Constants.NUM_NODES_ALL) {
      reassignBlocks(numNodes, assignmentMap);
    }

    return Lists.newArrayList(assignmentMap.values());
  }

  @Override
  public void getScanParams(long maxScanRangeLength,
      int numNodes, List<TScanRange> scanRanges, List<THostPort> hostPorts) {
    Preconditions.checkState(numNodes != Constants.NUM_NODES_ALL_RACKS);

    List<HostBlockAssignment> hostBlockAssignments =
      computeHostBlockAssignments(numNodes);

    if (partitions.size() > 0 && hostPorts != null) {
      hostPorts.clear();
    }

    LOG.info(hostBlockAssignments.toString());

    // Build a TScanRange for each host, with one file split per block range.
    for (HostBlockAssignment blockAssignment: hostBlockAssignments) {
      TScanRange scanRange = new TScanRange(id.asInt());
      for (int i = 0; i < blockAssignment.blockMetadata.size(); ++i) {
        HdfsTable.BlockMetadata metadata = blockAssignment.blockMetadata.get(i);
        int hostPortIndex = blockAssignment.blockHostPortIndex.get(i).intValue();
        int volumeId = metadata.getVolumeId(hostPortIndex);
        BlockLocation blockLocation = metadata.getLocation();
        long currentOffset = blockLocation.getOffset();
        long remainingLength = blockLocation.getLength();
        while (remainingLength > 0) {
          long currentLength = remainingLength;
          if (maxScanRangeLength > 0 && remainingLength > maxScanRangeLength) {
            currentLength = maxScanRangeLength;
          }
          THdfsFileSplit fileSplit =
              new THdfsFileSplit(metadata.getFileName(),
                  currentOffset,
                  currentLength,
                  metadata.getPartition().getId(),
                  volumeId);
          scanRange.addToHdfsFileSplits(fileSplit);
          remainingLength -= currentLength;
          currentOffset += currentLength;
        }
      }
      scanRanges.add(scanRange);
      if (hostPorts != null) {
        hostPorts.add(blockAssignment.getAddress());
      }
    }

    if (hostPorts != null) {
      LOG.info(hostPorts.toString());
    }
  }

  private static final Comparator<Entry<String, HostBlockAssignment>>
      MAX_BYTES_COMPARATOR =
      new Comparator<Entry<String, HostBlockAssignment>>() {
    public int compare(
        Entry<String, HostBlockAssignment> entry1,
        Entry<String, HostBlockAssignment> entry2) {
      long assignedBytes1 = entry1.getValue().getAssignedBytes();
      long assignedBytes2 = entry2.getValue().getAssignedBytes();
      if (assignedBytes1 < assignedBytes2) {
        return -1;
      } else if (assignedBytes1 > assignedBytes2) {
        return 1;
      } else {
        return 0;
      }
    }
  };

  private static final Comparator<Entry<String, HostBlockAssignment>>
      MIN_BYTES_COMPARATOR =
      new Comparator<Entry<String, HostBlockAssignment>>() {
    public int compare(
        Entry<String, HostBlockAssignment> entry1,
        Entry<String, HostBlockAssignment> entry2) {
      long assignedBytes1 = entry1.getValue().getAssignedBytes();
      long assignedBytes2 = entry2.getValue().getAssignedBytes();

      if (assignedBytes2 < assignedBytes1) {
        return -1;
      } else if (assignedBytes2 > assignedBytes1) {
        return 1;
      } else {
        return 0;
      }
    }
  };

  /**
   * Pick numPartitions hosts with the most assigned bytes, then assign
   * the other blocks to those, trying to even out total # of bytes assigned to nodes
   * locationMap: map from host to list of (file path, block location)
   * TODO: reassign to optimize block locality (this will conflict with assignment
   * based on data size, so we need to figure out what a good compromise is;
   * this is probably a fruitful area for further experimentation)
   */
  private void reassignBlocks(
      int numPartitions, Map<String, HostBlockAssignment> locationMap) {
    if (locationMap.isEmpty()) {
      return;
    }
    // create a priority queue of map entries, ordered by decreasing number of
    // assigned bytes
    PriorityQueue<Map.Entry<String, HostBlockAssignment>> maxBytesQueue =
        new PriorityQueue<Map.Entry<String, HostBlockAssignment>>(
          locationMap.size(), MAX_BYTES_COMPARATOR);
    for (Map.Entry<String, HostBlockAssignment> entry: locationMap.entrySet()) {
      maxBytesQueue.add(entry);
    }

    // pull out the top 'numPartitions' elements and insert them into a queue
    // that orders by increasing number of assigned bytes
    PriorityQueue<Map.Entry<String, HostBlockAssignment>> minBytesQueue =
        new PriorityQueue<Map.Entry<String, HostBlockAssignment>>(
          locationMap.size(), MIN_BYTES_COMPARATOR);

    for (int i = 0; i < numPartitions; ++i) {
      Map.Entry<String, HostBlockAssignment> entry = maxBytesQueue.poll();
      if (entry == null) {
        break;
      }
      minBytesQueue.add(entry);
    }

    // assign the remaining hosts' blocks
    // TODO: spread these round-robin
    while (!maxBytesQueue.isEmpty()) {
      Map.Entry<String, HostBlockAssignment> source = maxBytesQueue.poll();
      Map.Entry<String, HostBlockAssignment> dest = minBytesQueue.poll();
      dest.getValue().add(source.getValue());
      minBytesQueue.add(dest);
    }

    // re-create locationMap from minBytesQueue
    locationMap.clear();
    while (true) {
      Map.Entry<String, HostBlockAssignment> entry = minBytesQueue.poll();
      if (entry == null) {
        break;
      }
      locationMap.put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  protected String getExplainString(String prefix, TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    output.append(prefix + "SCAN HDFS table=" + desc.getTable().getFullName());
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
}
