// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.hadoop.fs.BlockLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.Pair;
import com.cloudera.impala.thrift.Constants;
import com.cloudera.impala.thrift.THdfsFileSplit;
import com.cloudera.impala.thrift.THdfsScanNode;
import com.cloudera.impala.thrift.TPlanNode;
import com.cloudera.impala.thrift.TScanRange;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Scan of a single single table. Currently limited to full-table scans.
 * TODO: pass in range restrictions.
 */
public abstract class HdfsScanNode extends ScanNode {
  private final static Logger LOG = LoggerFactory.getLogger(HdfsScanNode.class);

  private final HdfsTable tbl;
  private ArrayList<String> filePaths;  // data files to scan
  private ArrayList<Long> fileLengths;  // corresp. to filePaths

  // Regex that will be evaluated over filenames to generate partition key values
  private String partitionKeyRegex;

  /**
   * Constructs node to scan given data files of table 'tbl'.
   */
  public HdfsScanNode(int id, TupleDescriptor desc, HdfsTable tbl) {
    super(id, desc);
    this.tbl = tbl;
  }

  @Override
  protected String debugString() {
    return Objects.toStringHelper(this)
        .add("filePaths", Joiner.on(", ").join(filePaths))
        .addValue(super.debugString())
        .toString();
  }

  /**
   * Compute file paths and key values based on key ranges.
   */
  @Override  public void finalize(Analyzer analyzer) throws InternalException {
    filePaths = Lists.newArrayList();
    fileLengths = Lists.newArrayList();

    for (HdfsTable.Partition p: tbl.getPartitions()) {
      Preconditions.checkState(p.keyValues.size() == tbl.getNumClusteringCols());
      if (keyRanges != null) {
        // check partition key values against key ranges, if set
        Preconditions.checkState(keyRanges.size() <= p.keyValues.size());
        boolean matchingPartition = true;
        for (int i = 0; i < keyRanges.size(); ++i) {
          ValueRange keyRange = keyRanges.get(i);
          if (keyRange != null && !keyRange.isInRange(analyzer, p.keyValues.get(i))) {
            matchingPartition = false;
            break;
          }
        }
        if (!matchingPartition) {
          // skip this partition, it's outside the key ranges
          continue;
        }
      }

      filePaths.addAll(p.filePaths);
      fileLengths.addAll(p.fileLengths);
    }

    if (tbl.getNumClusteringCols() > 0) {
      partitionKeyRegex = ".*/";
      for (int i = 0; i < tbl.getNumClusteringCols(); ++i) {
        Column col = tbl.getColumns().get(i);
        partitionKeyRegex += col.getName() + "=([^\\/]*)/";
      }
    } else {
      partitionKeyRegex = "";
    }
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.hdfs_scan_node = new THdfsScanNode(desc.getId().asInt(), partitionKeyRegex);
  }

  // block assignment data for a single host
  static private class HostBlockInfo {
    public String hostname;

    public long assignedBytes;

    // list of (file path, block location)
    public List<Pair<String, BlockLocation>> blockLocations;

    HostBlockInfo(String hostname) {
      this.hostname = hostname;
      this.assignedBytes = 0;
      this.blockLocations = Lists.newArrayList();
    }

    public void AddLocation(Pair<String, BlockLocation> location) {
      blockLocations.add(location);
      assignedBytes += location.second.getLength();
    }

    public void Add(HostBlockInfo info) {
      blockLocations.addAll(info.blockLocations);
      assignedBytes += info.assignedBytes;
    }
  }

  @Override
  public void getScanParams(
      int numPartitions, List<TScanRange> scanRanges, List<String> hostports) {
    Preconditions.checkState(numPartitions != Constants.NUM_NODES_ALL_RACKS);

    // map from host to list of (file path, block location)
    Map<String, HostBlockInfo> locationMap = Maps.newHashMap();
    List<Pair<String, BlockLocation>> blockLocations =
        HdfsTable.getBlockLocations(filePaths);
    for (Pair<String, BlockLocation> location: blockLocations) {
      String[] blockHosts = null;
      try {
        blockHosts = location.second.getHosts();
        LOG.info(Arrays.toString(blockHosts));
      } catch (IOException e) {
        // this shouldn't happen, getHosts() doesn't throw anything
        String errorMsg = "BlockLocation.getHosts() failed:\n" + e.getMessage();
        LOG.error(errorMsg);
        throw new IllegalStateException(errorMsg);
      }

      // greedy block assignment: find host with fewest assigned bytes
      Preconditions.checkState(blockHosts.length > 0);
      HostBlockInfo minHost = locationMap.get(blockHosts[0]);
      for (String host: blockHosts) {
        if (locationMap.containsKey(host)) {
          HostBlockInfo info = locationMap.get(host);
          if (minHost.assignedBytes > info.assignedBytes) {
            minHost = info;
          }
        } else {
          // new host with 0 bytes so far
          minHost = new HostBlockInfo(host);
          locationMap.put(host, minHost);
          break;
        }
      }
      minHost.AddLocation(location);
    }

    if (numPartitions != Constants.NUM_NODES_ALL) {
      reassignBlocks(numPartitions, locationMap);
    }

    if (hostports != null) {
      hostports.clear();
    }
    LOG.info(locationMap.toString());
    for (Map.Entry<String, HostBlockInfo> entry: locationMap.entrySet()) {
      TScanRange scanRange = new TScanRange(id);
      for (Pair<String, BlockLocation> location: entry.getValue().blockLocations) {
        BlockLocation blockLocation = location.second;
        scanRange.addToHdfsFileSplits(
            new THdfsFileSplit(location.first, blockLocation.getOffset(),
                               blockLocation.getLength()));
      }
      scanRanges.add(scanRange);
      if (hostports != null) {
        hostports.add(entry.getKey());
      }
    }
    if (hostports != null) {
      LOG.info(hostports.toString());
    }
  }

  // Pick numPartitions hosts with the most assigned bytes, then assign
  // the other blocks to those, trying to even out total # of bytes assigned to nodes
  // locationMap: map from host to list of (file path, block location)
  // TODO: reassign to optimize block locality (this will conflict with assignment
  // based on data size, so we need to figure out what a good compromise is;
  // this is probably a fruitful area for further experimentation)
  private void reassignBlocks(
      int numPartitions, Map<String, HostBlockInfo> locationMap) {
    if (locationMap.isEmpty()) {
      return;
    }
    // create a priority queue of map entries, ordered by decreasing number of
    // assigned bytes
    PriorityQueue<Map.Entry<String, HostBlockInfo>> maxBytesQueue =
        new PriorityQueue<Map.Entry<String, HostBlockInfo>>(
          locationMap.size(),
          new Comparator<Map.Entry<String, HostBlockInfo>>() {
            public int compare(
                Map.Entry<String, HostBlockInfo> entry1,
                Map.Entry<String, HostBlockInfo> entry2) {
              if (entry1.getValue().assignedBytes < entry2.getValue().assignedBytes) {
                return -1;
              } else if (entry1.getValue().assignedBytes
                  > entry2.getValue().assignedBytes) {
                return 1;
              } else {
                return 0;
              }
            }
          });
    for (Map.Entry<String, HostBlockInfo> entry: locationMap.entrySet()) {
      maxBytesQueue.add(entry);
    }

    // pull out the top 'numPartitions' elements and insert them into a queue
    // that orders by increasing number of assigned bytes
    PriorityQueue<Map.Entry<String, HostBlockInfo>> minBytesQueue =
        new PriorityQueue<Map.Entry<String, HostBlockInfo>>(
          locationMap.size(),
          new Comparator<Map.Entry<String, HostBlockInfo>>() {
            public int compare(
                Map.Entry<String, HostBlockInfo> entry1,
                Map.Entry<String, HostBlockInfo> entry2) {
              if (entry2.getValue().assignedBytes < entry1.getValue().assignedBytes) {
                return -1;
              } else if (entry2.getValue().assignedBytes
                  > entry1.getValue().assignedBytes) {
                return 1;
              } else {
                return 0;
              }
            }
          });
    for (int i = 0; i < numPartitions; ++i) {
      Map.Entry<String, HostBlockInfo> entry = maxBytesQueue.poll();
      if (entry == null) {
        break;
      }
      minBytesQueue.add(entry);
    }

    // assign the remaining hosts' blocks
    // TODO: spread these round-robin
    while (!maxBytesQueue.isEmpty()) {
      Map.Entry<String, HostBlockInfo> source = maxBytesQueue.poll();
      Map.Entry<String, HostBlockInfo> dest = minBytesQueue.poll();
      dest.getValue().Add(source.getValue());
      minBytesQueue.add(dest);
    }

    // re-create locationMap from minBytesQueue
    locationMap.clear();
    while (true) {
      Map.Entry<String, HostBlockInfo> entry = minBytesQueue.poll();
      if (entry == null) {
        break;
      }
      locationMap.put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  protected String getExplainString(String prefix) {
    StringBuilder output = new StringBuilder();
    output.append(prefix + "SCAN HDFS table=" + desc.getTable().getFullName());
    output.append(" (" + id + ")\n");
    if (!conjuncts.isEmpty()) {
      output.append(prefix + "  PREDICATES: " + getExplainString(conjuncts) + "\n");
    }
    if (partitionKeyRegex != "") {
      output.append(prefix + "  REGEX: " + partitionKeyRegex + "\n");
    }
    output.append(super.getExplainString(prefix));
    return output.toString();
  }
}
