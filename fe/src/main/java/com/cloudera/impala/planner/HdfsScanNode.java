// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;

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
  public HdfsScanNode(TupleDescriptor desc, HdfsTable tbl) {
    super(desc);
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

  @Override
  public void getScanParams(
      int numPartitions, List<TScanRange> scanRanges, List<String> hostports) {
    Preconditions.checkState(numPartitions != Constants.NUM_NODES_ALL_RACKS);

    // map from "host:port" string to list of (file path, block location)
    Map<String, List<Pair<String, BlockLocation>>> locationMap = Maps.newHashMap();
    List<Pair<String, BlockLocation>> blockLocations =
        HdfsTable.getBlockLocations(filePaths);
    // use pseudo-random number generator to get repeatable test results
    Random rand = new Random(0);
    for (Pair<String, BlockLocation> location: blockLocations) {
      // pick random host for this block
      String[] blockHosts = null;
      try {
        blockHosts = location.second.getNames();
        LOG.info(Arrays.toString(blockHosts));
      } catch (IOException e) {
        // this shouldn't happen, getHosts() doesn't throw anything
        String errorMsg = "BlockLocation.getHosts() failed:\n" + e.getMessage();
        LOG.error(errorMsg);
        throw new IllegalStateException(errorMsg);
      }
      String host = blockHosts[rand.nextInt(blockHosts.length)];
      if (locationMap.containsKey(host)) {
        locationMap.get(host).add(location);
      } else {
        locationMap.put(host, Lists.newArrayList(location));
      }
    }

    if (numPartitions != Constants.NUM_NODES_ALL) {
      reassignBlocks(numPartitions, locationMap);
    }

    if (hostports != null) {
      hostports.clear();
    }
    LOG.info(locationMap.toString());
    for (Map.Entry<String, List<Pair<String, BlockLocation>>> entry:
        locationMap.entrySet()) {
      TScanRange scanRange = new TScanRange(id);
      for (Pair<String, BlockLocation> location: entry.getValue()) {
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
    if (hostports != null) LOG.info(hostports.toString());
  }

  // Pick numPartitions hosts with the most blocks, then assign
  // the other blocks to those.
  // locationMap: map from "host:port" string to list of (file path, block location)
  // TODO: reassign blocks to even out total # of bytes assigned to nodes
  // TODO: reassign to optimize block locality (this will conflict with assignment
  // based on data size, so we need to figure out what a good compromise is;
  // this is probably a fruitful area for further experimentation)
  private void reassignBlocks(
      int numPartitions,
      Map<String, List<Pair<String, BlockLocation>>> locationMap) {
    if (locationMap.isEmpty()) return;
    // create a priority queue of map entries, ordered by # of blocks
    PriorityQueue<Map.Entry<String, List<Pair<String, BlockLocation>>>> maxCountQueue =
        new PriorityQueue<Map.Entry<String, List<Pair<String, BlockLocation>>>>(
          locationMap.size(),
          new Comparator<Map.Entry<String, List<Pair<String, BlockLocation>>>>() {
            public int compare(
                Map.Entry<String, List<Pair<String, BlockLocation>>> entry1,
                Map.Entry<String, List<Pair<String, BlockLocation>>> entry2) {
              return entry1.getValue().size() - entry2.getValue().size();
            }
          });
    for (Map.Entry<String, List<Pair<String, BlockLocation>>> entry:
        locationMap.entrySet()) {
      maxCountQueue.add(entry);
    }

    // pull out the top 'numPartitions' elements and insert them into a queue
    // that orders by increasing number of blocks
    PriorityQueue<Map.Entry<String, List<Pair<String, BlockLocation>>>> minCountQueue =
        new PriorityQueue<Map.Entry<String, List<Pair<String, BlockLocation>>>>(
          locationMap.size(),
          new Comparator<Map.Entry<String, List<Pair<String, BlockLocation>>>>() {
            public int compare(
                Map.Entry<String, List<Pair<String, BlockLocation>>> entry1,
                Map.Entry<String, List<Pair<String, BlockLocation>>> entry2) {
              return entry2.getValue().size() - entry1.getValue().size();
            }
          });
    for (int i = 0; i < numPartitions; ++i) {
      Map.Entry<String, List<Pair<String, BlockLocation>>> entry = maxCountQueue.poll();
      if (entry == null) break;
      minCountQueue.add(entry);
    }

    // assign the remaining hosts' blocks
    // TODO: spread these round-robin
    while (!maxCountQueue.isEmpty()) {
      Map.Entry<String, List<Pair<String, BlockLocation>>> source =
          maxCountQueue.poll();
      Map.Entry<String, List<Pair<String, BlockLocation>>> dest = minCountQueue.poll();
      dest.getValue().addAll(source.getValue());
      minCountQueue.add(dest);
    }

    // re-create locationMap from minCountQueue
    locationMap.clear();
    while (true) {
      Map.Entry<String, List<Pair<String, BlockLocation>>> entry = minCountQueue.poll();
      if (entry == null) {
        break;
      }
      locationMap.put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  protected String getExplainString(String prefix) {
    StringBuilder output = new StringBuilder();
    output.append(prefix + "SCAN HDFS table=" + desc.getTable().getFullName() + "\n");
    if (!conjuncts.isEmpty()) {
      output.append(prefix + "  PREDICATES: " + getExplainString(conjuncts) + "\n");
    }
    if (partitionKeyRegex != "") {
      output.append(prefix + "  REGEX: " + partitionKeyRegex + "\n");
    }
    output.append(prefix + "  FILES:");
    if (!filePaths.isEmpty()) {
      output.append("\n    " + prefix);
      output.append(Joiner.on("\n    " + prefix).join(filePaths));
    }
    output.append("\n" + super.getExplainString(prefix));
    return output.toString();
  }
}
