// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
      partitionKeyRegex = "^file:/.*/";
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
      int numPartitions, List<TScanRange> scanRanges, List<String> hosts) {
    if (numPartitions == 1) {
      TScanRange scanRange = new TScanRange(id);
      Preconditions.checkState(filePaths.size() == fileLengths.size());
      for (int i = 0; i < filePaths.size(); ++i) {
        scanRange.addToHdfsFileSplits(
            new THdfsFileSplit(filePaths.get(i), 0, fileLengths.get(i)));
      }
      scanRanges.add(scanRange);
      return;
    }

    // map from host name to list of (file path, block location)
    Map<String, List<Pair<String, BlockLocation>>> locationMap = Maps.newHashMap();
    List<Pair<String, BlockLocation>> blockLocations =
        HdfsTable.getBlockLocations(filePaths);
    // use pseudo-random number generator to get repeatable test results
    Random rand = new Random(0);
    for (Pair<String, BlockLocation> location: blockLocations) {
      // pick random host for this block
      String[] blockHosts = null;
      try {
        blockHosts = location.second.getHosts();
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
      // TODO: implement
      // pick numPartitions hosts with the most blocks, then assign
      // the other blocks to those
    }

    if (hosts != null) {
      hosts.clear();
    }
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
      if (hosts != null) {
        hosts.add(entry.getKey());
      }
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
