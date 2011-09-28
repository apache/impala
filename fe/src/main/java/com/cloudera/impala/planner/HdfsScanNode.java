// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import java.util.List;

import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.LiteralExpr;
import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.thrift.THdfsScanNode;
import com.cloudera.impala.thrift.TPlanNode;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Scan of a single single table. Currently limited to full-table scans.
 * TODO: pass in range restrictions.
 */
public abstract class HdfsScanNode extends ScanNode {
  private final HdfsTable tbl;
  private List<String> filePaths;  // data files to scan
  public List<LiteralExpr> keyValues; // partition key values per file

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
  @Override
  public void finalize(Analyzer analyzer) throws InternalException {
    filePaths = Lists.newArrayList();
    keyValues = Lists.newArrayList();
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
      keyValues.addAll(p.keyValues);
    }
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.hdfs_scan_node = new THdfsScanNode(desc.getId().asInt(), filePaths);
    if (!keyValues.isEmpty()) {
      msg.hdfs_scan_node.setKey_values(Expr.treesToThrift(keyValues));
    }
  }

  @Override
  protected String getExplainString(String prefix) {
    StringBuilder output = new StringBuilder();
    output.append(prefix + "SCAN HDFS table=" + desc.getTable().getFullName() + "\n");
    output.append(prefix + "  PREDICATES: " + getExplainString(conjuncts) + "\n");
    output.append(getLimitExplainString(prefix));
    output.append(prefix + "  FILES:");
    if (!filePaths.isEmpty()) {
      output.append("\n    " + prefix);
      output.append(Joiner.on("\n    " + prefix).join(filePaths));
    }
    return output.toString();
  }
}
