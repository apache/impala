// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import java.util.List;

import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.thrift.TScanRange;
import com.google.common.base.Objects;

/**
 * Representation of the common elements of all scan nodes.
 */
abstract public class ScanNode extends PlanNode {
  protected final TupleDescriptor desc;

  /**
   * One range per clustering column. The range bounds are expected to be constants.
   * A null entry means there's no range restriction for that particular key.
   * Might contain fewer entries than there are keys (ie, there are no trailing
   * null entries).
   */
  protected List<ValueRange> keyRanges;

  public ScanNode(TupleDescriptor desc) {
    super(desc.getId().asList());
    this.desc = desc;
  }

  public void setKeyRanges(List<ValueRange> keyRanges) {
    if (!keyRanges.isEmpty()) {
      this.keyRanges = keyRanges;
    }
  }

  /**
   * Returns one TScanRange per partition of the scan. If 'hosts' is non-null and there
   * are multiple partitions, also returns hosts on which scan partitions are executed,
   * one per range.
   * @param numPartitions number of scan partitions; same semantics as
   *     TQueryRequest.numNodes; must be >= 1 or one of these special values:
   *     NUM_NODES_ALL: as many partitions as there are nodes that contain relevant data
   *     NUM_NODES_ALL_RACKS: as many partitions as there are racks that holds relevant
   *     data
   * @param scanRanges output parameter
   * @param "host:port" strings (output parameter)
   */
  abstract public void getScanParams(
      int numPartitions, List<TScanRange> scanRanges, List<String> hostports);

  @Override
  protected String debugString() {
    return Objects.toStringHelper(this)
        .add("tid", desc.getId().asInt())
        .add("tblName", desc.getTable().getFullName())
        .add("keyRanges", "")
        .addValue(super.debugString())
        .toString();
  }

}
