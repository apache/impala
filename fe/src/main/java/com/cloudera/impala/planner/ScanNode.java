// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import java.util.List;

import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.thrift.THostPort;
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

  public ScanNode(int id, TupleDescriptor desc) {
    super(id, desc.getId().asList());
    this.desc = desc;
  }

  public void setKeyRanges(List<ValueRange> keyRanges) {
    if (!keyRanges.isEmpty()) {
      this.keyRanges = keyRanges;
    }
  }

  /**
   * Returns one TScanRange per partition of the scan. If 'hostports' is non-null and
   * there are multiple partitions, also returns locations on which scan ranges are
   * located, one per range.
   * @param maxScanRangeLength the maximum number of bytes each scan range should scan;
   *     only applicable to HDFS; less than or equal to zero means no maximum
   * @param numPartitions number of scan partitions; same semantics as
   *     TClientRequest.numNodes; must be >= 1 or one of these special values:
   *     NUM_NODES_ALL: as many partitions as there are nodes that contain relevant data
   *     NUM_NODES_ALL_RACKS: as many partitions as there are racks that holds relevant
   *     data
   * @param scanRanges output parameter
   * @param dataLocations output parameter
   */
  abstract public void getScanParams(long maxScanRangeLength,
      int numPartitions, List<TScanRange> scanRanges, List<THostPort> dataLocations);

  @Override
  protected String debugString() {
    return Objects.toStringHelper(this)
        .add("tid", desc.getId().asInt())
        .add("tblName", desc.getTable().getFullName())
        .add("keyRanges", "")
        .addValue(super.debugString())
        .toString();
  }

  /**
   * Helper function to parse a "host:port" address string into THostPort
   * This is called with ipaddress:port when doing scan range assigment.  
   */
  protected static THostPort addressToTHostPort(String address) {
    THostPort result = new THostPort();
    String[] hostPort = address.split(":");
    // In this context we don't have or need a hostname,
    // so we just set it to the ipaddress.
    result.hostname = hostPort[0];
    result.ipaddress = hostPort[0];
    result.port = Integer.parseInt(hostPort[1]);
    return result;
  }

}
