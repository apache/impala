// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import java.util.List;

import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.thrift.THostPort;
import com.cloudera.impala.thrift.TScanRangeLocations;
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

  public ScanNode(PlanNodeId id, TupleDescriptor desc) {
    super(id, desc.getId().asList());
    this.desc = desc;
  }

  public void setKeyRanges(List<ValueRange> keyRanges) {
    if (!keyRanges.isEmpty()) {
      this.keyRanges = keyRanges;
    }
  }

  /**
   * Returns all scan ranges plus their locations. Needs to be preceded by a call to
   * finalize().
   * @param maxScanRangeLength The maximum number of bytes each scan range should scan;
   *     only applicable to HDFS; less than or equal to zero means no maximum.
   */
  abstract public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength);

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
