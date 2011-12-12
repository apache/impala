// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import com.cloudera.impala.thrift.TDataSink;
import com.cloudera.impala.thrift.TDataSinkType;
import com.cloudera.impala.thrift.TStreamDataSink;

/**
 * Data sink that forwards data to an exchange node.
 *
 */
public class StreamDataSink extends DataSink {
  private final int exchNodeId;

  public StreamDataSink(int exchNodeId) {
    this.exchNodeId = exchNodeId;
  }

  @Override
  public String getExplainString() {
    StringBuilder strBuilder = new StringBuilder();
    strBuilder.append("STREAM DATA SINK\n");
    strBuilder.append("  EXCHANGE ID: " + exchNodeId + "\n");
    return strBuilder.toString();
  }

  @Override
  protected TDataSink toThrift() {
    TDataSink tdataSink = new TDataSink(TDataSinkType.STREAM_DATA_SINK);
    tdataSink.setStream_data_sink(new TStreamDataSink(exchNodeId));
    return tdataSink;
  }
}
