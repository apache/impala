// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import com.cloudera.impala.thrift.TDataSink;
import com.cloudera.impala.thrift.TDataSink2;
import com.cloudera.impala.thrift.TDataSinkType;
import com.cloudera.impala.thrift.TDataStreamSink;
import com.cloudera.impala.thrift.TDataStreamSink2;
import com.cloudera.impala.thrift.TExplainLevel;

/**
 * Data sink that forwards data to an exchange node.
 *
 */
public class DataStreamSink extends DataSink {
  private final PlanNodeId exchNodeId;
  private DataPartition outputPartition;

  public DataStreamSink(PlanNodeId exchNodeId) {
    this.exchNodeId = exchNodeId;
  }

  public void setPartition(DataPartition partition) {
    outputPartition = partition;
  }

  @Override
  public String getExplainString(String prefix, TExplainLevel explainLevel) {
    StringBuilder strBuilder = new StringBuilder();
    strBuilder.append(prefix + "STREAM DATA SINK\n");
    strBuilder.append(prefix + "  EXCHANGE ID: " + exchNodeId + "\n");
    if (outputPartition != null) {
      strBuilder.append(prefix + "  "
          + outputPartition.getExplainString(explainLevel) + "\n");
    }
    return strBuilder.toString();
  }

  @Override
  protected TDataSink toThrift() {
    TDataSink tdataSink = new TDataSink(TDataSinkType.DATA_STREAM_SINK);
    tdataSink.setDataStreamSink(new TDataStreamSink(exchNodeId.asInt()));
    return tdataSink;
  }

  @Override
  protected TDataSink2 toThrift2() {
    TDataSink2 result = new TDataSink2(TDataSinkType.DATA_STREAM_SINK);
    TDataStreamSink2 tStreamSink = new TDataStreamSink2(exchNodeId.asInt());
    tStreamSink.setOutput_partition(outputPartition.toThrift());
    result.setStream_sink(tStreamSink);
    return result;
  }
}
