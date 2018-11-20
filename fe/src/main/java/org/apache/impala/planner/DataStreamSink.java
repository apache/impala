// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.planner;

import org.apache.impala.thrift.TDataSink;
import org.apache.impala.thrift.TDataSinkType;
import org.apache.impala.thrift.TDataStreamSink;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TQueryOptions;

import com.google.common.base.Preconditions;

/**
 * Data sink that forwards data to an exchange node.
 */
public class DataStreamSink extends DataSink {
  private final ExchangeNode exchNode_;
  private final DataPartition outputPartition_;

  public DataStreamSink(ExchangeNode exchNode, DataPartition partition) {
    Preconditions.checkNotNull(exchNode);
    Preconditions.checkNotNull(partition);
    exchNode_ = exchNode;
    outputPartition_ = partition;
  }

  @Override
  public void appendSinkExplainString(String prefix, String detailPrefix,
      TQueryOptions queryOptions, TExplainLevel detailLevel, StringBuilder output) {
    output.append(
        String.format("%sDATASTREAM SINK [FRAGMENT=%s, EXCHANGE=%s, %s]",
        prefix, exchNode_.getFragment().getId().toString(),
        exchNode_.getId().toString(), exchNode_.getDisplayLabelDetail()));
    output.append("\n");
  }

  @Override
  protected String getLabel() {
    return "EXCHANGE SENDER";
  }

  @Override
  public void computeResourceProfile(TQueryOptions queryOptions) {
    resourceProfile_ = ResourceProfile.noReservation(0);
  }

  @Override
  protected void toThriftImpl(TDataSink tsink) {
    TDataStreamSink tStreamSink =
        new TDataStreamSink(exchNode_.getId().asInt(), outputPartition_.toThrift());
    tsink.setStream_sink(tStreamSink);
  }

  @Override
  protected TDataSinkType getSinkType() {
    return TDataSinkType.DATA_STREAM_SINK;
  }

  public DataPartition getOutputPartition() { return outputPartition_; }
}
