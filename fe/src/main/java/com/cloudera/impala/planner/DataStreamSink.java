// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.planner;

import com.cloudera.impala.thrift.TDataSink;
import com.cloudera.impala.thrift.TDataSinkType;
import com.cloudera.impala.thrift.TDataStreamSink;
import com.cloudera.impala.thrift.TExplainLevel;
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
  public String getExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    output.append(
        String.format("%sDATASTREAM SINK [FRAGMENT=%s, EXCHANGE=%s, %s]",
        prefix, exchNode_.getFragment().getId().toString(),
        exchNode_.getId().toString(), exchNode_.getDisplayLabelDetail()));
    return output.toString();
  }

  @Override
  protected TDataSink toThrift() {
    TDataSink result = new TDataSink(TDataSinkType.DATA_STREAM_SINK);
    TDataStreamSink tStreamSink =
        new TDataStreamSink(exchNode_.getId().asInt(), outputPartition_.toThrift());
    result.setStream_sink(tStreamSink);
    return result;
  }

  public DataPartition getOutputPartition() { return outputPartition_; }
}
