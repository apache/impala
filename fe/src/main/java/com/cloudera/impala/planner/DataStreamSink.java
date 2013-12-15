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

/**
 * Data sink that forwards data to an exchange node.
 *
 */
public class DataStreamSink extends DataSink {
  private final PlanNodeId exchNodeId_;
  private DataPartition outputPartition_;

  public DataStreamSink(PlanNodeId exchNodeId) {
    exchNodeId_ = exchNodeId;
  }

  public void setPartition(DataPartition partition) {
    outputPartition_ = partition;
  }

  @Override
  public String getExplainString(String prefix, TExplainLevel explainLevel) {
    StringBuilder strBuilder = new StringBuilder();
    strBuilder.append(prefix + "STREAM DATA SINK\n");
    strBuilder.append(prefix + "  EXCHANGE ID: " + exchNodeId_ + "\n");
    if (outputPartition_ != null) {
      strBuilder.append(prefix + "  "
          + outputPartition_.getExplainString(explainLevel));
    }
    return strBuilder.toString();
  }

  @Override
  protected TDataSink toThrift() {
    TDataSink result = new TDataSink(TDataSinkType.DATA_STREAM_SINK);
    TDataStreamSink tStreamSink =
        new TDataStreamSink(exchNodeId_.asInt(), outputPartition_.toThrift());
    result.setStream_sink(tStreamSink);
    return result;
  }
}
