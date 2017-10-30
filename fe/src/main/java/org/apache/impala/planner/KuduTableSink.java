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

import java.util.ArrayList;
import java.util.List;

import org.apache.impala.analysis.DescriptorTable;
import org.apache.impala.catalog.Table;
import org.apache.impala.thrift.TDataSink;
import org.apache.impala.thrift.TDataSinkType;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TKuduTableSink;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TTableSink;
import org.apache.impala.thrift.TTableSinkType;

import com.google.common.collect.Lists;

/**
 * Class used to represent a Sink that will transport
 * data from a plan fragment into an Kudu table using a Kudu client.
 */
public class KuduTableSink extends TableSink {

  // Optional list of referenced Kudu table column indices. The position of a result
  // expression i matches a column index into the Kudu schema at targetColdIdxs[i].
  private final ArrayList<Integer> targetColIdxs_;

  public KuduTableSink(Table targetTable, Op sinkOp,
      List<Integer> referencedColumns) {
    super(targetTable, sinkOp);
    targetColIdxs_ = referencedColumns != null
        ? Lists.newArrayList(referencedColumns) : null;
  }

  @Override
  public void appendSinkExplainString(String prefix, String detailPrefix,
      TQueryOptions queryOptions, TExplainLevel explainLevel, StringBuilder output) {
    output.append(prefix + sinkOp_.toExplainString());
    output.append(" KUDU [" + targetTable_.getFullName() + "]\n");
  }

  @Override
  public void computeResourceProfile(TQueryOptions queryOptions) {
    // TODO: add a memory estimate
    resourceProfile_ = ResourceProfile.noReservation(0);
  }

  @Override
  protected TDataSink toThrift() {
    TDataSink result = new TDataSink(TDataSinkType.TABLE_SINK);
    TTableSink tTableSink = new TTableSink(DescriptorTable.TABLE_SINK_ID,
        TTableSinkType.KUDU, sinkOp_.toThrift());
    TKuduTableSink tKuduSink = new TKuduTableSink();
    tKuduSink.setReferenced_columns(targetColIdxs_);
    tTableSink.setKudu_table_sink(tKuduSink);
    result.table_sink = tTableSink;
    return result;
  }
}
