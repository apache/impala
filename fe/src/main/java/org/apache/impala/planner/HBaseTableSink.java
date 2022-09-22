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

import java.util.List;

import org.apache.impala.analysis.DescriptorTable;
import org.apache.impala.analysis.Expr;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.thrift.TDataSink;
import org.apache.impala.thrift.TDataSinkType;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TTableSink;
import org.apache.impala.thrift.TTableSinkType;

/**
 * Class used to represent a Sink that will transport
 * data from a plan fragment into an HBase table using HTable.
 */
public class HBaseTableSink extends TableSink {
  public HBaseTableSink(FeTable targetTable, List<Expr> outputExprs) {
    super(targetTable, Op.INSERT, outputExprs);
  }

  @Override
  public void appendSinkExplainString(String prefix, String detailPrefix,
      TQueryOptions queryOptions, TExplainLevel explainLevel, StringBuilder output) {
    output.append(prefix + "WRITE TO HBASE table=" + targetTable_.getFullName() + "\n");
    if (explainLevel.ordinal() >= TExplainLevel.EXTENDED.ordinal()) {
      output.append(detailPrefix + "output exprs: ")
          .append(Expr.getExplainString(outputExprs_, explainLevel) + "\n");
    }
  }

  @Override
  protected String getLabel() {
    return "HBASE WRITER";
  }

  @Override
  public void computeProcessingCost(TQueryOptions queryOptions) {
    // The processing cost to export rows.
    processingCost_ = computeDefaultProcessingCost();
  }

  @Override
  public void computeResourceProfile(TQueryOptions queryOptions) {
    resourceProfile_ = ResourceProfile.noReservation(0);
  }

  @Override
  protected void toThriftImpl(TDataSink tsink) {
    TTableSink tTableSink = new TTableSink(DescriptorTable.TABLE_SINK_ID,
        TTableSinkType.HBASE, sinkOp_.toThrift());
    tsink.table_sink = tTableSink;
    tsink.output_exprs = Expr.treesToThrift(outputExprs_);
  }

  @Override
  protected TDataSinkType getSinkType() {
    return TDataSinkType.TABLE_SINK;
  }

  @Override
  public void collectExprs(List<Expr> exprs) {
    exprs.addAll(outputExprs_);
  }
}
