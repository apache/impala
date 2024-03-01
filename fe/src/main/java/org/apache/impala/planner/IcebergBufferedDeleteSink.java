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

import org.apache.impala.analysis.DescriptorTable;
import org.apache.impala.analysis.Expr;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.common.ByteUnits;
import org.apache.impala.thrift.TDataSink;
import org.apache.impala.thrift.TDataSinkType;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TIcebergDeleteSink;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TTableSink;
import org.apache.impala.thrift.TTableSinkType;

import java.util.List;

public class IcebergBufferedDeleteSink extends TableSink {

  final private int deleteTableId_;

  // Exprs for computing the output partition(s).
  protected final List<Expr> partitionKeyExprs_;

  public IcebergBufferedDeleteSink(FeIcebergTable targetTable,
      List<Expr> partitionKeyExprs, List<Expr> outputExprs) {
    this(targetTable, partitionKeyExprs, outputExprs, 0);
  }

  public IcebergBufferedDeleteSink(FeIcebergTable targetTable,
      List<Expr> partitionKeyExprs, List<Expr> outputExprs,
      int deleteTableId) {
    super(targetTable, Op.DELETE, outputExprs);
    partitionKeyExprs_ = partitionKeyExprs;
    deleteTableId_ = deleteTableId;
  }

  @Override
  public void computeProcessingCost(TQueryOptions queryOptions) {
    // The processing cost to export rows.
    processingCost_ = computeDefaultProcessingCost();
  }

  @Override
  public void computeResourceProfile(TQueryOptions queryOptions) {
    PlanNode inputNode = fragment_.getPlanRoot();
    final int numInstances = fragment_.getNumInstances();
    // For regular Parquet files we estimate 1GB memory consumption which is already
    // a conservative, i.e. probably too high memory estimate.
    // Writing out position delete files means we are writing filenames and positions
    // per partition. So assuming 0.5 GB per position delete file writer can be still
    // considered a very conservative estimate.
    final long perPartitionMemReq = 512L * ByteUnits.MEGABYTE;
    // The writer also buffers the file paths and positions before it can start writing
    // out files. Let's assume it needs to buffer 20K file paths and 1M positions
    // per sink, that is around 20K * 200 byte + 1M * 8 bytes = 12 megabytes. Let's
    // make it 16 MBs.
    final long bufferedData = 16 * ByteUnits.MEGABYTE;

    long perInstanceMemEstimate;
    // The estimate is based purely on the per-partition mem req if the input cardinality_
    // or the avg row size is unknown.
    if (inputNode.getCardinality() == -1 || inputNode.getAvgRowSize() == -1) {
      perInstanceMemEstimate = perPartitionMemReq + bufferedData;
    } else {
      // The per-partition estimate may be higher than the memory required to buffer
      // the entire input data.
      long perInstanceInputCardinality =
          Math.max(1L, inputNode.getCardinality() / numInstances);
      perInstanceMemEstimate =
          (long) Math.ceil(perInstanceInputCardinality * inputNode.getAvgRowSize());
    }
    resourceProfile_ = ResourceProfile.noReservation(perInstanceMemEstimate);
  }

  @Override
  public void appendSinkExplainString(String prefix, String detailPrefix,
      TQueryOptions queryOptions, TExplainLevel explainLevel, StringBuilder output) {
    output.append(String.format("%sBUFFERED DELETE FROM ICEBERG [%s]\n", prefix,
        targetTable_.getFullName()));
    if (explainLevel.ordinal() >= TExplainLevel.EXTENDED.ordinal()) {
      output.append(detailPrefix + "output exprs: ")
          .append(Expr.getExplainString(outputExprs_, explainLevel) + "\n");
      if (!partitionKeyExprs_.isEmpty()) {
        output.append(detailPrefix + "partition keys: ")
            .append(Expr.getExplainString(partitionKeyExprs_, explainLevel) + "\n");
      }
    }
  }

  @Override
  protected String getLabel() {
    return "ICEBERG BUFFERED DELETER";
  }

  @Override
  protected void toThriftImpl(TDataSink tsink) {
    TIcebergDeleteSink icebergDeleteSink = new TIcebergDeleteSink();
    icebergDeleteSink.setPartition_key_exprs(Expr.treesToThrift(partitionKeyExprs_));
    TTableSink tTableSink = new TTableSink(DescriptorTable.TABLE_SINK_ID,
        TTableSinkType.HDFS, sinkOp_.toThrift());
    tTableSink.iceberg_delete_sink = icebergDeleteSink;
    tTableSink.setTarget_table_id(deleteTableId_);
    tsink.table_sink = tTableSink;
    tsink.output_exprs = Expr.treesToThrift(outputExprs_);
  }

  @Override
  protected TDataSinkType getSinkType() {
    return TDataSinkType.TABLE_SINK;
  }

  @Override
  public void collectExprs(List<Expr> exprs) {
    exprs.addAll(partitionKeyExprs_);
    exprs.addAll(outputExprs_);
  }

  @Override
  public void computeRowConsumptionAndProductionToCost() {
    super.computeRowConsumptionAndProductionToCost();
    fragment_.setFixedInstanceCount(fragment_.getNumInstances());
  }
}
