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
import org.apache.impala.catalog.FeIcebergTable;

import org.apache.impala.thrift.TDataSink;
import org.apache.impala.thrift.TDataSinkType;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TIcebergDeleteSink;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TTableSink;
import org.apache.impala.thrift.TTableSinkType;

/**
 * Sink for deleting data from Iceberg tables. Impala deletes data via 'merge-on-read'
 * strategy, which means it writes Iceberg position delete files. These files contain
 * information (file_path, position) about the deleted rows. Query engines reading from
 * an Iceberg table need to exclude the deleted rows from the result of the table scan.
 * Impala does this by doing an ANTI JOIN between data files and delete files.
 */
public class IcebergDeleteSink extends TableSink {

  // Set the limit on the maximum number of hdfs table sink instances.
  // A value of 0 means no limit.
  private int maxHdfsSinks_;

  // Exprs for computing the output partition(s).
  protected final List<Expr> partitionKeyExprs_;

  public IcebergDeleteSink(FeIcebergTable targetTable, List<Expr> partitionKeyExprs,
      List<Expr> outputExprs, int maxTableSinks) {
    super(targetTable, Op.DELETE, outputExprs);
    partitionKeyExprs_ = partitionKeyExprs;
    maxHdfsSinks_ = maxTableSinks;
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
    // Input is clustered, so it produces a single partition at a time.
    final long numBufferedPartitionsPerInstance = 1;
    // For regular Parquet files we estimate 1GB memory consumption which is already
    // a conservative, i.e. probably too high memory estimate.
    // Writing out position delete files means we are writing filenames and positions
    // per partition. So assuming 0.5 GB per position delete file writer can be still
    // considered a very conservative estimate.
    final long perPartitionMemReq = 512L * 1024L * 1024L;

    long perInstanceMemEstimate;
    // The estimate is based purely on the per-partition mem req if the input cardinality_
    // or the avg row size is unknown.
    if (inputNode.getCardinality() == -1 || inputNode.getAvgRowSize() == -1) {
      perInstanceMemEstimate = numBufferedPartitionsPerInstance * perPartitionMemReq;
    } else {
      // The per-partition estimate may be higher than the memory required to buffer
      // the entire input data.
      long perInstanceInputCardinality =
          Math.max(1L, inputNode.getCardinality() / numInstances);
      long perInstanceInputBytes =
          (long) Math.ceil(perInstanceInputCardinality * inputNode.getAvgRowSize());
      long perInstanceMemReq =
          PlanNode.checkedMultiply(numBufferedPartitionsPerInstance, perPartitionMemReq);
      perInstanceMemEstimate = Math.min(perInstanceInputBytes, perInstanceMemReq);
    }
    resourceProfile_ = ResourceProfile.noReservation(perInstanceMemEstimate);
  }

  @Override
  public void appendSinkExplainString(String prefix, String detailPrefix,
      TQueryOptions queryOptions, TExplainLevel explainLevel, StringBuilder output) {
    output.append(String.format("%sDELETE FROM ICEBERG [%s]\n", prefix,
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
    return "ICEBERG DELETER";
  }

  @Override
  protected void toThriftImpl(TDataSink tsink) {
    TIcebergDeleteSink icebergDeleteSink = new TIcebergDeleteSink(
        Expr.treesToThrift(partitionKeyExprs_));
    TTableSink tTableSink = new TTableSink(DescriptorTable.TABLE_SINK_ID,
            TTableSinkType.HDFS, sinkOp_.toThrift());
    tTableSink.iceberg_delete_sink = icebergDeleteSink;
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

  /**
   * Return an estimate of the number of nodes the fragment with this sink will
   * run on. This is based on the number of nodes set for the plan root and has an
   * upper limit set by the MAX_HDFS_WRITER query option.
   */
  public int getNumNodes() {
    int num_nodes = getFragment().getPlanRoot().getNumNodes();
    if (maxHdfsSinks_ > 0) {
      // If there are more nodes than instances where the fragment was initially
      // planned to run then, then the instances will be distributed evenly across them.
      num_nodes = Math.min(num_nodes, getNumInstances());
    }
    return num_nodes;
  }

  /**
   * Return an estimate of the number of instances the fragment with this sink
   * will run on. This is based on the number of instances set for the plan root
   * and has an upper limit set by the MAX_HDFS_WRITER query option.
   */
  public int getNumInstances() {
    int num_instances = getFragment().getPlanRoot().getNumInstances();
    if (maxHdfsSinks_ > 0) {
      num_instances =  Math.min(num_instances, maxHdfsSinks_);
    }
    return num_instances;
  }

  @Override
  public void computeRowConsumptionAndProductionToCost() {
    super.computeRowConsumptionAndProductionToCost();
    fragment_.setFixedInstanceCount(fragment_.getNumInstances());
  }
}
