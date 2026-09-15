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

import java.util.Collections;
import java.util.List;

import org.apache.impala.analysis.DescriptorTable;
import org.apache.impala.analysis.Expr;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TDataSink;
import org.apache.impala.thrift.TDataSinkType;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TKuduTableSink;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TTableSink;
import org.apache.impala.thrift.TTableSinkType;
import org.apache.impala.util.KuduUtil;
import org.apache.kudu.client.KuduClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Class used to represent a Sink that will transport
 * data from a plan fragment into an Kudu table using a Kudu client.
 */
public class KuduTableSink extends TableSink {
  private final static Logger LOG = LoggerFactory.getLogger(KuduTableSink.class);

  // Optional list of referenced Kudu table column indices. The position of a result
  // expression i matches a column index into the Kudu schema at targetColdIdxs[i].
  private final List<Integer> targetColIdxs_;

  // Serialized metadata of transaction object which is set by the Frontend if the
  // target table is Kudu table and transaction for Kudu is enabled.
  private java.nio.ByteBuffer txnToken_;

  // Indicate whether Kudu cluster supports IGNORE write operations or not.
  private boolean supportsIgnoreOperations_ = false;

  // Upper limit on the number of instances of the fragment containing this sink.
  // <= 0 means unbounded. Used to avoid scheduling more writer instances than the
  // target table's Kudu partition count, since the KUDU-partitioned exchange feeding
  // this sink routes rows to a channel by partition index modulo channel count, so
  // any instance beyond the partition count never receives rows.
  private final int maxKuduSinks_;

  public KuduTableSink(FeTable targetTable, Op sinkOp, List<Integer> referencedColumns,
      List<Expr> outputExprs, java.nio.ByteBuffer txnToken, int maxTableSinks) {
    super(targetTable, sinkOp, outputExprs);
    targetColIdxs_ = referencedColumns != null
        ? Lists.newArrayList(referencedColumns) : null;
    txnToken_ =
        txnToken != null ? org.apache.thrift.TBaseHelper.copyBinary(txnToken) : null;
    maxKuduSinks_ = maxTableSinks;

    // Check if Kudu cluster supports IGNORE write operations.
    Preconditions.checkState(targetTable instanceof FeKuduTable);
    KuduClient client =
        KuduUtil.getKuduClient(((FeKuduTable) targetTable).getKuduMasterHosts());
    try {
      supportsIgnoreOperations_ = client.supportsIgnoreOperations();
    } catch (Exception e) {
      LOG.error("Unable to check Kudu ignore operation support", e);
    }
  }

  @Override
  public void appendSinkExplainString(String prefix, String detailPrefix,
      TQueryOptions queryOptions, TExplainLevel explainLevel, StringBuilder output) {
    output.append(prefix + sinkOp_.toExplainString());
    output.append(" KUDU [" + targetTable_.getFullName() + "]\n");
    if (explainLevel.ordinal() >= TExplainLevel.EXTENDED.ordinal()) {
      output.append(detailPrefix + "output exprs: ")
          .append(Expr.getExplainString(outputExprs_, explainLevel) + "\n");
    }
  }

  @Override
  protected String getLabel() {
    return "KUDU WRITER";
  }

  /** Returns true if the fragment's instance count is capped by 'maxKuduSinks_'. */
  public boolean hasInstanceLimit() { return maxKuduSinks_ > 0; }

  /**
   * Return an estimate of the number of nodes the fragment with this sink will run on.
   * Bounded above by 'maxKuduSinks_', if set.
   */
  public int getNumNodes() {
    int numNodes = getFragment().getPlanRoot().getNumNodes();
    if (hasInstanceLimit()) numNodes = Math.min(numNodes, getNumInstances());
    return numNodes;
  }

  /**
   * Return an estimate of the number of instances the fragment with this sink will run
   * on. Bounded above by 'maxKuduSinks_', if set.
   */
  public int getNumInstances() {
    int numInstances = getFragment().getPlanRoot().getNumInstances();
    if (hasInstanceLimit()) numInstances = Math.min(numInstances, maxKuduSinks_);
    return numInstances;
  }

  @Override
  public void computeRowConsumptionAndProductionToCost() {
    super.computeRowConsumptionAndProductionToCost();
    if (hasInstanceLimit()) {
      fragment_.setFixedInstanceCount(getNumInstances());
    }
  }

  @Override
  public void computeProcessingCost(TQueryOptions queryOptions) {
    // The processing cost to export rows.
    processingCost_ = computeDefaultProcessingCost();
  }

  @Override
  public void computeResourceProfile(TQueryOptions queryOptions) {
    // The major chunk of memory used by this node is untracked. Part of which
    // is allocated by the KuduSession on the write path and the rest is the
    // memory used to store kudu client error messages. Fortunately, both of
    // them have an upper limit which is used directly to set the estimates here.
    long kuduMutationBufferSize = BackendConfig.INSTANCE.getBackendCfg().
        kudu_mutation_buffer_size;
    long kuduErrorBufferSize = BackendConfig.INSTANCE.getBackendCfg().
        kudu_error_buffer_size;
    resourceProfile_ = ResourceProfile.noReservation(kuduMutationBufferSize +
        kuduErrorBufferSize);
  }

  @Override
  protected void toThriftImpl(TDataSink tsink) {
    TTableSink tTableSink = new TTableSink(DescriptorTable.TABLE_SINK_ID,
        TTableSinkType.KUDU, sinkOp_.toThrift());
    TKuduTableSink tKuduSink = new TKuduTableSink();
    tKuduSink.setReferenced_columns(targetColIdxs_);
    if (txnToken_ != null) tKuduSink.setKudu_txn_token(txnToken_);
    tKuduSink.setIgnore_not_found_or_duplicate(supportsIgnoreOperations_);
    tTableSink.setKudu_table_sink(tKuduSink);
    tsink.table_sink = tTableSink;
    tsink.output_exprs = Expr.treesToThrift(outputExprs_);
  }

  @Override
  protected TDataSinkType getSinkType() {
    return TDataSinkType.TABLE_SINK;
  }

  @Override
  public void collectExprsForLineage(List<Expr> exprs) {
    exprs.addAll(outputExprs_);
  }

  public List<Integer> getTargetColIdxs() {
    if (targetColIdxs_ == null) return Collections.emptyList();
    return Collections.unmodifiableList(targetColIdxs_);
  }
}
