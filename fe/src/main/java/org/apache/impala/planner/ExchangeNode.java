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

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.SortInfo;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.thrift.TExchangeNode;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TPlanNode;
import org.apache.impala.thrift.TPlanNodeType;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TSortInfo;
import com.google.common.base.Preconditions;

/**
 * Receiver side of a 1:n data stream. Logically, an ExchangeNode consumes the data
 * produced by its children. For each of the sending child nodes the actual data
 * transmission is performed by the DataStreamSink of the PlanFragment housing
 * that child node. Typically, an ExchangeNode only has a single sender child but,
 * e.g., for distributed union queries an ExchangeNode may have one sender child per
 * union operand.
 *
 * If a (optional) SortInfo field is set, the ExchangeNode will merge its
 * inputs on the parameters specified in the SortInfo object. It is assumed that the
 * inputs are also sorted individually on the same SortInfo parameter.
 */
public class ExchangeNode extends PlanNode {
  // The serialization overhead per tuple in bytes when sent over an exchange.
  // Currently it accounts only for the tuple_offset entry per tuple (4B) in a
  // BE TRowBatch. If we modify the RowBatch serialization, then we need to
  // update this constant as well.
  private static final double PER_TUPLE_SERIALIZATION_OVERHEAD = 4.0;

  // The parameters based on which sorted input streams are merged by this
  // exchange node. Null if this exchange does not merge sorted streams
  private SortInfo mergeInfo_;

  // Offset after which the exchange begins returning rows. Currently valid
  // only if mergeInfo_ is non-null, i.e. this is a merging exchange node.
  private long offset_;

  public ExchangeNode(PlanNodeId id, PlanNode input) {
    super(id, "EXCHANGE");
    offset_ = 0;
    children_.add(input);
    // Only apply the limit at the receiver if there are multiple senders.
    if (input.getFragment().isPartitioned()) limit_ = input.limit_;
    computeTupleIds();
  }

  @Override
  public void computeTupleIds() {
    clearTupleIds();
    tupleIds_.addAll(getChild(0).getTupleIds());
    tblRefIds_.addAll(getChild(0).getTblRefIds());
    nullableTupleIds_.addAll(getChild(0).getNullableTupleIds());
  }

  @Override
  public void init(Analyzer analyzer) throws ImpalaException {
    super.init(analyzer);
    Preconditions.checkState(conjuncts_.isEmpty());
  }

  @Override
  public void computeStats(Analyzer analyzer) {
    Preconditions.checkState(!children_.isEmpty(),
        "ExchangeNode must have at least one child");
    cardinality_ = 0;
    for (PlanNode child: children_) {
      if (child.getCardinality() == -1) {
        cardinality_ = -1;
        break;
      }
      cardinality_ = checkedAdd(cardinality_, child.getCardinality());
    }

    if (hasLimit()) {
      if (cardinality_ == -1) {
        cardinality_ = limit_;
      } else {
        cardinality_ = Math.min(limit_, cardinality_);
      }
    }

    // Apply the offset correction if there's a valid cardinality
    if (cardinality_ > -1) {
      cardinality_ = Math.max(0, cardinality_ - offset_);
    }

    // Pick the max numNodes_ and avgRowSize_ of all children.
    numNodes_ = Integer.MIN_VALUE;
    avgRowSize_ = Integer.MIN_VALUE;
    for (PlanNode child: children_) {
      numNodes_ = Math.max(child.numNodes_, numNodes_);
      avgRowSize_ = Math.max(child.avgRowSize_, avgRowSize_);
    }
  }

  /**
   * Set the parameters used to merge sorted input streams. This can be called
   * after init().
   */
  public void setMergeInfo(SortInfo info, long offset) {
    mergeInfo_ = info;
    offset_ = offset;
    displayName_ = "MERGING-EXCHANGE";
  }

  @Override
  protected String getNodeExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    output.append(String.format("%s%s [%s]\n", prefix,
        getDisplayLabel(), getDisplayLabelDetail()));

    if (offset_ > 0) {
      output.append(detailPrefix + "offset: ").append(offset_).append("\n");
    }

    if (mergeInfo_ != null && detailLevel.ordinal() > TExplainLevel.MINIMAL.ordinal()) {
      output.append(detailPrefix + "order by: ");
      for (int i = 0; i < mergeInfo_.getOrderingExprs().size(); ++i) {
        if (i > 0) output.append(", ");
        output.append(mergeInfo_.getOrderingExprs().get(i).toSql() + " ");
        output.append(mergeInfo_.getIsAscOrder().get(i) ? "ASC" : "DESC");

        Boolean nullsFirstParam = mergeInfo_.getNullsFirstParams().get(i);
        if (nullsFirstParam != null) {
          output.append(nullsFirstParam ? " NULLS FIRST" : " NULLS LAST");
        }
      }
      output.append("\n");
    }
    return output.toString();
  }

  @Override
  protected String getDisplayLabelDetail() {
    // For the non-fragmented explain levels, print the data partition
    // of the data stream sink that sends to this exchange node.
    Preconditions.checkState(!children_.isEmpty());
    DataSink sink = getChild(0).getFragment().getSink();
    if (sink == null) return "";
    Preconditions.checkState(sink instanceof DataStreamSink);
    DataStreamSink streamSink = (DataStreamSink) sink;
    if (!streamSink.getOutputPartition().isPartitioned() &&
        fragment_.isPartitioned()) {
      // If the output of the sink is not partitioned but the target fragment is
      // partitioned, then the data exchange is broadcast.
      return "BROADCAST";
    } else {
      return streamSink.getOutputPartition().getExplainString();
    }
  }

  /**
   * Returns the average size of rows produced by 'exchInput' when serialized for
   * being sent through an exchange.
   */
  public static double getAvgSerializedRowSize(PlanNode exchInput) {
    return exchInput.getAvgRowSize() +
        (exchInput.getTupleIds().size() * PER_TUPLE_SERIALIZATION_OVERHEAD);
  }

  @Override
  public void computeNodeResourceProfile(TQueryOptions queryOptions) {
    // TODO: add an estimate
    nodeResourceProfile_ =  ResourceProfile.noReservation(0);
  }

  @Override
  public ExecPhaseResourceProfiles computeTreeResourceProfiles(
      TQueryOptions queryOptions) {
    // Don't include resources of child in different plan fragment.
    return new ExecPhaseResourceProfiles(nodeResourceProfile_, nodeResourceProfile_);
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.EXCHANGE_NODE;
    msg.exchange_node = new TExchangeNode();
    for (TupleId tid: tupleIds_) {
      msg.exchange_node.addToInput_row_tuples(tid.asInt());
    }

    if (mergeInfo_ != null) {
      TSortInfo sortInfo = new TSortInfo(
          Expr.treesToThrift(mergeInfo_.getOrderingExprs()), mergeInfo_.getIsAscOrder(),
          mergeInfo_.getNullsFirst());
      msg.exchange_node.setSort_info(sortInfo);
      msg.exchange_node.setOffset(offset_);
    }
  }
}
