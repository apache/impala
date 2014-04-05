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

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.TupleId;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.NotImplementedException;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.TPartitionType;
import com.cloudera.impala.thrift.TPlanFragment;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;

/**
 * A PlanFragment is part of a tree of such fragments that together make
 * up a complete execution plan for a single query. Each plan fragment can have
 * one or many instances, each of which in turn is executed by a single node and the
 * output sent to a specific instance of the destination fragment (or, in the case
 * of the root fragment, is materialized in some form).
 *
 * The plan fragment encapsulates the specific tree of execution nodes that
 * are used to produce the output of the plan fragment, as well as output exprs,
 * destination node, etc. If there are no output exprs, the full row that is
 * is produced by the plan root is marked as materialized.
 *
 * The sequence of calls is:
 * - c'tor
 * - assemble with getters, etc.
 * - finalize()
 * - toThrift()
 */
public class PlanFragment {
  private final static Logger LOG = LoggerFactory.getLogger(PlanFragment.class);

  private final PlanFragmentId fragmentId_;

  // root of plan tree executed by this fragment
  private PlanNode planRoot_;

  // exchange node to which this fragment sends its output
  private ExchangeNode destNode_;

  // if null, outputs the entire row produced by planRoot_
  private ArrayList<Expr> outputExprs_;

  // created in finalize() or set in setSink()
  private DataSink sink_;

  // specification of the partition of the input of this fragment;
  // an UNPARTITIONED fragment is executed on only a single node
  // TODO: improve this comment, "input" is a bit misleading
  private DataPartition dataPartition_;

  // specification of how the output of this fragment is partitioned (i.e., how
  // it's sent to its destination);
  // if the output is UNPARTITIONED, it is being broadcast
  private DataPartition outputPartition_;

  /**
   * C'tor for fragment with specific partition; the output is by default broadcast.
   */
  public PlanFragment(PlanFragmentId id, PlanNode root, DataPartition partition) {
    fragmentId_ = id;
    planRoot_ = root;
    dataPartition_ = partition;
    outputPartition_ = DataPartition.UNPARTITIONED;
    setFragmentInPlanTree(planRoot_);
  }

  /**
   * Assigns 'this' as fragment of all PlanNodes in the plan tree rooted at node.
   * Does not traverse the children of ExchangeNodes because those must belong to a
   * different fragment.
   */
  private void setFragmentInPlanTree(PlanNode node) {
    node.setFragment(this);
    if (!(node instanceof ExchangeNode)) {
      for (PlanNode child : node.getChildren()) {
        setFragmentInPlanTree(child);
      }
    }
  }

  public void setOutputExprs(ArrayList<Expr> outputExprs) {
    this.outputExprs_ = Expr.cloneList(outputExprs, null);
  }

  /**
   * Finalize plan tree and create stream sink, if needed.
   */
  public void finalize(Analyzer analyzer, boolean validateFileFormats)
      throws InternalException, NotImplementedException {
    if (planRoot_ != null) setRowTupleIds(planRoot_, null);
    if (destNode_ != null) {
      Preconditions.checkState(sink_ == null);
      // we're streaming to an exchange node
      DataStreamSink streamSink = new DataStreamSink(destNode_, outputPartition_);
      streamSink.setFragment(this);
      sink_ = streamSink;
    }

    if (planRoot_ != null && validateFileFormats) {
      // verify that after partition pruning hdfs partitions only use supported formats
      ArrayList<HdfsScanNode> hdfsScans = Lists.newArrayList();
      planRoot_.collect(Predicates.instanceOf(HdfsScanNode.class), hdfsScans);
      for (HdfsScanNode hdfsScanNode: hdfsScans) {
        hdfsScanNode.validateFileFormat();
      }
    }
  }

  /**
   * Return the number of nodes on which the plan fragment will execute.
   * invalid: -1
   */
  public int getNumNodes() {
    return dataPartition_ == DataPartition.UNPARTITIONED ? 1 : planRoot_.getNumNodes();
  }

  /**
   * Sets node.rowTupleIds, which is either the parent's rowTupleIds or the
   * list of materialized ids. Propagates row tuple ids to the children according
   * to the requirements of the particular node.
   */
  private void setRowTupleIds(PlanNode node, ArrayList<TupleId> parentRowTupleIds) {
    if (parentRowTupleIds != null) {
      // we don't output less than we materialize
      Preconditions.checkState(parentRowTupleIds.containsAll(node.tupleIds_));
      node.rowTupleIds_ = parentRowTupleIds;
    } else {
      node.rowTupleIds_ = node.tupleIds_;
    }

    if (node instanceof ScanNode || node instanceof ExchangeNode) {
      // nothing to propagate for leaves
      return;
    } else if (node instanceof AggregationNode || node instanceof MergeNode) {
      for (PlanNode child: node.getChildren()) {
        // row composition changes at an aggregation node
        setRowTupleIds(child, null);
      }
    } else if (node instanceof SortNode) {
      Preconditions.checkState(node.getChildren().size() == 1);
      // top-n only materializes rows as wide as the input
      node.rowTupleIds_ = node.tupleIds_;
      setRowTupleIds(node.getChild(0), null);
    } else if (node instanceof SelectNode) {
      // propagate parent's row composition to child
      Preconditions.checkState(node.getChildren().size() == 1);
      setRowTupleIds(node.getChild(0), node.rowTupleIds_);
    } else if (node instanceof HashJoinNode || node instanceof CrossJoinNode) {
      // propagate parent's row composition only to left child
      Preconditions.checkState(node.getChildren().size() == 2);
      setRowTupleIds(node.getChild(0), node.rowTupleIds_);
      setRowTupleIds(node.getChild(1), null);
    }
  }

  /**
   * Estimates the per-node number of distinct values of exprs based on the data
   * partition of this fragment and its number of nodes. Returns -1 for an invalid
   * estimate, e.g., because getNumDistinctValues() failed on one of the exprs.
   */
  public long getNumDistinctValues(List<Expr> exprs) {
    Preconditions.checkNotNull(dataPartition_);
    long result = 1;
    int numNodes = getNumNodes();
    Preconditions.checkState(numNodes >= 0);
    // The number of nodes is zero for empty tables.
    if (numNodes == 0) return 0;
    for (Expr expr: exprs) {
      long numDistinct = expr.getNumDistinctValues();
      if (numDistinct == -1) {
        result = -1;
        break;
      }
      if (dataPartition_.getPartitionExprs().contains(expr)) {
        result *= Math.max((double) numDistinct / (double) numNodes, 1L);
      } else {
        result *= numDistinct;
      }
    }
    return result;
  }

  public TPlanFragment toThrift() {
    TPlanFragment result = new TPlanFragment();
    if (planRoot_ != null) result.setPlan(planRoot_.treeToThrift());
    if (outputExprs_ != null) {
      result.setOutput_exprs(Expr.treesToThrift(outputExprs_));
    }
    if (sink_ != null) result.setOutput_sink(sink_.toThrift());
    result.setPartition(dataPartition_.toThrift());
    return result;
  }

  public String getExplainString(TExplainLevel explainLevel) {
    StringBuilder str = new StringBuilder();
    Preconditions.checkState(dataPartition_ != null);
    String rootPrefix = "";
    String prefix = "";
    String detailPrefix = "|  ";
    if (explainLevel == TExplainLevel.VERBOSE) {
      prefix = "  ";
      rootPrefix = "  ";
      detailPrefix = prefix + "|  ";
      str.append(String.format("%s:PLAN FRAGMENT [%s]\n", fragmentId_.toString(),
          dataPartition_.getExplainString()));
      if (sink_ != null && sink_ instanceof DataStreamSink) {
        str.append(sink_.getExplainString(prefix, detailPrefix, explainLevel) + "\n");
      }
    }
    // Always print table sinks.
    if (sink_ != null && sink_ instanceof TableSink) {
      str.append(sink_.getExplainString(prefix, detailPrefix, explainLevel));
      if (explainLevel.ordinal() >= TExplainLevel.STANDARD.ordinal()) {
        str.append(prefix + "|\n");
      }
    }
    if (planRoot_ != null) {
      str.append(planRoot_.getExplainString(rootPrefix, prefix, explainLevel));
    }
    return str.toString();
  }

  /** Returns true if this fragment is partitioned. */
  public boolean isPartitioned() {
    return (dataPartition_.getType() != TPartitionType.UNPARTITIONED);
  }

  public PlanFragmentId getId() { return fragmentId_; }
  public PlanFragment getDestFragment() {
    if (destNode_ == null) return null;
    return destNode_.getFragment();
  }
  public ExchangeNode getDestNode() { return destNode_; }
  public DataPartition getDataPartition() { return dataPartition_; }
  public void setDataPartition(DataPartition dataPartition) {
    this.dataPartition_ = dataPartition;
  }
  public DataPartition getOutputPartition() { return outputPartition_; }
  public void setOutputPartition(DataPartition outputPartition) {
    this.outputPartition_ = outputPartition;
  }
  public PlanNode getPlanRoot() { return planRoot_; }
  public void setPlanRoot(PlanNode root) {
    planRoot_ = root;
    setFragmentInPlanTree(planRoot_);
  }

  public void setDestination(ExchangeNode destNode) { destNode_ = destNode; }
  public boolean hasSink() { return sink_ != null; }
  public DataSink getSink() { return sink_; }
  public void setSink(DataSink sink) {
    Preconditions.checkState(this.sink_ == null);
    Preconditions.checkNotNull(sink);
    sink.setFragment(this);
    this.sink_ = sink;
  }

  /**
   * Adds a node as the new root to the plan tree. Connects the existing
   * root as the child of newRoot.
   */
  public void addPlanRoot(PlanNode newRoot) {
    Preconditions.checkState(newRoot.getChildren().size() == 1);
    newRoot.setChild(0, planRoot_);
    planRoot_ = newRoot;
    planRoot_.setFragment(this);
  }
}
