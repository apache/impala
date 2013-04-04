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
import java.util.HashSet;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.SlotDescriptor;
import com.cloudera.impala.analysis.SlotId;
import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.analysis.TupleId;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.NotImplementedException;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.TPartitionType;
import com.cloudera.impala.thrift.TPlanFragment;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

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

  // root of plan tree executed by this fragment
  private PlanNode planRoot;

  // fragment that consumes the output of this one
  private PlanFragment destFragment;

  // id of ExchangeNode to which this fragment sends its output
  private PlanNodeId destNodeId;

  // if null, outputs the entire row produced by planRoot
  private ArrayList<Expr> outputExprs;

  // created in finalize() or set in setSink()
  private DataSink sink;

  // specification of the partition of the input of this fragment;
  // an UNPARTITIONED fragment is executed on only a single node
  // TODO: improve this comment, "input" is a bit misleading
  private final DataPartition dataPartition;

  // specification of how the output of this fragment is partitioned (i.e., how
  // it's sent to its destination);
  // if the output is UNPARTITIONED, it is being broadcast
  private DataPartition outputPartition;

  // TODO: SubstitutionMap outputSmap;
  // substitution map to remap exprs onto the output of this fragment, to be applied
  // at destination fragment

  /**
   * C'tor for unpartitioned fragment that produces final output
   */
  public PlanFragment(PlanNode root, ArrayList<Expr> outputExprs) {
    this.planRoot = root;
    this.outputExprs = Expr.cloneList(outputExprs, null);
    this.dataPartition = DataPartition.UNPARTITIONED;
    this.outputPartition = this.dataPartition;
  }

  /**
   * C'tor for fragment with specific partition; the output is by default broadcast.
   */
  public PlanFragment(PlanNode root, DataPartition partition) {
    this.planRoot = root;
    this.dataPartition = partition;
    this.outputPartition = DataPartition.UNPARTITIONED;
  }

  public void setOutputExprs(ArrayList<Expr> outputExprs) {
    this.outputExprs = Expr.cloneList(outputExprs, null);
  }

  /**
   * Finalize plan tree and create stream sink, if needed.
   */
  public void finalize(Analyzer analyzer, boolean validateFileFormats)
      throws InternalException, NotImplementedException {
    if (planRoot != null) {
      setRowTupleIds(planRoot, null);
    }
    if (destNodeId != null) {
      Preconditions.checkState(sink == null);
      // we're streaming to an exchange node
      DataStreamSink streamSink = new DataStreamSink(destNodeId);
      streamSink.setPartition(outputPartition);
      sink = streamSink;
    }

    if (planRoot != null && validateFileFormats) {
      // verify that after partition pruning hdfs partitions only use supported formats
      ArrayList<HdfsScanNode> hdfsScans = Lists.newArrayList();
      planRoot.collectSubclasses(HdfsScanNode.class, hdfsScans);
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
    return dataPartition == DataPartition.UNPARTITIONED ? 1 : planRoot.getNumNodes();
  }

  /**
   * Sets node.rowTupleIds, which is either the parent's rowTupleIds or the
   * list of materialized ids. Propagates row tuple ids to the children according
   * to the requirements of the particular node.
   */
  private void setRowTupleIds(PlanNode node, ArrayList<TupleId> parentRowTupleIds) {
    if (parentRowTupleIds != null) {
      // we don't output less than we materialize
      Preconditions.checkState(parentRowTupleIds.containsAll(node.tupleIds));
      node.rowTupleIds = parentRowTupleIds;
    } else {
      node.rowTupleIds = node.tupleIds;
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
      node.rowTupleIds = node.tupleIds;
      setRowTupleIds(node.getChild(0), null);
    } else if (node instanceof SelectNode) {
      // propagate parent's row composition to child
      Preconditions.checkState(node.getChildren().size() == 1);
      setRowTupleIds(node.getChild(0), node.rowTupleIds);
    } else if (node instanceof HashJoinNode) {
      // propagate parent's row composition only to left child
      Preconditions.checkState(node.getChildren().size() == 2);
      setRowTupleIds(node.getChild(0), node.rowTupleIds);
      setRowTupleIds(node.getChild(1), null);
    }
  }

  public TPlanFragment toThrift() {
    TPlanFragment result = new TPlanFragment();
    if (planRoot != null) {
      result.setPlan(planRoot.treeToThrift());
    }
    if (outputExprs != null) {
      result.setOutput_exprs(Expr.treesToThrift(outputExprs));
    }
    if (sink != null) {
      result.setOutput_sink(sink.toThrift());
    }
    result.setPartition(dataPartition.toThrift());
    return result;
  }

  public String getExplainString(TExplainLevel explainLevel) {
    StringBuilder str = new StringBuilder();
    Preconditions.checkState(dataPartition != null);
    str.append("  " + dataPartition.getExplainString(explainLevel));
    if (sink != null) {
      str.append(sink.getExplainString("  ", explainLevel));
    }
    if (planRoot != null) {
      str.append(planRoot.getExplainString("  ", explainLevel));
    }
    return str.toString();
  }

  /** Returns true if this fragment is partitioned. */
  public boolean isPartitioned() {
    return (dataPartition.getType() != TPartitionType.UNPARTITIONED);
  }

  public PlanFragment getDestFragment() {
    return destFragment;
  }

  public void setDestination(PlanFragment fragment, PlanNodeId exchangeId) {
    destFragment = fragment;
    destNodeId = exchangeId;
    // TODO: check that fragment contains node w/ exchangeId
  }

  public void setSink(DataSink sink) {
    Preconditions.checkState(this.sink == null);
    Preconditions.checkNotNull(sink);
    this.sink = sink;
  }

  public PlanNodeId getDestNodeId() {
    return destNodeId;
  }

  public DataPartition getDataPartition() {
    return dataPartition;
  }

  public DataPartition getOutputPartition() {
    return outputPartition;
  }

  public void setOutputPartition(DataPartition outputPartition) {
    this.outputPartition = outputPartition;
  }

  public PlanNode getPlanRoot() {
    return planRoot;
  }

  public void setPlanRoot(PlanNode root) {
    planRoot = root;
  }

  /**
   * Adds a node as the new root to the plan tree. Connects the existing
   * root as the child of newRoot.
   */
  public void addPlanRoot(PlanNode newRoot) {
    Preconditions.checkState(newRoot.getChildren().size() == 1);
    newRoot.setChild(0, planRoot);
    planRoot = newRoot;
  }

}
