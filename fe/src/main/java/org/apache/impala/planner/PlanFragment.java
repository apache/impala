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
import java.util.Set;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.NotImplementedException;
import org.apache.impala.common.PrintUtils;
import org.apache.impala.common.TreeNode;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TPartitionType;
import org.apache.impala.thrift.TPlanFragment;
import org.apache.impala.thrift.TPlanFragmentTree;
import org.apache.impala.thrift.TQueryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * PlanFragments form a tree structure via their ExchangeNodes. A tree of fragments
 * connected in that way forms a plan. The output of a plan is produced by the root
 * fragment and is either the result of the query or an intermediate result
 * needed by a different plan (such as a hash table).
 *
 * Plans are grouped into cohorts based on the consumer of their output: all
 * plans that materialize intermediate results for a particular consumer plan
 * are grouped into a single cohort.
 *
 * A PlanFragment encapsulates the specific tree of execution nodes that
 * are used to produce the output of the plan fragment, as well as output exprs,
 * destination node, etc. If there are no output exprs, the full row that is
 * is produced by the plan root is marked as materialized.
 *
 * A plan fragment can have one or many instances, each of which in turn is executed by
 * an individual node and the output sent to a specific instance of the destination
 * fragment (or, in the case of the root fragment, is materialized in some form).
 *
 * A hash-partitioned plan fragment is the result of one or more hash-partitioning data
 * streams being received by plan nodes in this fragment. In the future, a fragment's
 * data partition could also be hash partitioned based on a scan node that is reading
 * from a physically hash-partitioned table.
 *
 * The sequence of calls is:
 * - c'tor
 * - assemble with getters, etc.
 * - finalize()
 * - toThrift()
 *
 * TODO: the tree of PlanNodes is connected across fragment boundaries, which makes
 *   it impossible search for things within a fragment (using TreeNode functions);
 *   fix that
 */
public class PlanFragment extends TreeNode<PlanFragment> {
  private final static Logger LOG = LoggerFactory.getLogger(PlanFragment.class);

  private final PlanFragmentId fragmentId_;
  private PlanId planId_;
  private CohortId cohortId_;

  // root of plan tree executed by this fragment
  private PlanNode planRoot_;

  // exchange node to which this fragment sends its output
  private ExchangeNode destNode_;

  // if null, outputs the entire row produced by planRoot_
  private List<Expr> outputExprs_;

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
  public void setFragmentInPlanTree(PlanNode node) {
    if (node == null) return;
    node.setFragment(this);
    if (node instanceof ExchangeNode) return;
    for (PlanNode child : node.getChildren()) setFragmentInPlanTree(child);
  }

  /**
   * Collect all PlanNodes that belong to the exec tree of this fragment.
   */
  public void collectPlanNodes(List<PlanNode> nodes) {
    Preconditions.checkNotNull(nodes);
    collectPlanNodesHelper(planRoot_, nodes);
  }

  private void collectPlanNodesHelper(PlanNode root, List<PlanNode> nodes) {
    if (root == null) return;
    nodes.add(root);
    if (root instanceof ExchangeNode) return;
    for (PlanNode child: root.getChildren()) collectPlanNodesHelper(child, nodes);
  }

  public void setOutputExprs(List<Expr> outputExprs) {
    outputExprs_ = Expr.cloneList(outputExprs);
  }
  public List<Expr> getOutputExprs() { return outputExprs_; }

  /**
   * Finalize plan tree and create stream sink, if needed.
   * Computes resource profiles for all nodes and sinks in this fragment.
   * If this fragment is hash partitioned, ensures that the corresponding partition
   * exprs of all hash-partitioning senders are cast to identical types.
   * Otherwise, the hashes generated for identical partition values may differ
   * among senders if the partition-expr types are not identical.
   */
  public void finalize(Analyzer analyzer)
      throws InternalException, NotImplementedException {
    if (destNode_ != null) {
      Preconditions.checkState(sink_ == null);
      // we're streaming to an exchange node
      DataStreamSink streamSink = new DataStreamSink(destNode_, outputPartition_);
      streamSink.setFragment(this);
      sink_ = streamSink;
    }
    computeResourceProfile(analyzer);

    if (!dataPartition_.isHashPartitioned()) return;

    // This fragment is hash partitioned. Gather all exchange nodes and ensure
    // that all hash-partitioning senders hash on exprs-values of the same type.
    List<ExchangeNode> exchNodes = Lists.newArrayList();
    planRoot_.collect(Predicates.instanceOf(ExchangeNode.class), exchNodes);

    // Contains partition-expr lists of all hash-partitioning sender fragments.
    List<List<Expr>> senderPartitionExprs = Lists.newArrayList();
    for (ExchangeNode exchNode: exchNodes) {
      Preconditions.checkState(!exchNode.getChildren().isEmpty());
      PlanFragment senderFragment = exchNode.getChild(0).getFragment();
      Preconditions.checkNotNull(senderFragment);
      if (!senderFragment.getOutputPartition().isHashPartitioned()) continue;
      List<Expr> partExprs = senderFragment.getOutputPartition().getPartitionExprs();
      // All hash-partitioning senders must have compatible partition exprs, otherwise
      // this fragment's data partition must not be hash partitioned.
      Preconditions.checkState(
          partExprs.size() == dataPartition_.getPartitionExprs().size());
      senderPartitionExprs.add(partExprs);
    }

    // Cast all corresponding hash partition exprs of all hash-partitioning senders
    // to their compatible types. Also cast the data partition's exprs for consistency,
    // although not strictly necessary. They should already be type identical to the
    // exprs of one of the senders and they are not directly used for hashing in the BE.
    senderPartitionExprs.add(dataPartition_.getPartitionExprs());
    try {
      analyzer.castToUnionCompatibleTypes(senderPartitionExprs);
    } catch (AnalysisException e) {
      // Should never happen. Analysis should have ensured type compatibility already.
      throw new IllegalStateException(e);
    }
  }

  /**
   * Compute the resource profile of the fragment. Must be called after all the
   * plan nodes and sinks are added to the fragment.
   */
  private void computeResourceProfile(Analyzer analyzer) {
    sink_.computeResourceProfile(analyzer.getQueryOptions());
    List<PlanNode> nodes = Lists.newArrayList();
    collectPlanNodes(nodes);
    for (PlanNode node: nodes) {
      node.computeResourceProfile(analyzer.getQueryOptions());
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
   * Return the number of instances of this fragment per host that it executes on.
   * invalid: -1
   */
  public int getNumInstancesPerHost(int mt_dop) {
    Preconditions.checkState(mt_dop >= 0);
    if (dataPartition_ == DataPartition.UNPARTITIONED) return 1;
    return mt_dop == 0 ? 1 : mt_dop;
  }

  /**
   * Return the total number of instances of this fragment across all hosts.
   * invalid: -1
   */
  public int getNumInstances(int mt_dop) {
    if (dataPartition_ == DataPartition.UNPARTITIONED) return 1;
    int numNodes = planRoot_.getNumNodes();
    if (numNodes == -1) return -1;
    return getNumInstancesPerHost(mt_dop) * numNodes;
  }

  /**
    * Estimates the number of distinct values of exprs per fragment instance based on the
    * data partition of this fragment, the number of nodes, and the degree of parallelism.
    * Returns -1 for an invalid estimate, e.g., because getNumDistinctValues() failed on
    * one of the exprs.
    */
  public long getPerInstanceNdv(int mt_dop, List<Expr> exprs) {
    Preconditions.checkNotNull(dataPartition_);
    long result = 1;
    int numInstances = getNumInstances(mt_dop);
    Preconditions.checkState(numInstances >= 0);
    // The number of nodes is zero for empty tables.
    if (numInstances == 0) return 0;
    for (Expr expr: exprs) {
      long numDistinct = expr.getNumDistinctValues();
      if (numDistinct == -1) {
        result = -1;
        break;
      }
      if (dataPartition_.getPartitionExprs().contains(expr)) {
        numDistinct = (long)Math.max((double) numDistinct / (double) numInstances, 1L);
      }
      result = PlanNode.multiplyCardinalities(result, numDistinct);
    }
    return result;
  }

  public TPlanFragment toThrift() {
    TPlanFragment result = new TPlanFragment();
    result.setDisplay_name(fragmentId_.toString());
    if (planRoot_ != null) result.setPlan(planRoot_.treeToThrift());
    if (outputExprs_ != null) {
      result.setOutput_exprs(Expr.treesToThrift(outputExprs_));
    }
    if (sink_ != null) result.setOutput_sink(sink_.toThrift());
    result.setPartition(dataPartition_.toThrift());
    return result;
  }

  public TPlanFragmentTree treeToThrift() {
    TPlanFragmentTree result = new TPlanFragmentTree();
    treeToThriftHelper(result);
    return result;
  }

  private void treeToThriftHelper(TPlanFragmentTree plan) {
    plan.addToFragments(toThrift());
    for (PlanFragment child: children_) {
      child.treeToThriftHelper(plan);
    }
  }

  public String getExplainString(TQueryOptions queryOptions, TExplainLevel detailLevel) {
    return getExplainString("", "", queryOptions, detailLevel);
  }

  /**
   * The root of the output tree will be prefixed by rootPrefix and the remaining plan
   * output will be prefixed by prefix.
   */
  protected final String getExplainString(String rootPrefix, String prefix,
      TQueryOptions queryOptions, TExplainLevel detailLevel) {
    StringBuilder str = new StringBuilder();
    Preconditions.checkState(dataPartition_ != null);
    String detailPrefix = prefix + "|  ";  // sink detail
    if (detailLevel == TExplainLevel.VERBOSE) {
      // we're printing a new tree, start over with the indentation
      prefix = "  ";
      rootPrefix = "  ";
      detailPrefix = prefix + "|  ";
      str.append(getFragmentHeaderString(queryOptions.getMt_dop()));
      str.append("\n");
      if (sink_ != null && sink_ instanceof DataStreamSink) {
        str.append(
            sink_.getExplainString(rootPrefix, detailPrefix, queryOptions, detailLevel));
      }
    } else if (detailLevel == TExplainLevel.EXTENDED) {
      // Print a fragment prefix displaying the # nodes and # instances
      str.append(rootPrefix);
      str.append(getFragmentHeaderString(queryOptions.getMt_dop()));
      str.append("\n");
      rootPrefix = prefix;
    }

    String planRootPrefix = rootPrefix;
    // Always print sinks other than DataStreamSinks.
    if (sink_ != null && !(sink_ instanceof DataStreamSink)) {
      str.append(
          sink_.getExplainString(rootPrefix, detailPrefix, queryOptions, detailLevel));
      if (detailLevel.ordinal() >= TExplainLevel.STANDARD.ordinal()) {
        str.append(prefix + "|\n");
      }
      // we already used the root prefix for the sink
      planRootPrefix = prefix;
    }
    if (planRoot_ != null) {
      str.append(
          planRoot_.getExplainString(planRootPrefix, prefix, queryOptions, detailLevel));
    }
    return str.toString();
  }

  /**
   * Get a header string for a fragment in an explain plan.
   */
  public String getFragmentHeaderString(int mt_dop) {
    StringBuilder builder = new StringBuilder();
    builder.append(String.format("%s:PLAN FRAGMENT [%s]", fragmentId_.toString(),
        dataPartition_.getExplainString()));
    builder.append(PrintUtils.printNumHosts(" ", getNumNodes()));
    builder.append(PrintUtils.printNumInstances(" ", getNumInstances(mt_dop)));
    return builder.toString();
  }

  /** Returns true if this fragment is partitioned. */
  public boolean isPartitioned() {
    return (dataPartition_.getType() != TPartitionType.UNPARTITIONED);
  }

  public PlanFragmentId getId() { return fragmentId_; }
  public PlanId getPlanId() { return planId_; }
  public void setPlanId(PlanId id) { planId_ = id; }
  public CohortId getCohortId() { return cohortId_; }
  public void setCohortId(CohortId id) { cohortId_ = id; }
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

  public void setDestination(ExchangeNode destNode) {
    destNode_ = destNode;
    PlanFragment dest = getDestFragment();
    Preconditions.checkNotNull(dest);
    dest.addChild(this);
  }

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

  /**
   * Verify that the tree of PlanFragments and their contained tree of
   * PlanNodes is constructed correctly.
   */
  public void verifyTree() {
    // PlanNode.fragment_ is set correctly
    List<PlanNode> nodes = Lists.newArrayList();
    collectPlanNodes(nodes);
    List<PlanNode> exchNodes = Lists.newArrayList();
    for (PlanNode node: nodes) {
      if (node instanceof ExchangeNode) exchNodes.add(node);
      Preconditions.checkState(node.getFragment() == this);
    }

    // all ExchangeNodes have registered input fragments
    Preconditions.checkState(exchNodes.size() == getChildren().size());
    List<PlanFragment> childFragments = Lists.newArrayList();
    for (PlanNode exchNode: exchNodes) {
      PlanFragment childFragment = exchNode.getChild(0).getFragment();
      Preconditions.checkState(!childFragments.contains(childFragment));
      childFragments.add(childFragment);
      Preconditions.checkState(childFragment.getDestNode() == exchNode);
    }
    // all registered children are accounted for
    Preconditions.checkState(getChildren().containsAll(childFragments));

    for (PlanFragment child: getChildren()) child.verifyTree();
  }

  /**
   * Returns true if 'exprs' reference a tuple that is made nullable in this fragment,
   * but not in any of its input fragments.
   */
  public boolean refsNullableTupleId(List<Expr> exprs) {
    Preconditions.checkNotNull(planRoot_);
    List<TupleId> tids = Lists.newArrayList();
    for (Expr e: exprs) e.getIds(tids, null);
    Set<TupleId> nullableTids = Sets.newHashSet(planRoot_.getNullableTupleIds());
    // Remove all tuple ids that were made nullable in an input fragment.
    List<ExchangeNode> exchNodes = Lists.newArrayList();
    planRoot_.collect(ExchangeNode.class, exchNodes);
    for (ExchangeNode exchNode: exchNodes) {
      nullableTids.removeAll(exchNode.getNullableTupleIds());
    }
    for (TupleId tid: tids) if (nullableTids.contains(tid)) return true;
    return false;
  }
}
