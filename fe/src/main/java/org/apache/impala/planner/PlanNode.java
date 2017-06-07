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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.ExprId;
import org.apache.impala.analysis.ExprSubstitutionMap;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.PrintUtils;
import org.apache.impala.common.TreeNode;
import org.apache.impala.planner.RuntimeFilterGenerator.RuntimeFilter;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TExecStats;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TPlan;
import org.apache.impala.thrift.TPlanNode;
import org.apache.impala.thrift.TQueryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.math.LongMath;

/**
 * Each PlanNode represents a single relational operator
 * and encapsulates the information needed by the planner to
 * make optimization decisions.
 *
 * finalize(): Computes internal state, such as keys for scan nodes; gets called once on
 * the root of the plan tree before the call to toThrift(). Also finalizes the set
 * of conjuncts, such that each remaining one requires all of its referenced slots to
 * be materialized (ie, can be evaluated by calling GetValue(), rather than being
 * implicitly evaluated as part of a scan key).
 *
 * conjuncts_: Each node has a list of conjuncts that can be executed in the context of
 * this node, ie, they only reference tuples materialized by this node or one of
 * its children (= are bound by tupleIds_).
 */
abstract public class PlanNode extends TreeNode<PlanNode> {
  private final static Logger LOG = LoggerFactory.getLogger(PlanNode.class);

  // String used for this node in getExplainString().
  protected String displayName_;

  // unique w/in plan tree; assigned by planner, and not necessarily in c'tor
  protected PlanNodeId id_;

  protected long limit_; // max. # of rows to be returned; 0: no limit_

  // ids materialized by the tree rooted at this node
  protected ArrayList<TupleId> tupleIds_;

  // ids of the TblRefs "materialized" by this node; identical with tupleIds_
  // if the tree rooted at this node only materializes BaseTblRefs;
  // useful during plan generation
  protected ArrayList<TupleId> tblRefIds_;

  // A set of nullable TupleId produced by this node. It is a subset of tupleIds_.
  // A tuple is nullable within a particular plan tree if it's the "nullable" side of
  // an outer join, which has nothing to do with the schema.
  protected Set<TupleId> nullableTupleIds_ = Sets.newHashSet();

  protected List<Expr> conjuncts_ = Lists.newArrayList();

  // Fragment that this PlanNode is executed in. Valid only after this PlanNode has been
  // assigned to a fragment. Set and maintained by enclosing PlanFragment.
  protected PlanFragment fragment_;

  // If set, needs to be applied by parent node to reference this node's output. The
  // entries need to be propagated all the way to the root node.
  protected ExprSubstitutionMap outputSmap_;

  // global state of planning wrt conjunct assignment; used by planner as a shortcut
  // to avoid having to pass assigned conjuncts back and forth
  // (the planner uses this to save and reset the global state in between join tree
  // alternatives)
  // TODO for 2.3: Save this state in the PlannerContext instead.
  protected Set<ExprId> assignedConjuncts_;

  // estimate of the output cardinality of this node; set in computeStats();
  // invalid: -1
  protected long cardinality_;

  // number of nodes on which the plan tree rooted at this node would execute;
  // set in computeStats(); invalid: -1
  protected int numNodes_;

  // resource requirements and estimates for this plan node.
  // Initialized with a dummy value. Gets set correctly in
  // computeResourceProfile().
  protected ResourceProfile nodeResourceProfile_ = ResourceProfile.invalid();

  // sum of tupleIds_' avgSerializedSizes; set in computeStats()
  protected float avgRowSize_;

  // If true, disable codegen for this plan node.
  protected boolean disableCodegen_;

  // Runtime filters assigned to this node.
  protected List<RuntimeFilter> runtimeFilters_ = Lists.newArrayList();

  protected PlanNode(PlanNodeId id, List<TupleId> tupleIds, String displayName) {
    this(id, displayName);
    tupleIds_.addAll(tupleIds);
    tblRefIds_.addAll(tupleIds);
  }

  /**
   * Deferred id_ assignment.
   */
  protected PlanNode(String displayName) {
    this(null, displayName);
  }

  protected PlanNode(PlanNodeId id, String displayName) {
    id_ = id;
    limit_ = -1;
    tupleIds_ = Lists.newArrayList();
    tblRefIds_ = Lists.newArrayList();
    cardinality_ = -1;
    numNodes_ = -1;
    displayName_ = displayName;
    disableCodegen_ = false;
  }

  /**
   * Copy c'tor. Also passes in new id_.
   */
  protected PlanNode(PlanNodeId id, PlanNode node, String displayName) {
    id_ = id;
    limit_ = node.limit_;
    tupleIds_ = Lists.newArrayList(node.tupleIds_);
    tblRefIds_ = Lists.newArrayList(node.tblRefIds_);
    nullableTupleIds_ = Sets.newHashSet(node.nullableTupleIds_);
    conjuncts_ = Expr.cloneList(node.conjuncts_);
    cardinality_ = -1;
    numNodes_ = -1;
    displayName_ = displayName;
    disableCodegen_ = node.disableCodegen_;
  }

  /**
   * Sets tblRefIds_, tupleIds_, and nullableTupleIds_.
   * The default implementation is a no-op.
   */
  public void computeTupleIds() {
    Preconditions.checkState(children_.isEmpty() || !tupleIds_.isEmpty());
  }

  /**
   * Clears tblRefIds_, tupleIds_, and nullableTupleIds_.
   */
  protected void clearTupleIds() {
    tblRefIds_.clear();
    tupleIds_.clear();
    nullableTupleIds_.clear();
  }

  public PlanNodeId getId() { return id_; }
  public void setId(PlanNodeId id) {
    Preconditions.checkState(id_ == null);
    id_ = id;
  }
  public long getLimit() { return limit_; }
  public boolean hasLimit() { return limit_ > -1; }
  public long getCardinality() { return cardinality_; }
  public int getNumNodes() { return numNodes_; }
  public ResourceProfile getNodeResourceProfile() { return nodeResourceProfile_; }
  public float getAvgRowSize() { return avgRowSize_; }
  public void setFragment(PlanFragment fragment) { fragment_ = fragment; }
  public PlanFragment getFragment() { return fragment_; }
  public List<Expr> getConjuncts() { return conjuncts_; }
  public ExprSubstitutionMap getOutputSmap() { return outputSmap_; }
  public void setOutputSmap(ExprSubstitutionMap smap) { outputSmap_ = smap; }
  public Set<ExprId> getAssignedConjuncts() { return assignedConjuncts_; }
  public void setAssignedConjuncts(Set<ExprId> conjuncts) {
    assignedConjuncts_ = conjuncts;
  }

  /**
   * Set the limit_ to the given limit_ only if the limit_ hasn't been set, or the new limit_
   * is lower.
   */
  public void setLimit(long limit) {
    if (limit_ == -1 || (limit != -1 && limit_ > limit)) limit_ = limit;
  }

  public void unsetLimit() { limit_ = -1; }

  public ArrayList<TupleId> getTupleIds() {
    Preconditions.checkState(tupleIds_ != null);
    return tupleIds_;
  }

  public ArrayList<TupleId> getTblRefIds() { return tblRefIds_; }
  public void setTblRefIds(ArrayList<TupleId> ids) { tblRefIds_ = ids; }

  public Set<TupleId> getNullableTupleIds() {
    Preconditions.checkState(nullableTupleIds_ != null);
    return nullableTupleIds_;
  }

  public void addConjuncts(List<Expr> conjuncts) {
    if (conjuncts == null)  return;
    conjuncts_.addAll(conjuncts);
  }

  public void transferConjuncts(PlanNode recipient) {
    recipient.conjuncts_.addAll(conjuncts_);
    conjuncts_.clear();
  }

  public String getExplainString(TQueryOptions queryOptions) {
    return getExplainString("", "", queryOptions, TExplainLevel.VERBOSE);
  }

  protected void setDisplayName(String s) { displayName_ = s; }

  final protected String getDisplayLabel() {
    return String.format("%s:%s", id_.toString(), displayName_);
  }

  /**
   * Subclasses can override to provide a node specific detail string that
   * is displayed to the user.
   * e.g. scan can return the table name.
   */
  protected String getDisplayLabelDetail() { return ""; }

  /**
   * Generate the explain plan tree. The plan will be in the form of:
   *
   * root
   * |
   * |----child 3
   * |      limit:1
   * |
   * |----child 2
   * |      limit:2
   * |
   * child 1
   *
   * The root node header line will be prefixed by rootPrefix and the remaining plan
   * output will be prefixed by prefix.
   */
  protected final String getExplainString(String rootPrefix, String prefix,
      TQueryOptions queryOptions, TExplainLevel detailLevel) {
    StringBuilder expBuilder = new StringBuilder();
    String detailPrefix = prefix;
    String filler;
    boolean printFiller = (detailLevel.ordinal() >= TExplainLevel.STANDARD.ordinal());

    // Do not traverse into the children of an Exchange node to avoid crossing
    // fragment boundaries.
    boolean traverseChildren = !children_.isEmpty() &&
        !(this instanceof ExchangeNode && detailLevel == TExplainLevel.VERBOSE);

    if (traverseChildren) {
      detailPrefix += "|  ";
      filler = prefix + "|";
    } else {
      detailPrefix += "   ";
      filler = prefix;
    }

    // Print the current node
    // The plan node header line will be prefixed by rootPrefix and the remaining details
    // will be prefixed by detailPrefix.
    expBuilder.append(getNodeExplainString(rootPrefix, detailPrefix, detailLevel));

    if (detailLevel.ordinal() >= TExplainLevel.STANDARD.ordinal() &&
        !(this instanceof SortNode)) {
      if (limit_ != -1) expBuilder.append(detailPrefix + "limit: " + limit_ + "\n");
      expBuilder.append(getOffsetExplainString(detailPrefix));
    }

    // Output cardinality, cost estimates and tuple Ids only when explain plan level
    // is extended or above.
    if (detailLevel.ordinal() >= TExplainLevel.EXTENDED.ordinal()) {
      // Print resource profile.
      expBuilder.append(detailPrefix);
      expBuilder.append(nodeResourceProfile_.getExplainString());
      expBuilder.append("\n");

      // Print tuple ids, row size and cardinality.
      expBuilder.append(detailPrefix + "tuple-ids=");
      for (int i = 0; i < tupleIds_.size(); ++i) {
        TupleId tupleId = tupleIds_.get(i);
        String nullIndicator = nullableTupleIds_.contains(tupleId) ? "N" : "";
        expBuilder.append(tupleId.asInt() + nullIndicator);
        if (i + 1 != tupleIds_.size()) expBuilder.append(",");
      }
      expBuilder.append(" row-size=" + PrintUtils.printBytes(Math.round(avgRowSize_)));
      expBuilder.append(PrintUtils.printCardinality(" ", cardinality_));
      expBuilder.append("\n");
    }

    // Print the children. Do not traverse into the children of an Exchange node to
    // avoid crossing fragment boundaries.
    if (traverseChildren) {
      if (printFiller) expBuilder.append(filler + "\n");
      String childHeadlinePrefix = prefix + "|--";
      String childDetailPrefix = prefix + "|  ";
      for (int i = children_.size() - 1; i >= 1; --i) {
        PlanNode child = getChild(i);
        if (fragment_ != child.fragment_) {
          // we're crossing a fragment boundary
          expBuilder.append(
              child.fragment_.getExplainString(
                childHeadlinePrefix, childDetailPrefix, queryOptions, detailLevel));
        } else {
          expBuilder.append(child.getExplainString(childHeadlinePrefix,
              childDetailPrefix, queryOptions, detailLevel));
        }
        if (printFiller) expBuilder.append(filler + "\n");
      }
      PlanFragment childFragment = children_.get(0).fragment_;
      if (fragment_ != childFragment && detailLevel == TExplainLevel.EXTENDED) {
        // we're crossing a fragment boundary - print the fragment header.
        expBuilder.append(childFragment.getFragmentHeaderString(prefix, prefix,
            queryOptions.getMt_dop()));
      }
      expBuilder.append(
          children_.get(0).getExplainString(prefix, prefix, queryOptions, detailLevel));
    }
    return expBuilder.toString();
  }

  /**
   * Return the node-specific details.
   * Subclass should override this function.
   * Each line should be prefixed by detailPrefix.
   */
  protected String getNodeExplainString(String rootPrefix, String detailPrefix,
      TExplainLevel detailLevel) {
    return "";
  }

  /**
   * Return the offset_ details, if applicable. This is available separately from
   * 'getNodeExplainString' because we want to output 'limit: ...' (which can be printed
   * from PlanNode) before 'offset: ...', which is only printed from SortNodes right
   * now.
   */
  protected String getOffsetExplainString(String prefix) {
    return "";
  }

  // Convert this plan node, including all children, to its Thrift representation.
  public TPlan treeToThrift() {
    TPlan result = new TPlan();
    treeToThriftHelper(result);
    return result;
  }

  // Append a flattened version of this plan node, including all children, to 'container'.
  private void treeToThriftHelper(TPlan container) {
    TPlanNode msg = new TPlanNode();
    msg.node_id = id_.asInt();
    msg.limit = limit_;

    TExecStats estimatedStats = new TExecStats();
    estimatedStats.setCardinality(cardinality_);
    estimatedStats.setMemory_used(nodeResourceProfile_.getMemEstimateBytes());
    msg.setLabel(getDisplayLabel());
    msg.setLabel_detail(getDisplayLabelDetail());
    msg.setEstimated_stats(estimatedStats);

    Preconditions.checkState(tupleIds_.size() > 0);
    msg.setRow_tuples(Lists.<Integer>newArrayListWithCapacity(tupleIds_.size()));
    msg.setNullable_tuples(Lists.<Boolean>newArrayListWithCapacity(tupleIds_.size()));
    for (TupleId tid: tupleIds_) {
      msg.addToRow_tuples(tid.asInt());
      msg.addToNullable_tuples(nullableTupleIds_.contains(tid));
    }
    for (Expr e: conjuncts_) {
      msg.addToConjuncts(e.treeToThrift());
    }
    // Serialize any runtime filters
    for (RuntimeFilter filter : runtimeFilters_) {
      msg.addToRuntime_filters(filter.toThrift());
    }
    msg.setDisable_codegen(disableCodegen_);
    toThrift(msg);
    container.addToNodes(msg);
    // For the purpose of the BE consider ExchangeNodes to have no children.
    if (this instanceof ExchangeNode) {
      msg.num_children = 0;
      return;
    } else {
      msg.num_children = children_.size();
      for (PlanNode child: children_) {
        child.treeToThriftHelper(container);
      }
    }
  }

  /**
   * Computes the full internal state, including smap and planner-relevant statistics
   * (calls computeStats()), marks all slots referenced by this node as materialized
   * and computes the mem layout of all materialized tuples (with the assumption that
   * slots that are needed by ancestor PlanNodes have already been marked).
   * Also performs final expr substitution with childrens' smaps and computes internal
   * state required for toThrift(). This is called directly after construction.
   * Throws if an expr substitution or evaluation fails.
   */
  public void init(Analyzer analyzer) throws ImpalaException {
    assignConjuncts(analyzer);
    computeStats(analyzer);
    createDefaultSmap(analyzer);
  }

  /**
   * Assign remaining unassigned conjuncts.
   */
  protected void assignConjuncts(Analyzer analyzer) {
    List<Expr> unassigned = analyzer.getUnassignedConjuncts(this);
    conjuncts_.addAll(unassigned);
    analyzer.markConjunctsAssigned(unassigned);
  }

  /**
   * Returns an smap that combines the childrens' smaps.
   */
  protected ExprSubstitutionMap getCombinedChildSmap() {
    if (getChildren().size() == 0) return new ExprSubstitutionMap();
    if (getChildren().size() == 1) return getChild(0).getOutputSmap();
    ExprSubstitutionMap result = ExprSubstitutionMap.combine(
        getChild(0).getOutputSmap(), getChild(1).getOutputSmap());
    for (int i = 2; i < getChildren().size(); ++i) {
      result = ExprSubstitutionMap.combine(result, getChild(i).getOutputSmap());
    }
    return result;
  }

  /**
   * Sets outputSmap_ to compose(existing smap, combined child smap). Also
   * substitutes conjuncts_ using the combined child smap.
   */
  protected void createDefaultSmap(Analyzer analyzer) {
    ExprSubstitutionMap combinedChildSmap = getCombinedChildSmap();
    outputSmap_ =
        ExprSubstitutionMap.compose(outputSmap_, combinedChildSmap, analyzer);
    conjuncts_ = Expr.substituteList(conjuncts_, outputSmap_, analyzer, false);
  }

  /**
   * Computes planner statistics: avgRowSize_, numNodes_, cardinality_.
   * Subclasses need to override this.
   * Assumes that it has already been called on all children.
   * and that DescriptorTable.computePhysMemLayout() has been called.
   * This is broken out of init() so that it can be called separately
   * from init() (to facilitate inserting additional nodes during plan
   * partitioning w/o the need to call init() recursively on the whole tree again).
   */
  protected void computeStats(Analyzer analyzer) {
    avgRowSize_ = 0.0F;
    for (TupleId tid: tupleIds_) {
      TupleDescriptor desc = analyzer.getTupleDesc(tid);
      avgRowSize_ += desc.getAvgSerializedSize();
    }
    if (!children_.isEmpty()) numNodes_ = getChild(0).numNodes_;
  }

  protected long capAtLimit(long cardinality) {
    if (hasLimit()) {
      if (cardinality == -1) {
        return limit_;
      } else {
        return Math.min(cardinality, limit_);
      }
    }
    return cardinality;
  }

  /**
   * Call computeMemLayout() for all materialized tuples.
   */
  protected void computeMemLayout(Analyzer analyzer) {
    for (TupleId id: tupleIds_) {
      analyzer.getDescTbl().getTupleDesc(id).computeMemLayout();
    }
  }

  /**
   * Returns the estimated combined selectivity of all conjuncts. Uses heuristics to
   * address the following estimation challenges:
   * 1. The individual selectivities of conjuncts may be unknown.
   * 2. Two selectivities, whether known or unknown, could be correlated. Assuming
   *    independence can lead to significant underestimation.
   *
   * The first issue is addressed by using a single default selectivity that is
   * representative of all conjuncts with unknown selectivities.
   * The second issue is addressed by an exponential backoff when multiplying each
   * additional selectivity into the final result.
   */
  static protected double computeCombinedSelectivity(List<Expr> conjuncts) {
    // Collect all estimated selectivities.
    List<Double> selectivities = Lists.newArrayList();
    for (Expr e: conjuncts) {
      if (e.hasSelectivity()) selectivities.add(e.getSelectivity());
    }
    if (selectivities.size() != conjuncts.size()) {
      // Some conjuncts have no estimated selectivity. Use a single default
      // representative selectivity for all those conjuncts.
      selectivities.add(Expr.DEFAULT_SELECTIVITY);
    }
    // Sort the selectivities to get a consistent estimate, regardless of the original
    // conjunct order. Sort in ascending order such that the most selective conjunct
    // is fully applied.
    Collections.sort(selectivities);
    double result = 1.0;
    for (int i = 0; i < selectivities.size(); ++i) {
      // Exponential backoff for each selectivity multiplied into the final result.
      result *= Math.pow(selectivities.get(i), 1.0 / (double) (i + 1));
    }
    // Bound result in [0, 1]
    return Math.max(0.0, Math.min(1.0, result));
  }

  protected double computeSelectivity() {
    return computeCombinedSelectivity(conjuncts_);
  }

  // Convert this plan node into msg (excluding children), which requires setting
  // the node type and the node-specific field.
  protected abstract void toThrift(TPlanNode msg);

  protected String debugString() {
    // not using Objects.toStrHelper because
    // PlanNode.debugString() is embedded by debug strings of the subclasses
    StringBuilder output = new StringBuilder();
    output.append("preds=" + Expr.debugString(conjuncts_));
    output.append(" limit=" + Long.toString(limit_));
    return output.toString();
  }

  protected String getExplainString(List<? extends Expr> exprs) {
    if (exprs == null) return "";
    StringBuilder output = new StringBuilder();
    for (int i = 0; i < exprs.size(); ++i) {
      if (i > 0) output.append(", ");
      output.append(exprs.get(i).toSql());
    }
    return output.toString();
  }

  /**
   * Returns true if stats-related variables are valid.
   */
  protected boolean hasValidStats() {
    return (numNodes_ == -1 || numNodes_ >= 0) &&
           (cardinality_ == -1 || cardinality_ >= 0);
  }

  /**
   * Computes and returns the sum of two long values. If an overflow occurs,
   * the maximum Long value is returned (Long.MAX_VALUE).
   */
  public static long checkedAdd(long a, long b) {
    try {
      return LongMath.checkedAdd(a, b);
    } catch (ArithmeticException e) {
      LOG.warn("overflow when adding longs: " + a + ", " + b);
      return Long.MAX_VALUE;
    }
  }

  /**
   * Computes and returns the product of two cardinalities. If an overflow
   * occurs, the maximum Long value is returned (Long.MAX_VALUE).
   */
  public static long checkedMultiply(long a, long b) {
    try {
      return LongMath.checkedMultiply(a, b);
    } catch (ArithmeticException e) {
      LOG.warn("overflow when multiplying longs: " + a + ", " + b);
      return Long.MAX_VALUE;
    }
  }

  /**
   * Returns true if this plan node can output its first row only after consuming
   * all rows of all its children. This method is used to group plan nodes
   * into pipelined units for resource estimation.
   */
  public boolean isBlockingNode() { return false; }

  /**
   * Compute peak resources consumed when executing this PlanNode, initializing
   * 'nodeResourceProfile_'. May only be called after this PlanNode has been placed in
   * a PlanFragment because the cost computation is dependent on the enclosing fragment's
   * data partition.
   */
  public abstract void computeNodeResourceProfile(TQueryOptions queryOptions);

  /**
   * Wrapper class to represent resource profiles during different phases of execution.
   */
  public static class ExecPhaseResourceProfiles {
    public ExecPhaseResourceProfiles(
        ResourceProfile duringOpenProfile, ResourceProfile postOpenProfile) {
      this.duringOpenProfile = duringOpenProfile;
      this.postOpenProfile = postOpenProfile;
    }

    /** Peak resources consumed while Open() is executing for this subtree */
    public final ResourceProfile duringOpenProfile;

    /**
     * Peak resources consumed for this subtree from the time when ExecNode::Open()
     * returns until the time when ExecNode::Close() returns.
     */
    public final ResourceProfile postOpenProfile;
  }

  /**
   * Recursive function used to compute the peak resources consumed by this subtree of
   * the plan within a fragment instance. The default implementation of this function
   * is correct for streaming and blocking PlanNodes with a single child. PlanNodes
   * that don't meet this description must override this function.
   *
   * Not called for PlanNodes inside a subplan: the root SubplanNode is responsible for
   * computing the peak resources for the entire subplan.
   *
   * computeNodeResourceProfile() must be called on all plan nodes in this subtree before
   * calling this function.
   */
  public ExecPhaseResourceProfiles computeTreeResourceProfiles(
      TQueryOptions queryOptions) {
    Preconditions.checkState(
        children_.size() <= 1, "Plan nodes with > 1 child must override");
    if (children_.isEmpty()) {
      return new ExecPhaseResourceProfiles(nodeResourceProfile_, nodeResourceProfile_);
    }
    ExecPhaseResourceProfiles childResources =
        getChild(0).computeTreeResourceProfiles(queryOptions);
    if (isBlockingNode()) {
      // This does not consume resources until after child's Open() returns. The child is
      // then closed before Open() of this node returns.
      ResourceProfile duringOpenProfile = childResources.duringOpenProfile.max(
          childResources.postOpenProfile.sum(nodeResourceProfile_));
      return new ExecPhaseResourceProfiles(duringOpenProfile, nodeResourceProfile_);
    } else {
      // Streaming node: this node, child and ancestor execute concurrently.
      return new ExecPhaseResourceProfiles(
          childResources.duringOpenProfile.sum(nodeResourceProfile_),
          childResources.postOpenProfile.sum(nodeResourceProfile_));
    }
  }

  /**
   * The default size of buffer used in spilling nodes. Used in
   * computeNodeResourceProfile().
   */
  protected final static long getDefaultSpillableBufferBytes() {
    // BufferedBlockMgr uses --read_size to determine buffer size.
    // TODO: IMPALA-3200: get from query option
    return BackendConfig.INSTANCE.getReadSize();
  }

  /**
   * The input cardinality is the sum of output cardinalities of its children.
   * For scan nodes the input cardinality is the expected number of rows scanned.
   */
  public long getInputCardinality() {
    long sum = 0;
    for(PlanNode p : children_) {
      long tmp = p.getCardinality();
      if (tmp == -1) return -1;
      sum = checkedAdd(sum, tmp);
    }
    return sum;
  }

  protected void addRuntimeFilter(RuntimeFilter filter) { runtimeFilters_.add(filter); }

  protected Collection<RuntimeFilter> getRuntimeFilters() { return runtimeFilters_; }

  protected String getRuntimeFilterExplainString(boolean isBuildNode) {
    if (runtimeFilters_.isEmpty()) return "";
    final String applyNodeFilterFormat = "%s -> %s";
    final String buildNodeFilterFormat = "%s <- %s";
    String format = isBuildNode ? buildNodeFilterFormat : applyNodeFilterFormat;
    StringBuilder output = new StringBuilder();
    List<String> filtersStr = Lists.newArrayList();
    for (RuntimeFilter filter: runtimeFilters_) {
      Expr expr = null;
      if (isBuildNode) {
        expr = filter.getSrcExpr();
      } else {
        expr = filter.getTargetExpr(getId());
      }
      Preconditions.checkNotNull(expr);
      filtersStr.add(String.format(format, filter.getFilterId(), expr.toSql()));
    }
    output.append(Joiner.on(", ").join(filtersStr) + "\n");
    return output.toString();
  }

  /**
   * Sort a list of conjuncts into an estimated cheapest order to evaluate them in, based
   * on estimates of the cost to evaluate and selectivity of the expressions. Should be
   * called during PlanNode.init for any PlanNode that could have a conjunct list.
   *
   * The conjuncts are sorted by repeatedly iterating over them and choosing the conjunct
   * that would result in the least total estimated work were it to be applied before the
   * remaining conjuncts.
   *
   * As in computeCombinedSelecivity, the selectivities are exponentially backed off over
   * the iterations, to reflect the possibility that the conjuncts may be correlated, and
   * Exprs without selectivity estimates are given a reasonable default.
   */
  public static <T extends Expr> List<T> orderConjunctsByCost(List<T> conjuncts) {
    if (conjuncts.size() <= 1) return conjuncts;

    float totalCost = 0;
    int numWithoutSel = 0;
    List<T> remaining = Lists.newArrayListWithCapacity(conjuncts.size());
    for (T e : conjuncts) {
      Preconditions.checkState(e.hasCost(), e.toSql());
      totalCost += e.getCost();
      remaining.add(e);
      if (!e.hasSelectivity()) {
        ++numWithoutSel;
      }
    }

    // We distribute the DEFAULT_SELECTIVITY over the conjuncts without a selectivity
    // estimate so that their combined selectivities equal DEFAULT_SELECTIVITY, i.e.
    // Math.pow(defaultSel, numWithoutSel) = Expr.DEFAULT_SELECTIVITY
    double defaultSel = Expr.DEFAULT_SELECTIVITY;
    if (numWithoutSel != 0) {
      defaultSel = Math.pow(Math.E, Math.log(Expr.DEFAULT_SELECTIVITY) / numWithoutSel);
    }

    List<T> sortedConjuncts = Lists.newArrayListWithCapacity(conjuncts.size());
    while (!remaining.isEmpty()) {
      double smallestCost = Float.MAX_VALUE;
      T bestConjunct =  null;
      double backoffExp = 1.0 / (double) (sortedConjuncts.size() + 1);
      for (T e : remaining) {
        double sel = Math.pow(e.hasSelectivity() ? e.getSelectivity() : defaultSel,
            backoffExp);

        // The cost of evaluating this conjunct first is estimated as the cost of
        // applying this conjunct to all rows plus the cost of applying all the
        // remaining conjuncts to the number of rows we expect to remain given
        // this conjunct's selectivity, exponentially backed off.
        double cost = e.getCost() + (totalCost - e.getCost()) * sel;
        if (cost < smallestCost) {
          smallestCost = cost;
          bestConjunct = e;
        } else if (cost == smallestCost) {
          // Break ties based on toSql() to get a consistent display in explain plans.
          if (e.toSql().compareTo(bestConjunct.toSql()) < 0) {
            smallestCost = cost;
            bestConjunct = e;
          }
        }
      }

      sortedConjuncts.add(bestConjunct);
      remaining.remove(bestConjunct);
      totalCost -= bestConjunct.getCost();
    }

    return sortedConjuncts;
  }

  public void setDisableCodegen(boolean disableCodegen) {
    disableCodegen_ = disableCodegen;
  }
}
