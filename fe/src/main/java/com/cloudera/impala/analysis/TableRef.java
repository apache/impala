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

package com.cloudera.impala.analysis;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.cloudera.impala.analysis.Path.PathType;
import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.authorization.PrivilegeRequestBuilder;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.catalog.TableLoadingException;
import com.cloudera.impala.catalog.View;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.Pair;
import com.cloudera.impala.planner.PlanNode;
import com.cloudera.impala.planner.JoinNode.DistributionMode;
import com.cloudera.impala.thrift.TAccessEvent;
import com.cloudera.impala.thrift.TCatalogObjectType;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Superclass of all table references, including references to views, base tables
 * (Hdfs, HBase or DataSource tables), and nested collections. Contains the join
 * specification. An instance of a TableRef (and not a subclass thereof) represents
 * an unresolved table reference that must be resolved during analysis. All resolved
 * table references are subclasses of TableRef.
 *
 * The analysis of table refs follows a two-step process:
 *
 * 1. Resolution: A table ref's path is resolved and then the generic TableRef is
 * replaced by a concrete table ref (a BaseTableRef, CollectionTabeRef or ViewRef)
 * in the originating stmt and that is given the resolved path. This step is driven by
 * Analyzer.resolveTableRef() which calls into TableRef.analyze().
 *
 * 2. Analysis/registration: After resolution, the concrete table ref is analyzed
 * to register a tuple descriptor for its resolved path and register other table-ref
 * specific state with the analyzer (e.g., whether it is outer/semi joined, etc.).
 *
 * Therefore, subclasses of TableRef should never call the analyze() of its superclass.
 *
 * TODO for 2.3: The current TableRef class hierarchy and the related two-phase analysis
 * feels convoluted and is hard to follow. We should reorganize the TableRef class
 * structure for clarity of analysis and avoid a table ref 'switching genders' in between
 * resolution and registration.
 *
 * TODO for 2.3: Rename this class to CollectionRef and re-consider the naming and
 * structure of all subclasses.
 */
public class TableRef implements ParseNode {
  // Path to a collection type. Not set for inline views.
  protected List<String> rawPath_;

  // Legal aliases of this table ref. Contains the explicit alias as its sole element if
  // there is one. Otherwise, contains the two implicit aliases. Implicit aliases are set
  // in the c'tor of the corresponding resolved table ref (subclasses of TableRef) during
  // analysis. By convention, for table refs with multiple implicit aliases, aliases_[0]
  // contains the fully-qualified implicit alias to ensure that aliases_[0] always
  // uniquely identifies this table ref regardless of whether it has an explicit alias.
  protected String[] aliases_;

  // Indicates whether this table ref is given an explicit alias,
  protected boolean hasExplicitAlias_;

  protected JoinOperator joinOp_;
  protected ArrayList<String> joinHints_;
  protected List<String> usingColNames_;

  // Hinted distribution mode for this table ref; set after analyzeJoinHints()
  // TODO: Move join-specific members out of TableRef.
  private DistributionMode distrMode_ = DistributionMode.NONE;

  /////////////////////////////////////////
  // BEGIN: Members that need to be reset()

  // Resolution of rawPath_ if applicable. Result of analysis.
  protected Path resolvedPath_;

  protected Expr onClause_;

  // the ref to the left of us, if we're part of a JOIN clause
  protected TableRef leftTblRef_;

  // true if this TableRef has been analyzed; implementing subclass should set it to true
  // at the end of analyze() call.
  protected boolean isAnalyzed_;

  // all (logical) TupleIds referenced in the On clause
  protected List<TupleId> onClauseTupleIds_ = Lists.newArrayList();

  // All physical tuple ids that this table ref is correlated with:
  // Tuple ids of root descriptors from outer query blocks that this table ref
  // (if a CollectionTableRef) or contained CollectionTableRefs (if an InlineViewRef)
  // are rooted at. Populated during analysis.
  protected List<TupleId> correlatedTupleIds_ = Lists.newArrayList();

  // analysis output
  protected TupleDescriptor desc_;

  // END: Members that need to be reset()
  /////////////////////////////////////////

  public TableRef(List<String> path, String alias) {
    super();
    rawPath_ = path;
    if (alias != null) {
      aliases_ = new String[] { alias.toLowerCase() };
      hasExplicitAlias_ = true;
    } else {
      hasExplicitAlias_ = false;
    }
    isAnalyzed_ = false;
  }

  /**
   * C'tor for cloning.
   */
  protected TableRef(TableRef other) {
    rawPath_ = other.rawPath_;
    resolvedPath_ = other.resolvedPath_;
    aliases_ = other.aliases_;
    hasExplicitAlias_ = other.hasExplicitAlias_;
    joinOp_ = other.joinOp_;
    joinHints_ =
        (other.joinHints_ != null) ? Lists.newArrayList(other.joinHints_) : null;
    onClause_ = (other.onClause_ != null) ? other.onClause_.clone() : null;
    usingColNames_ =
        (other.usingColNames_ != null) ? Lists.newArrayList(other.usingColNames_) : null;
    distrMode_ = other.distrMode_;
    // The table ref links are created at the statement level, so cloning a set of linked
    // table refs is the responsibility of the statement.
    leftTblRef_ = null;
    isAnalyzed_ = other.isAnalyzed_;
    onClauseTupleIds_ = Lists.newArrayList(other.onClauseTupleIds_);
    correlatedTupleIds_ = Lists.newArrayList(other.correlatedTupleIds_);
    desc_ = other.desc_;
  }

  /**
   * Resolves this table ref's raw path and adds privilege requests and audit events
   * for the referenced catalog entities.
   */
  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    try {
      resolvedPath_ = analyzer.resolvePath(rawPath_, PathType.TABLE_REF);
    } catch (AnalysisException e) {
      if (!analyzer.hasMissingTbls()) {
        // Register privilege requests to prefer reporting an authorization error over
        // an analysis error. We should not accidentally reveal the non-existence of a
        // table/database if the user is not authorized.
        if (rawPath_.size() > 1) {
          analyzer.registerPrivReq(new PrivilegeRequestBuilder()
              .onTable(rawPath_.get(0), rawPath_.get(1))
              .allOf(getPrivilegeRequirement()).toRequest());
        }
        analyzer.registerPrivReq(new PrivilegeRequestBuilder()
            .onTable(analyzer.getDefaultDb(), rawPath_.get(0))
            .allOf(getPrivilegeRequirement()).toRequest());
      }
      throw e;
    } catch (TableLoadingException e) {
      throw new AnalysisException(String.format(
          "Failed to load metadata for table: '%s'", Joiner.on(".").join(rawPath_)), e);
    }

    if (resolvedPath_.getRootTable() != null) {
      // Add access event for auditing.
      Table table = resolvedPath_.getRootTable();
      if (table instanceof View) {
        View view = (View) table;
        if (!view.isLocalView()) {
          analyzer.addAccessEvent(new TAccessEvent(
              table.getFullName(), TCatalogObjectType.VIEW,
              getPrivilegeRequirement().toString()));
        }
      } else {
        analyzer.addAccessEvent(new TAccessEvent(
            table.getFullName(), TCatalogObjectType.TABLE,
            getPrivilegeRequirement().toString()));
      }

      // Add privilege requests for authorization.
      TableName tableName = table.getTableName();
      analyzer.registerPrivReq(new PrivilegeRequestBuilder()
          .onTable(tableName.getDb(), tableName.getTbl())
          .allOf(getPrivilegeRequirement()).toRequest());
    }
  }

  /**
   * Creates and returns a empty TupleDescriptor registered with the analyzer
   * based on the resolvedPath_.
   * This method is called from the analyzer when registering this table reference.
   */
  public TupleDescriptor createTupleDescriptor(Analyzer analyzer)
      throws AnalysisException {
    TupleDescriptor result = analyzer.getDescTbl().createTupleDescriptor(
        getClass().getSimpleName() + " " + getUniqueAlias());
    result.setPath(resolvedPath_);
    return result;
  }

  /**
   * Set this table's context-dependent join attributes from the given table.
   * Does not clone the attributes.
   */
  protected void setJoinAttrs(TableRef other) {
    this.joinOp_ = other.joinOp_;
    this.joinHints_ = other.joinHints_;
    this.onClause_ = other.onClause_;
    this.usingColNames_ = other.usingColNames_;
  }

  public JoinOperator getJoinOp() {
    // if it's not explicitly set, we're doing an inner join
    return (joinOp_ == null ? JoinOperator.INNER_JOIN : joinOp_);
  }

  /**
   * Returns true if this table ref has a resolved path that is rooted at a registered
   * tuple descriptor, false otherwise.
   */
  public boolean isRelative() { return false; }

  /**
   * Indicates if this TableRef directly or indirectly references another TableRef from
   * an outer query block.
   */
  public boolean isCorrelated() { return !correlatedTupleIds_.isEmpty(); }

  public List<String> getPath() { return rawPath_; }
  public Path getResolvedPath() { return resolvedPath_; }

  /**
   * Returns all legal aliases of this table ref.
   */
  public String[] getAliases() { return aliases_; }

  /**
   * Returns the explicit alias or the fully-qualified implicit alias. The returned alias
   * is guaranteed to be unique (i.e., column/field references against the alias cannot
   * be ambiguous).
   */
  public String getUniqueAlias() { return aliases_[0]; }

  /**
   * Returns true if this table ref has an explicit alias.
   * Note that getAliases().length() == 1 does not imply an explicit alias because
   * nested collection refs have only a single implicit alias.
   */
  public boolean hasExplicitAlias() { return hasExplicitAlias_; }

  /**
   * Returns the explicit alias if this table ref has one, null otherwise.
   */
  public String getExplicitAlias() {
    if (hasExplicitAlias()) return getUniqueAlias();
    return null;
  }

  public Table getTable() {
    Preconditions.checkNotNull(resolvedPath_);
    return resolvedPath_.getRootTable();
  }
  public ArrayList<String> getJoinHints() { return joinHints_; }
  public Expr getOnClause() { return onClause_; }
  public List<String> getUsingClause() { return usingColNames_; }
  public void setJoinOp(JoinOperator op) { this.joinOp_ = op; }
  public void setOnClause(Expr e) { this.onClause_ = e; }
  public void setUsingClause(List<String> colNames) { this.usingColNames_ = colNames; }
  public TableRef getLeftTblRef() { return leftTblRef_; }
  public void setLeftTblRef(TableRef leftTblRef) { this.leftTblRef_ = leftTblRef; }
  public void setJoinHints(ArrayList<String> hints) { this.joinHints_ = hints; }
  public boolean isBroadcastJoin() { return distrMode_ == DistributionMode.BROADCAST; }
  public boolean isPartitionedJoin() {
    return distrMode_ == DistributionMode.PARTITIONED;
  }
  public DistributionMode getDistributionMode() { return distrMode_; }
  public List<TupleId> getOnClauseTupleIds() { return onClauseTupleIds_; }
  public List<TupleId> getCorrelatedTupleIds() { return correlatedTupleIds_; }
  public boolean isAnalyzed() { return isAnalyzed_; }
  public boolean isResolved() { return !getClass().equals(TableRef.class); }

  /**
   * This method should only be called after the TableRef has been analyzed.
   */
  public TupleDescriptor getDesc() {
    Preconditions.checkState(isAnalyzed_);
    // after analyze(), desc should be set.
    Preconditions.checkState(desc_ != null);
    return desc_;
  }

  /**
   * This method should only be called after the TableRef has been analyzed.
   */
  public TupleId getId() {
    Preconditions.checkState(isAnalyzed_);
    // after analyze(), desc should be set.
    Preconditions.checkNotNull(desc_);
    return desc_.getId();
  }

  public List<TupleId> getMaterializedTupleIds() {
    // This function should only be called after analyze().
    Preconditions.checkState(isAnalyzed_);
    Preconditions.checkNotNull(desc_);
    return desc_.getId().asList();
  }

  /**
   * Return the list of tuple ids materialized by the full sequence of
   * table refs up to this one.
   */
  public List<TupleId> getAllMaterializedTupleIds() {
    if (leftTblRef_ != null) {
      List<TupleId> result =
          Lists.newArrayList(leftTblRef_.getAllMaterializedTupleIds());
      result.addAll(getMaterializedTupleIds());
      return result;
    } else {
      return getMaterializedTupleIds();
    }
  }

  /**
   * Return the list of tuple ids of the full sequence of table refs up to this one.
   */
  public List<TupleId> getAllTupleIds() {
    Preconditions.checkState(isAnalyzed_);
    if (leftTblRef_ != null) {
      List<TupleId> result = leftTblRef_.getAllTupleIds();
      result.add(desc_.getId());
      return result;
    } else {
      return Lists.newArrayList(desc_.getId());
    }
  }

  private void analyzeJoinHints(Analyzer analyzer) throws AnalysisException {
    if (joinHints_ == null) return;
    for (String hint: joinHints_) {
      if (hint.equalsIgnoreCase("BROADCAST")) {
        if (joinOp_ == JoinOperator.RIGHT_OUTER_JOIN
            || joinOp_ == JoinOperator.FULL_OUTER_JOIN
            || joinOp_ == JoinOperator.RIGHT_SEMI_JOIN
            || joinOp_ == JoinOperator.RIGHT_ANTI_JOIN) {
          throw new AnalysisException(
              joinOp_.toString() + " does not support BROADCAST.");
        }
        if (isPartitionedJoin()) {
          throw new AnalysisException("Conflicting JOIN hint: " + hint);
        }
        distrMode_ = DistributionMode.BROADCAST;
        analyzer.setHasPlanHints();
      } else if (hint.equalsIgnoreCase("SHUFFLE")) {
        if (joinOp_ == JoinOperator.CROSS_JOIN) {
          throw new AnalysisException("CROSS JOIN does not support SHUFFLE.");
        }
        if (isBroadcastJoin()) {
          throw new AnalysisException("Conflicting JOIN hint: " + hint);
        }
        distrMode_ = DistributionMode.PARTITIONED;
        analyzer.setHasPlanHints();
      } else {
        analyzer.addWarning("JOIN hint not recognized: " + hint);
      }
    }
  }

  /**
   * Analyze the join clause.
   * The join clause can only be analyzed after the left table has been analyzed
   * and the TupleDescriptor (desc) of this table has been created.
   */
  public void analyzeJoin(Analyzer analyzer) throws AnalysisException {
    Preconditions.checkState(desc_ != null);
    analyzeJoinHints(analyzer);
    if (joinOp_ == JoinOperator.CROSS_JOIN) {
      // A CROSS JOIN is always a broadcast join, regardless of the join hints
      distrMode_ = DistributionMode.BROADCAST;
    }

    if (usingColNames_ != null) {
      Preconditions.checkState(joinOp_ != JoinOperator.CROSS_JOIN);
      // Turn USING clause into equivalent ON clause.
      onClause_ = null;
      for (String colName: usingColNames_) {
        // check whether colName exists both for our table and the one
        // to the left of us
        Path leftColPath = new Path(leftTblRef_.getDesc(),
            Lists.newArrayList(colName.toLowerCase()));
        if (!leftColPath.resolve()) {
          throw new AnalysisException(
              "unknown column " + colName + " for alias "
              + leftTblRef_.getUniqueAlias() + " (in \"" + this.toSql() + "\")");
        }
        Path rightColPath = new Path(desc_,
            Lists.newArrayList(colName.toLowerCase()));
        if (!rightColPath.resolve()) {
          throw new AnalysisException(
              "unknown column " + colName + " for alias "
              + getUniqueAlias() + " (in \"" + this.toSql() + "\")");
        }

        // create predicate "<left>.colName = <right>.colName"
        BinaryPredicate eqPred =
            new BinaryPredicate(BinaryPredicate.Operator.EQ,
              new SlotRef(Path.createRawPath(leftTblRef_.getUniqueAlias(), colName)),
              new SlotRef(Path.createRawPath(getUniqueAlias(), colName)));
        onClause_ = CompoundPredicate.createConjunction(eqPred, onClause_);
      }
    }

    // at this point, both 'this' and leftTblRef have been analyzed and registered;
    // register the tuple ids of the TableRefs on the nullable side of an outer join
    if (joinOp_ == JoinOperator.LEFT_OUTER_JOIN
        || joinOp_ == JoinOperator.FULL_OUTER_JOIN) {
      analyzer.registerOuterJoinedTids(getId().asList(), this);
    }
    if (joinOp_ == JoinOperator.RIGHT_OUTER_JOIN
        || joinOp_ == JoinOperator.FULL_OUTER_JOIN) {
      analyzer.registerOuterJoinedTids(leftTblRef_.getAllTupleIds(), this);
    }
    // register the tuple ids of a full outer join
    if (joinOp_ == JoinOperator.FULL_OUTER_JOIN) {
      analyzer.registerFullOuterJoinedTids(leftTblRef_.getAllTupleIds(), this);
      analyzer.registerFullOuterJoinedTids(getId().asList(), this);
    }
    // register the tuple id of the rhs of a left semi join
    TupleId semiJoinedTupleId = null;
    if (joinOp_ == JoinOperator.LEFT_SEMI_JOIN
        || joinOp_ == JoinOperator.LEFT_ANTI_JOIN
        || joinOp_ == JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN) {
      analyzer.registerSemiJoinedTid(getId(), this);
      semiJoinedTupleId = getId();
    }
    // register the tuple id of the lhs of a right semi join
    if (joinOp_ == JoinOperator.RIGHT_SEMI_JOIN
        || joinOp_ == JoinOperator.RIGHT_ANTI_JOIN) {
      analyzer.registerSemiJoinedTid(leftTblRef_.getId(), this);
      semiJoinedTupleId = leftTblRef_.getId();
    }

    if (onClause_ != null) {
      Preconditions.checkState(joinOp_ != JoinOperator.CROSS_JOIN);
      analyzer.setVisibleSemiJoinedTuple(semiJoinedTupleId);
      onClause_.analyze(analyzer);
      analyzer.setVisibleSemiJoinedTuple(null);
      onClause_.checkReturnsBool("ON clause", true);
        if (onClause_.contains(Expr.isAggregatePredicate())) {
          throw new AnalysisException(
              "aggregate function not allowed in ON clause: " + toSql());
      }
      if (onClause_.contains(AnalyticExpr.class)) {
        throw new AnalysisException(
            "analytic expression not allowed in ON clause: " + toSql());
      }
      Set<TupleId> onClauseTupleIds = Sets.newHashSet();
      for (Expr e: onClause_.getConjuncts()) {
        // Outer join clause conjuncts are registered for this particular table ref
        // (ie, can only be evaluated by the plan node that implements this join).
        // The exception are conjuncts that only pertain to the nullable side
        // of the outer join; those can be evaluated directly when materializing tuples
        // without violating outer join semantics.
        analyzer.registerOnClauseConjuncts(e, this);
        List<TupleId> tupleIds = Lists.newArrayList();
        e.getIds(tupleIds, null);
        onClauseTupleIds.addAll(tupleIds);
      }
      onClauseTupleIds_.addAll(onClauseTupleIds);
    } else if (!isRelative()
        && (getJoinOp().isOuterJoin() || getJoinOp().isSemiJoin())) {
      throw new AnalysisException(
          joinOp_.toString() + " requires an ON or USING clause.");
    }
  }

  /**
   * Inverts the join whose rhs is represented by this table ref. If necessary, this
   * function modifies the registered analysis state associated with this table ref,
   * as well as the chain of left table references in refPlans as appropriate.
   * Requires that this is the very first join in a series of joins.
   */
  public void invertJoin(List<Pair<TableRef, PlanNode>> refPlans, Analyzer analyzer) {
    // Assert that this is the first join in a series of joins.
    Preconditions.checkState(leftTblRef_.leftTblRef_ == null);
    // Find a table ref that references 'this' as its left table (if any) and change
    // it to reference 'this.leftTblRef_ 'instead, because 'this.leftTblRef_' will
    // become the new rhs of the inverted join.
    for (Pair<TableRef, PlanNode> refPlan: refPlans) {
      if (refPlan.first.leftTblRef_ == this) {
        refPlan.first.setLeftTblRef(leftTblRef_);
        break;
      }
    }
    if (joinOp_.isOuterJoin()) analyzer.invertOuterJoinState(this, leftTblRef_);
    leftTblRef_.setJoinOp(getJoinOp().invert());
    leftTblRef_.setLeftTblRef(this);
    leftTblRef_.setOnClause(onClause_);
    joinOp_ = null;
    leftTblRef_ = null;
    onClause_ = null;
  }

  protected String tableRefToSql() {
    String aliasSql = null;
    String alias = getExplicitAlias();
    if (alias != null) aliasSql = ToSqlUtils.getIdentSql(alias);
    List<String> path = rawPath_;
    if (resolvedPath_ != null) path = resolvedPath_.getFullyQualifiedRawPath();
    return ToSqlUtils.getPathSql(path) + ((aliasSql != null) ? " " + aliasSql : "");
  }

  @Override
  public String toSql() {
    if (joinOp_ == null) {
      // prepend "," if we're part of a sequence of table refs w/o an
      // explicit JOIN clause
      return (leftTblRef_ != null ? ", " : "") + tableRefToSql();
    }

    StringBuilder output = new StringBuilder(" " + joinOp_.toString() + " ");
    if(joinHints_ != null) output.append(ToSqlUtils.getPlanHintsSql(joinHints_) + " ");
    output.append(tableRefToSql());
    if (usingColNames_ != null) {
      output.append(" USING (").append(Joiner.on(", ").join(usingColNames_)).append(")");
    } else if (onClause_ != null) {
      output.append(" ON ").append(onClause_.toSql());
    }
    return output.toString();
  }

  /**
   * Gets the privilege requirement. This is always SELECT for TableRefs.
   */
  public Privilege getPrivilegeRequirement() { return Privilege.SELECT; }

  /**
   * Returns a deep clone of this table ref without also cloning the chain of table refs.
   * Sets leftTblRef_ in the returned clone to null.
   */
  @Override
  protected TableRef clone() { return new TableRef(this); }

  /**
   * Deep copies the given list of table refs and returns the clones in a new list.
   * The linking structure in the original table refs is preserved in the clones,
   * i.e., if the table refs were originally linked, then the corresponding clones
   * are linked in the same way. Similarly, if the original table refs were not linked
   * then the clones are also not linked.
   * Assumes that the given table refs are self-contained with respect to linking, i.e.,
   * that no table ref links to another table ref not in the list.
   */
  public static List<TableRef> cloneTableRefList(List<TableRef> tblRefs) {
    List<TableRef> clonedTblRefs = Lists.newArrayListWithCapacity(tblRefs.size());
    TableRef leftTblRef = null;
    for (TableRef tblRef: tblRefs) {
      TableRef tblRefClone = tblRef.clone();
      clonedTblRefs.add(tblRefClone);
      if (tblRef.leftTblRef_ != null) {
        Preconditions.checkState(tblRefs.contains(tblRef.leftTblRef_));
        tblRefClone.leftTblRef_ = leftTblRef;
      }
      leftTblRef = tblRefClone;
    }
    return clonedTblRefs;
  }

  public void reset() {
    isAnalyzed_ = false;
    resolvedPath_ = null;
    if (usingColNames_ != null) {
      // The using col names are converted into an on-clause predicate during analysis,
      // so unset the on-clause here.
      onClause_ = null;
    } else if (onClause_ != null) {
      onClause_.reset();
    }
    leftTblRef_ = null;
    onClauseTupleIds_.clear();
    desc_ = null;
    correlatedTupleIds_.clear();
  }
}
