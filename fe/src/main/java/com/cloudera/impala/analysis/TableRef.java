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

import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.Pair;
import com.cloudera.impala.planner.PlanNode;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Superclass of all table references, including references to inline views, catalog or
 * local views, or base tables (Hdfs, HBase or DataSource tables). Contains the join
 * specification. An instance of a TableRef (and not a subclass thereof) represents
 * an unresolved table reference that must be resolved during analysis. All resolved
 * table references are subclasses of TableRef.
 */
public class TableRef implements ParseNode {
  // Table or view name that is fully qualified in analyze().
  // Equivalent to alias_ for local view refs.
  protected TableName name_;
  protected final String alias_;

  protected JoinOperator joinOp_;
  protected ArrayList<String> joinHints_;
  protected Expr onClause_;
  protected List<String> usingColNames_;

  // set after analyzeJoinHints(); true if explicitly set via hints
  private boolean isBroadcastJoin_;
  private boolean isPartitionedJoin_;

  // the ref to the left of us, if we're part of a JOIN clause
  protected TableRef leftTblRef_;

  // true if this TableRef has been analyzed; implementing subclass should set it to true
  // at the end of analyze() call.
  protected boolean isAnalyzed_;

  // all (logical) TupleIds referenced in the On clause
  protected List<TupleId> onClauseTupleIds_ = Lists.newArrayList();

  // analysis output
  protected TupleDescriptor desc_;

  public TableRef(TableName tableName, String alias) {
    super();
    name_ = tableName;
    alias_ = alias;
    isAnalyzed_ = false;
  }

  /**
   * C'tor for cloning.
   */
  protected TableRef(TableRef other) {
    super();
    this.name_ = other.name_;
    this.alias_ = other.alias_;
    this.joinOp_ = other.joinOp_;
    this.joinHints_ =
        (other.joinHints_ != null) ? Lists.newArrayList(other.joinHints_) : null;
    this.usingColNames_ =
        (other.usingColNames_ != null) ? Lists.newArrayList(other.usingColNames_) : null;
    this.onClause_ = (other.onClause_ != null) ? other.onClause_.clone().reset() : null;
    isAnalyzed_ = false;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    throw new AnalysisException("Unresolved table reference: " + tableRefToSql());
  }

  /**
   * Creates and returns a empty TupleDescriptor registered with the analyzer. The
   * returned tuple descriptor must have its source table set via descTbl.setTable()).
   * This method is called from the analyzer when registering this table reference.
   */
  public TupleDescriptor createTupleDescriptor(Analyzer analyzer)
      throws AnalysisException {
    throw new AnalysisException("Unresolved table reference: " + tableRefToSql());
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

  public TableName getName() { return name_; }

  /**
   * Replaces name_ with the fully-qualified table name.
   */
  protected void setFullyQualifiedTableName(Analyzer analyzer) {
    name_ = analyzer.getFqTableName(name_);
  }

  /**
   * Returns true if this table ref has an explicit alias.
   */
  public boolean hasExplicitAlias() { return alias_ != null; }

  /**
   * Return alias by which this table is referenced in select block.
   */
  public String getAlias() {
    if (alias_ == null) return name_.toString().toLowerCase();
    return alias_;
  }

  public TableName getAliasAsName() {
    if (alias_ != null) return new TableName(null, alias_);
    return name_;
  }

  public ArrayList<String> getJoinHints() { return joinHints_; }
  public Expr getOnClause() { return onClause_; }
  public List<String> getUsingClause() { return usingColNames_; }
  public String getExplicitAlias() { return alias_; }
  public Table getTable() { return getDesc().getTable(); }
  public void setJoinOp(JoinOperator op) { this.joinOp_ = op; }
  public void setOnClause(Expr e) { this.onClause_ = e; }
  public void setUsingClause(List<String> colNames) { this.usingColNames_ = colNames; }
  public TableRef getLeftTblRef() { return leftTblRef_; }
  public void setLeftTblRef(TableRef leftTblRef) { this.leftTblRef_ = leftTblRef; }
  public void setJoinHints(ArrayList<String> hints) { this.joinHints_ = hints; }
  public boolean isBroadcastJoin() { return isBroadcastJoin_; }
  public boolean isPartitionedJoin() { return isPartitionedJoin_; }
  public List<TupleId> getOnClauseTupleIds() { return onClauseTupleIds_; }
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
    Preconditions.checkState(desc_ != null);
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
        if (isPartitionedJoin_) {
          throw new AnalysisException("Conflicting JOIN hint: " + hint);
        }
        isBroadcastJoin_ = true;
        analyzer.setHasPlanHints();
      } else if (hint.equalsIgnoreCase("SHUFFLE")) {
        if (joinOp_ == JoinOperator.CROSS_JOIN) {
          throw new AnalysisException("CROSS JOIN does not support SHUFFLE.");
        }
        if (isBroadcastJoin_) {
          throw new AnalysisException("Conflicting JOIN hint: " + hint);
        }
        isPartitionedJoin_ = true;
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
      isBroadcastJoin_ = true;
    }

    if (usingColNames_ != null) {
      Preconditions.checkState(joinOp_ != JoinOperator.CROSS_JOIN);
      // Turn USING clause into equivalent ON clause.
      onClause_ = null;
      for (String colName: usingColNames_) {
        // check whether colName exists both for our table and the one
        // to the left of us
        if (leftTblRef_.getDesc().getTable().getColumn(colName) == null) {
          throw new AnalysisException(
              "unknown column " + colName + " for alias "
              + leftTblRef_.getAlias() + " (in \"" + this.toSql() + "\")");
        }
        if (desc_.getTable().getColumn(colName) == null) {
          throw new AnalysisException(
              "unknown column " + colName + " for alias "
              + getAlias() + " (in \"" + this.toSql() + "\")");
        }

        // create predicate "<left>.colName = <right>.colName"
        BinaryPredicate eqPred =
            new BinaryPredicate(BinaryPredicate.Operator.EQ,
              new SlotRef(leftTblRef_.getAliasAsName(), colName),
              new SlotRef(getAliasAsName(), colName));
        onClause_ = CompoundPredicate.createConjunction(eqPred, onClause_);
      }
    }

    // at this point, both 'this' and leftTblRef have been analyzed and registered;
    // register the tuple ids of the TableRefs on the nullable side of an outer join
    boolean lhsIsNullable = false;
    boolean rhsIsNullable = false;
    if (joinOp_ == JoinOperator.LEFT_OUTER_JOIN
        || joinOp_ == JoinOperator.FULL_OUTER_JOIN) {
      analyzer.registerOuterJoinedTids(getId().asList(), this);
      rhsIsNullable = true;
    }
    if (joinOp_ == JoinOperator.RIGHT_OUTER_JOIN
        || joinOp_ == JoinOperator.FULL_OUTER_JOIN) {
      analyzer.registerOuterJoinedTids(leftTblRef_.getAllTupleIds(), this);
      lhsIsNullable = true;
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
    } else if (getJoinOp().isOuterJoin() || getJoinOp().isSemiJoin()) {
      throw new AnalysisException(joinOp_.toString() + " requires an ON or USING clause.");
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
    // Enclose the alias in quotes if Hive cannot parse it without quotes.
    // This is needed for view compatibility between Impala and Hive.
    String aliasSql = null;
    if (alias_ != null) aliasSql = ToSqlUtils.getIdentSql(alias_);
    return name_.toSql() + ((aliasSql != null) ? " " + aliasSql : "");
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

  @Override
  public TableRef clone() { return new TableRef(this); }
}
