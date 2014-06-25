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
import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * An abstract representation of a table reference. The actual table reference could be
 * an inline view, or a base table, such as Hive table or HBase table. This abstract
 * representation of table also contains the JOIN specification.
 */
public abstract class TableRef implements ParseNode {
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

  public TableRef(TableName name, String alias) {
    super();
    Preconditions.checkArgument(name == null || !name.toString().isEmpty());
    Preconditions.checkArgument(alias == null || !alias.isEmpty());
    this.name_ = name;
    this.alias_ = alias;
    isAnalyzed_ = false;
  }

  /**
   * Creates and returns a empty TupleDescriptor registered with the analyzer. The
   * returned tuple descriptor must have its source table set via descTbl.setTable()).
   * This method is called from the analyzer when registering this table reference.
   */
  public abstract TupleDescriptor createTupleDescriptor(Analyzer analyzer)
      throws AnalysisException, AuthorizationException;

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
    this.onClause_ = (other.onClause_ != null) ? other.onClause_.clone() : null;
    isAnalyzed_ = false;
  }

  public JoinOperator getJoinOp() {
    // if it's not explicitly set, we're doing an inner join
    return (joinOp_ == null ? JoinOperator.INNER_JOIN : joinOp_);
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

  /**
   * Return the list of of materialized tuple ids from the TableRef.
   * This method should only be called after the TableRef has been analyzed.
   */
  abstract public List<TupleId> getMaterializedTupleIds();

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

  /**
   * Parse hints.
   */
  private void analyzeJoinHints() throws AnalysisException {
    if (joinHints_ == null) return;
    for (String hint: joinHints_) {
      if (hint.toUpperCase().equals("BROADCAST")) {
        if (joinOp_ == JoinOperator.RIGHT_OUTER_JOIN ||
            joinOp_ == JoinOperator.FULL_OUTER_JOIN) {
          throw new AnalysisException(joinOp_.toString() + " does not support BROADCAST.");
        }
        if (isPartitionedJoin_) {
          throw new AnalysisException("Conflicting JOIN hint: " + hint);
        }
        isBroadcastJoin_ = true;
      } else if (hint.toUpperCase().equals("SHUFFLE")) {
        if (joinOp_ == JoinOperator.CROSS_JOIN) {
          throw new AnalysisException("CROSS JOIN does not support SHUFFLE.");
        }
        if (isBroadcastJoin_) {
          throw new AnalysisException("Conflicting JOIN hint: " + hint);
        }
        isPartitionedJoin_ = true;
      } else {
        throw new AnalysisException("JOIN hint not recognized: " + hint);
      }
    }
  }

  /**
   * Analyze the join clause.
   * The join clause can only be analyzed after the left table has been analyzed
   * and the TupleDescriptor (desc) of this table has been created.
   */
  public void analyzeJoin(Analyzer analyzer)
      throws AnalysisException, InternalException, AuthorizationException {
    Preconditions.checkState(desc_ != null);
    analyzeJoinHints();
    if (joinOp_ == JoinOperator.CROSS_JOIN) {
      // A CROSS JOIN is always a broadcast join, regardless of the join hints
      isBroadcastJoin_ = true;
    }

    if (usingColNames_ != null) {
      Preconditions.checkState(joinOp_ != JoinOperator.CROSS_JOIN);
      // Turn USING clause into equivalent ON clause.
      Preconditions.checkState(onClause_ == null);
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
        if (onClause_ == null) {
          onClause_ = eqPred;
        } else {
          onClause_ =
              new CompoundPredicate(CompoundPredicate.Operator.AND, onClause_, eqPred);
        }
      }
    }

    // at this point, both 'this' and leftTblRef have been analyzed
    // and registered;
    // we register the tuple ids of the TableRefs on the nullable side of an outer join
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

    if (onClause_ != null) {
      Preconditions.checkState(joinOp_ != JoinOperator.CROSS_JOIN);
      onClause_.analyze(analyzer);
      onClause_.checkReturnsBool("ON clause", true);
      Set<TupleId> onClauseTupleIds = Sets.newHashSet();
      for (Expr e: onClause_.getConjuncts()) {
        // Outer join clause conjuncts are registered for this particular table ref
        // (ie, can only be evaluated by the plan node that implements this join).
        // The exception are conjuncts that only pertain to the nullable side
        // of the outer join; those can be evaluated directly when materializing tuples
        // without violating outer join semantics.
        // TODO: remove this special case
        if (getJoinOp().isOuterJoin()) {
          if (lhsIsNullable && e.isBound(leftTblRef_.getId())
              || rhsIsNullable && e.isBound(getId())) {
            analyzer.registerConjuncts(e, this, false);
          } else {
            analyzer.registerConjuncts(e, this, false);
          }
        } else {
          analyzer.registerConjuncts(e, null, false);
        }
        List<TupleId> tupleIds = Lists.newArrayList();
        e.getIds(tupleIds, null);
        onClauseTupleIds.addAll(tupleIds);
      }
      onClauseTupleIds_.addAll(onClauseTupleIds);
    } else if (getJoinOp().isOuterJoin() || getJoinOp() == JoinOperator.LEFT_SEMI_JOIN) {
      throw new AnalysisException(joinOpToSql() + " requires an ON or USING clause.");
    }

    // Make constant expressions from inline view refs nullable in its substitution map.
    if (lhsIsNullable && leftTblRef_ instanceof InlineViewRef) {
      ((InlineViewRef) leftTblRef_).makeOutputNullable(analyzer);
    }
    if (rhsIsNullable && this instanceof InlineViewRef) {
      ((InlineViewRef) this).makeOutputNullable(analyzer);
    }
  }

  private String joinOpToSql() {
    Preconditions.checkState(joinOp_ != null);
    switch (joinOp_) {
      case INNER_JOIN: return "INNER JOIN";
      case LEFT_OUTER_JOIN: return "LEFT OUTER JOIN";
      case LEFT_SEMI_JOIN: return "LEFT SEMI JOIN";
      case RIGHT_OUTER_JOIN: return "RIGHT OUTER JOIN";
      case FULL_OUTER_JOIN: return "FULL OUTER JOIN";
      case CROSS_JOIN: return "CROSS JOIN";
      default: return "bad join op: " + joinOp_.toString();
    }
  }

  /**
   * Return the table ref presentation to be used in the toSql string
   */
  abstract protected String tableRefToSql();

  @Override
  public String toSql() {
    if (joinOp_ == null) {
      // prepend "," if we're part of a sequence of table refs w/o an
      // explicit JOIN clause
      return (leftTblRef_ != null ? ", " : "") + tableRefToSql();
    }

    StringBuilder output = new StringBuilder(" " + joinOpToSql() + " ");
    output.append(tableRefToSql());
    if (usingColNames_ != null) {
      output.append(" USING (").append(Joiner.on(", ").join(usingColNames_)).append(")");
    } else if (onClause_ != null) {
      output.append(" ON ").append(onClause_.toSql());
    }
    return output.toString();
  }

  /**
   * Returns the name of the table referred to. Before analysis, the table name
   * may not be fully qualified. If the table name is unqualified, the current
   * default database from the analyzer will be used as the db name.
   */
  public TableName getName() { return name_; }

  /**
   * Replaces name_ with the fully-qualified table name.
   */
  public void setFullyQualifiedTableName(Analyzer analyzer) {
    name_ = analyzer.getFullyQualifiedTableName(name_);
  }

  /**
   * Returns true if this table ref has an explicit table alias.
   */
  public boolean hasExplicitAlias() { return alias_ != null; }

  /**
   * Return the explicit alias of this table ref if one was given, otherwise the
   * fully-qualified table name.
   */
  public String getAlias() {
    if (alias_ == null) {
      return name_.toString().toLowerCase();
    } else {
      return alias_;
    }
  }

  public TableName getAliasAsName() {
    if (alias_ != null) {
      return new TableName(null, alias_);
    } else {
      return name_;
    }
  }

  @Override
  public abstract TableRef clone();

  /**
   * Gets the privilege requirement. This is always SELECT for TableRefs.
   */
  public Privilege getPrivilegeRequirement() { return Privilege.SELECT; }
}
