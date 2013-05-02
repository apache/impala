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

import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * An abstract representation of a table reference. The actual table reference could be
 * an inline view, or a base table, such as Hive table or HBase table. This abstract
 * representation of table also contains the JOIN specification.
 */
public abstract class TableRef implements ParseNode {
  // Table alias
  protected final String alias;

  protected JoinOperator joinOp;
  private ArrayList<String> joinHints;
  protected Expr onClause;
  protected List<String> usingColNames;

  // set after analyzeJoinHints(); true if explicitly set via hints
  private boolean isBroadcastJoin;
  private boolean isPartitionJoin;

  // the ref to the left of us, if we're part of a JOIN clause
  protected TableRef leftTblRef;

  // true if this TableRef has been analyzed; implementing subclass should set it to true
  // at the end of analyze() call.
  protected boolean isAnalyzed;

  // analysis output
  protected TupleDescriptor desc;

  public TableRef(String alias) {
    super();
    this.alias = alias;
    isAnalyzed = false;
  }

  public JoinOperator getJoinOp() {
    // if it's not explicitly set, we're doing an inner join
    return (joinOp == null ? JoinOperator.INNER_JOIN : joinOp);
  }

  public Expr getOnClause() {
    return onClause;
  }

  /**
   * This method should only be called after the TableRef has been analyzed.
   */
  public TupleDescriptor getDesc() {
    Preconditions.checkState(isAnalyzed);
    // after analyze(), desc should be set.
    Preconditions.checkState(desc != null);
    return desc;
  }

  /**
   * This method should only be called after the TableRef has been analyzed.
   */
  public TupleId getId() {
    Preconditions.checkState(isAnalyzed);
    // after analyze(), desc should be set.
    Preconditions.checkState(desc != null);
    return desc.getId();
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
    if (leftTblRef != null) {
      List<TupleId> result =
          Lists.newArrayList(leftTblRef.getAllMaterializedTupleIds());
      result.addAll(getMaterializedTupleIds());
      return result;
    } else {
      return getMaterializedTupleIds();
    }
  }

  public String getExplicitAlias() { return alias; }
  public Table getTable() { return desc.getTable(); }
  public void setJoinOp(JoinOperator op) { this.joinOp = op; }
  public void setOnClause(Expr e) { this.onClause = e; }
  public void setUsingClause(List<String> colNames) { this.usingColNames = colNames; }
  public TableRef getLeftTblRef() { return leftTblRef; }
  public void setLeftTblRef(TableRef leftTblRef) { this.leftTblRef = leftTblRef; }
  public void setJoinHints(ArrayList<String> hints) { this.joinHints = hints; }
  public boolean isBroadcastJoin() { return isBroadcastJoin; }
  public boolean isPartitionJoin() { return isPartitionJoin; }

  /**
   * Parse hints.
   */
  private void analyzeJoinHints() throws AnalysisException {
    if (joinHints == null) return;
    for (String hint: joinHints) {
      if (hint.toUpperCase().equals("BROADCAST")) {
        if (isPartitionJoin) {
          throw new AnalysisException("Conflicting JOIN hint: " + hint);
        }
        isBroadcastJoin = true;
      } else if (hint.toUpperCase().equals("SHUFFLE")) {
        if (isBroadcastJoin) {
          throw new AnalysisException("Conflicting JOIN hint: " + hint);
        }
        isPartitionJoin = true;
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
      throws AnalysisException, InternalException {
    Preconditions.checkState(desc != null);
    analyzeJoinHints();

    if (usingColNames != null) {
      // Turn USING clause into equivalent ON clause.
      Preconditions.checkState(onClause == null);
      for (String colName: usingColNames) {
        // check whether colName exists both for our table and the one
        // to the left of us
        if (leftTblRef.getDesc().getTable().getColumn(colName) == null) {
          throw new AnalysisException(
              "unknown column " + colName + " for alias "
              + leftTblRef.getAlias() + " (in \"" + this.toSql() + "\")");
        }
        if (desc.getTable().getColumn(colName) == null) {
          throw new AnalysisException(
              "unknown column " + colName + " for alias "
              + getAlias() + " (in \"" + this.toSql() + "\")");
        }

        // create predicate "<left>.colName = <right>.colName"
        BinaryPredicate eqPred =
            new BinaryPredicate(BinaryPredicate.Operator.EQ,
              new SlotRef(leftTblRef.getAliasAsName(), colName),
              new SlotRef(getAliasAsName(), colName));
        if (onClause == null) {
          onClause = eqPred;
        } else {
          onClause =
              new CompoundPredicate(CompoundPredicate.Operator.AND, onClause, eqPred);
        }
      }
    }

    // at this point, both 'this' and leftTblRef have been analyzed
    // and registered
    boolean lhsIsNullable = false;
    boolean rhsIsNullable = false;
    if (joinOp == JoinOperator.LEFT_OUTER_JOIN
        || joinOp == JoinOperator.FULL_OUTER_JOIN) {
      analyzer.registerOuterJoinedTids(getMaterializedTupleIds(), this);
      rhsIsNullable = true;
    }
    if (joinOp == JoinOperator.RIGHT_OUTER_JOIN
        || joinOp == JoinOperator.FULL_OUTER_JOIN) {
      analyzer.registerOuterJoinedTids(leftTblRef.getAllMaterializedTupleIds(), this);
      lhsIsNullable = true;
    }

    if (onClause != null) {
      onClause.analyze(analyzer);
      onClause.checkReturnsBool("ON clause", true);
      for (Expr e: onClause.getConjuncts()) {
        // Outer join clause conjuncts are registered for this particular table ref
        // (ie, can only be evaluated by the plan node that implements this join).
        // The exception are conjuncts that only pertain to the nullable side
        // of the outer join; those can be evaluated directly when materializing tuples
        // without violating outer join semantics.
        if (getJoinOp().isOuterJoin()) {
          if (lhsIsNullable && e.isBound(leftTblRef.getId())
              || rhsIsNullable && e.isBound(getId())) {
            analyzer.registerConjuncts(e, null, false);
          } else {
            analyzer.registerConjuncts(e, this, false);
          }
        } else {
          analyzer.registerConjuncts(e, null, false);
        }
      }
    } else if (getJoinOp().isOuterJoin() || getJoinOp() == JoinOperator.LEFT_SEMI_JOIN) {
      throw new AnalysisException(joinOpToSql() + " requires an ON or USING clause.");
    }

    // Make constant expressions from inline view refs nullable in its substitution map.
    if (lhsIsNullable && leftTblRef instanceof InlineViewRef) {
      ((InlineViewRef) leftTblRef).makeOutputNullable(analyzer);
    }
    if (rhsIsNullable && this instanceof InlineViewRef) {
      ((InlineViewRef) this).makeOutputNullable(analyzer);
    }
  }

  private String joinOpToSql() {
    Preconditions.checkState(joinOp != null);
    switch (joinOp) {
      case INNER_JOIN:
        return "INNER JOIN";
      case LEFT_OUTER_JOIN:
        return "LEFT OUTER JOIN";
      case LEFT_SEMI_JOIN:
        return "LEFT SEMI JOIN";
      case RIGHT_OUTER_JOIN:
        return "RIGHT OUTER JOIN";
      case FULL_OUTER_JOIN:
        return "FULL OUTER JOIN";
      default:
        return "bad join op: " + joinOp.toString();
    }
  }

  /**
   * Return the table ref presentation to be used in the toSql string
   */
  abstract protected String tableRefToSql();


  @Override
  public String toSql() {
    if (joinOp == null) {
      // prepend "," if we're part of a sequence of table refs w/o an
      // explicit JOIN clause
      return (leftTblRef != null ? ", " : "") + tableRefToSql();
    }

    StringBuilder output = new StringBuilder(" " + joinOpToSql() + " ");
    output.append(tableRefToSql()).append(" ");
    if (usingColNames != null) {
      output.append("USING (").append(Joiner.on(", ").join(usingColNames)).append(")");
    } else if (onClause != null) {
      output.append("ON (").append(onClause.toSql()).append(")");
    }
    return output.toString();
  }

  /**
   * Return alias by which table is referenced in select block.
   * @return
   */
  abstract public String getAlias();

  abstract public TableName getAliasAsName();

  /*
   * Gets the privilege requirement. This is always SELECT for TableRefs.
   */
  public Privilege getPrivilegeRequirement() {
    return Privilege.SELECT;
  }
}
