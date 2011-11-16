// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import java.util.List;

import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.common.AnalysisException;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class TableRef extends ParseNodeBase {
  private final TableName name;
  private final String alias;

  private JoinOperator joinOp;
  private Predicate onClause;
  private List<String> usingColNames;

  // the ref to the left of us, if we're part of a JOIN clause
  private TableRef leftTblRef;

  private TupleDescriptor desc;  // analysis output

  // conjuncts from the JOIN clause:
  // 1. equi-join predicates
  private List<Predicate> eqJoinConjuncts;
  // 2. the rest
  private List<Predicate> otherJoinConjuncts;

  public TableRef(TableName name, String alias) {
    super();
    Preconditions.checkArgument(!name.toString().isEmpty());
    this.name = name;
    Preconditions.checkArgument(alias == null || !alias.isEmpty());
    this.alias = alias;
  }

  public JoinOperator getJoinOp() {
    // if it's not explicitly set, we're doing an inner join
    return (joinOp == null ? JoinOperator.INNER_JOIN : joinOp);
  }

  public Predicate getOnClause() {
    return onClause;
  }

  public TupleDescriptor getDesc() {
    return desc;
  }

  public TupleId getId() {
    return desc.getId();
  }

  public TableName getName() {
    return name;
  }

  public String getExplicitAlias() {
    return alias;
  }

  public Table getTable() {
    return desc.getTable();
  }

  public void setJoinOp(JoinOperator op) {
    this.joinOp = op;
  }

  public void setOnClause(Predicate pred) {
    this.onClause = pred;
  }

  public void setUsingClause(List<String> colNames) {
    this.usingColNames = colNames;
  }

  public void setLeftTblRef(TableRef leftTblRef) {
    this.leftTblRef = leftTblRef;
  }

  public List<Predicate> getEqJoinConjuncts() {
    return eqJoinConjuncts;
  }

  public List<Predicate> getOtherJoinConjuncts() {
    return otherJoinConjuncts;
  }

  /**
   * Register this table ref and its ON conjuncts.
   * Call this after calling expandUsingClause().
   */
  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    desc = analyzer.registerTableRef(this);
    Preconditions.checkState(desc != null);

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

    if (onClause != null) {
      onClause.analyze(analyzer);
      // need to register conjuncts before being able to call isEqJoinConjunct()
      analyzer.registerConjuncts(onClause);
      eqJoinConjuncts = Lists.newArrayList();
      otherJoinConjuncts = Lists.newArrayList();
      for (Predicate p: onClause.getConjuncts()) {
        if (p.isEqJoinConjunct()) {
          eqJoinConjuncts.add(p);
        } else {
          otherJoinConjuncts.add(p);
        }
      }
    } else if (getJoinOp().isOuterJoin() || getJoinOp() == JoinOperator.LEFT_SEMI_JOIN) {
      throw new AnalysisException(joinOpToSql() + " requires an ON or USING clause.");
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


  @Override
  public String toSql() {
    if (joinOp == null) {
      // prepend "," if we're part of a sequence of table refs w/o an
      // explicit JOIN clause
      return (leftTblRef != null ? ", " : "")
          + name.toString() + (alias != null ? " " + alias : "");
    }

    StringBuilder output = new StringBuilder(joinOpToSql() + " ");
    output.append(name.toString()).append(" ");
    if (alias != null) {
      output.append(alias).append(" ");
    }
    if (usingColNames != null) {
      output.append("USING (").append(Joiner.on(", ").join(usingColNames)).append(")");
    } else if (onClause != null) {
      output.append("ON (").append(onClause.toSql()).append(")");
    }
    return output.toString();
  }

  // Return alias by which this table is referenced in select block.
  public String getAlias() {
    if (alias == null) {
      return name.toString().toLowerCase();
    } else {
      return alias;
    }
  }

  public TableName getAliasAsName() {
    if (alias != null) {
      return new TableName(null, alias);
    } else {
      return name;
    }
  }

}
