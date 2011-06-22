// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import java.util.List;

import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.common.AnalysisException;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

public class TableRef {
  private final TableName name;
  private final String alias;

  private JoinOperator joinOp;
  private Predicate onClause;
  private List<String> usingColNames;

  private TupleDescriptor desc;  // analysis output

  public TableRef(TableName name, String alias) {
    super();
    Preconditions.checkArgument(!name.toString().isEmpty());
    this.name = name;
    Preconditions.checkArgument(alias == null || !alias.isEmpty());
    this.alias = alias;
  }

  public JoinOperator getJoinOp() {
    return joinOp;
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

  public void setDesc(TupleDescriptor desc) {
    this.desc = desc;
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

  public void setJoinOperator(JoinOperator op) {
    this.joinOp = op;
  }

  public void setOnClause(Predicate pred) {
    this.onClause = pred;
  }

  public void setUsingClause(List<String> colNames) {
    this.usingColNames = colNames;
  }

  public void expandUsingClause(TableRef leftTblRef, Catalog catalog)
      throws AnalysisException {
    if (usingColNames == null) {
      return;
    }
    Preconditions.checkState(desc != null);
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

  public String toSql() {
    if (joinOp == null) {
      return name.toString() + (alias != null ? " " + alias : "");
    }

    StringBuilder output = new StringBuilder(joinOp.toString() + " ");
    output.append(name.toString()).append(" ");
    if (alias != null) {
      output.append(alias).append(" ");
    }
    if (usingColNames != null) {
      output.append("USING (").append(Joiner.on(", ").join(usingColNames)).append(")");
    } else {
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
