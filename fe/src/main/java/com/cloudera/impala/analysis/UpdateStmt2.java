package com.cloudera.impala.analysis;

import java.util.List;

import com.cloudera.impala.common.Pair;
import com.cloudera.impala.planner.DataSink;
import com.cloudera.impala.planner.KuduTableSink;
import com.google.common.base.Preconditions;

import static java.lang.String.format;

/**
 * Representation of an Update statement.
 *
 * Example UPDATE statement:
 *
 *     UPDATE target_table
 *       SET slotRef=expr, [slotRef=expr, ...]
 *       FROM table_ref_list
 *       WHERE conjunct_list
 *
 * An update statement consists of four major parts. First, the target table path,
 * second, the list of assignments, the optional FROM clause, and the optional where
 * clause. The type of the right-hand side of each assignments must be
 * assignment compatible with the left-hand side column type.
 *
 * Currently, only Kudu tables can be updated.
 */
public class UpdateStmt2 extends UpdateStmt {
  public UpdateStmt2(List<String> targetTablePath,
      FromClause tableRefs,
      List<Pair<SlotRef, Expr>> assignmentExprs,
      Expr wherePredicate) {
    super(targetTablePath, tableRefs, assignmentExprs, wherePredicate);
  }

  /**
   * Return an instance of a KuduTableSink specialized as an Update operation.
   */
  public DataSink createDataSink() {
    // analyze() must have been called before.
    Preconditions.checkState(table_ != null);
    return KuduTableSink.createUpdateSink(table_, referencedColumns_);
  }

  @Override
  public String toSql() {
    StringBuilder b = new StringBuilder();
    b.append("UPDATE ");

    if (fromClause_ == null) {
      b.append(targetTableRef_.toSql());
    } else {
      if (targetTableRef_.hasExplicitAlias()) {
        b.append(targetTableRef_.getExplicitAlias());
      } else {
        b.append(targetTableRef_.toSql());
      }
    }
    b.append(" SET");

    boolean first = true;
    for (Pair<SlotRef, Expr> i : assignments_) {
      if (!first) {
        b.append(",");
      } else {
        first = false;
      }
      b.append(format(" %s = %s",
          i.first.toSql(),
          i.second.toSql()));
    }

    b.append(fromClause_.toSql());

    if (wherePredicate_ != null) {
      b.append(" WHERE ");
      b.append(wherePredicate_.toSql());
    }
    return b.toString();
  }
}
