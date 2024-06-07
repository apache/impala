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

package org.apache.impala.analysis;

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.List;

import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.Pair;
import org.apache.impala.planner.DataSink;

import com.google.common.base.Preconditions;

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
 * Currently, only Kudu and Iceberg tables can be updated.
 */
public class UpdateStmt extends ModifyStmt {
  public UpdateStmt(List<String> targetTablePath, FromClause tableRefs,
      List<Pair<SlotRef, Expr>> assignmentExprs, Expr wherePredicate) {
    super(targetTablePath, tableRefs, assignmentExprs, wherePredicate);
  }

  public UpdateStmt(UpdateStmt other) {
    super(other.targetTablePath_, other.fromClause_.clone(),
        new ArrayList<>(), other.wherePredicate_);
  }

  @Override
  protected void createModifyImpl() {
    // Currently Kudu and Iceberg tables are supported.
    if (table_ instanceof FeKuduTable) {
      modifyImpl_ = new KuduUpdateImpl(this);
    } else if (table_ instanceof FeIcebergTable) {
      modifyImpl_ = new IcebergUpdateImpl(this);
    }
  }

  @Override
  public void substituteResultExprs(ExprSubstitutionMap smap, Analyzer analyzer) {
    modifyImpl_.substituteResultExprs(smap, analyzer);
  }

  /**
   * Return an instance of a KuduTableSink specialized as an Update operation.
   */
  @Override
  public DataSink createDataSink() {
    // analyze() must have been called before.
    return modifyImpl_.createDataSink();
  }

  @Override
  public UpdateStmt clone() {
    return new UpdateStmt(this);
  }

  @Override
  public String toSql(ToSqlOptions options) {
    if (!options.showRewritten() && sqlString_ != null) return sqlString_;

    StringBuilder b = new StringBuilder();
    b.append("UPDATE ");

    if (fromClause_ == null) {
      b.append(targetTableRef_.toSql(options));
    } else {
      if (targetTableRef_.hasExplicitAlias()) {
        b.append(targetTableRef_.getExplicitAlias());
      } else {
        b.append(targetTableRef_.toSql(options));
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
      b.append(format(" %s = %s", i.first.toSql(options), i.second.toSql(options)));
    }

    b.append(fromClause_.toSql(options));

    if (wherePredicate_ != null) {
      b.append(" WHERE ");
      b.append(wherePredicate_.toSql(options));
    }
    return b.toString();
  }
}
