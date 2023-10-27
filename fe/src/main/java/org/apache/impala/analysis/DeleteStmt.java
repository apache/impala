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

import java.util.ArrayList;
import java.util.List;

import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.Pair;
import org.apache.impala.planner.DataSink;
import org.apache.impala.planner.TableSink;
import org.apache.impala.thrift.TSortingOrder;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Representation of a DELETE statement.
 *
 * A delete statement contains three main parts, the target table reference, the from
 * clause and the optional where clause. Syntactically, this is represented as follows:
 *
 *     DELETE [FROM] dotted_path [WHERE expr]
 *     DELETE [table_alias] FROM table_ref_list [WHERE expr]
 *
 * Only the syntax using the explicit from clause can contain join conditions.
 */
public class DeleteStmt extends ModifyStmt {

  public DeleteStmt(List<String> targetTablePath, FromClause tableRefs,
      Expr wherePredicate) {
    super(targetTablePath, tableRefs, new ArrayList<>(),
        wherePredicate);
  }

  public DeleteStmt(DeleteStmt other) {
    super(other.targetTablePath_, other.fromClause_.clone(),
        new ArrayList<>(), other.wherePredicate_.clone());
  }

  @Override
  protected void createModifyImpl() {
    if (table_ instanceof FeKuduTable) {
      modifyImpl_ = new KuduDeleteImpl(this);
    } else if (table_ instanceof FeIcebergTable) {
      modifyImpl_ = new IcebergDeleteImpl(this);
    }
  }

  @Override
  public DataSink createDataSink() {
    return modifyImpl_.createDataSink();
  }

  @Override
  public void substituteResultExprs(ExprSubstitutionMap smap, Analyzer analyzer) {
    modifyImpl_.substituteResultExprs(smap, analyzer);
  }

  @Override
  public DeleteStmt clone() {
    return new DeleteStmt(this);
  }

  @Override
  public String toSql(ToSqlOptions options) {
    if (!options.showRewritten() && sqlString_ != null) return sqlString_;

    StringBuilder b = new StringBuilder();
    b.append("DELETE");
    if (fromClause_.size() > 1 || targetTableRef_.hasExplicitAlias()) {
      b.append(" ");
      if (targetTableRef_.hasExplicitAlias()) {
        b.append(targetTableRef_.getExplicitAlias());
      } else {
        b.append(targetTableRef_.toSql(options));
      }
    }
    b.append(fromClause_.toSql(options));
    if (wherePredicate_ != null) {
      b.append(" WHERE ");
      b.append(wherePredicate_.toSql(options));
    }
    return b.toString();
  }
}
