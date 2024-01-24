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

import java.util.Map;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.FeTable;

import com.google.common.base.Preconditions;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.planner.DataSink;
import org.apache.impala.thrift.TSortingOrder;

import java.util.List;

public abstract class DmlStatementBase extends StatementBase {
  // Target table of the DML statement.
  protected FeTable table_;

  // Set in analyze(). Set the limit on the maximum number of table sink instances.
  // A value of 0 means no limit.
  protected int maxTableSinks_ = 0;

  // Serialized metadata of transaction object which is set by the Frontend if the
  // target table is Kudu table and Kudu's transaction is enabled.
  protected java.nio.ByteBuffer kuduTxnToken_ = null;

  protected DmlStatementBase() {}

  protected DmlStatementBase(DmlStatementBase other) {
    super(other);
    table_ = other.table_;
    maxTableSinks_ = other.maxTableSinks_;
    kuduTxnToken_ = org.apache.thrift.TBaseHelper.copyBinary(other.kuduTxnToken_);
  }

  @Override
  public void reset() {
    super.reset();
    table_ = null;
    kuduTxnToken_ = null;
  }

  public FeTable getTargetTable() { return table_; }

  protected void setTargetTable(FeTable tbl) { table_ = tbl; }
  public void setMaxTableSinks(int maxTableSinks) { this.maxTableSinks_ = maxTableSinks; }

  public boolean hasShuffleHint() { return false; }
  public boolean hasNoShuffleHint() { return false; }
  public boolean hasClusteredHint() { return false; }
  public boolean hasNoClusteredHint() { return false; }

  abstract public DataSink createDataSink();
  abstract public void substituteResultExprs(ExprSubstitutionMap smap, Analyzer analyzer);
  abstract public List<Expr> getPartitionKeyExprs();
  abstract public List<Expr> getSortExprs();
  abstract public TSortingOrder getSortingOrder();

  /**
   * Return bytes of Kudu transaction token.
   */
  public java.nio.ByteBuffer getKuduTransactionToken() { return kuduTxnToken_; }

  /**
   * Set Kudu transaction token.
   */
  public void setKuduTransactionToken(byte[] kuduTxnToken) {
    Preconditions.checkState(table_ instanceof FeKuduTable);
    Preconditions.checkNotNull(kuduTxnToken);
    kuduTxnToken_ = java.nio.ByteBuffer.wrap(kuduTxnToken.clone());
  }

  public static void checkSubQuery(SlotRef lhsSlotRef, Expr rhsExpr)
      throws AnalysisException {
    if (rhsExpr.contains(Subquery.class)) {
      throw new AnalysisException(String.format(
          "Subqueries are not supported as update expressions for column '%s'",
          lhsSlotRef.toSql()));
    }
  }

  public static void checkCorrectTargetTable(SlotRef lhsSlotRef, Expr rhsExpr,
      TableRef tableRef) throws AnalysisException {
    if (!lhsSlotRef.isBoundByTupleIds(tableRef.getId().asList())) {
      throw new AnalysisException(String.format(
          "Left-hand side column '%s' in assignment expression '%s=%s' does not "
              + "belong to target table '%s'",
          lhsSlotRef.toSql(), lhsSlotRef.toSql(), rhsExpr.toSql(),
          tableRef.getDesc().getTable().getFullName()));
    }
  }

  public static void checkLhsIsColumnRef(SlotRef lhsSlotRef, Expr rhsExpr)
      throws AnalysisException {
    Column c = lhsSlotRef.getResolvedPath().destColumn();
    if (c == null) {
      throw new AnalysisException(String.format(
          "Left-hand side in assignment expression '%s=%s' must be a column reference",
          lhsSlotRef.toSql(), rhsExpr.toSql()));
    }
  }

  public static Expr checkTypeCompatibility(Analyzer analyzer, Column c, Expr rhsExpr,
      TableRef tableRef) throws AnalysisException {
    return StatementBase.checkTypeCompatibility(
        tableRef.getDesc().getTable().getFullName(), c, rhsExpr, analyzer,
        null /*widestTypeSrcExpr*/);
  }

  public static void checkLhsOnlyAppearsOnce(Map<Integer, Expr> colToExprs, Column c,
      SlotRef lhsSlotRef, Expr rhsExpr) throws AnalysisException {
    if (colToExprs.containsKey(c.getPosition())) {
      throw new AnalysisException(
          String.format("Left-hand side in assignment appears multiple times '%s=%s'",
              lhsSlotRef.toSql(), rhsExpr.toSql()));
    }
  }

  public static SlotRef createSlotRef(Analyzer analyzer, String tableName, String colName)
      throws AnalysisException {
    List<String> path = Path.createRawPath(tableName, colName);
    SlotRef ref = new SlotRef(path);
    ref.analyze(analyzer);
    return ref;
  }
}
