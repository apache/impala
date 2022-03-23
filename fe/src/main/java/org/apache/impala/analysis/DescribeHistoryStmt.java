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

import java.util.List;

import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.InternalException;
import org.apache.impala.thrift.TDescribeHistoryParams;
import org.apache.impala.util.ExprUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Representation of a DESCRIBE HISTORY statement, returns the available snapshot based
 * on the predicate.
 * Syntax: DESCRIBE HISTORY <table>;
 *         DESCRIBE HISTORY <table> BETWEEN <ts1> AND <ts2>;
 *         DESCRIBE HISTORY <table> FROM <ts>;
 *         DESCRIBE HISTORY <table> FROM now() - interval 1 days;
 */
public class DescribeHistoryStmt extends StatementBase {
  private final static Logger LOG = LoggerFactory.getLogger(TimeTravelSpec.class);

  // Represents the predicate with which this statement was called.
  public enum Kind {
    FROM,
    BETWEEN,
    NO_PREDICATE
  }

  // Table name, result of parsing.
  protected final TableName tableName_;

  // Expression used during: DESCRIBE HISRTORY <table> FROM <from_>
  protected Expr from_;

  // Expressions used during:
  //   DESCRIBE HISRTORY <table> BETWEEN <between_start_time_> AND <between_end_time_>
  protected Expr betweenStartTime_;
  protected Expr betweenEndTime_;

  // Store the Kind of this statement, it is used during analysis.
  final private Kind kind_;

  // Set during analysis.
  protected FeTable table_;

  // from_ expression after analyzis in milliseconds.
  long fromMillis_;

  // betweenStartTime_ expression after analyzis in milliseconds.
  long betweenStartTimeMillis_;

  // betweenEndTimeMilis_ expression after analyzis in milliseconds.
  long betweenEndTimeMillis_;

  public DescribeHistoryStmt(TableName tableName) {
    tableName_ = Preconditions.checkNotNull(tableName);
    kind_ = Kind.NO_PREDICATE;
  }

  public DescribeHistoryStmt(TableName tableName, Expr from) {
    tableName_ = Preconditions.checkNotNull(tableName);
    from_ = Preconditions.checkNotNull(from);
    kind_ = Kind.FROM;
  }

  public DescribeHistoryStmt(TableName tableName, Expr between1, Expr between2) {
    tableName_ = Preconditions.checkNotNull(tableName);
    betweenStartTime_ = Preconditions.checkNotNull(between1);
    betweenEndTime_ = Preconditions.checkNotNull(between2);
    kind_ = Kind.BETWEEN;
  }

  @Override
  public void collectTableRefs(List<TableRef> tblRefs) {
    tblRefs.add(new TableRef(tableName_.toPath(), null));
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    table_ = analyzer.getTable(tableName_, Privilege.VIEW_METADATA);
    Preconditions.checkNotNull(table_);
    if (!(table_ instanceof FeIcebergTable)) {
      throw new AnalysisException(String.format(
          "DESCRIBE HISTORY must specify an Iceberg table: %s", table_.getFullName()));
    }
    switch (kind_) {
      case FROM:
        fromMillis_ = analyzeExpr(analyzer, from_);
        break;
      case BETWEEN:
        betweenStartTimeMillis_ = analyzeExpr(analyzer, betweenStartTime_);
        betweenEndTimeMillis_ = analyzeExpr(analyzer, betweenEndTime_);
        break;
      case NO_PREDICATE:
      default:
        break;
    }
  }

  /**
  * Analyzes the provided expression then verfies if it is possible to obtain a
  * timestamp from it. Returns the timestamp in Unix time milliseconds.
  */
  private long analyzeExpr(Analyzer analyzer, Expr expr) throws AnalysisException {
    try {
      expr.analyze(analyzer);
    } catch (AnalysisException e) {
      throw new AnalysisException("Unsupported expression: '" + expr.toSql() + "'");
    }
    if (expr.getType().isStringType()) {
      expr = new CastExpr(Type.TIMESTAMP, expr);
    }
    if (!expr.getType().isTimestamp()) {
      throw new AnalysisException(kind_.toString() +
          " <expression> must be a timestamp type but is '" +
          expr.getType() + "': " + expr.toSql());
    }
    LOG.debug(kind_.toString() + " <expression>: " + String.valueOf(expr));
    long micros = 0;
    try {
      micros = ExprUtil.localTimestampToUnixTimeMicros(analyzer, expr);
    } catch (InternalException ie) {
      throw new AnalysisException(
          "Invalid TIMESTAMP expression: " + ie.getMessage(), ie);
    }
    return micros / 1000;
  }

  @Override
  public String toSql(ToSqlOptions options) {
    switch(kind_) {
      case FROM:
        return "DESCRIBE HISTORY " + tableName_.toString() + " FROM " + from_.toSql();
      case BETWEEN:
        return "DESCRIBE HISTORY " + tableName_.toString() + " BETWEEN " +
            betweenStartTime_.toSql() + " AND " + betweenEndTime_.toSql();
      case NO_PREDICATE:
      default:
        return "DESCRIBE HISTORY " + tableName_.toString();
    }
  }

  public TDescribeHistoryParams toThrift() {
    TDescribeHistoryParams describeHistoryParams = new TDescribeHistoryParams();
    TableName tableName = new TableName(table_.getDb().getName(), table_.getName());
    describeHistoryParams.setTable_name(tableName.toThrift());
    switch(kind_) {
      case FROM:
        describeHistoryParams.setFrom_time(fromMillis_);
        break;
      case BETWEEN:
        describeHistoryParams.setBetween_start_time(betweenStartTimeMillis_);
        describeHistoryParams.setBetween_end_time(betweenEndTimeMillis_);
        break;
      case NO_PREDICATE:
      default:
        break;
    }
    return describeHistoryParams;
  }
}
