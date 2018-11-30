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

import org.apache.impala.common.AnalysisException;

import com.google.common.base.Preconditions;

import static org.apache.impala.analysis.ToSqlOptions.DEFAULT;

/**
 * Represents a TABLESAMPLE clause.
 *
 * Syntax:
 * <tableref> TABLESAMPLE SYSTEM(<number>) [REPEATABLE(<number>)]
 *
 * The first number specifies the percent of table bytes to sample.
 * The second number specifies the random seed to use.
 */
public class TableSampleClause extends StmtNode {
  // Required percent of bytes to sample.
  private final long percentBytes_;
  // Optional random seed. Null if not specified.
  private final Long randomSeed_;

  public TableSampleClause(long percentBytes, Long randomSeed) {
    percentBytes_ = percentBytes;
    randomSeed_ = randomSeed;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (percentBytes_ < 0 || percentBytes_ > 100) {
      throw new AnalysisException(String.format(
          "Invalid percent of bytes value '%s'. " +
          "The percent of bytes to sample must be between 0 and 100.", percentBytes_));
    }
  }

  public long getPercentBytes() { return percentBytes_; }
  public boolean hasRandomSeed() { return randomSeed_ != null; }
  public long getRandomSeed() {
    Preconditions.checkState(hasRandomSeed());
    return randomSeed_.longValue();
  }

  @Override
  public TableSampleClause clone() {
    return new TableSampleClause(percentBytes_, randomSeed_);
  }

  @Override
  public final String toSql() {
    return toSql(DEFAULT);
  }

  @Override
  public String toSql(ToSqlOptions options) {
    return toSql(randomSeed_);
  }

  /**
   * Prints the SQL of this TABLESAMPLE clause. The optional REPEATABLE clause is
   * included if 'randomSeed' is non-NULL.
   */
  public String toSql(Long randomSeed) {
    StringBuilder builder = new StringBuilder();
    builder.append("TABLESAMPLE SYSTEM(" + percentBytes_ + ")");
    if (randomSeed != null) builder.append(" REPEATABLE(" + randomSeed + ")");
    return builder.toString();
  }
}
