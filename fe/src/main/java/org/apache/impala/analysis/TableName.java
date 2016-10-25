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

import org.apache.impala.common.AnalysisException;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.thrift.TTableName;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Represents a table/view name that optionally includes its database (a fully qualified
 * table name). Analysis of this table name checks for validity of the database and
 * table name according to the Metastore's policy (see @MetaStoreUtils).
 * According to that definition, we can still use "invalid" table names for tables/views
 * that are not stored in the Metastore, e.g., for Inline Views or WITH-clause views.
 */
public class TableName {
  private final String db_;
  private final String tbl_;

  public TableName(String db, String tbl) {
    super();
    Preconditions.checkArgument(db == null || !db.isEmpty());
    this.db_ = db;
    Preconditions.checkNotNull(tbl);
    this.tbl_ = tbl;
  }

  public String getDb() { return db_; }
  public String getTbl() { return tbl_; }
  public boolean isEmpty() { return tbl_.isEmpty(); }

  /**
   * Checks whether the db and table name meet the Metastore's requirements.
   */
  public void analyze() throws AnalysisException {
    if (db_ != null) {
      if (!MetastoreShim.validateName(db_)) {
        throw new AnalysisException("Invalid database name: " + db_);
      }
    }
    Preconditions.checkNotNull(tbl_);
    if (!MetastoreShim.validateName(tbl_)) {
      throw new AnalysisException("Invalid table/view name: " + tbl_);
    }
  }

  /**
   * Returns true if this name has a non-empty database field and a non-empty
   * table name.
   */
  public boolean isFullyQualified() {
    return db_ != null && !db_.isEmpty() && !tbl_.isEmpty();
  }

  public String toSql() {
    // Enclose the database and/or table name in quotes if Hive cannot parse them
    // without quotes. This is needed for view compatibility between Impala and Hive.
    if (db_ == null) {
      return ToSqlUtils.getIdentSql(tbl_);
    } else {
      return ToSqlUtils.getIdentSql(db_) + "." + ToSqlUtils.getIdentSql(tbl_);
    }
  }

  @Override
  public String toString() {
    if (db_ == null) {
      return tbl_;
    } else {
      return db_ + "." + tbl_;
    }
  }

  public List<String> toPath() {
    List<String> result = Lists.newArrayListWithCapacity(2);
    if (db_ != null) result.add(db_);
    result.add(tbl_);
    return result;
  }

  public static TableName fromThrift(TTableName tableName) {
    return new TableName(tableName.getDb_name(), tableName.getTable_name());
  }

  public TTableName toThrift() { return new TTableName(db_, tbl_); }

  /**
   * Returns true of the table names are considered equals. To check for equality,
   * a case-insensitive comparison of the database and table name is performed.
   */
  @Override
  public boolean equals(Object anObject) {
    if (anObject instanceof TableName) {
      return toString().toLowerCase().equals(anObject.toString().toLowerCase());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return toString().toLowerCase().hashCode();
  }
}
