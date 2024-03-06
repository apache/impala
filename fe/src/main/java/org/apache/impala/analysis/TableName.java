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

import org.apache.impala.catalog.Catalog;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.thrift.TTableName;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.impala.util.CatalogBlacklistUtils;

/**
 * Represents a table/view name that optionally includes its database (a fully qualified
 * table name) or a virtual table which requires fully qualified name. Analysis of this
 * table name checks for validity of the database and table name according to the
 * Metastore's policy (see @MetaStoreUtils). According to that definition, we can still
 * use "invalid" table names for tables/views that are not stored in the Metastore, e.g.,
 * for Inline Views or WITH-clause views. Virtual tables are metadata tables of the "real"
 * tables, currently only supported for Iceberg tables.
 */
public class TableName {
  private final String db_;
  private final String tbl_;
  private final String vTbl_;

  public TableName(String db, String tbl) {
    this(db, tbl, null);
  }

  public TableName(String db, String tbl, String vTbl) {
    super();
    Preconditions.checkArgument(db == null || !db.isEmpty());
    this.db_ = db;
    Preconditions.checkNotNull(tbl);
    this.tbl_ = tbl;
    Preconditions.checkArgument(vTbl == null || !vTbl.isEmpty());
    this.vTbl_ = vTbl;
  }

  /**
   * Parse the given full name (in format <db>.<tbl>.<vTbl>) and return a TableName
   * object. Return null for any failures. Note that we keep table names in lower case so
   * the string will be converted to lower case first.
   */
  public static TableName parse(String fullName) {
    // Avoid "db1." and ".tbl1" being treated as the same. We resolve ".tbl1" as
    // "default.tbl1". But we reject "db1." since it only gives the database name.
    if (fullName == null || fullName.trim().endsWith(".")) return null;
    List<String> parts = Lists.newArrayList(Splitter.on('.').trimResults()
        .omitEmptyStrings().splitToList(fullName.toLowerCase()));
    if (parts.size() == 1) {
      return new TableName(Catalog.DEFAULT_DB, parts.get(0));
    }
    if (parts.size() == 2) {
      return new TableName(parts.get(0), parts.get(1));
    }
    if (parts.size() == 3) {
      // Only fully qualified names are supported for vtables
      return new TableName(parts.get(0), parts.get(1), parts.get(2));
    }
    return null;
  }

  public String getDb() { return db_; }
  public String getTbl() { return tbl_; }
  public String getVTbl() { return vTbl_; }
  public boolean isEmpty() { return tbl_.isEmpty(); }

  /**
   * Checks whether the db and table name meet the Metastore's requirements. and not in
   * our blacklist. 'db_' is assumed to be resolved.
   */
  public void analyze() throws AnalysisException {
    Preconditions.checkNotNull(isFullyQualified());
    if (!MetastoreShim.validateName(db_)) {
      throw new AnalysisException("Invalid database name: " + db_);
    }
    if (!MetastoreShim.validateName(tbl_)) {
      throw new AnalysisException("Invalid table/view name: " + tbl_);
    }
    CatalogBlacklistUtils.verifyTableName(this);
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
    StringBuilder result = new StringBuilder();
    if (db_ == null) {
      result.append(ToSqlUtils.getIdentSql(tbl_));
    } else {
      result.append(ToSqlUtils.getIdentSql(db_) + "." + ToSqlUtils.getIdentSql(tbl_));
      if (vTbl_ != null && !vTbl_.isEmpty()) {
        result.append("." + ToSqlUtils.getIdentSql(vTbl_));
      }
    }
    return result.toString();
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    if (db_ == null) {
      result.append(tbl_);
    } else {
      result.append(db_ + "." + tbl_);
      if (vTbl_ != null && !vTbl_.isEmpty()) {
        result.append( "." + vTbl_);
      }
    }
    return result.toString();
  }

  public List<String> toPath() {
    List<String> result = Lists.newArrayListWithCapacity(3);
    if (db_ != null) result.add(db_);
    result.add(tbl_);
    if (vTbl_ != null && !vTbl_.isEmpty()) result.add(vTbl_);
    return result;
  }

  public static TableName fromThrift(TTableName tableName) {
    return new TableName(tableName.getDb_name(), tableName.getTable_name());
  }

  public static String thriftToString(TTableName tableName) {
    return fromThrift(tableName).toString();
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
