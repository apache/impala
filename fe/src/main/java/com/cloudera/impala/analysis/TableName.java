// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.analysis;

import com.cloudera.impala.thrift.TTableName;
import com.google.common.base.Preconditions;

public class TableName {
  private final String db;
  private final String tbl;

  public TableName(String db, String tbl) {
    super();
    Preconditions.checkArgument(db == null || !db.isEmpty());
    this.db = db;
    Preconditions.checkNotNull(tbl);
    this.tbl = tbl;
  }

  public String getDb() {
    return db;
  }

  public String getTbl() {
    return tbl;
  }

  public boolean isEmpty() {
    return tbl.isEmpty();
  }

  /**
   * Returns true if this name has a non-empty database field and a non-empty
   * table name.
   */
  public boolean isFullyQualified() {
    return db != null && !db.isEmpty() && !tbl.isEmpty();
  }

  public String toSql() {
    // Enclose the database and/or table name in quotes if Hive cannot parse them
    // without quotes. This is needed for view compatibility between Impala and Hive.
    if (db == null) {
      return ToSqlUtils.getHiveIdentSql(tbl);
    } else {
      return ToSqlUtils.getHiveIdentSql(db) + "." + ToSqlUtils.getHiveIdentSql(tbl);
    }
  }

  @Override
  public String toString() {
    if (db == null) {
      return tbl;
    } else {
      return db + "." + tbl;
    }
  }

  public static TableName fromThrift(TTableName tableName) {
    return new TableName(tableName.getDb_name(), tableName.getTable_name());
  }
}
