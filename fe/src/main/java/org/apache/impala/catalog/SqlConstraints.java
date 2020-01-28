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

package org.apache.impala.catalog;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.impala.thrift.TSqlConstraints;

/**
 * This class encapsulates all the SQL constraints for a given table.
 */
public class SqlConstraints {

  // List of primary keys and foreign keys for a table. An empty list could
  // mean either the table does not have any keys or the table is not loaded.
  private final List<SQLPrimaryKey> primaryKeys_;
  private final List<SQLForeignKey> foreignKeys_;

  public SqlConstraints(List<SQLPrimaryKey> primaryKeys,
      List<SQLForeignKey> foreignKeys) {
    this.primaryKeys_ = primaryKeys == null ? new ArrayList<>() : primaryKeys;
    this.foreignKeys_ = foreignKeys == null ? new ArrayList<>() : foreignKeys;
    sortSqlConstraints();
  }

  /**
   * Utility method to sort SQL constraints. This is done to prevent flaky tests if HMS
   * returns the constraints in inconsistent orders.
   */
  private void sortSqlConstraints() {
    // We sort SQLPrimaryKeys in alphabetical order of the
    // primary key name. If the primary key names are same (composite primary key), we
    // will sort in increasing order of key_seq.
    primaryKeys_.sort((pk1, pk2) -> {
      int keyNameComp = pk1.getPk_name().compareTo(pk2.getPk_name());
      if (keyNameComp == 0) {
        return Integer.compare(pk1.getKey_seq(), pk2.getKey_seq());
      }
      return keyNameComp;
    });

    // We sort SQLForeignKeys in alphabetical order of the
    // parent table name. If the parent table names are same (composite primary key in
    // parent table), we will sort in increasing order of key_seq.
    foreignKeys_.sort((fk1, fk2) -> {
      int parentTableNameComp = fk1.getPktable_name().compareTo(fk2.getPktable_name());
      if (parentTableNameComp == 0) {
        return Integer.compare(fk1.getKey_seq(), fk2.getKey_seq());
      }
      return parentTableNameComp;
    });
  }

  public List<SQLPrimaryKey> getPrimaryKeys() {
    Preconditions.checkNotNull(primaryKeys_);
    return ImmutableList.copyOf(primaryKeys_);
  }

  public List<SQLForeignKey> getForeignKeys() {
    Preconditions.checkNotNull(foreignKeys_);
    return ImmutableList.copyOf(foreignKeys_);
  }

  /**
   * @return the thirft SqlConstraints for this table.
   */
  public TSqlConstraints toThrift()  {
    Preconditions.checkNotNull(primaryKeys_);
    Preconditions.checkNotNull(foreignKeys_);
    TSqlConstraints tSqlConstraints = new TSqlConstraints();
    tSqlConstraints.setPrimary_keys(primaryKeys_);
    tSqlConstraints.setForeign_keys(foreignKeys_);
    return tSqlConstraints;
  }

  public static SqlConstraints fromThrift(TSqlConstraints tSqlConstraints) {
    return new SqlConstraints(tSqlConstraints.getPrimary_keys(),
        tSqlConstraints.getForeign_keys());
  }
}
