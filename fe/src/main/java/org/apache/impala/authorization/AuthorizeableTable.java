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

package org.apache.impala.authorization;

import java.util.List;

import org.apache.sentry.core.model.db.DBModelAuthorizable;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * Class used to authorize access to a table or view.
 * Even though Hive's spec includes an authorizable object 'view', we chose
 * to treat views the same way as tables for the sake of authorization.
 */
public class AuthorizeableTable extends Authorizeable {
  // Constant to represent privileges in the policy for "ANY" table in a
  // a database.
  public final static String ANY_TABLE_NAME =
      org.apache.sentry.core.model.db.AccessConstants.ALL;

  private final org.apache.sentry.core.model.db.Table table_;
  private final org.apache.sentry.core.model.db.Database database_;

  public AuthorizeableTable(String dbName, String tableName) {
    Preconditions.checkState(!Strings.isNullOrEmpty(tableName));
    Preconditions.checkState(!Strings.isNullOrEmpty(dbName));
    table_ = new org.apache.sentry.core.model.db.Table(tableName);
    database_ = new org.apache.sentry.core.model.db.Database(dbName);
  }

  @Override
  public List<DBModelAuthorizable> getHiveAuthorizeableHierarchy() {
    return Lists.newArrayList(database_, table_);
  }

  @Override
  public String getName() { return database_.getName() + "." + table_.getName(); }

  @Override
  public String getDbName() { return database_.getName(); }
  public String getTblName() { return table_.getName(); }

  @Override
  public String getFullTableName() { return getName(); }
}
