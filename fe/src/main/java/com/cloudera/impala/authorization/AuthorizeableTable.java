// Copyright 2013 Cloudera Inc.
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

package com.cloudera.impala.authorization;

import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/*
 * Class used to authorize access to a table.
 */
public class AuthorizeableTable implements Authorizeable {
  // Constant to represent privileges in the policy for "ANY" table in a
  // a database.
  public final static String ANY_TABLE_NAME = org.apache.access.core.AccessConstants.ALL;

  private final org.apache.access.core.Table table;
  private final org.apache.access.core.Database database;

  public AuthorizeableTable(String dbName, String tableName) {
    Preconditions.checkState(tableName != null && !tableName.isEmpty());
    Preconditions.checkState(dbName != null && !dbName.isEmpty());
    this.table = new org.apache.access.core.Table(tableName);
    this.database = new org.apache.access.core.Database(dbName);
  }

  @Override
  public List<org.apache.access.core.Authorizable> getHiveAuthorizeableHierarchy() {
    return Lists.newArrayList(database, table);
  }

  @Override
  public String getName() {
    return database.getName() + "." + table.getName();
  }
}