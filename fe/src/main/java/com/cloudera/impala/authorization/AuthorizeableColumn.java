// Copyright 2015 Cloudera Inc.
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

import org.apache.sentry.core.model.db.DBModelAuthorizable;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * Class used to authorize access to a column.
 */
public class AuthorizeableColumn extends Authorizeable {
  private final org.apache.sentry.core.model.db.Column column_;
  private final org.apache.sentry.core.model.db.Table table_;
  private final org.apache.sentry.core.model.db.Database database_;
  public final static String ANY_COLUMN_NAME =
      org.apache.sentry.core.model.db.AccessConstants.ALL;

  public AuthorizeableColumn(String dbName, String tableName, String columnName) {
    Preconditions.checkState(!Strings.isNullOrEmpty(dbName));
    Preconditions.checkState(!Strings.isNullOrEmpty(tableName));
    Preconditions.checkState(!Strings.isNullOrEmpty(columnName));
    column_ = new org.apache.sentry.core.model.db.Column(columnName);
    table_ = new org.apache.sentry.core.model.db.Table(tableName);
    database_ = new org.apache.sentry.core.model.db.Database(dbName);
  }

  @Override
  public List<DBModelAuthorizable> getHiveAuthorizeableHierarchy() {
    return Lists.newArrayList(database_, table_, column_);
  }

  @Override
  public String getName() { return database_.getName() + "." + table_.getName() + "."
      + column_.getName(); }

  @Override
  public String getFullTableName() {
    return database_.getName() + "." + table_.getName();
  }

  @Override
  public String getDbName() { return database_.getName(); }

  public String getTblName() { return table_.getName(); }
  public String getColumnName() { return column_.getName(); }
}
