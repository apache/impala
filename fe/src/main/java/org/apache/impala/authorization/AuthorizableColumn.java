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

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * A class to authorize access to a column.
 */
public class AuthorizableColumn extends Authorizable {
  private final String dbName_;
  private final String tableName_;
  private final String columnName_;
  // Owner for the parent db or table that this Authorizable corresponds to.
  @Nullable
  private final String ownerUser_;

  public AuthorizableColumn(
      String dbName, String tableName, String columnName, @Nullable String ownerUser) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(dbName));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(columnName));
    dbName_ = dbName;
    tableName_ = tableName;
    columnName_ = columnName;
    ownerUser_ = ownerUser;
  }

  @Override
  public String getName() { return dbName_ + "." + tableName_ + "." + columnName_; }

  @Override
  public Type getType() { return Type.COLUMN; }

  @Override
  public String getFullTableName() { return dbName_ + "." + tableName_; }

  @Override
  public String getDbName() { return dbName_; }

  @Override
  public String getTableName() { return tableName_; }

  @Override
  public String getColumnName() { return columnName_; }

  @Override
  public String getOwnerUser() { return ownerUser_; }
}
