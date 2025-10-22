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
public class AuthorizableColumn extends AuthorizableTable {
  private final String columnName_;

  public AuthorizableColumn(
      String dbName, String tableName, String columnName, @Nullable String ownerUser) {
    super(dbName, tableName, ownerUser);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(columnName));
    columnName_ = columnName;
  }

  @Override
  public String getName() {
    return getDbName() + "." + getTableName() + "." + columnName_;
  }

  @Override
  public Type getType() { return Type.COLUMN; }

  @Override
  public String getColumnName() { return columnName_; }
}
