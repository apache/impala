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
 * A class to authorize access to a table.
 */
public class AuthorizableTable extends Authorizable {
  private final String dbName_;
  private final String tableName_;
  @Nullable // Is null if the owner is not set.
  private final String ownerUser_;
  // TODO(IMPALA-10122): Remove the following field once we can properly process a
  // PrivilegeRequest for a view whose creation was not authorized.
  private final boolean viewCreatedWithoutAuthz_;

  public AuthorizableTable(String dbName, String tableName, @Nullable String ownerUser,
      boolean viewCreatedWithoutAuthz) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(dbName));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName));
    dbName_ = dbName;
    tableName_ = tableName;
    ownerUser_ = ownerUser;
    viewCreatedWithoutAuthz_ = viewCreatedWithoutAuthz;
  }

  @Override
  public String getName() { return dbName_ + "." + tableName_; }

  @Override
  public Type getType() { return Type.TABLE; }

  @Override
  public String getDbName() { return dbName_; }

  @Override
  public String getTableName() { return tableName_; }

  @Override
  public String getFullTableName() { return getName(); }

  @Override
  public String getOwnerUser() { return ownerUser_; }

  // TODO(IMPALA-10122): Remove the following method once we can properly process a
  // PrivilegeRequest for a view whose creation was not authorized.
  public boolean isViewCreatedWithoutAuthz() { return viewCreatedWithoutAuthz_; }
}
