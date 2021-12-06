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

import java.util.Objects;
import java.util.List;

import com.google.common.base.Strings;
/*
 * Abstract class representing an authorizable object (Table, Db, Column, etc).
 */
public abstract class Authorizable {
  public enum Type {
    SERVER,
    DB,
    TABLE,
    COLUMN,
    FUNCTION,
    URI,
    STORAGEHANDLER_URI
  }

  // Returns the name of the object.
  public abstract String getName();

  // Returns the type of the authorizable.
  public abstract Type getType();

  // Returns the full table name if applicable, null otherwise.
  public String getFullTableName() { return null; }

  // Returns the table name if applicable, null otherwise.
  public String getTableName() { return null; }

  // Returns the database name if applicable, null otherwise.
  public String getDbName() { return null; }

  // Returns the column name if applicable, null otherwise.
  public String getColumnName() { return null; }

  // Returns the function name if applicable, null otherwise.
  public String getFnName() { return null; }

  public String getStorageType() { return null; }

  public String getStorageUri() { return null; }

  // Returns the owner for this authorizable if applicable, null otherwise.
  // Currently, ownership is applicable only for database and table objects.
  // Only used by RangerAuthorizationChecker.
  public String getOwnerUser() { return null; }

  @Override
  public int hashCode() {
    int ownerHash = getOwnerUser() == null ? 0 : getOwnerUser().hashCode();
    int nameHash = getName().hashCode();
    return nameHash * 31 + ownerHash;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) return false;
    if (o.getClass() != this.getClass()) return false;
    Authorizable temp = (Authorizable) o;
    return temp.getName().equals(this.getName())
        && Strings.nullToEmpty(temp.getOwnerUser()).equals(
            Strings.nullToEmpty(this.getOwnerUser()));
  }
}
