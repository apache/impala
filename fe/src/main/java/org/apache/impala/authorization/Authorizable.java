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
    URI
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

  @Override
  public int hashCode() { return getName().hashCode(); }

  @Override
  public boolean equals(Object o) {
    if (o == null) return false;
    if (o.getClass() != this.getClass()) return false;
    return ((Authorizable) o).getName().equals(this.getName());
  }
}
