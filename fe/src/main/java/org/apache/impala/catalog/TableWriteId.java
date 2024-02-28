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

/**
 * A class that associate a table with write id. This is used to track what table and
 * what write id is allocated for a transaction.
 */
public class TableWriteId {
  // Database name
  private String dbName_;
  // Table name
  private String tblName_;
  // Create event id, which help us determine if a table is recreated
  private long createEventId_;
  // Write id of this table in the transaction
  private long writeId_;

  public TableWriteId(String dbName, String tblName, long createEventId, long writeId) {
    Preconditions.checkArgument(dbName != null && tblName != null);
    this.dbName_ = dbName;
    this.tblName_ = tblName;
    this.createEventId_ = createEventId;
    this.writeId_ = writeId;
  }

  public String getDbName() {
    return dbName_;
  }

  public String getTblName() {
    return tblName_;
  }

  public long getCreateEventId() {
    return createEventId_;
  }

  public long getWriteId() {
    return writeId_;
  }

  public boolean equals(Object object) {
    if (this == object) return true;
    if (object == null || getClass() != object.getClass()) return false;
    TableWriteId that = (TableWriteId) object;
    return dbName_.equals(that.dbName_) && tblName_.equals(that.tblName_) &&
        createEventId_ == that.createEventId_ && writeId_ == that.writeId_;
  }

  public int hashCode() {
    return java.util.Objects.hash(dbName_, tblName_, createEventId_, writeId_);
  }
}
