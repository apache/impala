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
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Partition;

/**
 * Transient holder for a write event on a table during COMMIT_TXN processing.
 * Built per partition from either an insert WriteEventInfo or a tracked truncate,
 * e.g., a truncate spanning multiple partitions creates one TableWriteEvent per
 * partition. Not stored in long-lived catalog state.
 */
public class TableWriteEvent {

  private final TableWriteId tableWriteId_;
  private final Table table_;
  // null for a non-partitioned table or a whole-table truncate.
  private final Partition partition_;

  public TableWriteEvent(long writeId, Table table) {
    this(writeId, table, null);
  }

  public TableWriteEvent(long writeId, Table table, Partition partition) {
    Preconditions.checkArgument(table != null);
    this.tableWriteId_ = new TableWriteId(table.getDbName(), table.getTableName(),
        writeId);
    this.table_ = table;
    this.partition_ = partition;
  }

  public TableWriteId getTableWriteId() {
    return tableWriteId_;
  }

  public Table getTable() {
    return table_;
  }

  public Partition getPartition() {
    return partition_;
  }

  public long getWriteId() {
    return tableWriteId_.getWriteId();
  }

  @Override
  public int hashCode() {
    return tableWriteId_.hashCode() * 31 +
        (partition_ == null ? 0 : partition_.getValues().hashCode());
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) return true;
    if (object == null || getClass() != object.getClass()) return false;
    TableWriteEvent that = (TableWriteEvent) object;
    if (!tableWriteId_.equals(that.tableWriteId_)) return false;
    if (partition_ == null && that.partition_ == null) return true;
    if (partition_ == null || that.partition_ == null) return false;
    return partition_.getValues().equals(that.partition_.getValues());
  }
}
