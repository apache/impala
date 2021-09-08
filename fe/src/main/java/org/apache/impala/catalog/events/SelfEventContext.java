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

package org.apache.impala.catalog.events;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEventPropertyKey;
import org.apache.impala.thrift.TPartitionKeyValue;

/**
 * Helper class which encapsulates all the information needed to evaluate if given
 * event is self-event or not
 */
public class SelfEventContext {
  private final String dbName_;
  private final String tblName_;
  // eventids for the insert events. In case of an un-partitioned table insert
  // this list contains only one event id for the insert. In case of partition level
  // inserts, this contains events 1 to 1 with the partition key values
  // partitionKeyValues_. For example insertEventIds_.get(0) is the insert event for
  // partition with values partitionKeyValues_.get(0) and so on
  private final List<Long> insertEventIds_;
  // the partition key values for self-event evaluation at the partition level.
  private final List<List<TPartitionKeyValue>> partitionKeyValues_;
  // version number from the event object parameters used for self-event detection
  private final long versionNumberFromEvent_;
  // service id from the event object parameters used for self-event detection
  private final String serviceidFromEvent_;

  SelfEventContext(String dbName, String tblName,
      Map<String, String> parameters) {
    this(dbName, tblName, null, parameters, null);
  }

  SelfEventContext(String dbName, String tblName,
      List<List<TPartitionKeyValue>> partitionKeyValues, Map<String, String> parameters) {
    this(dbName, tblName, partitionKeyValues, parameters, null);
  }

  /**
   * Creates a self-event context for self-event evaluation for database, table or
   * partition events.
   *
   * @param dbName Database name
   * @param tblName Table name
   * @param partitionKeyValues Partition key-values in case of self-event
   * context is for partition.
   * @param parameters this could be database, table or partition parameters.
   * @param insertEventIds In case this is self-event context for an insert event, this
   *                       parameter provides the list of insert event ids which map to
   *                       the partitions. In case of unpartitioned table must contain
   *                       only one event id.
   */
  SelfEventContext(String dbName, @Nullable String tblName,
      @Nullable List<List<TPartitionKeyValue>> partitionKeyValues,
      Map<String, String> parameters, List<Long> insertEventIds) {
    Preconditions.checkNotNull(parameters);
    // if this is for an insert event on partitions then the size of insertEventIds
    // must be equal to number of TPartitionKeyValues
    Preconditions.checkArgument((partitionKeyValues == null ||
        insertEventIds == null) || partitionKeyValues.size() == insertEventIds.size());
    this.dbName_ = Preconditions.checkNotNull(dbName);
    this.tblName_ = tblName;
    this.partitionKeyValues_ = partitionKeyValues;
    this.insertEventIds_ = insertEventIds;
    this.versionNumberFromEvent_ = Long.parseLong(
        MetastoreEvents.getStringProperty(parameters,
            MetastoreEventPropertyKey.CATALOG_VERSION.getKey(), "-1"));
    this.serviceidFromEvent_ = MetastoreEvents.getStringProperty(
        parameters, MetastoreEventPropertyKey.CATALOG_SERVICE_ID.getKey(), "");
  }

  public String getDbName() {
    return dbName_;
  }

  public String getTblName() {
    return tblName_;
  }

  public long getInsertEventId(int idx) {
    return insertEventIds_.get(idx);
  }

  public long getVersionNumberFromEvent() {
    return versionNumberFromEvent_;
  }

  public String getServiceIdFromEvent() { return serviceidFromEvent_; }

  public List<List<TPartitionKeyValue>> getPartitionKeyValues() {
    return partitionKeyValues_ == null ?
     null : Collections.unmodifiableList(partitionKeyValues_);
  }

  public boolean isInsertEventContext() {
    return insertEventIds_ != null;
  }
}
