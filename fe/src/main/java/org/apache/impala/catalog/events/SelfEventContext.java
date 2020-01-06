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
  // version number from the event object parameters used for self-event detection
  private final long versionNumberFromEvent_;
  // service id from the event object parameters used for self-event detection
  private final String serviceIdFromEvent_;
  private final List<List<TPartitionKeyValue>> partitionKeyValues_;

  SelfEventContext(String dbName, String tblName,
      Map<String, String> parameters) {
    this(dbName, tblName, null, parameters);
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
   */
  SelfEventContext(String dbName, @Nullable String tblName,
      @Nullable List<List<TPartitionKeyValue>> partitionKeyValues,
      Map<String, String> parameters) {
    Preconditions.checkNotNull(parameters);
    this.dbName_ = Preconditions.checkNotNull(dbName);
    this.tblName_ = tblName;
    this.partitionKeyValues_ = partitionKeyValues;
    versionNumberFromEvent_ = Long.parseLong(
        MetastoreEvents.getStringProperty(parameters,
            MetastoreEventPropertyKey.CATALOG_VERSION.getKey(), "-1"));
    serviceIdFromEvent_ =
        MetastoreEvents.getStringProperty(parameters,
            MetastoreEventPropertyKey.CATALOG_SERVICE_ID.getKey(), "");
  }

  public String getDbName() {
    return dbName_;
  }

  public String getTblName() {
    return tblName_;
  }

  public long getVersionNumberFromEvent() {
    return versionNumberFromEvent_;
  }

  public String getServiceIdFromEvent() {
    return serviceIdFromEvent_;
  }

  public List<List<TPartitionKeyValue>> getPartitionKeyValues() {
    return partitionKeyValues_ == null ?
     null : Collections.unmodifiableList(partitionKeyValues_);
  }
}
