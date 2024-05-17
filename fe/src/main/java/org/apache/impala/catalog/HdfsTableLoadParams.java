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

import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.impala.util.EventSequence;

/**
 * The arguments required to load the HdfsTable object
 *
 * Use HdfsTableLoadParamsBuilder to create this instance.
 */
public class HdfsTableLoadParams {
  private final boolean reuseMetadata_;
  private final IMetaStoreClient client_;
  private final Table msTbl_;
  private final boolean loadPartitionFileMetadata_;
  private final boolean loadTableSchema_;
  private final boolean refreshUpdatedPartitions_;
  private final Set<String> partitionsToUpdate_;
  private final String debugAction_;
  private final Map<String, Long> partitionToEventId_;
  private final String reason_;
  private final EventSequence catalogTimeline_;
  private final boolean isPreLoadForInsert_;

  public HdfsTableLoadParams(boolean reuseMetadata, IMetaStoreClient client,
      Table msTbl, boolean loadPartitionFileMetadata, boolean loadTableSchema,
      boolean refreshUpdatedPartitions, Set<String> partitionsToUpdate,
      String debugAction, Map<String, Long> partitionToEventId, String reason,
      EventSequence catalogTimeline, boolean isPreLoadForInsert) {
    reuseMetadata_ = reuseMetadata;
    client_ = client;
    msTbl_ = msTbl;
    loadPartitionFileMetadata_ = loadPartitionFileMetadata;
    loadTableSchema_ = loadTableSchema;
    refreshUpdatedPartitions_ = refreshUpdatedPartitions;
    partitionsToUpdate_ = partitionsToUpdate;
    debugAction_ = debugAction;
    partitionToEventId_ = partitionToEventId;
    reason_ = reason;
    catalogTimeline_ = catalogTimeline;
    isPreLoadForInsert_ = isPreLoadForInsert;
  }

  public boolean getIsReuseMetadata() {
    return reuseMetadata_;
  }

  public IMetaStoreClient getMsClient() {
    return client_;
  }

  public Table getMsTable() {
    return msTbl_;
  }

  public boolean isLoadPartitionFileMetadata() {
    return loadPartitionFileMetadata_;
  }

  public boolean getLoadTableSchema() {
    return loadTableSchema_;
  }

  public boolean getRefreshUpdatedPartitions() {
    return refreshUpdatedPartitions_;
  }

  public Set<String> getPartitionsToUpdate() {
    return partitionsToUpdate_;
  }

  public String getDebugAction() {
    return debugAction_;
  }

  public Map<String, Long> getPartitionToEventId() {
    return partitionToEventId_;
  }

  public String getReason() {
    return reason_;
  }

  public EventSequence getCatalogTimeline() {
    return catalogTimeline_;
  }

  public boolean getIsPreLoadForInsert() {
    return isPreLoadForInsert_;
  }
}
