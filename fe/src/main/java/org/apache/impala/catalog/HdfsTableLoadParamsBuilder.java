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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.impala.util.EventSequence;

/**
 * Utility class to help set up the various parameters for load() method in HdfsTable.
 */
public class HdfsTableLoadParamsBuilder {
  boolean reuseMetadata_;
  IMetaStoreClient client_;
  Table msTbl_;
  boolean loadPartitionFileMetadata_;
  boolean loadTableSchema_;
  boolean refreshUpdatedPartitions_;
  Set<String> partitionsToUpdate_;
  String debugAction_;
  Map<String, Long> partitionToEventId_;
  String reason_;
  EventSequence catalogTimeline_;
  boolean isPreLoadForInsert_;

  public HdfsTableLoadParamsBuilder(IMetaStoreClient client, Table msTbl) {
    client_ = client;
    msTbl_ = msTbl;
  }

  public HdfsTableLoadParamsBuilder reuseMetadata(boolean reuseMetadata) {
    reuseMetadata_ = reuseMetadata;
    return this;
  }

  public HdfsTableLoadParamsBuilder setMsClient(IMetaStoreClient client) {
    client_ = client;
    return this;
  }

  public HdfsTableLoadParamsBuilder setMetastoreTable(Table msTbl) {
    msTbl_ = msTbl;
    return this;
  }

  public HdfsTableLoadParamsBuilder setLoadPartitionFileMetadata(
    boolean loadPartitionFileMetadata) {
    loadPartitionFileMetadata_ = loadPartitionFileMetadata;
    return this;
  }

  public HdfsTableLoadParamsBuilder setLoadTableSchema(boolean loadTableSchema) {
    loadTableSchema_ = loadTableSchema;
    return this;
  }

  public HdfsTableLoadParamsBuilder setRefreshUpdatedPartitions(
    boolean refreshUpdatedPartitions) {
    refreshUpdatedPartitions_ = refreshUpdatedPartitions;
    return this;
  }

  public HdfsTableLoadParamsBuilder setPartitionsToUpdate(
    Set<String> partitionsToUpdate) {
    // 'partitionsToUpdate' should be a pass-by-value argument as
    // HdfsTable#updatePartitionsFromHms() may modify this HashSet if the
    // partitions already exists is metastore but not in Impala's cache.
    // Reason: 'partitionsToUpdate' is used by HdfsTable#load() to reload file
    // metadata of newly created partitions in Impala.
    if (partitionsToUpdate != null) { //partitionsToUpdate can be null
      partitionsToUpdate_ = new HashSet<>(partitionsToUpdate);
    }
    return this;
  }

  public HdfsTableLoadParamsBuilder setDebugAction(String debugAction) {
    debugAction_ = debugAction;
    return this;
  }

  public HdfsTableLoadParamsBuilder setPartitionToEventId(
    Map<String, Long> partitionToEventId) {
    partitionToEventId_ = partitionToEventId;
    return this;
  }

  public HdfsTableLoadParamsBuilder setReason(String reason) {
    reason_ = reason;
    return this;
  }

  public HdfsTableLoadParamsBuilder setCatalogTimeline(EventSequence catalogTimeline) {
    catalogTimeline_ = catalogTimeline;
    return this;
  }

  public HdfsTableLoadParamsBuilder setIsPreLoadForInsert(boolean isPreLoadForInsert) {
    isPreLoadForInsert_ = isPreLoadForInsert;
    return this;
  }

  public HdfsTableLoadParams build() {
    return new HdfsTableLoadParams(reuseMetadata_, client_, msTbl_,
        loadPartitionFileMetadata_, loadTableSchema_, refreshUpdatedPartitions_,
        partitionsToUpdate_, debugAction_, partitionToEventId_, reason_,
        catalogTimeline_, isPreLoadForInsert_);
  }
}