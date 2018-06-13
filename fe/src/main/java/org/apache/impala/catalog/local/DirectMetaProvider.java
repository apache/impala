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

package org.apache.impala.catalog.local;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.impala.catalog.MetaStoreClientPool;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TBackendGflags;
import org.apache.impala.util.MetaStoreUtil;
import org.apache.thrift.TException;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

/**
 * Metadata provider which calls out directly to the source systems
 * (filesystem, HMS, etc) with no caching.
 */
class DirectMetaProvider implements MetaProvider {
  private static MetaStoreClientPool msClientPool_;

  DirectMetaProvider() {
    initMsClientPool();
  }

  private static synchronized void initMsClientPool() {
    // Lazy-init the metastore client pool based on the backend configuration.
    // TODO(todd): this should probably be a process-wide singleton.
    if (msClientPool_ == null) {
      TBackendGflags cfg = BackendConfig.INSTANCE.getBackendCfg();
      msClientPool_ = new MetaStoreClientPool(cfg.num_metadata_loading_threads,
          cfg.initial_hms_cnxn_timeout_s);
    }
  }

  @Override
  public ImmutableList<String> loadDbList() throws TException {
    try (MetaStoreClient c = msClientPool_.getClient()) {
      return ImmutableList.copyOf(c.getHiveClient().getAllDatabases());
    }
  }

  @Override
  public Database loadDb(String dbName) throws TException {
    try (MetaStoreClient c = msClientPool_.getClient()) {
      return c.getHiveClient().getDatabase(dbName);
    }
  }

  @Override
  public ImmutableList<String> loadTableNames(String dbName)
      throws MetaException, UnknownDBException, TException {
    try (MetaStoreClient c = msClientPool_.getClient()) {
      return ImmutableList.copyOf(c.getHiveClient().getAllTables(dbName));
    }
  }

  @Override
  public Table loadTable(String dbName, String tableName)
      throws MetaException, NoSuchObjectException, TException {
    try (MetaStoreClient c = msClientPool_.getClient()) {
      return c.getHiveClient().getTable(dbName, tableName);
    }
  }

  @Override
  public String loadNullPartitionKeyValue() throws MetaException, TException {
    try (MetaStoreClient c = msClientPool_.getClient()) {
      return MetaStoreUtil.getNullPartitionKeyValue(c.getHiveClient());
    }
  }

  @Override
  public List<String> loadPartitionNames(String dbName, String tableName)
      throws MetaException, TException {
    try (MetaStoreClient c = msClientPool_.getClient()) {
      return c.getHiveClient().listPartitionNames(dbName, tableName,
          /*max_parts=*/(short)-1);
    }
  }

  @Override
  public Map<String, Partition> loadPartitionsByNames(
      String dbName, String tableName, List<String> partitionColumnNames,
      List<String> partitionNames) throws MetaException, TException {
    Preconditions.checkNotNull(dbName);
    Preconditions.checkNotNull(tableName);
    Preconditions.checkArgument(!partitionColumnNames.isEmpty());
    Preconditions.checkNotNull(partitionNames);

    Map<String, Partition> ret = Maps.newHashMapWithExpectedSize(
        partitionNames.size());
    if (partitionNames.isEmpty()) return ret;

    // Fetch the partitions.
    List<Partition> parts;
    try (MetaStoreClient c = msClientPool_.getClient()) {
      parts = MetaStoreUtil.fetchPartitionsByName(
          c.getHiveClient(), partitionNames, dbName, tableName);
    }

    // HMS may return fewer partition objects than requested, and the
    // returned partition objects don't carry enough information to get their
    // names. So, we map the returned partitions back to the requested names
    // using the passed-in partition column names.
    Set<String> namesSet = ImmutableSet.copyOf(partitionNames);
    for (Partition p: parts) {
      List<String> vals = p.getValues();
      if (vals.size() != partitionColumnNames.size()) {
        throw new MetaException("Unexpected number of partition values for " +
          "partition " + vals + " (expected " + partitionColumnNames.size() + ")");
      }
      String partName = FileUtils.makePartName(partitionColumnNames, p.getValues());
      if (!namesSet.contains(partName)) {
        throw new MetaException("HMS returned unexpected partition " + partName +
            " which was not requested. Requested: " + namesSet);
      }
      Partition existing = ret.put(partName, p);
      if (existing != null) {
        throw new MetaException("HMS returned multiple partitions with name " +
            partName);
      }
    }

    return ret;
  }
}
