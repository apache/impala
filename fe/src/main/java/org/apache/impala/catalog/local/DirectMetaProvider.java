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

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.impala.catalog.MetaStoreClientPool;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TBackendGflags;
import org.apache.thrift.TException;

import com.google.common.collect.ImmutableList;

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
}
