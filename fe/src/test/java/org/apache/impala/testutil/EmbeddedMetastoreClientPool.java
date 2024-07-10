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

package org.apache.impala.testutil;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.impala.catalog.MetaStoreClientPool;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.log4j.Logger;

import java.nio.file.Path;

/**
 * An implementation of MetastoreClientPool that creates HiveMetastoreClient objects
 * with HMS running in an embedded fashion on the client side. It connects to a Derby
 * database backed by local file system storage.
 *
 * Since local Derby db allows a single connection at any point, there is no use in
 * creating a metastore client pool bigger than that size.
 */
public class EmbeddedMetastoreClientPool extends  MetaStoreClientPool {

  private static final Logger LOG = Logger.getLogger(EmbeddedMetastoreClientPool.class);

  private static final String CONNECTION_URL_TEMPLATE =
      "jdbc:derby:;databaseName=%s;create=true";

  private Path derbyDataStorePath_;

  public EmbeddedMetastoreClientPool(int initialCnxnTimeoutSec, Path dbStorePath) {
    super(1, initialCnxnTimeoutSec, generateEmbeddedHMSConf(dbStorePath));
    derbyDataStorePath_ = dbStorePath;
  }

  // Embedded HMS instantiates partition expression proxy which by default brings in a
  // lot of runtime dependencies from hive-exec. Since we don't depend on this, we
  // should use a DefaultPartitionExpressionProxy which is a no-op implementation of
  // PartitionExpressionProxy interface. It throws UnsupportedOperationException on its
  // APIs so we will find them in tests in case we start using these features when
  // using embedded HMS
  private static final String DEFAULT_PARTITION_EXPRESSION_PROXY_CLASS = "org.apache"
      + ".hadoop.hive.metastore.DefaultPartitionExpressionProxy";
  // In HMS-3 the default value of "metastore.task.thread.always is set to some classes
  // which are present in hive-exec. We don't need this for our tests as of 05/07/2019
  // Setting this to default EventCleanerTask avoid pulling in unnecessary dependencies
  // from hive-exec for running these tests. Note that this config key is not available
  // in HMS-2. But adding this is still okay since it only print a warning of unknown
  // config with no other side-effects
  private static final String METASTORE_TASK_THREAD_ALWAYS_KEY = "metastore.task"
      + ".threads.always";
  private static final String DEFAULT_EVENT_CLEANER_TASK_CLASS = "org.apache.hadoop"
      + ".hive.metastore.events.EventCleanerTask";

  /**
   * Generates the HiveConf required to connect to an embedded metastore backed by
   * derby DB.
   */
  private static HiveConf generateEmbeddedHMSConf(Path dbStorePath) {
    LOG.info("Creating embedded HMS instance at path: " + dbStorePath);
    HiveConf conf = new HiveConf(EmbeddedMetastoreClientPool.class);
    // An embedded HMS with local derby backend requires the following settings
    // hive.metastore.uris - empty
    // javax.jdo.option.ConnectionDriverName - org.apache.derby.jdbc.EmbeddedDriver
    // javax.jdo.option.ConnectionURL - jdbc:derby:;databaseName=<path>;create=true"
    conf.set(ConfVars.METASTORE_CONNECTION_DRIVER.varname,
        "org.apache.derby.jdbc.EmbeddedDriver");
    conf.setBoolean(ConfVars.METASTORE_SCHEMA_VERIFICATION.varname, false);
    conf.setBoolean(ConfVars.METASTORE_AUTO_CREATE_ALL.varname, true);
    conf.set(ConfVars.METASTORE_EXPRESSION_PROXY_CLASS.varname,
        DEFAULT_PARTITION_EXPRESSION_PROXY_CLASS);
    conf.set(METASTORE_TASK_THREAD_ALWAYS_KEY, DEFAULT_EVENT_CLEANER_TASK_CLASS);
    // Disabling notification event listeners
    conf.set(ConfVars.METASTORE_TRANSACTIONAL_EVENT_LISTENERS.varname, "");
    return conf;
  }

  @Override
  public void close() {
    super.close();
    // Cleanup the metastore directory.
    FileUtils.deleteQuietly(derbyDataStorePath_.toFile());
  }
}
