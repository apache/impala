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
    conf.set(HiveConf.ConfVars.METASTOREURIS.toString(), "");
    conf.set(HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER.toString(),
        "org.apache.derby.jdbc.EmbeddedDriver");
    conf.setBoolean(HiveConf.ConfVars.METASTORE_SCHEMA_VERIFICATION.toString(), false);
    conf.setBoolean(HiveConf.ConfVars.METASTORE_AUTO_CREATE_ALL.toString(), true);
    conf.set(HiveConf.ConfVars.METASTORECONNECTURLKEY.toString(),
        String.format(CONNECTION_URL_TEMPLATE, dbStorePath.toString()));
    return conf;
  }

  @Override
  public void close() {
    super.close();
    // Cleanup the metastore directory.
    FileUtils.deleteQuietly(derbyDataStorePath_.toFile());
  }
}
