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

package org.apache.impala.catalog.metastore;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.impala.authorization.NoopAuthorizationFactory;
import org.apache.impala.authorization.NoopAuthorizationFactory.NoopAuthorizationManager;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.testutil.CatalogServiceTestCatalog;
import org.apache.impala.testutil.CatalogTestMetastoreServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Base class for Catalog metastore server tests which sets up a Catalog metastore
 * server endpoint at a random available port.
 */
public abstract class AbstractCatalogMetastoreTest {
  protected static CatalogServiceTestCatalog catalog_;
  protected static CatalogOpExecutor catalogOpExecutor_;
  protected static CatalogMetastoreServer catalogMetastoreServer_;
  protected static HiveMetaStoreClient catalogHmsClient_;
  protected static final Configuration CONF = MetastoreConf.newMetastoreConf();

  @BeforeClass
  public static void setup() throws Exception {
    catalog_ = CatalogServiceTestCatalog.create();
    catalogOpExecutor_ = catalog_.getCatalogOpExecutor();
    catalogMetastoreServer_ = new CatalogTestMetastoreServer(
        catalogOpExecutor_);
    catalog_.setCatalogMetastoreServer(catalogMetastoreServer_);
    catalogMetastoreServer_.start();
    MetastoreConf.setVar(CONF, ConfVars.THRIFT_URIS,
        "thrift://localhost:" + catalogMetastoreServer_.getPort());
    // metastore clients which connect to catalogd's HMS endpoint need this
    // configuration set since the forwarded HMS call use catalogd's HMS client
    // not the end-user's UGI.
    CONF.set("hive.metastore.execute.setugi", "false");
    catalogHmsClient_ = new HiveMetaStoreClient(CONF);
  }

  /**
   * Sort the given partitions by names.
   */
  protected static void sortPartitionsByNames(List<FieldSchema> partitionKeys,
      List<Partition> retPartitions) {
    assertTrue(retPartitions.isEmpty() || partitionKeys.size() == retPartitions.get(0)
        .getValuesSize());
    List<String> partitionColNames = new ArrayList<>();
    for (FieldSchema partSchema : partitionKeys) {
      partitionColNames.add(partSchema.getName());
    }
    retPartitions.sort(Comparator.comparing(
        (part) -> MetastoreShim.makePartName(partitionColNames, part.getValues())));
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    catalogMetastoreServer_.stop();
    catalog_.close();
  }

}
