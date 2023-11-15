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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.impala.catalog.MetaStoreClientPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of MetastoreClientPool that creates HiveMetastoreClient objects
 * using wrong HMS address to mimic HMS connection failures. Its getClient() will always
 * fail and throws MetastoreClientInstantiationException.
 */
public class IncompetentMetastoreClientPool extends MetaStoreClientPool {
  private static final Logger LOG =
      LoggerFactory.getLogger(IncompetentMetastoreClientPool.class);

  public IncompetentMetastoreClientPool(int initialSize, int initialCnxnTimeoutSec) {
    super(initialSize, initialCnxnTimeoutSec, generateHMSConfWithWrongAddr());
  }

  private static HiveConf generateHMSConfWithWrongAddr() {
    LOG.info("Creating IncompetentMetastoreClientPool using a wrong HMS port");
    HiveConf conf = new HiveConf(IncompetentMetastoreClientPool.class);
    MetastoreConf.setVar(conf, ConfVars.THRIFT_URIS, "thrift://localhost:123");
    return conf;
  }
}
