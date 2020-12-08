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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.testutil.CatalogServiceTestCatalog;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test for the {@link HdfsPartition#compareSd(StorageDescriptor)} method which is used
 * to detect partitions which have been changed in HMS and needs to be reloaded.
 */
public class HdfsPartitionSdCompareTest {
  private CatalogServiceCatalog catalog_;
  private Partition hmsPartition_ = null;

  @Before
  public void init() throws Exception {
    catalog_ = CatalogServiceTestCatalog.create();
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      hmsPartition_ = client.getHiveClient()
          .getPartition("functional", "alltypes", "year=2009/month=1");
    }
  }

  @After
  public void cleanUp() { catalog_.close(); }

  /**
   * Test compares a HdfsPartition in catalog with a test storage descriptor and confirms
   * that the comparison returns expected results.
   */
  @Test
  public void testCompareSds() throws Exception {
    assertNotNull(hmsPartition_);
    assertNotNull(hmsPartition_.getSd());
    StorageDescriptor hmsSd = hmsPartition_.getSd();
    HdfsTable tbl = (HdfsTable) catalog_
        .getOrLoadTable("functional", "alltypes", "test", null);
    HdfsPartition hdfsPartition = tbl
        .getPartitionsForNames(Arrays.asList("year=2009/month=1")).get(0);
    // make sure that the sd in HMS without any change matches with the sd in catalog.
    assertTrue(hdfsPartition.compareSd(hmsSd));

    // test location change
    StorageDescriptor testSd = new StorageDescriptor(hmsSd);
    testSd.setLocation("file:///tmp/year=2009/month=1");
    assertFalse(hdfsPartition.compareSd(testSd));
    // test input format change
    testSd = new StorageDescriptor(hmsSd);
    testSd
        .setInputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
    assertFalse(hdfsPartition.compareSd(testSd));
    // test output format change
    testSd = new StorageDescriptor(hmsSd);
    testSd.setOutputFormat(
        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat");
    assertFalse(hdfsPartition.compareSd(testSd));
    // test change in cols
    testSd = new StorageDescriptor(hmsSd);
    testSd.addToCols(new FieldSchema("c1", "int", "comment"));
    assertFalse(hdfsPartition.compareSd(testSd));
    // test change in sortCols
    testSd = new StorageDescriptor(hmsSd);
    testSd.addToSortCols(new Order());
    testSd.setLocation("file:///tmp/year=2009/month=1");
    assertFalse(hdfsPartition.compareSd(testSd));
    // test serde library change
    testSd = new StorageDescriptor(hmsSd);
    testSd.setSerdeInfo(new SerDeInfo("parquet",
        "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", new HashMap<>()));
    assertFalse(hdfsPartition.compareSd(testSd));
    // test compressed flag change
    testSd = new StorageDescriptor(hmsSd);
    testSd.setCompressed(!hmsSd.isCompressed());
    assertFalse(hdfsPartition.compareSd(testSd));
    // test number of buckets change
    testSd = new StorageDescriptor(hmsSd);
    testSd.setNumBuckets(hmsSd.getNumBuckets() + 1);
    assertFalse(hdfsPartition.compareSd(testSd));
    // test sd params change
    testSd = new StorageDescriptor(hmsSd);
    testSd.putToParameters("test", "value");
    assertFalse(hdfsPartition.compareSd(testSd));
  }
}
