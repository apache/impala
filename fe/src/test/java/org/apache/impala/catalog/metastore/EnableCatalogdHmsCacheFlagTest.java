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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesResult;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.impala.catalog.HdfsPartition;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.service.BackendConfig;
import org.junit.Test;

public class EnableCatalogdHmsCacheFlagTest extends AbstractCatalogMetastoreTest {

  /**
   * The test fetches partitions of a table over HMS API and then compares if the
   * deserialized filemetadata from the response matches with what we have in catalogd.
   */
  @Test
  public void testEnableCatalogdCachingFlag() throws Exception {

    BackendConfig.INSTANCE.setEnableCatalogdHMSCache(true);

    // get partitions from catalog directly
    HdfsTable tbl = (HdfsTable) catalog_
            .getOrLoadTable("functional", "alltypes", "test", null);
    HdfsPartition hdfsPartition1 = tbl
            .getPartitionsForNames(Arrays.asList("year=2009/month=1")).get(0);
    HdfsPartition hdfsPartition2 = tbl
            .getPartitionsForNames(Arrays.asList("year=2009/month=2")).get(0);

    BackendConfig.INSTANCE.setEnableCatalogdHMSCache(false);

    // test empty partitions result case.
    GetPartitionsByNamesRequest request = new GetPartitionsByNamesRequest();
    String dbName = MetaStoreUtils.prependCatalogToDbName("functional", CONF);
    request.setDb_name(dbName);
    request.setTbl_name("alltypes");
    // no names are set so the result is expected to be empty
    request.setNames(new ArrayList<>());
    request.setGetFileMetadata(true);
    GetPartitionsByNamesResult result = catalogHmsClient_.getPartitionsByNames(request);
    assertTrue(result.getPartitions().isEmpty());
    Map<Partition, List<FileDescriptor>> fds = CatalogHmsClientUtils
            .extractFileDescriptors(result, tbl.getHostIndex());
    assertTrue(fds.isEmpty());

    // get the partitions over HMS API.
    request = new GetPartitionsByNamesRequest();
    dbName = MetaStoreUtils.prependCatalogToDbName("functional", CONF);
    request.setDb_name(dbName);
    request.setTbl_name("alltypes");
    request.setNames(Arrays.asList("year=2009/month=1", "year=2009/month=2"));
    request.setGetFileMetadata(true);
    result = catalogHmsClient_.getPartitionsByNames(request);
    // sort partitions by names to avoid flakiness
    sortPartitionsByNames(tbl.getMetaStoreTable().getPartitionKeys(),
        result.getPartitions());
    for (Partition part : result.getPartitions()) {
      assertNotNull(part.getFileMetadata());
    }
    assertNotNull(result.getDictionary());
    fds = CatalogHmsClientUtils
            .extractFileDescriptors(result, tbl.getHostIndex());
    assertEquals(2, fds.size());
    for (List<FileDescriptor> partFds : fds.values()) {
      assertFalse(partFds.isEmpty());
      assertEquals(1, partFds.size());
    }

    // make sure that the FileDescriptors from catalog and over HMS API are the same
    // for the same hostIndex
    CatalogHmsFileMetadataTest.assertFdsAreSame(hdfsPartition1.getFileDescriptors(),
            fds.get(result.getPartitions().get(0)));
    CatalogHmsFileMetadataTest.assertFdsAreSame(hdfsPartition2.getFileDescriptors(),
            fds.get(result.getPartitions().get(1)));
  }
}