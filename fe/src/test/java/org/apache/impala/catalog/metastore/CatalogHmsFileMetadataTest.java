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

import com.google.common.collect.Iterables;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesResult;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.catalog.HdfsPartition;
import org.apache.impala.catalog.HdfsPartition.FileBlock;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.fb.FbFileBlock;
import org.apache.impala.testutil.CatalogServiceTestCatalog;
import org.apache.impala.testutil.CatalogServiceTestCatalog.CatalogServiceTestHMSCatalog;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class CatalogHmsFileMetadataTest {
  private static CatalogServiceCatalog catalog_;
  private static HiveMetaStoreClient catalogHmsClient_;
  private static final Configuration CONF = MetastoreConf.newMetastoreConf();

  @BeforeClass
  public static void setup() throws Exception {
    catalog_ = CatalogServiceTestCatalog.createTestCatalogMetastoreServer();
    MetastoreConf.setVar(CONF, ConfVars.THRIFT_URIS,
        "thrift://localhost:" + ((CatalogServiceTestHMSCatalog) catalog_).getPort());
    // metastore clients which connect to catalogd's HMS endpoint need this
    // configuration set since the forwarded HMS call use catalogd's HMS client
    // not the end-user's UGI.
    CONF.set("hive.metastore.execute.setugi", "false");
    catalogHmsClient_ = new HiveMetaStoreClient(CONF);
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    catalog_.close();
  }

  /**
   * The test fetches partitions of a table over HMS API and then compares if the
   * deserialized filemetadata from the response matches with what we have in catalogd.
   */
  @Test
  public void testFileMetadataForPartitions() throws Exception {
    // get partitions from catalog directly
    HdfsTable tbl = (HdfsTable) catalog_
        .getOrLoadTable("functional", "alltypes", "test", null);
    HdfsPartition hdfsPartition1 = tbl
        .getPartitionsForNames(Arrays.asList("year=2009/month=1")).get(0);
    HdfsPartition hdfsPartition2 = tbl
        .getPartitionsForNames(Arrays.asList("year=2009/month=2")).get(0);

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
    assertFdsAreSame(hdfsPartition1.getFileDescriptors(),
        fds.get(result.getPartitions().get(0)));
    assertFdsAreSame(hdfsPartition2.getFileDescriptors(),
        fds.get(result.getPartitions().get(1)));
  }

  public static void assertFdsAreSame(List<FileDescriptor> fdsFromCatalog,
      List<FileDescriptor> fdsFromHMS) {
    assertEquals(fdsFromCatalog.size(), fdsFromHMS.size());
    for (int i=0; i<fdsFromCatalog.size(); i++) {
      FileDescriptor fdFromCatalog = fdsFromCatalog.get(i);
      FileDescriptor fdFromHMS = fdsFromHMS.get(i);
      assertEquals(fdFromCatalog.getRelativePath(), fdFromHMS.getRelativePath());
      assertEquals(fdFromCatalog.getFileCompression(), fdFromHMS.getFileCompression());
      assertEquals(fdFromCatalog.getFileLength(), fdFromHMS.getFileLength());
      assertEquals(fdFromCatalog.getIsEc(), fdFromHMS.getIsEc());
      assertEquals(fdFromCatalog.getModificationTime(), fdFromHMS.getModificationTime());
      assertEquals(fdFromCatalog.getNumFileBlocks(), fdFromHMS.getNumFileBlocks());
      for (int j=0; j<fdFromCatalog.getNumFileBlocks(); j++) {
        FbFileBlock blockFromCat = fdFromCatalog.getFbFileBlock(j);
        FbFileBlock blockFromHMS = fdFromCatalog.getFbFileBlock(j);
        // quick and dirty way to compare the relevant fields within the file blocks.
        assertEquals(FileBlock.debugString(blockFromCat),
            FileBlock.debugString(blockFromHMS));
      }
    }
  }

  /**
   * Test requests a table over HMS API with file-metadata and then compares if the
   * file-metadata returned is same as what we have in catalogd.
   */
  @Test
  public void testFileMetadataForTable() throws Exception {
    Table tbl = catalogHmsClient_
        .getTable(null, "functional", "zipcode_incomes", null, false, null, true);
    assertNotNull(tbl.getFileMetadata());
    HdfsTable catTbl = (HdfsTable) catalog_
        .getOrLoadTable("functional", "zipcode_incomes", "test", null);
    HdfsPartition part = (HdfsPartition) Iterables.getOnlyElement(catTbl.getPartitions());
    List<FileDescriptor> hmsTblFds = CatalogHmsClientUtils
        .extractFileDescriptors(tbl, catTbl.getHostIndex());
    assertFdsAreSame(part.getFileDescriptors(), hmsTblFds);
  }
}
