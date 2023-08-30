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

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.Reference;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.testutil.CatalogServiceTestCatalog;
import org.apache.impala.testutil.HiveJdbcClientPool;
import org.apache.impala.testutil.HiveJdbcClientPool.HiveJdbcClient;
import org.apache.impala.testutil.ImpalaJdbcClient;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TGetPartialCatalogObjectRequest;
import org.apache.impala.thrift.TGetPartialCatalogObjectResponse;
import org.apache.impala.thrift.THdfsFileDesc;
import org.apache.impala.thrift.THdfsTable;
import org.apache.impala.thrift.TImpalaTableType;
import org.apache.impala.thrift.TPartialPartitionInfo;
import org.apache.impala.thrift.TTable;
import org.apache.impala.thrift.TTableInfoSelector;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.util.AcidUtils;
import org.apache.impala.util.EventSequence;
import org.apache.impala.util.NoOpEventSequence;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;

/**
 * Tests related to getPartialCatalogObject API support for ValidWriteIdList. The tests
 * execute various scenarios when the catalog cache state is either behind or ahead of the
 * client provided ValidWriteIdList. TODO: The test is more appropriate for a e2e test in
 * pytest framework since sometimes the compactions take long time.
 * But that would need frontend support for sending the ValidWriteIdList
 * which is more complex and needs to be done as a separate change (IMPALA-8788).
 */
public class PartialCatalogInfoWriteIdTest {

  private static final Logger LOG = LoggerFactory
      .getLogger(PartialCatalogInfoWriteIdTest.class);
  private static CatalogServiceCatalog catalog_;
  private static HiveJdbcClientPool hiveClientPool_;
  private static final String testDbName = "partial_catalog_info_test";
  private static final String testTblName = "insert_only";
  private static final String testPartitionedTbl = "insert_only_partitioned";
  private static final String testAcidTblName = "test_full_acid";

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setupTestEnv() throws SQLException, ClassNotFoundException {
    catalog_ = CatalogServiceTestCatalog.create();
    hiveClientPool_ = HiveJdbcClientPool.create(1);
  }

  @AfterClass
  public static void shutdown() {
    if (catalog_ != null) {
      catalog_.close();
    }
    if (hiveClientPool_ != null) {
      hiveClientPool_.close();
    }
  }

  @Before
  public void createTestTbls() throws Exception {
    LOG.info("Creating test tables for {}", name.getMethodName());
    Stopwatch st = Stopwatch.createStarted();
    ImpalaJdbcClient client = ImpalaJdbcClient
        .createClientUsingHiveJdbcDriver();
    client.connect();
    try {
      client.execStatement("drop database if exists " + testDbName + " cascade");
      client.execStatement("create database " + testDbName);
      client.execStatement("create table " + getTestTblName() + " like "
          + "functional.insert_only_transactional_table stored as parquet");
      client.execStatement("insert into " + getTestTblName() + " values (1)");
      client.execStatement("create table " + getPartitionedTblName() + " (c1 int) "
              + "partitioned by (part int) stored as parquet " + getTblProperties());
      client.execStatement("insert into " + getPartitionedTblName() +
          " partition (part=1) values (1)");
    } finally {
      LOG.info("Time taken for createTestTbls {} msec",
          st.stop().elapsed(TimeUnit.MILLISECONDS));
      client.close();
    }
    catalog_.reset(NoOpEventSequence.INSTANCE);
  }

  private static String getTblProperties() {
    return "tblproperties ('transactional'='true', 'transactional_properties' = "
      + "'insert_only')";
  }

  @After
  public void dropTestTbls() throws Exception {
    ImpalaJdbcClient client = ImpalaJdbcClient
        .createClientUsingHiveJdbcDriver();
    client.connect();
    try {
      client.execStatement("drop database if exists " + testDbName + " cascade");
    } finally {
      client.close();
    }
  }

  /**
   * Catalog does not have have the table loaded. This is the base case when the table is
   * first loaded. It makes sure that the returned writeIdList is consistent with the
   * client's writeListIdList.
   */
  @Test
  public void testCatalogLoadWithWriteIds()
    throws CatalogException, InternalException, TException {
    Assume.assumeTrue(MetastoreShim.getMajorVersion() >= 3);
    // invalidate the ACID tables if it already exists
    invalidateTbl(testDbName, testTblName);
    long prevVersion =
      catalog_.getOrLoadTable(testDbName, testTblName, "test", null).getCatalogVersion();
    ValidWriteIdList validWriteIdList = getValidWriteIdList(testDbName, testTblName);
    TGetPartialCatalogObjectRequest req = new RequestBuilder()
      .db(testDbName)
      .tbl(testTblName)
      .writeId(validWriteIdList)
      .wantFiles()
      .build();
    TGetPartialCatalogObjectResponse response = sendRequest(req);
    Assert.assertEquals(MetastoreShim.convertToTValidWriteIdList(validWriteIdList),
      response.table_info.valid_write_ids);
    // make sure the table was not loaded in the cache hit scenario
    Assert.assertTrue(
      catalog_.getTable(testDbName, testTblName).getCatalogVersion() == prevVersion);
  }

  /**
   * Test exercises the code path when catalog has a stale transactional table in its
   * cache when compared the to the client provided ValidWriteIdList. It makes sure that
   * the table is reloaded and the returned writeId is consistent with the requested
   * writeIdList of the table.
   */
  @Test
  public void testCatalogBehindClientWriteIds() throws Exception {
    Assume.assumeTrue(MetastoreShim.getMajorVersion() >= 3);
    Table tbl = catalog_.getOrLoadTable(testDbName, testTblName, "test", null);
    Assert.assertFalse("Table must be loaded",
      tbl instanceof IncompleteTable);
    long previousVersion = tbl.getCatalogVersion();
    // do some hive operations to advance the writeIds in HMS
    executeHiveSql("insert into " + getTestTblName() + " values (2)");
    // get the latest validWriteIdList
    ValidWriteIdList validWriteIdList = getValidWriteIdList(testDbName, testTblName);
    TGetPartialCatalogObjectRequest req = new RequestBuilder()
      .db(testDbName)
      .tbl(testTblName)
      .writeId(validWriteIdList)
      .wantFiles()
      .build();
    TGetPartialCatalogObjectResponse response = sendRequest(req);
    Assert.assertEquals(MetastoreShim.convertToTValidWriteIdList(validWriteIdList),
      response.table_info.valid_write_ids);
    // this should trigger a load of the table and hence the version should be higher
    Assert.assertTrue(
      catalog_.getTable(testDbName, testTblName).getCatalogVersion() > previousVersion);
  }

  /**
   * Test exercises the code path when catalog has a more recent version of transactional
   * table in its cache when compared the to the client provided ValidWriteIdList. It
   * makes sure that the table which is loaded on the Impalad has the writeId which is
   * consistent with the requested writeIdList of the table.
   */
  @Test
  public void testCatalogAheadOfClientWriteIds() throws Exception {
    Assume.assumeTrue(MetastoreShim.getMajorVersion() >= 3);
    Table tbl = catalog_.getOrLoadTable(testDbName, testTblName, "test", null);
    Assert.assertFalse("Table must be loaded",
      tbl instanceof IncompleteTable);
    ValidWriteIdList validWriteIdList = getValidWriteIdList(testDbName, testTblName);
    // now insert into the table to advance the writeId
    executeHiveSql("insert into " + getTestTblName() + " values (2)");
    catalog_.invalidateTable(new TTableName(testDbName, testTblName), new Reference<>(),
        new Reference<>(), NoOpEventSequence.INSTANCE);
    Table tblAfterReload = catalog_.getOrLoadTable(testDbName, testTblName, "test", null);
    long tblVersion = tblAfterReload.getCatalogVersion();
    // issue a request which is older than what we have in catalog
    TGetPartialCatalogObjectRequest req = new RequestBuilder()
      .db(testDbName)
      .tbl(testTblName)
      .writeId(validWriteIdList)
      .wantFiles()
      .build();
    TGetPartialCatalogObjectResponse response = sendRequest(req);
    TPartialPartitionInfo partialPartitionInfo =
      Iterables.getOnlyElement(response.table_info.partitions);
    // since the client requested before the second file was added the number of files
    // should be only 1
    Assert.assertEquals(1, partialPartitionInfo.file_descriptors.size());
    // we don't expect catalog to load the table since catalog is already ahead of client.
    Assert.assertEquals(tblVersion,
      catalog_.getOrLoadTable(testDbName, testTblName, "test", null).getCatalogVersion());
  }

  /**
   * ValidWriteId support only applies for the file-metadata. If the cached
   * ValidWriteIdList of the transactional table is ahead of requested one, it should
   * still return all the partitions. However, the partitions which are returned should
   * always have files which are consistent with the requested writeIDs
   */
  @Test
  public void testFetchGranularityWithWriteIds() throws Exception {
    Assume.assumeTrue(MetastoreShim.getMajorVersion() >= 3);
    Table tbl = catalog_.getOrLoadTable(testDbName, testPartitionedTbl, "test", null);
    long olderVersion = tbl.getCatalogVersion();
    Assert.assertFalse("Table must be loaded",
      tbl instanceof IncompleteTable);
    ValidWriteIdList olderWriteIdList = getValidWriteIdList(testDbName,
      testPartitionedTbl);
    executeHiveSql("insert into " + getPartitionedTblName() + " partition (part=2) "
      + "values (2)");
    ValidWriteIdList currentWriteIdList = getValidWriteIdList(testDbName,
      testPartitionedTbl);
    // client requests olderWriteIdList which is not loaded in Catalog, this is still a
    // cache hit scenario since catalog can satisfy what client requires without reloading
    TGetPartialCatalogObjectRequest request = new RequestBuilder()
      .db(testDbName)
      .tbl(testPartitionedTbl)
      .writeId(olderWriteIdList)
      .wantFiles()
      .build();
    TGetPartialCatalogObjectResponse response = sendRequest(request);
    Assert.assertEquals(1, response.getTable_info().getPartitionsSize());
    Assert.assertEquals(1, response.getTable_info().getPartition_prefixesSize());
    Assert.assertNotNull(
        response.getTable_info().getPartitions().get(0).getFile_descriptors());
    Assert.assertNotNull(
        response.getTable_info().getPartitions().get(0).getLocation());
    Assert.assertEquals(0,
        response.getTable_info().getPartitions().get(0).getLocation().getPrefix_index());
    Assert.assertNotNull(
        response.getTable_info().getPartitions().get(0).getHdfs_storage_descriptor());
    Assert.assertNotNull(
        response.getTable_info().getPartitions().get(0).getHms_parameters());

    // skipping request for file-metadata should not affect the result
    request = new RequestBuilder()
      .db(testDbName)
      .tbl(testPartitionedTbl)
      .writeId(olderWriteIdList)
      .wantPartitionNames()
      .build();
    response = sendRequest(request);
    Assert.assertEquals(1, response.getTable_info().getPartitionsSize());
    for (TPartialPartitionInfo partInfo : response.getTable_info().getPartitions()) {
      Assert.assertNull(partInfo.getFile_descriptors());
      Assert.assertNull(partInfo.getHdfs_storage_descriptor());
      Assert.assertNull(partInfo.getLocation());
    }

    // we request a newer WriteIdList now, and catalog needs to reload
    request = new RequestBuilder()
      .db(testDbName)
      .tbl(testPartitionedTbl)
      .writeId(currentWriteIdList)
      .wantFiles()
      .build();
    response = sendRequest(request);
    Assert.assertEquals(2, response.getTable_info().getPartitionsSize());
    Assert.assertEquals(1, response.getTable_info().getPartition_prefixesSize());
    // we expect both the partitions to have the file-metadata in the response
    for (TPartialPartitionInfo partInfo : response.getTable_info().getPartitions()) {
      Assert.assertNotNull(partInfo.getFile_descriptors());
      Assert.assertNotNull(partInfo.getHdfs_storage_descriptor());
      Assert.assertNotNull(partInfo.getLocation());
      Assert.assertEquals(0, partInfo.getLocation().getPrefix_index());
    }
    // table must be reloaded now
    long newerVersion = catalog_.getTable(testDbName, testPartitionedTbl)
      .getCatalogVersion();
    Assert.assertTrue(newerVersion > olderVersion);

    request = new RequestBuilder()
      .db(testDbName)
      .tbl(testPartitionedTbl)
      .writeId(olderWriteIdList)
      .wantFiles()
      .build();
    response = sendRequest(request);
    // HMS metadata provides read-committed isolation level and hence it is possible
    // that we see partitions which are from a writeId which is ahead of the requested
    // writeId. However, we should not see files pertaining to such partitions
    Assert.assertEquals(2, response.getTable_info().getPartitionsSize());
    // since we requested with an older writeIdList, we expect the second partition to
    // be empty
    for (TPartialPartitionInfo partitionInfo : response.getTable_info().getPartitions()) {
      if (partitionInfo.getName().equalsIgnoreCase("part=2")) {
        Assert.assertTrue(partitionInfo.getFile_descriptors().isEmpty());
      } else {
        Assert.assertFalse(partitionInfo.getFile_descriptors().isEmpty());
      }
    }
  }

  private long getMetricCount(String db, String tbl, String name)
      throws CatalogException {
    return catalog_.getTable(db, tbl).getMetrics().getCounter(name).getCount();
  }

  /**
   * Test makes sure that the metadata which is requested after a table has been major
   * compacted is consistent with the validWriteId provided.
   * @throws Exception
   */
  @Test
  public void fetchAfterMajorCompaction() throws Exception {
    Assume.assumeTrue(MetastoreShim.getMajorVersion() >= 3);
    Table tbl = catalog_.getOrLoadTable(testDbName, testPartitionedTbl, "test", null);
    Assert.assertFalse("Table must be loaded",
        tbl instanceof IncompleteTable);
    // row 2
    executeHiveSql("insert into " + getPartitionedTblName() + " partition (part=1) "
        + "values (2)");
    ValidWriteIdList olderWriteIdList = getValidWriteIdList(testDbName,
        testPartitionedTbl);
    // row 3
    executeHiveSql("insert into " + getPartitionedTblName() + " partition (part=1) "
        + "values (2)");
    // row 4
    executeHiveSql("insert into " + getPartitionedTblName() + " partition (part=2) "
        + "values (2)");
    executeHiveSql(
        "alter table " + getPartitionedTblName()
            + " partition(part=1) compact 'major' and wait");
    long numMisses = getMetricCount(testDbName, testPartitionedTbl,
        HdfsTable.FILEMETADATA_CACHE_MISS_METRIC);
    long numHits = getMetricCount(testDbName, testPartitionedTbl,
        HdfsTable.FILEMETADATA_CACHE_HIT_METRIC);
    ValidWriteIdList currentWriteIdList = getValidWriteIdList(testDbName,
        testPartitionedTbl);
    // issue a get request at latest writeIdList to trigger a load
    TGetPartialCatalogObjectRequest request = new RequestBuilder()
        .db(testDbName)
        .tbl(testPartitionedTbl)
        .writeId(currentWriteIdList)
        .wantFiles()
        .build();
    TGetPartialCatalogObjectResponse response = sendRequest(request);
    Assert.assertEquals(2, response.getTable_info().getPartitionsSize());
    for (TPartialPartitionInfo partitionInfo : response.getTable_info().getPartitions()) {
        Assert.assertEquals(1, partitionInfo.getFile_descriptors().size());
        Assert.assertEquals(0, partitionInfo.getInsert_file_descriptors().size());
        Assert.assertEquals(0, partitionInfo.getDelete_file_descriptors().size());
    }
    long numMissesAfter = getMetricCount(testDbName, testPartitionedTbl,
        HdfsTable.FILEMETADATA_CACHE_MISS_METRIC);
    long numHitsAfter = getMetricCount(testDbName, testPartitionedTbl,
        HdfsTable.FILEMETADATA_CACHE_HIT_METRIC);
    // the hit count increases by 2, one for each partition
    Assert.assertEquals(numHits + 2, numHitsAfter);
    Assert.assertEquals(numMisses, numMissesAfter);
    // now issue a request with older writeId
    request = new RequestBuilder()
        .db(testDbName)
        .tbl(testPartitionedTbl)
        .writeId(olderWriteIdList)
        .wantFiles()
        .build();
    response = sendRequest(request);
    // older writeIds should see both the partitions but only one of the partitions should
    // have file-metadata (2 files)
    Assert.assertEquals(2, response.getTable_info().getPartitionsSize());
    for (TPartialPartitionInfo partitionInfo : response.getTable_info().getPartitions()) {
      if (partitionInfo.getName().equals("part=1")) {
        Assert.assertEquals(2, partitionInfo.getFile_descriptors().size());
      } else {
        Assert.assertTrue(partitionInfo.getFile_descriptors().isEmpty());
      }
      Assert.assertEquals(0, partitionInfo.getInsert_file_descriptors().size());
      Assert.assertEquals(0, partitionInfo.getDelete_file_descriptors().size());
    }

    numMisses = getMetricCount(testDbName, testPartitionedTbl,
        HdfsTable.FILEMETADATA_CACHE_MISS_METRIC);
    numHits = getMetricCount(testDbName, testPartitionedTbl,
        HdfsTable.FILEMETADATA_CACHE_HIT_METRIC);
    // hit count increases by 1 since for part=2 we can ignore all the files and there was
    // no need to reload
    Assert.assertEquals(numHitsAfter+1, numHits);
    // Catalog reloads the filemetadata for one partition and hence the number of misses
    // should be 1 higher
    Assert.assertEquals(numMissesAfter+1, numMisses);
    // issue a request with current writeId to make we didn't mess up the table's metadata
    request = new RequestBuilder()
        .db(testDbName)
        .tbl(testPartitionedTbl)
        .writeId(currentWriteIdList)
        .wantFiles()
        .build();
    response = sendRequest(request);
    Assert.assertEquals(2, response.getTable_info().getPartitionsSize());
    for (TPartialPartitionInfo partitionInfo : response.getTable_info().getPartitions()) {
      Assert.assertEquals(1, partitionInfo.getFile_descriptors().size());
      Assert.assertEquals(0, partitionInfo.getInsert_file_descriptors().size());
      Assert.assertEquals(0, partitionInfo.getDelete_file_descriptors().size());
    }
  }

  /**
   * Similar to testFetchAfterMajorCompaction but does a minor compaction instead.
   * @throws Exception
   */
  @Test
  public void testFetchAfterMinorCompaction() throws Exception {
    Assume.assumeTrue(MetastoreShim.getMajorVersion() >= 3);
    Table tbl = catalog_.getOrLoadTable(testDbName, testTblName, "test", null);
    Assert.assertFalse("Table must be loaded",
        tbl instanceof IncompleteTable);
    // row 2, first row is in the setup method
    executeHiveSql("insert into " + getTestTblName() + " values (2)");
    ValidWriteIdList olderWriteIdList = getValidWriteIdList(testDbName,
        testTblName);
    // row 3
    executeHiveSql("insert into " + getTestTblName() + " values (3)");
    executeHiveSql(
        "alter table " + getTestTblName()+ " compact 'minor' and wait");
    ValidWriteIdList currentWriteIdList = getValidWriteIdList(testDbName,
        testTblName);
    long numMisses = getMetricCount(testDbName, testTblName,
        HdfsTable.FILEMETADATA_CACHE_MISS_METRIC);
    long numHits = getMetricCount(testDbName, testTblName,
        HdfsTable.FILEMETADATA_CACHE_HIT_METRIC);
    // issue a get request at latest writeIdList to trigger a load
    TGetPartialCatalogObjectRequest request = new RequestBuilder()
        .db(testDbName)
        .tbl(testTblName)
        .writeId(currentWriteIdList)
        .wantFiles()
        .build();
    TGetPartialCatalogObjectResponse response = sendRequest(request);
    Assert.assertEquals(1, response.getTable_info().getPartitionsSize());
    for (TPartialPartitionInfo partitionInfo : response.getTable_info().getPartitions()) {
      Assert.assertEquals(1, partitionInfo.getFile_descriptors().size());
    }
    long numMissesAfter = getMetricCount(testDbName, testTblName,
        HdfsTable.FILEMETADATA_CACHE_MISS_METRIC);
    long numHitsAfter = getMetricCount(testDbName, testTblName,
        HdfsTable.FILEMETADATA_CACHE_HIT_METRIC);
    // we triggered a reload of the table. We expect that filemetadata should be a cache
    // hit
    Assert.assertEquals(numHits + 1, numHitsAfter);
    Assert.assertEquals(numMisses, numMissesAfter);
    // issue a request with writeId before the minor compaction
    request = new RequestBuilder()
        .db(testDbName)
        .tbl(testTblName)
        .writeId(olderWriteIdList)
        .wantFiles()
        .build();
    response = sendRequest(request);
    Assert.assertEquals(1, response.getTable_info().getPartitionsSize());
    for (TPartialPartitionInfo partitionInfo : response.getTable_info().getPartitions()) {
      // we expect that catalog will load the files from FileSystem for this case so
      // the number of delta files will be 2 (files before minor compaction)
      Assert.assertEquals(2, partitionInfo.getFile_descriptors().size());
    }
    long numMisses1 = getMetricCount(testDbName, testTblName,
        HdfsTable.FILEMETADATA_CACHE_MISS_METRIC);
    long numHits1 = getMetricCount(testDbName, testTblName,
        HdfsTable.FILEMETADATA_CACHE_HIT_METRIC);
    // we expect the miss count to increase by 1 for the only partition
    Assert.assertEquals(numMissesAfter + 1, numMisses1 );
    Assert.assertEquals(numHitsAfter, numHits1 );
  }

  @Test
  public void testTableFileMetadataAfterMajorCompaction() throws Exception {
    // compacted table should return only one file
    testFileMetadataAfterCompaction(testTblName, "", true, 1);
  }

  @Test
  public void testTableFileMetadataAfterMinorCompaction() throws Exception {
    // compacted table should return only one file
    testFileMetadataAfterCompaction(testTblName, "", false, 1);
  }

  @Test
  public void testPartitionFileMetadataAfterMajorCompaction() throws Exception {
    String partition = "partition (part=1)";
    // compacted partition should return only one file
    testFileMetadataAfterCompaction(testPartitionedTbl, partition, true, 1);
  }

  @Test
  public void testPartitionFileMetadataAfterMinorCompaction() throws Exception {
    String partition = "partition (part=1)";
    // compacted partition should return only one file
    testFileMetadataAfterCompaction(testPartitionedTbl, partition, false, 1);
  }

  @Test
  public void testFullAcidFileMetadataAfterMajorCompaction() throws Exception {
    String partition = "partition (part=1)";
    executeHiveSql("create table " + getTestFullAcidTblName() +
        " (c1 int) partitioned by (part int) stored as orc" +
        " tblproperties ('transactional' = 'true')");
    executeHiveSql(
        "insert into " + getTestFullAcidTblName() + " " + partition + " values (1)");
    executeHiveSql("delete from " + getTestFullAcidTblName() + " where c1 = 1");
    // add incompleteTable so that the table will be loaded
    catalog_.addIncompleteTable(testDbName, testAcidTblName, TImpalaTableType.TABLE,
        /*tblComment*/null);
    // After major compaction, the partition should return only base file
    testFileMetadataAfterCompaction(testAcidTblName, partition, true, 1);
  }

  @Test
  public void testFullAcidFileMetadataAfterMinorCompaction() throws Exception {
    String partition = "partition (part=1)";
    executeHiveSql("create table " + getTestFullAcidTblName() +
        " (c1 int) partitioned by (part int) stored as orc" +
        " tblproperties ('transactional' = 'true')");
    executeHiveSql(
        "insert into " + getTestFullAcidTblName() + " " + partition + " values (1)");
    executeHiveSql("delete from " + getTestFullAcidTblName() + " where c1 = 1");
    // add incompleteTable so that the table will be loaded
    catalog_.addIncompleteTable(testDbName, testAcidTblName, TImpalaTableType.TABLE,
        /*tblComment*/null);
    // After minor compaction, the partition should return one delta file and one delete
    // delta file
    testFileMetadataAfterCompaction(testAcidTblName, partition, false, 2);
  }

  private void testFileMetadataAfterCompaction(String tableName, String partition,
      boolean isMajorCompaction, int expectedFileCount) throws Exception {
    Assume.assumeTrue(MetastoreShim.getMajorVersion() >= 3);
    String tableOrPartition = testDbName + "." + tableName + " " + partition;
    executeHiveSql("insert into " + tableOrPartition + " values (2)");
    executeHiveSql("insert into " + tableOrPartition + " values (3)");
    // load data after insertion so that writeIdList will not change afterward
    ValidWriteIdList currentWriteIdList = getValidWriteIdList(testDbName, tableName);
    TGetPartialCatalogObjectRequest request = new RequestBuilder()
                                                  .db(testDbName)
                                                  .tbl(tableName)
                                                  .writeId(currentWriteIdList)
                                                  .wantFiles()
                                                  .build();
    TGetPartialCatalogObjectResponse response = sendRequest(request);
    TPartialPartitionInfo prePartitionInfo =
        Iterables.getOnlyElement(response.getTable_info().getPartitions());
    int preFileCount = prePartitionInfo.getFile_descriptorsSize()
        + prePartitionInfo.getInsert_file_descriptorsSize()
        + prePartitionInfo.getDelete_file_descriptorsSize();
    Assert.assertTrue(preFileCount > 1);

    String compactionType = isMajorCompaction ? "'major'" : "'minor'";
    executeHiveSql(
        "alter table " + tableOrPartition + " compact " + compactionType + " and wait");
    long numMisses =
        getMetricCount(testDbName, tableName, HdfsTable.FILEMETADATA_CACHE_MISS_METRIC);
    long numHits =
        getMetricCount(testDbName, tableName, HdfsTable.FILEMETADATA_CACHE_HIT_METRIC);
    // issue a get request at current writeIdList
    // it shouldn't trigger a table reload but file metadata refreshing
    request = new RequestBuilder()
                  .db(testDbName)
                  .tbl(tableName)
                  .writeId(currentWriteIdList)
                  .wantFiles()
                  .build();
    response = sendRequest(request);
    TPartialPartitionInfo afterPartitionInfo =
        Iterables.getOnlyElement(response.getTable_info().getPartitions());
    int afterFileCount = afterPartitionInfo.getFile_descriptorsSize()
        + afterPartitionInfo.getInsert_file_descriptorsSize()
        + afterPartitionInfo.getDelete_file_descriptorsSize();
    String message = "Actual file_descriptors:\n" +
        getPathsFromFileDescriptors(afterPartitionInfo.getFile_descriptors()) +
        "\nActual insert_file_descriptors:\n" +
        getPathsFromFileDescriptors(afterPartitionInfo.getInsert_file_descriptors()) +
        "\nActual delete_file_descriptors:\n" +
        getPathsFromFileDescriptors(afterPartitionInfo.getDelete_file_descriptors());
    Assert.assertEquals(message, expectedFileCount, afterFileCount);
    long numMissesAfterMinor =
        getMetricCount(testDbName, tableName, HdfsTable.FILEMETADATA_CACHE_MISS_METRIC);
    long numHitsAfterMinor =
        getMetricCount(testDbName, tableName, HdfsTable.FILEMETADATA_CACHE_HIT_METRIC);
    Assert.assertEquals(numHits + 1, numHitsAfterMinor);
    Assert.assertEquals(numMisses, numMissesAfterMinor);
  }

  private List<String> getPathsFromFileDescriptors(List<THdfsFileDesc> fileDescriptors) {
    return fileDescriptors.stream()
        .map(HdfsPartition.FileDescriptor::fromThrift)
        .map(HdfsPartition.FileDescriptor::getPath)
        .collect(Collectors.toList());
  }

  /**
   * This test make sure that the table returned is consistent with given writeId list
   * even if the table was dropped and recreated from outside.
   * @throws Exception
   */
  @Test
  public void testFetchAfterDropAndRecreate() throws Exception {
    Assume.assumeTrue(MetastoreShim.getMajorVersion() >= 3);
    // row 2, first row is in the setup method
    executeImpalaSql("insert into " + getTestTblName() + " values (2)");
    Table tbl = catalog_.getOrLoadTable(testDbName, testTblName, "test", null);
    Assert.assertFalse("Table must be loaded",
        tbl instanceof IncompleteTable);
    ValidWriteIdList olderWriteIdList = getValidWriteIdList(testDbName,
        testTblName);
    Assert.assertEquals(olderWriteIdList.toString(), tbl.getValidWriteIds().toString());
    TGetPartialCatalogObjectRequest request = new RequestBuilder()
        .db(testDbName)
        .tbl(testTblName)
        .writeId(olderWriteIdList)
        .wantFiles()
        .build();
    TGetPartialCatalogObjectResponse response = sendRequest(request);
    Assert.assertEquals(1, response.getTable_info().getPartitionsSize());
    List<THdfsFileDesc> oldFds = response.getTable_info().getPartitions()
        .get(0).file_descriptors;
    Assert.assertEquals(2, oldFds.size());
    // now recreate the table from hive so that Impala is not aware of it
    executeHiveSql("drop table " + getTestTblName());
    executeHiveSql("create table " + getTestTblName() + " like "
        + "functional.insert_only_transactional_table stored as parquet");
    // we do 2 more inserts into the table so that the high-watermark is same
    // as olderWriteIdList.
    executeHiveSql("insert into " + getTestTblName() + " values (1)");
    executeHiveSql("insert into " + getTestTblName() + " values (2)");
    ValidWriteIdList newerWriteIdList = getValidWriteIdList(testDbName, testTblName);
    // the validWriteIdList itself is compatible
    Assert.assertTrue(AcidUtils.compare(newerWriteIdList, olderWriteIdList) == 0);
    // now a client with the newerValidWriteIdList must re-trigger a load
    request = new RequestBuilder()
        .db(testDbName)
        .tbl(testTblName)
        .writeId(newerWriteIdList)
        .tableId(getTableId(testDbName, testTblName))
        .wantFiles()
        .build();
    response = sendRequest(request);
    Assert.assertEquals(1, response.getTable_info().getPartitionsSize());
    List<THdfsFileDesc> newFds = response.getTable_info().getPartitions()
        .get(0).file_descriptors;
    Assert.assertEquals(2, newFds.size());
    for (int i=0; i<newFds.size(); i++) {
      // we expect that table was reloaded and hence the file descriptors should be
      // different
      Assert.assertNotEquals("Found the new file descriptor same as old one",
          newFds.get(i), oldFds.get(i));
    }
  }

  @Test
  public void testFullAcidCompaction() throws Exception {
    Assume.assumeTrue(MetastoreShim.getMajorVersion() >= 3);
    // Create Full ACID table.
    executeImpalaSql("create table " + getTestFullAcidTblName() + " like "
        + "functional_orc_def.alltypes");
    executeHiveSql("insert into " + getTestFullAcidTblName() + " select * from "
        + "functional_orc_def.alltypes");
    executeHiveSql("delete from " + getTestFullAcidTblName()
        + " where id % 2 = 0");
    catalog_.reset(NoOpEventSequence.INSTANCE);
    Table tbl = catalog_.getOrLoadTable(testDbName, testAcidTblName, "test", null);
    Assert.assertFalse("Table must be loaded", tbl instanceof IncompleteTable);
    ValidWriteIdList olderWriteIdList = getValidWriteIdList(testDbName, testAcidTblName);
    // Let's delete again to generate a new write id for the table.
    executeHiveSql("delete from " + getTestFullAcidTblName()
        + " where id % 3 = 0");
    executeHiveSql("alter table " + getTestFullAcidTblName()
        + " partition(year=2010,month=10) compact 'major' and wait");
    ValidWriteIdList currWriteIdList = getValidWriteIdList(testDbName, testAcidTblName);
    // Issue a request with currWriteIdList to refresh Catalog.
    TGetPartialCatalogObjectRequest request = new RequestBuilder()
        .db(testDbName)
        .tbl(testAcidTblName)
        .writeId(currWriteIdList)
        .wantFiles()
        .build();
    TGetPartialCatalogObjectResponse response = sendRequest(request);
    // Check that we see the current state of the table.
    Assert.assertEquals(24, response.getTable_info().getPartitionsSize());
    for (TPartialPartitionInfo part : response.getTable_info().getPartitions()) {
      if (part.getName().equalsIgnoreCase("year=2010/month=10")) {
        // The compacted directory only contains a single file. And since there's no
        // delete file, it is put into 'file_descriptors'.
        Assert.assertEquals(1, part.file_descriptors.size());
        Assert.assertEquals(0, part.insert_file_descriptors.size());
        Assert.assertEquals(0, part.delete_file_descriptors.size());
      } else {
        Assert.assertEquals(0, part.file_descriptors.size());
        Assert.assertEquals(1, part.insert_file_descriptors.size());
        Assert.assertEquals(2, part.delete_file_descriptors.size());
      }
    }
    // Now let's retrieve table metadata with the older write id list.
    request = new RequestBuilder()
        .db(testDbName)
        .tbl(testAcidTblName)
        .writeId(olderWriteIdList)
        .wantFiles()
        .build();
    response = sendRequest(request);
    // Let's check that we don't see the second delete, nor the compacted directory.
    Assert.assertEquals(24, response.getTable_info().getPartitionsSize());
    for (TPartialPartitionInfo part : response.getTable_info().getPartitions()) {
      Assert.assertEquals(0, part.file_descriptors.size());
      Assert.assertEquals(1, part.insert_file_descriptors.size());
      Assert.assertEquals(1, part.delete_file_descriptors.size());
    }
  }

  private void executeHiveSql(String query) throws Exception {
    try (HiveJdbcClient hiveClient = hiveClientPool_.getClient()) {
      hiveClient.executeSql(query);
    }
  }

  private void executeImpalaSql(String query) throws Exception {
    ImpalaJdbcClient client = ImpalaJdbcClient
        .createClientUsingHiveJdbcDriver();
    client.connect();
    try {
      client.execStatement(query);
    } finally {
      client.close();
    }
  }

  /**
   * Simple Request builder class. Assumes all the metadata at higher granularity is
   * required if a specific level is requested. For examples, if files are requested,
   * assumes that partitions names and partitions are also requested.
   */
  private static class RequestBuilder {

    boolean wantFileMetadata;
    boolean wantPartitionMeta;
    boolean wantPartitionNames;
    String tblName, dbName;
    ValidWriteIdList writeIdList;
    long tableId = -1;

    RequestBuilder db(String db) {
      this.dbName = db;
      return this;
    }

    RequestBuilder tbl(String tbl) {
      this.tblName = tbl;
      return this;
    }

    RequestBuilder writeId(ValidWriteIdList validWriteIdList) {
      this.writeIdList = validWriteIdList;
      return this;
    }

    RequestBuilder tableId(long id) {
      this.tableId = id;
      return this;
    }

    RequestBuilder wantFiles() {
      wantFileMetadata = true;
      wantPartitionMeta = true;
      wantPartitionNames = true;
      return this;
    }

    RequestBuilder wantPartitions() {
      wantPartitionMeta = true;
      wantFileMetadata = true;
      return this;
    }

    RequestBuilder wantPartitionNames() {
      wantPartitionNames = true;
      return this;
    }

    TGetPartialCatalogObjectRequest build() {
      TGetPartialCatalogObjectRequest req = new TGetPartialCatalogObjectRequest();
      req.object_desc = new TCatalogObject();
      req.object_desc.setType(TCatalogObjectType.TABLE);
      req.object_desc.table = new TTable(dbName, tblName);
      req.object_desc.table.hdfs_table = new THdfsTable();
      req.table_info_selector = new TTableInfoSelector();
      req.table_info_selector.valid_write_ids =
        MetastoreShim.convertToTValidWriteIdList(writeIdList);
      req.table_info_selector.table_id = tableId;
      req.table_info_selector.want_hms_table = true;
      if (wantPartitionNames) {
        req.table_info_selector.want_partition_names = true;
      }
      if (wantPartitionMeta) {
        req.table_info_selector.want_partition_metadata = true;
      }
      if (wantFileMetadata) {
        req.table_info_selector.want_partition_files = true;
      }
      return req;
    }
  }

  /**
   * Gets the table id from the HMS.
   */
  private long getTableId(String db, String tbl) throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return MetastoreShim.getTableId(client.getHiveClient().getTable(db, tbl));
    }
  }

  private ValidWriteIdList getValidWriteIdList(String db, String tbl) throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getValidWriteIds(db + "." + tbl);
    }
  }

  private TGetPartialCatalogObjectResponse sendRequest(
    TGetPartialCatalogObjectRequest req)
    throws CatalogException, InternalException, TException {
    TGetPartialCatalogObjectResponse resp;
    resp = catalog_.getPartialCatalogObject(req);
    // Round-trip the response through serialization, so if we accidentally forgot to
    // set the "isset" flag for any fields, we'll catch that bug.
    byte[] respBytes = new TSerializer().serialize(resp);
    resp.clear();
    new TDeserializer().deserialize(resp, respBytes);
    return resp;
  }

  private static String getTestTblName() {
    return testDbName + "." + testTblName;
  }

  private static String getTestFullAcidTblName() {
    return testDbName + "." + testAcidTblName;
  }

  private static String getPartitionedTblName() {
    return testDbName + "." + testPartitionedTbl;
  }

  private void invalidateTbl(String db, String tbl) throws CatalogException {
    catalog_.invalidateTable(new TTableName(db, tbl), new Reference<>(),
      new Reference<>(), NoOpEventSequence.INSTANCE);
    Assert.assertTrue("Table must not be loaded",
      catalog_.getTable(db, tbl) instanceof IncompleteTable);
  }
}
