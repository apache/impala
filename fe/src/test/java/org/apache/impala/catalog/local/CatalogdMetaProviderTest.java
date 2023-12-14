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

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.impala.catalog.HdfsPartition;
import org.apache.impala.catalog.local.CatalogdMetaProvider.SizeOfWeigher;
import org.apache.impala.catalog.local.MetaProvider.PartitionMetadata;
import org.apache.impala.catalog.local.MetaProvider.PartitionRef;
import org.apache.impala.catalog.local.MetaProvider.TableMetaRef;
import org.apache.impala.common.Pair;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.service.FeSupport;
import org.apache.impala.service.Frontend;
import org.apache.impala.service.FrontendProfile;
import org.apache.impala.testutil.HiveJdbcClientPool;
import org.apache.impala.testutil.HiveJdbcClientPool.HiveJdbcClient;
import org.apache.impala.testutil.ImpalaJdbcClient;
import org.apache.impala.testutil.TestUtils;
import org.apache.impala.thrift.TBackendGflags;
import org.apache.impala.thrift.TBriefTableMeta;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TCounter;
import org.apache.impala.thrift.TDatabase;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TRuntimeProfileNode;
import org.apache.impala.thrift.TTable;
import org.apache.impala.util.ListMap;
import org.apache.impala.util.TByteBuffer;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.google.common.cache.CacheStats;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

public class CatalogdMetaProviderTest {

  private final static Logger LOG = LoggerFactory.getLogger(
      CatalogdMetaProviderTest.class);
  private final static ListMap<TNetworkAddress> HOST_INDEX = new ListMap<>();

  private final CatalogdMetaProvider provider_;
  private final TableMetaRef tableRef_;

  private CacheStats prevStats_;
  @Rule
  public TestName name = new TestName();

  private static HiveJdbcClientPool hiveJdbcClientPool_;
  private static final String testDbName_ = "catalogd_meta_provider_test";
  private static final String testTblName_ = "insert_only";
  private static final String testPartitionedTblName_ = "insert_only_partitioned";
  private static final String testFullAcidTblName_ = "full_acid";

  static {
    FeSupport.loadLibrary();
  }

  public CatalogdMetaProviderTest() throws Exception {
    // Set sufficient expiration/capacity for the test to not evict.
    TBackendGflags flags = new TBackendGflags();
    flags.setLocal_catalog_cache_expiration_s(3600);
    flags.setLocal_catalog_cache_mb(100);
    provider_ = new CatalogdMetaProvider(flags);
    Pair<Table, TableMetaRef> tablePair = provider_.loadTable("functional", "alltypes");
    tableRef_ = tablePair.second;
    prevStats_ = provider_.getCacheStats();
    hiveJdbcClientPool_ = HiveJdbcClientPool.create(1);
  }

  private CacheStats diffStats() {
    CacheStats s = provider_.getCacheStats();
    CacheStats diff = s.minus(prevStats_);
    prevStats_ = s;
    LOG.info("Stats: {}", diff);
    return diff;
  }

  private void createTestTbls() throws Exception {
    LOG.info("Creating test tables for {}", name.getMethodName());
    Stopwatch st = Stopwatch.createStarted();
    ImpalaJdbcClient client = ImpalaJdbcClient.createClientUsingHiveJdbcDriver();
    client.connect();
    try {
      client.execStatement("drop database if exists " + testDbName_ + " cascade");
      client.execStatement("create database " + testDbName_);
      client.execStatement("create table " + getTestTblName() + " like "
          + "functional.insert_only_transactional_table stored as parquet");
      client.execStatement("create table " + getTestPartitionedTblName()
          + " (c1 int) partitioned by (part int) stored as parquet "
          + "tblproperties ('transactional'='true', 'transactional_properties'="
          + "'insert_only')");
      client.execStatement("create table " + getTestFullAcidTblName()
          + " (c1 int) partitioned by (part int) stored as orc "
          + "tblproperties ('transactional'='true')");
    } finally {
      LOG.info("Time taken for createTestTbls {} msec",
          st.stop().elapsed(TimeUnit.MILLISECONDS));
      client.close();
    }
  }

  private void dropTestTbls() throws Exception {
    try (HiveJdbcClient hiveClient = hiveJdbcClientPool_.getClient()) {
      hiveClient.executeSql("drop database if exists " + testDbName_ + " cascade");
    }
  }

  private static String getTestTblName() {
    return testDbName_ + "." + testTblName_;
  }

  private static String getTestPartitionedTblName() {
    return testDbName_ + "." + testPartitionedTblName_;
  }

  private static String getTestFullAcidTblName() {
    return testDbName_ + "." + testFullAcidTblName_;
  }

  private void executeHiveSql(String query) throws Exception {
    try (HiveJdbcClient hiveClient = hiveJdbcClientPool_.getClient()) {
      hiveClient.executeSql(query);
    }
  }

  @Test
  public void testCachePartitionList() throws Exception {
    List<PartitionRef> partList = provider_.loadPartitionList(tableRef_);
    CacheStats stats = diffStats();
    assertEquals(1, stats.requestCount());
    assertEquals(1, stats.loadCount());
    assertEquals(0, stats.hitCount());

    List<PartitionRef> partListHit = provider_.loadPartitionList(tableRef_);
    stats = diffStats();
    assertEquals(1, stats.requestCount());
    assertEquals(1, stats.hitCount());

    // Results should be the same.
    assertEquals(partList, partListHit);
  }

  @Test
  public void testCachePartitionsByRef() throws Exception {
    List<PartitionRef> allRefs = provider_.loadPartitionList(tableRef_);
    List<PartitionRef> partialRefs = allRefs.subList(3, 8);
    CacheStats stats = diffStats();

    // Should get no hits on the initial load of partitions.
    Map<String, PartitionMetadata> partMap = loadPartitions(tableRef_, partialRefs);
    assertEquals(partialRefs.size(), partMap.size());
    stats = diffStats();
    assertEquals(0, stats.hitCount());

    // Load the same partitions again and we should get a hit for each partition.
    Map<String, PartitionMetadata> partMapHit = loadPartitions(tableRef_, partialRefs);
    stats = diffStats();
    assertEquals(stats.hitCount(), partMapHit.size());

    // Load all of the partitions: we should get some hits and some misses.
    Map<String, PartitionMetadata> allParts = loadPartitions(tableRef_, allRefs);
    assertEquals(allRefs.size(), allParts.size());
    stats = diffStats();
    assertEquals(stats.hitCount(), partMapHit.size());
  }

  /**
   * Helper method for loading partitions by refs.
   */
  private Map<String, PartitionMetadata> loadPartitions(TableMetaRef tableMetaRef,
     List<PartitionRef> partRefs) throws Exception {
    return provider_.loadPartitionsByRefs(
        tableMetaRef, /* partitionColumnNames unused by this impl */null, HOST_INDEX,
        partRefs);
  }

  /**
   * Helper method for loading partitions by dbName and tableName. Retries when there is
   * inconsistent metadata.
   */
  private Map<String, PartitionMetadata> loadPartitions(String dbName, String tableName)
      throws Exception {
    Frontend.RetryTracker retryTracker = new Frontend.RetryTracker(
        String.format("load partitions for table %s.%s", dbName, tableName));
    while (true) {
      try {
        Pair<Table, TableMetaRef> tablePair = provider_.loadTable(dbName, tableName);
        List<PartitionRef> allRefs = provider_.loadPartitionList(tablePair.second);
        return loadPartitions(tablePair.second, allRefs);
      } catch (InconsistentMetadataFetchException e) {
        diffStats();
        retryTracker.handleRetryOrThrow(e);
      }
    }
  }

  @Test
  public void testCacheColumnStats() throws Exception {
    ImmutableList<String> colNames = ImmutableList.of("month", "id");
    List<ColumnStatisticsObj> colStats = provider_.loadTableColumnStatistics(tableRef_,
        colNames);
    // Only 'id' has stats -- 'month' is a partition column and therefore has none.
    assertEquals(1, colStats.size());
    CacheStats stats = diffStats();
    // We should have missed on both columns.
    assertEquals(2, stats.requestCount());
    assertEquals(2, stats.missCount());

    // Look up again, and we should get the same results.
    List<ColumnStatisticsObj> colStats2 = provider_.loadTableColumnStatistics(tableRef_,
        colNames);
    assertEquals(colStats, colStats2);

    // Should have gotten hits on both columns (one positive, one negative).
    stats = diffStats();
    assertEquals(2, stats.requestCount());
    assertEquals(2, stats.hitCount());
    assertEquals(0, stats.missCount());
  }

  @Test
  public void testWeights() throws Exception {
    List<PartitionRef> refs = provider_.loadPartitionList(tableRef_);
    ListMap<TNetworkAddress> hostIndex = new ListMap<>();
    provider_.loadPartitionsByRefs(tableRef_, /* ignored */null, hostIndex , refs);

    // Unfortunately Guava doesn't provide a statistic on the total weight of cached
    // elements. So, we'll just instantiate the weigher directly and sanity check
    // the size loosely. The size will grow if we add more fields into
    // TPartialPartitionInfo in future.
    SizeOfWeigher weigher = new SizeOfWeigher();
    int weigh = weigher.weigh(refs, null);
    assertTrue("Actual weigh: " + weigh, weigh > 4000);
    assertTrue("Actual weigh: " + weigh, weigh < 5000);

    // Also continue to test ehcache.
    weigher = new SizeOfWeigher(false, null);
    weigh = weigher.weigh(refs, null);
    assertTrue("Actual weigh: " + weigh, weigh > 4000);
    assertTrue("Actual weigh: " + weigh, weigh < 5000);
  }

  @Test
  public void testCacheAndEvictDatabase() throws Exception {
    // Load a database.
    Database db = provider_.loadDb("functional");
    CacheStats stats = diffStats();
    assertEquals(1, stats.missCount());

    // ... and the table list for it.
    ImmutableCollection<TBriefTableMeta> tableList = provider_.loadTableList(
        "functional");
    stats = diffStats();
    assertEquals(1, stats.missCount());

    // Load them again, should hit cache.
    Database dbHit = provider_.loadDb("functional");
    assertEquals(db, dbHit);
    ImmutableCollection<TBriefTableMeta> tableListHit = provider_.loadTableList(
        "functional");
    assertEquals(tableList, tableListHit);

    stats = diffStats();
    assertEquals(2, stats.hitCount());
    assertEquals(0, stats.missCount());

    // Invalidate the DB.
    TCatalogObject obj = new TCatalogObject(TCatalogObjectType.DATABASE, 0);
    obj.setDb(new TDatabase("functional"));
    provider_.invalidateCacheForObject(obj);

    // Load another time, should miss cache.
    Database dbMiss = provider_.loadDb("functional");
    assertEquals(db, dbMiss);
    ImmutableCollection<TBriefTableMeta> tableListMiss = provider_.loadTableList(
        "functional");
    assertEquals(tableList, tableListMiss);
    stats = diffStats();
    assertEquals(0, stats.hitCount());
    assertEquals(2, stats.missCount());
  }

  @Test
  public void testCacheAndEvictTable() throws Exception {
    // 'alltypes' was already loaded in the setup function, so we should get a hit
    // if we load it again.
    provider_.loadTable("functional", "alltypes");
    CacheStats stats = diffStats();
    assertEquals(1, stats.hitCount());
    assertEquals(1, stats.missCount());   // missing the table list

    // Load the table list then load the table again.
    provider_.loadTableList("functional");
    stats = diffStats();
    assertEquals(0, stats.hitCount());
    assertEquals(1, stats.missCount());
    provider_.loadTable("functional", "alltypes");
    stats = diffStats();
    assertEquals(2, stats.hitCount());    // hit the table and the table list
    assertEquals(0, stats.missCount());

    // Invalidate it.
    TCatalogObject obj = new TCatalogObject(TCatalogObjectType.TABLE, 0);
    obj.setTable(new TTable("functional", "alltypes"));
    provider_.invalidateCacheForObject(obj);

    // Should get a miss if we re-load it.
    provider_.loadTable("functional", "alltypes");
    stats = diffStats();
    assertEquals(0, stats.hitCount());
    assertEquals(2, stats.missCount());   // miss the table and the table list
  }

  @Test
  public void testProfile() throws Exception {
    FrontendProfile profile;
    try (FrontendProfile.Scope scope = FrontendProfile.createNewWithScope()) {
      // This table has been loaded in the constructor. Hit cache.
      provider_.loadTable("functional", "alltypes");
      // Load all partition ids. This will create a PartitionLists miss.
      List<PartitionRef> allRefs = provider_.loadPartitionList(tableRef_);
      // Load all partitions. This will create one partition miss per partition.
      loadPartitions(tableRef_, allRefs);
      profile = FrontendProfile.getCurrent();
    }
    TRuntimeProfileNode prof = profile.emitAsThrift();
    Map<String, TCounter> counters = Maps.uniqueIndex(prof.counters, TCounter::getName);
    assertEquals(prof.counters.toString(), 16, counters.size());
    assertEquals(1, counters.get("CatalogFetch.Tables.Hits").getValue());
    assertEquals(0, counters.get("CatalogFetch.Tables.Misses").getValue());
    assertEquals(1, counters.get("CatalogFetch.Tables.Requests").getValue());
    assertTrue(counters.containsKey("CatalogFetch.Tables.Time"));
    assertEquals(0, counters.get("CatalogFetch.PartitionLists.Hits").getValue());
    assertEquals(1, counters.get("CatalogFetch.PartitionLists.Misses").getValue());
    assertEquals(1, counters.get("CatalogFetch.PartitionLists.Requests").getValue());
    assertTrue(counters.containsKey("CatalogFetch.PartitionLists.Time"));
    assertEquals(0, counters.get("CatalogFetch.Partitions.Hits").getValue());
    assertEquals(24, counters.get("CatalogFetch.Partitions.Misses").getValue());
    assertEquals(24, counters.get("CatalogFetch.Partitions.Requests").getValue());
    assertTrue(counters.containsKey("CatalogFetch.Partitions.Time"));
    assertTrue(counters.containsKey("CatalogFetch.RPCs.Bytes"));
    assertTrue(counters.containsKey("CatalogFetch.RPCs.Time"));
    // 2 RPCs: one for fetching partition list, the other one for fetching partitions.
    assertEquals(2, counters.get("CatalogFetch.RPCs.Requests").getValue());
    // Should contains StorageLoad.Time since we have loaded partitions from catalogd.
    assertTrue(counters.containsKey("CatalogFetch.StorageLoad.Time"));
  }

  @Test
  public void testPiggybackSuccess() throws Exception {
    // TODO: investigate the cause of flakiness (IMPALA-8794)
    Assume.assumeTrue(
        "Skipping this test because it is flaky with Hive3",
        TestUtils.getHiveMajorVersion() == 2);

    doTestPiggyback(/*success=*/true);
  }

  @Test
  public void testPiggybackFailure() throws Exception {
    // TODO: investigate the cause of flakiness (IMPALA-8794)
    Assume.assumeTrue(
        "Skipping this test because it is flaky with Hive3",
        TestUtils.getHiveMajorVersion() == 2);

    doTestPiggyback(/*success=*/false);
  }

  private void doTestPiggyback(boolean testSuccessCase) throws Exception {
    // To test success, we load an existing table. Otherwise, load one that doesn't
    // exist, which will throw an exception.
    final String tableName = testSuccessCase ? "alltypes" : "table-does-not-exist";
    final AtomicInteger counterToWatch = testSuccessCase ?
        provider_.piggybackSuccessCountForTests :
        provider_.piggybackExceptionCountForTests;

    final int kNumThreads = 8;
    ExecutorService exec = Executors.newFixedThreadPool(kNumThreads);
    try {
      // Run for at least 60 seconds to try to provoke the desired behavior.
      Stopwatch sw = Stopwatch.createStarted();
      while (sw.elapsed(TimeUnit.SECONDS) < 60) {
        // Submit a wave of parallel tasks which all fetch the same table, concurently.
        // One of these should win whereas the others are likely to piggy-back on the
        // same request.
        List<Future<Object>> futures = new ArrayList<>();
        for (int i = 0; i < kNumThreads; i++) {
          futures.add(exec.submit(() -> provider_.loadTable("functional", tableName)));
        }
        for (Future<Object> f : futures) {
          try {
            assertNotNull(f.get());
            if (!testSuccessCase) fail("Did not get expected exception");
          } catch (Exception e) {
            // If we expected success, but got an exception, we should rethrow it.
            if (testSuccessCase) throw e;
          }
        }
        if (counterToWatch.get() > 20) {
          return;
        }

        TCatalogObject obj = new TCatalogObject(TCatalogObjectType.TABLE, 0);
        obj.setTable(new TTable("functional", tableName));
        provider_.invalidateCacheForObject(obj);
      }
      fail("Did not see enough piggybacked loads!");
    } finally {
      exec.shutdown();
      assertTrue(exec.awaitTermination(60, TimeUnit.SECONDS));
    }

    // Check that, in the success case, the table was left in the cache.
    // In the failure case, we should not have any "failed" entry persisting.
    diffStats();
    try {
      provider_.loadTable("functonal", tableName);
    } catch (Exception e) {}
    CacheStats stats = diffStats();
    if (testSuccessCase) {
      assertEquals(1, stats.hitCount());
      assertEquals(0, stats.missCount());
    } else {
      assertEquals(0, stats.hitCount());
      assertEquals(1, stats.missCount());
    }
  }

  // Test loading and invalidation of databases, tables with upper case
  // names. Expected behavior is the local catalog should treat these
  // names as case-insensitive.
  @Test
  public void testInvalidateObjectsCaseInsensitive() throws Exception {
    provider_.loadDb("tpch");
    provider_.loadTable("tpch", "nation");

    testInvalidateDb("TPCH");
    testInvalidateTable("TPCH", "nation");
    testInvalidateTable("tpch", "NATION");
  }

  private void testInvalidateTable(String dbName, String tblName) throws Exception {
    CacheStats stats = diffStats();

    provider_.loadTable(dbName, tblName);

    // should get a cache hit since dbName,tblName should be treated as case-insensitive
    stats = diffStats();
    assertEquals(1, stats.hitCount());
    assertEquals(1, stats.missCount());   // missing the table list

    // Invalidate it.
    TCatalogObject obj = new TCatalogObject(TCatalogObjectType.TABLE, 0);
    obj.setTable(new TTable(dbName, tblName));
    provider_.invalidateCacheForObject(obj);

    // should get a cache miss if we reload it
    provider_.loadTable(dbName, tblName);
    stats = diffStats();
    assertEquals(0, stats.hitCount());
    assertEquals(2, stats.missCount());   // missing the table and the table list
  }

  private void testInvalidateDb(String dbName) throws Exception {
    CacheStats stats = diffStats();

    provider_.loadDb(dbName);

    // should get a cache hit since dbName should be treated as case-insensitive
    stats = diffStats();
    assertEquals(1, stats.hitCount());
    assertEquals(0, stats.missCount());

    // Invalidate it.
    TCatalogObject obj = new TCatalogObject(TCatalogObjectType.DATABASE, 0);
    obj.setDb(new TDatabase(dbName));
    provider_.invalidateCacheForObject(obj);

    // should get a cache miss if we reload it
    provider_.loadDb(dbName);
    stats = diffStats();
    assertEquals(0, stats.hitCount());
    assertEquals(1, stats.missCount());
  }

  @Test
  public void testFullAcidFileMetadataAfterMajorCompaction() throws Exception {
    boolean origFlag = BackendConfig.INSTANCE.isAutoCheckCompaction();
    try {
      BackendConfig.INSTANCE.getBackendCfg().setAuto_check_compaction(true);
      createTestTbls();
      String partition = "partition (part=1)";
      testFileMetadataAfterCompaction(testDbName_, testFullAcidTblName_, partition,
          true);
    } finally {
      BackendConfig.INSTANCE.getBackendCfg().setAuto_check_compaction(origFlag);
      dropTestTbls();
    }
  }

  @Test
  public void testFullAcidFileMetadataAfterMinorCompaction() throws Exception {
    boolean origFlag = BackendConfig.INSTANCE.isAutoCheckCompaction();
    try {
      BackendConfig.INSTANCE.getBackendCfg().setAuto_check_compaction(true);
      createTestTbls();
      String partition = "partition (part=1)";
      testFileMetadataAfterCompaction(testDbName_, testFullAcidTblName_, partition,
          false);
    } finally {
      BackendConfig.INSTANCE.getBackendCfg().setAuto_check_compaction(origFlag);
      dropTestTbls();
    }
  }

  @Test
  public void testTableFileMetadataAfterMajorCompaction() throws Exception {
    boolean origFlag = BackendConfig.INSTANCE.isAutoCheckCompaction();
    try {
      BackendConfig.INSTANCE.getBackendCfg().setAuto_check_compaction(true);
      createTestTbls();
      testFileMetadataAfterCompaction(testDbName_, testTblName_, "", true);
    } finally {
      BackendConfig.INSTANCE.getBackendCfg().setAuto_check_compaction(origFlag);
      dropTestTbls();
    }
  }

  @Test
  public void testTableFileMetadataAfterMinorCompaction() throws Exception {
    boolean origFlag = BackendConfig.INSTANCE.isAutoCheckCompaction();
    try {
      BackendConfig.INSTANCE.getBackendCfg().setAuto_check_compaction(true);
      createTestTbls();
      testFileMetadataAfterCompaction(testDbName_, testTblName_, "", false);
    } finally {
      BackendConfig.INSTANCE.getBackendCfg().setAuto_check_compaction(origFlag);
      dropTestTbls();
    }
  }

  @Test
  public void testPartitionFileMetadataAfterMajorCompaction() throws Exception {
    boolean origFlag = BackendConfig.INSTANCE.isAutoCheckCompaction();
    try {
      BackendConfig.INSTANCE.getBackendCfg().setAuto_check_compaction(true);
      createTestTbls();
      String partition = "partition (part=1)";
      testFileMetadataAfterCompaction(testDbName_, testPartitionedTblName_, partition,
          true);
    } finally {
      BackendConfig.INSTANCE.getBackendCfg().setAuto_check_compaction(origFlag);
      dropTestTbls();
    }
  }

  @Test
  public void testPartitionFileMetadataAfterMinorCompaction() throws Exception {
    boolean origFlag = BackendConfig.INSTANCE.isAutoCheckCompaction();
    try {
      BackendConfig.INSTANCE.getBackendCfg().setAuto_check_compaction(true);
      createTestTbls();
      String partition = "partition (part=1)";
      testFileMetadataAfterCompaction(testDbName_, testPartitionedTblName_, partition,
          false);
    } finally {
      BackendConfig.INSTANCE.getBackendCfg().setAuto_check_compaction(origFlag);
      dropTestTbls();
    }
  }

  @Test
  public void testLoadNullPartitionKeyValue() throws Exception {
    provider_.cache_.invalidateAll();
    CacheStats stats = diffStats();
    String nullPartitionName = provider_.loadNullPartitionKeyValue();
    assertNotNull(nullPartitionName);
    stats = diffStats();
    assertEquals(1, stats.missCount());
    assertEquals(0, stats.hitCount());
    assertEquals(nullPartitionName, provider_.loadNullPartitionKeyValue());
    stats = diffStats();
    assertEquals(0, stats.missCount());
    assertEquals(1, stats.hitCount());
  }

  private void testFileMetadataAfterCompaction(String dbName, String tableName,
      String partition, boolean isMajorCompaction) throws Exception {
    String tableOrPartition = dbName + "." + tableName + " " + partition;
    executeHiveSql("insert into " + tableOrPartition + " values (1)");
    executeHiveSql("insert into " + tableOrPartition + " values (2)");
    loadPartitions(dbName, tableName);
    // load again to make sure the partition is in cache.
    Map<String, PartitionMetadata> partMap;
    try (FrontendProfile.Scope scope = FrontendProfile.createNewWithScope()) {
      partMap = loadPartitions(dbName, tableName);
      FrontendProfile profile = FrontendProfile.getCurrent();
      TRuntimeProfileNode prof = profile.emitAsThrift();
      Map<String, TCounter> counters = Maps.uniqueIndex(prof.counters, TCounter::getName);
      assertEquals(1, counters.get("CatalogFetch.Partitions.Requests").getValue());
      assertEquals(1, counters.get("CatalogFetch.Partitions.Hits").getValue());
      int preFileCount = partMap.values().stream()
          .map(PartitionMetadata::getFileDescriptors).mapToInt(List::size).sum();
      assertEquals(2, preFileCount);
    }

    String compactionType = isMajorCompaction ? "'major'" : "'minor'";
    executeHiveSql(
        "alter table " + tableOrPartition + " compact " + compactionType + " and wait");
    // After Compaction, it should fetch the partition from catalogd again, and
    // file metadata should be updated.
    try (FrontendProfile.Scope scope = FrontendProfile.createNewWithScope()) {
      partMap = loadPartitions(dbName, tableName);
      FrontendProfile profile = FrontendProfile.getCurrent();
      TRuntimeProfileNode prof = profile.emitAsThrift();
      Map<String, TCounter> counters = Maps.uniqueIndex(prof.counters, TCounter::getName);
      assertEquals(1, counters.get("CatalogFetch.Partitions.Requests").getValue());
      assertEquals(1, counters.get("CatalogFetch.Partitions.Misses").getValue());
      List<String> paths = partMap.values().stream()
          .map(PartitionMetadata::getFileDescriptors)
          .flatMap(Collection::stream)
          .map(HdfsPartition.FileDescriptor::getPath)
          .collect(Collectors.toList());
      assertEquals("Actual paths: " + paths, 1, paths.size());
    }
  }

  public void testLargeTConfiguration() throws Exception {
    // THRIFT-5696: Test that TByteBuffer init pass with large max message size beyond
    // TConfiguration.DEFAULT_MAX_MESSAGE_SIZE.
    int maxSize = BackendConfig.INSTANCE.getThriftRpcMaxMessageSize();
    assertEquals(1024 * 1024 * 1024, maxSize);
    int bufferSize = (100 * 1024 + 512) * 1024;
    final TConfiguration configLarge = new TConfiguration(maxSize,
        TConfiguration.DEFAULT_MAX_FRAME_SIZE, TConfiguration.DEFAULT_RECURSION_DEPTH);
    TByteBuffer validTByteBuffer =
        new TByteBuffer(configLarge, ByteBuffer.allocate(bufferSize));
    validTByteBuffer.close();
  }
}
