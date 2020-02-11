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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.impala.catalog.local.CatalogdMetaProvider.SizeOfWeigher;
import org.apache.impala.catalog.local.MetaProvider.PartitionMetadata;
import org.apache.impala.catalog.local.MetaProvider.PartitionRef;
import org.apache.impala.catalog.local.MetaProvider.TableMetaRef;
import org.apache.impala.common.Pair;
import org.apache.impala.service.FeSupport;
import org.apache.impala.service.FrontendProfile;
import org.apache.impala.testutil.TestUtils;
import org.apache.impala.thrift.TBackendGflags;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TDatabase;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TRuntimeProfileNode;
import org.apache.impala.thrift.TTable;
import org.apache.impala.util.ListMap;
import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.google.common.cache.CacheStats;
import com.google.common.collect.ImmutableList;

public class CatalogdMetaProviderTest {

  private final static Logger LOG = LoggerFactory.getLogger(
      CatalogdMetaProviderTest.class);

  private final CatalogdMetaProvider provider_;
  private final TableMetaRef tableRef_;

  private CacheStats prevStats_;

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
  }

  private CacheStats diffStats() {
    CacheStats s = provider_.getCacheStats();
    CacheStats diff = s.minus(prevStats_);
    prevStats_ = s;
    LOG.info("Stats: {}", diff);
    return diff;
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
    ListMap<TNetworkAddress> hostIndex = new ListMap<>();
    CacheStats stats = diffStats();

    // Should get no hits on the initial load of partitions.
    Map<String, PartitionMetadata> partMap = provider_.loadPartitionsByRefs(
        tableRef_, /* partitionColumnNames unused by this impl */null, hostIndex,
        partialRefs);
    assertEquals(partialRefs.size(), partMap.size());
    stats = diffStats();
    assertEquals(0, stats.hitCount());

    // Load the same partitions again and we should get a hit for each partition.
    Map<String, PartitionMetadata> partMapHit = provider_.loadPartitionsByRefs(
        tableRef_, /* partitionColumnNames unused by this impl */null, hostIndex,
        partialRefs);
    stats = diffStats();
    assertEquals(stats.hitCount(), partMapHit.size());

    // Load all of the partitions: we should get some hits and some misses.
    Map<String, PartitionMetadata> allParts = provider_.loadPartitionsByRefs(
        tableRef_, /* partitionColumnNames unused by this impl */null, hostIndex,
        allRefs);
    assertEquals(allRefs.size(), allParts.size());
    stats = diffStats();
    assertEquals(stats.hitCount(), partMapHit.size());
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
    // the size loosely.
    SizeOfWeigher weigher = new SizeOfWeigher();
    assertTrue(weigher.weigh(refs, null) > 3000);
    assertTrue(weigher.weigh(refs, null) < 4000);
  }

  @Test
  public void testCacheAndEvictDatabase() throws Exception {
    // Load a database.
    Database db = provider_.loadDb("functional");
    CacheStats stats = diffStats();
    assertEquals(1, stats.missCount());

    // ... and the table names for it.
    ImmutableList<String> tableNames = provider_.loadTableNames("functional");
    stats = diffStats();
    assertEquals(1, stats.missCount());

    // Load them again, should hit cache.
    Database dbHit = provider_.loadDb("functional");
    assertEquals(db, dbHit);
    ImmutableList<String> tableNamesHit = provider_.loadTableNames("functional");
    assertEquals(tableNames, tableNamesHit);

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
    ImmutableList<String> tableNamesMiss = provider_.loadTableNames("functional");
    assertEquals(tableNames, tableNamesMiss);
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
    assertEquals(0, stats.missCount());

    // Invalidate it.
    TCatalogObject obj = new TCatalogObject(TCatalogObjectType.TABLE, 0);
    obj.setTable(new TTable("functional", "alltypes"));
    provider_.invalidateCacheForObject(obj);

    // Should get a miss if we re-load it.
    provider_.loadTable("functional", "alltypes");
    stats = diffStats();
    assertEquals(0, stats.hitCount());
    assertEquals(1, stats.missCount());
  }

  @Test
  public void testProfile() throws Exception {
    FrontendProfile profile;
    try (FrontendProfile.Scope scope = FrontendProfile.createNewWithScope()) {
      provider_.loadTable("functional", "alltypes");
      profile = FrontendProfile.getCurrent();
    }
    TRuntimeProfileNode prof = profile.emitAsThrift();
    assertEquals(4, prof.counters.size());
    Collections.sort(prof.counters);
    assertEquals("TCounter(name:CatalogFetch.Tables.Hits, unit:NONE, value:1)",
        prof.counters.get(0).toString());
    assertEquals("TCounter(name:CatalogFetch.Tables.Misses, unit:NONE, value:0)",
        prof.counters.get(1).toString());
    assertEquals("TCounter(name:CatalogFetch.Tables.Requests, unit:NONE, value:1)",
        prof.counters.get(2).toString());
    assertEquals("CatalogFetch.Tables.Time", prof.counters.get(3).name);
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

}
