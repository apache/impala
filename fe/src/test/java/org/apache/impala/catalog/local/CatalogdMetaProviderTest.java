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

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.impala.catalog.local.CatalogdMetaProvider.SizeOfWeigher;
import org.apache.impala.catalog.local.MetaProvider.PartitionMetadata;
import org.apache.impala.catalog.local.MetaProvider.PartitionRef;
import org.apache.impala.catalog.local.MetaProvider.TableMetaRef;
import org.apache.impala.common.Pair;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TBackendGflags;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.util.ListMap;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

}