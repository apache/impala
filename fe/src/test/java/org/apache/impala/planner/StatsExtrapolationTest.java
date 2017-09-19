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

package org.apache.impala.planner;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.Table;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TBackendGflags;
import org.junit.Test;

import com.google.common.base.Preconditions;

/**
 * Tests the behavior of stats extrapolation with valid, invalid, and unset stats,
 * as well as extreme values and other edge cases.
 */
public class StatsExtrapolationTest extends FrontendTestBase {

  /**
   * Sets the row count and total file size stats in the given table.
   * Unsets the corresponding statistic if a null value is passed.
   */
  private void setStats(Table tbl, Long rowCount, Long totalSize) {
    org.apache.hadoop.hive.metastore.api.Table msTbl =
        new org.apache.hadoop.hive.metastore.api.Table();
    msTbl.setParameters(new HashMap<String, String>());
    if (rowCount != null) {
      msTbl.getParameters().put(StatsSetupConst.ROW_COUNT,
          String.valueOf(rowCount));
    }
    if (totalSize != null) {
      msTbl.getParameters().put(StatsSetupConst.TOTAL_SIZE,
          String.valueOf(totalSize));
    }
    tbl.setMetaStoreTable(msTbl);
    tbl.setTableStats(msTbl);
  }

  private void runTest(Table tbl, Long rowCount, Long totalSize,
      long fileBytes, long expectedExtrapNumRows) {
    Preconditions.checkState(tbl instanceof HdfsTable);
    setStats(tbl, rowCount, totalSize);
    long actualExrtapNumRows = ((HdfsTable)tbl).getExtrapolatedNumRows(fileBytes);
    assertEquals(expectedExtrapNumRows, actualExrtapNumRows);
  }

  private void testInvalidStats(Table tbl, Long rowCount, Long totalSize) {
    runTest(tbl, rowCount, totalSize, 0, 0);
    runTest(tbl, rowCount, totalSize, 1, -1);
    runTest(tbl, rowCount, totalSize, 100, -1);
    runTest(tbl, rowCount, totalSize, 1000000000, -1);
    runTest(tbl, rowCount, totalSize, Long.MAX_VALUE, -1);
    runTest(tbl, rowCount, totalSize, Long.MIN_VALUE, -1);
  }

  @Test
  public void TestStatsExtrapolation() {
    addTestDb("extrap_stats", null);
    Table tbl = addTestTable("create table extrap_stats.t (i int)");

    // Replace/restore the static backend config for this test.
    BackendConfig origInstance = BackendConfig.INSTANCE;
    try {
      // Create a fake config with extrapolation enabled.
      TBackendGflags testGflags = new TBackendGflags();
      testGflags.setEnable_stats_extrapolation(true);
      BackendConfig.create(testGflags);

      // Both stats are set to a meaningful value.
      runTest(tbl, 100L, 1000L, 0, 0);
      runTest(tbl, 100L, 1000L, 100, 10);
      runTest(tbl, 100L, 1000L, 1000000000, 100000000);
      runTest(tbl, 100L, 1000L, Long.MAX_VALUE, 922337203685477632L);
      runTest(tbl, 100L, 1000L, -100, -1);
      // The extrapolated number of rows should double/triple when the
      // actual data volume doubles/triples.
      runTest(tbl, 1000000000L, 123456789L, 123456789*2, 2000000000L);
      runTest(tbl, 1000000000L, 123456789L, 123456789*3, 3000000000L);
      runTest(tbl, 7777777777L, 33333333L, 33333333L*2, 15555555554L);
      runTest(tbl, 7777777777L, 33333333L, 33333333L*3, 23333333331L);
      // Very small row count and very big total file size.
      runTest(tbl, 1L, Long.MAX_VALUE, 1, 1);
      runTest(tbl, 1L, Long.MAX_VALUE, 100, 1);
      runTest(tbl, 1L, Long.MAX_VALUE, 1000000000, 1);
      runTest(tbl, 1L, Long.MAX_VALUE, Long.MAX_VALUE, 1);
      runTest(tbl, 1L, Long.MAX_VALUE, -100, -1);
      // Very large row count and very small total file size.
      runTest(tbl, Long.MAX_VALUE, 1L, 1, Long.MAX_VALUE);
      runTest(tbl, Long.MAX_VALUE, 1L, 100, Long.MAX_VALUE);
      runTest(tbl, Long.MAX_VALUE, 1L, 1000000000, Long.MAX_VALUE);
      runTest(tbl, Long.MAX_VALUE, 1L, Long.MAX_VALUE, Long.MAX_VALUE);
      runTest(tbl, Long.MAX_VALUE, 1L, -100, -1);

      // No stats are set.
      testInvalidStats(tbl, null, null);
      // Only one of the stats fields is set.
      testInvalidStats(tbl, 100L, null);
      testInvalidStats(tbl, null, 1000L);
      // Stats are set to invalid values.
      testInvalidStats(tbl, -100L, -1000L);
      testInvalidStats(tbl, -100L, 1000L);
      testInvalidStats(tbl, 100L, -1000L);
      // Stats are zero.
      runTest(tbl, 0L, 0L, 0, 0);
      testInvalidStats(tbl, 0L, 0L);
      testInvalidStats(tbl, 100L, 0L);
      testInvalidStats(tbl, 0L, 1000L);

      // Invalid file bytes input.
      runTest(tbl, 100L, 1000L, -1, -1);
      runTest(tbl, 100L, 1000L, Long.MIN_VALUE, -1);
    } finally {
      BackendConfig.INSTANCE = origInstance;
    }
  }

  @Test
  public void TestStatsExtrapolationDisabled() {
    addTestDb("extrap_stats", null);
    Table tbl = addTestTable("create table extrap_stats.t (i int)");

    // Replace/restore the static backend config for this test.
    BackendConfig origInstance = BackendConfig.INSTANCE;
    try {
      // Create a fake config with extrapolation disabled.
      TBackendGflags testGflags = new TBackendGflags();
      testGflags.setEnable_stats_extrapolation(false);
      BackendConfig.create(testGflags);

      // Always expect -1 even with legitimate stats.
      runTest(tbl, 100L, 1000L, 0, -1);
      runTest(tbl, 100L, 1000L, 100, -1);
      runTest(tbl, 100L, 1000L, 1000000000, -1);
      runTest(tbl, 100L, 1000L, Long.MAX_VALUE, -1);
      runTest(tbl, 100L, 1000L, -100, -1);
    } finally {
      BackendConfig.INSTANCE = origInstance;
    }
  }
}
