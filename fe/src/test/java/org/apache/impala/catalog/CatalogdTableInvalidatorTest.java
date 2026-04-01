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

import com.google.common.base.Ticker;
import org.apache.impala.common.Reference;
import org.apache.impala.testutil.CatalogServiceTestCatalog;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.util.NoOpEventSequence;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static java.lang.Thread.sleep;


public class CatalogdTableInvalidatorTest {
  private static CatalogServiceCatalog catalog_ = CatalogServiceTestCatalog.create();

  @AfterClass
  public static void tearDown() { catalog_.close(); }

  private long waitForTrigger(long previousTriggerCount) throws InterruptedException {
    long triggerCount;
    do {
      sleep(5);
      triggerCount = catalog_.getCatalogdTableInvalidator().scanCount_.get();
    } while (triggerCount == previousTriggerCount);
    return triggerCount;
  }

  /**
   * Test time-based invalidation in CatalogdTableInvalidator.
   */
  @Test
  public void testCatalogdTableInvalidator()
      throws CatalogException, InterruptedException {
    Reference<Boolean> tblWasRemoved = new Reference<>();
    Reference<Boolean> dbWasAdded = new Reference<>();
    String dbName = "functional";
    String tblName = "alltypes";
    catalog_.invalidateTable(new TTableName(dbName, tblName), tblWasRemoved, dbWasAdded,
        NoOpEventSequence.INSTANCE);
    MockTicker ticker = new MockTicker();
    CatalogdTableInvalidator.TIME_SOURCE = ticker;
    catalog_.setCatalogdTableInvalidator(
        new CatalogdTableInvalidator(catalog_, /*unusedTableTtlSec=*/
            2, /*invalidateTablesOnMemoryPressure=*/false, /*oldGenFullThreshold=*/
            0.6, /*gcInvalidationFraction=*/0.1));
    Assert.assertFalse(catalog_.getDb(dbName).getTable(tblName).isLoaded());
    Table table = catalog_.getOrLoadTable(dbName, tblName, "test", null);
    Assert.assertTrue(table.isLoaded());
    Assert.assertEquals(ticker.now_, table.getLastUsedTime());
    long previousTriggerCount = catalog_.getCatalogdTableInvalidator().scanCount_.get();
    ticker.set(TimeUnit.SECONDS.toNanos(1));
    table.refreshLastUsedTime();
    ticker.set(TimeUnit.SECONDS.toNanos(3));
    previousTriggerCount = waitForTrigger(previousTriggerCount);
    // The last used time is refreshed so the table won't be invalidated
    Assert.assertTrue(catalog_.getTable(dbName, tblName).isLoaded());
    ticker.set(TimeUnit.SECONDS.toNanos(6));
    waitForTrigger(previousTriggerCount);
    // The table is now invalidated
    Assert.assertFalse(catalog_.getTable(dbName, tblName).isLoaded());
  }

  /**
   * TTL invalidation metrics: totals, sliding windows (staggered batches), last-batch
   * fields, and isolation from memory-pressure counters.
   * Only functional.alltypes is loaded first; after (ttlSec+10)s the daemon invalidates
   * it (first batch, one table). Then functional.alltypesagg is loaded; after another
   * (ttlSec+10)s the second batch runs. The two batches are spaced >10s apart in
   * wall-clock time so the 10s sliding window counts only the latest batch (1 table)
   * while 1m, 5m, and 30m include both (2 tables).
   * Memory-pressure counters stay unchanged throughout.
   */
  @Test
  public void testInvalidationMetrics() throws CatalogException, InterruptedException {
    Reference<Boolean> tblWasRemoved = new Reference<>();
    Reference<Boolean> dbWasAdded = new Reference<>();
    String dbName = "functional";
    String tblName1 = "alltypes";
    String tblName2 = "alltypesagg";

    catalog_.invalidateTable(new TTableName(dbName, tblName1), tblWasRemoved, dbWasAdded,
        NoOpEventSequence.INSTANCE);
    catalog_.invalidateTable(new TTableName(dbName, tblName2), tblWasRemoved, dbWasAdded,
        NoOpEventSequence.INSTANCE);

    long initialTtlCount = catalog_.getNumTtlInvalidatedTables();
    long initialMemoryCount = catalog_.getNumMemoryPressureInvalidatedTables();

    MockTicker ticker = new MockTicker();
    CatalogdTableInvalidator.TIME_SOURCE = ticker;
    // 1s TTL: invalidation batches are spaced >10s apart in wall-clock time so the 10s
    // window sees only the latest batch while longer windows include both.
    final long ttlSec = 1;
    catalog_.setCatalogdTableInvalidator(
        new CatalogdTableInvalidator(catalog_, /*unusedTableTtlSec=*/ttlSec,
            /*invalidateTablesOnMemoryPressure=*/false, /*oldGenFullThreshold=*/0.6,
            /*gcInvalidationFraction=*/0.1));

    final long baseNanos = TimeUnit.HOURS.toNanos(1);
    ticker.set(baseNanos);

    Table table1 = catalog_.getOrLoadTable(dbName, tblName1, "test", null);
    Assert.assertTrue(table1.isLoaded());

    long previousTriggerCount = catalog_.getCatalogdTableInvalidator().scanCount_.get();
    ticker.set(baseNanos + TimeUnit.SECONDS.toNanos(ttlSec + 10));
    previousTriggerCount = waitForTrigger(previousTriggerCount);
    Assert.assertFalse(catalog_.getTable(dbName, tblName1).isLoaded());

    // Sliding-window metrics use wall-clock time; wait so the first batch falls outside
    // the 10s window before the second batch runs.
    sleep(11000);

    ticker.set(baseNanos + TimeUnit.SECONDS.toNanos(ttlSec + 11));
    previousTriggerCount = waitForTrigger(previousTriggerCount);
    Table table2 = catalog_.getOrLoadTable(dbName, tblName2, "test", null);
    Assert.assertTrue(table2.isLoaded());

    ticker.set(baseNanos + TimeUnit.SECONDS.toNanos(2 * ttlSec + 21));
    waitForTrigger(previousTriggerCount);
    Assert.assertFalse(catalog_.getTable(dbName, tblName2).isLoaded());

    Assert.assertEquals("TTL invalidation count should increase by 2",
        initialTtlCount + 2, catalog_.getNumTtlInvalidatedTables());
    Assert.assertEquals("Memory pressure invalidation count should not change",
        initialMemoryCount, catalog_.getNumMemoryPressureInvalidatedTables());

    CatalogdTableInvalidator.InvalidationMetrics metrics =
        catalog_.getCatalogdTableInvalidator().getMetrics();

    Assert.assertEquals("10-sec window should count only the latest batch", 1,
        metrics.ttlInvalidations10Sec());
    Assert.assertEquals("1-min window should include both batches", 2,
        metrics.ttlInvalidations1Min());
    Assert.assertEquals("5-min window should include both batches", 2,
        metrics.ttlInvalidations5Min());
    Assert.assertEquals("30-min window should include both batches", 2,
        metrics.ttlInvalidations30Min());
    Assert.assertTrue("Longer windows should be at least as large as shorter ones",
        metrics.ttlInvalidations30Min() >= metrics.ttlInvalidations5Min()
            && metrics.ttlInvalidations5Min() >= metrics.ttlInvalidations1Min()
            && metrics.ttlInvalidations1Min() >= metrics.ttlInvalidations10Sec());

    Assert.assertEquals("Last TTL batch should report 1 table", 1,
        metrics.lastTtlInvalidatedTables());
    Assert.assertTrue("Last TTL batch timestamp should be set",
        metrics.lastTtlInvalidationMillis() > 0);
  }

  @After
  public void cleanUp() {
    catalog_.getCatalogdTableInvalidator().stop();
    catalog_.setCatalogdTableInvalidator(null);
    CatalogdTableInvalidator.TIME_SOURCE = Ticker.systemTicker();
  }

  class MockTicker extends Ticker {
    long now_ = 1000;

    @Override
    synchronized public long read() {
      return now_;
    }

    void set(long nanoSec) {
      synchronized (this) {
        now_ = nanoSec;
      }
      catalog_.getCatalogdTableInvalidator().wakeUpForTests();
    }
  }
}
