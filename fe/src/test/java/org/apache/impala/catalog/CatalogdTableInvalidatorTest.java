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
