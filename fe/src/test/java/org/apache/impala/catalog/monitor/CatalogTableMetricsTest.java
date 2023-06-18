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

package org.apache.impala.catalog.monitor;

import org.apache.impala.catalog.DatabaseNotFoundException;
import org.apache.impala.catalog.Table;
import org.apache.impala.testutil.CatalogServiceTestCatalog;
import org.junit.Test;

import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.apache.impala.catalog.monitor.CatalogTableMetrics.INSTANCE;
import static org.junit.Assert.assertEquals;

public class CatalogTableMetricsTest {
  private final CatalogServiceTestCatalog catalog_ = CatalogServiceTestCatalog.create();

  @Test
  public void testAddRemoveInverse() throws DatabaseNotFoundException {
    INSTANCE.removeAllTables();
    Table table = catalog_.getTable("functional", "alltypes");
    assertAddRemoveInverse(table, INSTANCE::updateFrequentlyAccessedTables,
        () -> INSTANCE.getFrequentlyAccessedTables().size());
    assertAddRemoveInverse(
        table, INSTANCE::updateLargestTables, () -> INSTANCE.getLargestTables().size());
    assertAddRemoveInverse(table, INSTANCE::updateHighFileCountTables,
        () -> INSTANCE.getHighFileCountTables().size());
    assertAddRemoveInverse(table, INSTANCE::updateLongMetadataLoadingTables,
        () -> INSTANCE.getLongMetadataLoadingTables().size());
  }

  private void assertAddRemoveInverse(
      Table table, Consumer<Table> metricUpdater, Supplier<Integer> tableCounter) {
    assertEquals(0, tableCounter.get().intValue());
    metricUpdater.accept(table);
    assertEquals(1, tableCounter.get().intValue());
    INSTANCE.removeTable(table);
    assertEquals(0, tableCounter.get().intValue());
  }
}