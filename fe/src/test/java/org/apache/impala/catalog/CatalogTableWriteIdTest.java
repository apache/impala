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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import org.apache.impala.testutil.CatalogServiceTestCatalog;

import static org.junit.Assert.*;

public class CatalogTableWriteIdTest {

  private CatalogServiceCatalog catalog_;

  @Before
  public void init() {
    catalog_ = CatalogServiceTestCatalog.create();
  }

  @After
  public void cleanUp() {
    catalog_.close();
  }

  @Test
  public void test() {
    TableWriteId tableWriteId1 = new TableWriteId("default", "table1", -1L, 1L);
    catalog_.addWriteId(1L, tableWriteId1);

    TableWriteId tableWriteId2 = new TableWriteId("default", "table2", -1L, 2L);
    catalog_.addWriteId(1L, tableWriteId2);

    TableWriteId tableWriteId3 = new TableWriteId("default", "table3", -1L, 3L);
    catalog_.addWriteId(2L, tableWriteId3);

    Set<TableWriteId> set = catalog_.getWriteIds(1L);
    assertNotNull(set);
    assertTrue(set.contains(tableWriteId1));
    assertTrue(set.contains(tableWriteId2));

    set = catalog_.getWriteIds(2L);
    assertNotNull(set);
    assertTrue(set.contains(tableWriteId3));

    catalog_.removeWriteIds(1L);
    catalog_.removeWriteIds(2L);

    set = catalog_.getWriteIds(1L);
    assertNotNull(set);
    assertTrue(set.isEmpty());
    set = catalog_.getWriteIds(2L);
    assertNotNull(set);
    assertTrue(set.isEmpty());
  }
}