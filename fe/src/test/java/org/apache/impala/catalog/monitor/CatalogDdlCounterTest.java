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

import org.apache.impala.thrift.TDdlType;
import org.apache.impala.thrift.TOperationUsageCounter;
import org.apache.impala.thrift.TTableName;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class CatalogDdlCounterTest {
  private CatalogDdlCounter catalogDdlCounter;

  private final TDdlType TEST_DDL_CREATE = TDdlType.CREATE_TABLE;
  private final TDdlType TEST_DDL_DROP = TDdlType.DROP_TABLE;
  private final String TEST_DB_NAME = "TEST";
  private final String TEST_TABLE_NAME = "TABLE";
  private final Optional<TTableName> TEST_TTABLENAME =
      Optional.of(new TTableName(TEST_DB_NAME, TEST_TABLE_NAME));

  @Before
  public void setUp() {
    catalogDdlCounter = new CatalogDdlCounter();
  }

  @Test
  public void testIncrementOperationIncrementsCounter() {
    catalogDdlCounter.incrementOperation(TEST_DDL_CREATE, TEST_TTABLENAME);
    TOperationUsageCounter operationUsageCounter =
        catalogDdlCounter.getOperationUsage().get(0);
    assertEquals(TEST_DDL_CREATE.name(), operationUsageCounter.catalog_op_name);
    assertEquals(1, operationUsageCounter.op_counter);
    assertEquals(TEST_DB_NAME + "." + TEST_TABLE_NAME, operationUsageCounter.table_name);
  }

  @Test
  public void testDecrementOperationDecrementsCounter() {
    catalogDdlCounter.incrementOperation(TEST_DDL_CREATE, TEST_TTABLENAME);
    catalogDdlCounter.decrementOperation(TEST_DDL_CREATE, TEST_TTABLENAME);
    List<TOperationUsageCounter> operationUsage = catalogDdlCounter.getOperationUsage();
    assertEquals(0, operationUsage.size());
  }

  @Test
  public void testGetOperationUsageReturnsListOfCounters() {
    catalogDdlCounter.incrementOperation(TEST_DDL_CREATE, TEST_TTABLENAME);
    catalogDdlCounter.incrementOperation(TEST_DDL_DROP, TEST_TTABLENAME);
    List<TOperationUsageCounter> operationUsage = catalogDdlCounter.getOperationUsage();
    assertEquals(2, operationUsage.size());
  }
}