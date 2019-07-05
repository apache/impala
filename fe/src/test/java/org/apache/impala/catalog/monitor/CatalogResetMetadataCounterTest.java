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

import org.apache.impala.thrift.TOperationUsageCounter;
import org.apache.impala.thrift.TResetMetadataRequest;
import org.apache.impala.thrift.TTableName;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class CatalogResetMetadataCounterTest {
  private CatalogResetMetadataCounter catalogResetMetadataCounter;

  private final String TEST_DB_NAME = "TEST";
  private final String TEST_TABLE_NAME = "TABLE";

  @Before
  public void setUp() {
    catalogResetMetadataCounter = new CatalogResetMetadataCounter();
  }

  @Test
  public void testIncrementOperationIncrementsRefreshCounter() {
    TResetMetadataRequest testTResetMetadataRequest =
        createTestTResetMetadataRequest(TEST_DB_NAME, TEST_TABLE_NAME, true);
    catalogResetMetadataCounter.incrementOperation(testTResetMetadataRequest);
    List<TOperationUsageCounter> operationUsage =
        catalogResetMetadataCounter.getOperationUsage();
    assertEquals(CatalogResetMetadataCounter.ResetMetadataType.REFRESH.name(),
        operationUsage.get(0).catalog_op_name);
    assertEquals(1, operationUsage.get(0).op_counter);
    assertEquals(TEST_DB_NAME + "." + TEST_TABLE_NAME, operationUsage.get(0).table_name);
  }

  @Test
  public void testIncrementOperationIncrementsInvalidateCounter() {
    TResetMetadataRequest testTResetMetadataRequest =
        createTestTResetMetadataRequest(TEST_DB_NAME, TEST_TABLE_NAME, false);
    catalogResetMetadataCounter.incrementOperation(testTResetMetadataRequest);
    List<TOperationUsageCounter> operationUsage =
        catalogResetMetadataCounter.getOperationUsage();
    assertEquals(CatalogResetMetadataCounter.ResetMetadataType.INVALIDATE_METADATA.name(),
        operationUsage.get(0).catalog_op_name);
    assertEquals(1, operationUsage.get(0).op_counter);
    assertEquals(TEST_DB_NAME + "." + TEST_TABLE_NAME, operationUsage.get(0).table_name);
  }

  @Test
  public void testIncrementOperationIncrementsGlobalInvalidateCounter() {
    TResetMetadataRequest testTResetMetadataRequest =
        createTestTResetMetadataRequest(null, null, false);
    catalogResetMetadataCounter.incrementOperation(testTResetMetadataRequest);
    List<TOperationUsageCounter> operationUsage =
        catalogResetMetadataCounter.getOperationUsage();
    assertEquals(
        CatalogResetMetadataCounter.ResetMetadataType.INVALIDATE_METADATA_GLOBAL.name(),
        operationUsage.get(0).catalog_op_name);
    assertEquals(1, operationUsage.get(0).op_counter);
    assertEquals("Not available", operationUsage.get(0).table_name);
  }

  @Test
  public void testDecrementOperationDecrementsCounter() {
    TResetMetadataRequest testTResetMetadataRequest =
        createTestTResetMetadataRequest(TEST_DB_NAME, TEST_TABLE_NAME, true);
    catalogResetMetadataCounter.incrementOperation(testTResetMetadataRequest);
    catalogResetMetadataCounter.decrementOperation(testTResetMetadataRequest);
    List<TOperationUsageCounter> operationUsage =
        catalogResetMetadataCounter.getOperationUsage();
    assertEquals(0, operationUsage.size());
  }

  @Test
  public void testGetOperationUsageReturnsMultipleDMLs() {
    TResetMetadataRequest testTResetMetadataRequest1 =
        createTestTResetMetadataRequest(TEST_DB_NAME, TEST_TABLE_NAME, true);
    TResetMetadataRequest testTResetMetadataRequest2 =
        createTestTResetMetadataRequest(TEST_DB_NAME, TEST_TABLE_NAME, false);
    catalogResetMetadataCounter.incrementOperation(testTResetMetadataRequest1);
    catalogResetMetadataCounter.incrementOperation(testTResetMetadataRequest2);
    List<TOperationUsageCounter> operationUsage =
        catalogResetMetadataCounter.getOperationUsage();
    assertEquals(2, operationUsage.size());
  }

  private TResetMetadataRequest createTestTResetMetadataRequest(
      String dbName, String tableName, boolean isRefresh) {
    TResetMetadataRequest tResetMetadataRequest = new TResetMetadataRequest();
    if (dbName != null && tableName != null) {
      tResetMetadataRequest.setTable_name(new TTableName(dbName, tableName));
    } else {
      tResetMetadataRequest.setTable_name(null);
    }
    tResetMetadataRequest.setIs_refresh(isRefresh);
    return tResetMetadataRequest;
  }
}