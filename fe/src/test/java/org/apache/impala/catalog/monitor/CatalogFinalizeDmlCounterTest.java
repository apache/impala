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

import org.apache.impala.thrift.TCatalogServiceRequestHeader;
import org.apache.impala.thrift.TOperationUsageCounter;
import org.apache.impala.thrift.TUpdateCatalogRequest;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.apache.impala.catalog.monitor.CatalogFinalizeDmlCounter.*;
import static org.junit.Assert.assertEquals;

public class CatalogFinalizeDmlCounterTest {
  private CatalogFinalizeDmlCounter catalogFinalizeDmlCounter;

  private final String TEST_UPDATE_SQL = "UPDATE table SET c3 = 'test';";
  private final String TEST_CTAS_SQL = "CREATE TABLE test AS SELECT * FROM t;";
  private final String TEST_INSERT_SQL = "insert into table text_table "
      + "select * from tab1;";
  private static final String TEST_DML_SQL = "load data inpath 'test.txt' "
      + "into table t1;";
  private final String TEST_DB_NAME = "TEST";
  private final String TEST_TABLE_NAME = "TABLE";

  private final TUpdateCatalogRequest testUpdateRequest =
      createTestTUpdateCatalogRequest(TEST_DB_NAME, TEST_TABLE_NAME, TEST_UPDATE_SQL);

  @Before
  public void setUp() {
    catalogFinalizeDmlCounter = new CatalogFinalizeDmlCounter();
  }

  @Test
  public void testIncrementOperationIncrementsCounter() {
    catalogFinalizeDmlCounter.incrementOperation(testUpdateRequest);
    TOperationUsageCounter tOperationUsageCounter =
        catalogFinalizeDmlCounter.getOperationUsage().get(0);
    assertEquals(
        FinalizeDmlType.FINALIZE_UPDATE.name(), tOperationUsageCounter.catalog_op_name);
    assertEquals(1, tOperationUsageCounter.op_counter);
    assertEquals(TEST_DB_NAME + "." + TEST_TABLE_NAME, tOperationUsageCounter.table_name);
  }

  @Test
  public void testDecrementOperationDecrementsCounter() {
    catalogFinalizeDmlCounter.incrementOperation(testUpdateRequest);
    catalogFinalizeDmlCounter.decrementOperation(testUpdateRequest);
    List<TOperationUsageCounter> counterList =
        catalogFinalizeDmlCounter.getOperationUsage();
    assertEquals(0, counterList.size());
  }

  @Test
  public void testIncrementOperationInfersCTAS() {
    TUpdateCatalogRequest testCTASRequest =
        createTestTUpdateCatalogRequest(TEST_DB_NAME, TEST_TABLE_NAME, TEST_CTAS_SQL);
    catalogFinalizeDmlCounter.incrementOperation(testCTASRequest);
    TOperationUsageCounter operationUsageCounter =
        catalogFinalizeDmlCounter.getOperationUsage().get(0);
    assertEquals(FinalizeDmlType.FINALIZE_CREATE_TABLE_AS_SELECT.name(),
        operationUsageCounter.catalog_op_name);
  }

  @Test
  public void testIncrementOperationInfersInsertInto() {
    TUpdateCatalogRequest testInsertRequest =
        createTestTUpdateCatalogRequest(TEST_DB_NAME, TEST_TABLE_NAME, TEST_INSERT_SQL);
    catalogFinalizeDmlCounter.incrementOperation(testInsertRequest);
    TOperationUsageCounter operationUsageCounter =
        catalogFinalizeDmlCounter.getOperationUsage().get(0);
    assertEquals(FinalizeDmlType.FINALIZE_INSERT_INTO.name(),
        operationUsageCounter.catalog_op_name);
  }

  @Test
  public void testIncrementOperationInfersDml() {
    TUpdateCatalogRequest testDmlRequest =
        createTestTUpdateCatalogRequest(TEST_DB_NAME, TEST_TABLE_NAME, TEST_DML_SQL);
    catalogFinalizeDmlCounter.incrementOperation(testDmlRequest);
    TOperationUsageCounter operationUsageCounter =
        catalogFinalizeDmlCounter.getOperationUsage().get(0);
    assertEquals(
        FinalizeDmlType.FINALIZE_DML.name(), operationUsageCounter.catalog_op_name);
  }

  @Test
  public void testGetOperationUsageReturnsMultipleDMLs() {
    TUpdateCatalogRequest testCTASRequest =
        createTestTUpdateCatalogRequest(TEST_DB_NAME, TEST_TABLE_NAME, TEST_CTAS_SQL);
    catalogFinalizeDmlCounter.incrementOperation(testUpdateRequest);
    catalogFinalizeDmlCounter.incrementOperation(testCTASRequest);
    List<TOperationUsageCounter> operationUsage =
        catalogFinalizeDmlCounter.getOperationUsage();
    assertEquals(2, operationUsage.size());
  }

  private TUpdateCatalogRequest createTestTUpdateCatalogRequest(
      String dBName, String tableName, String redacted_sql_stmt) {
    TUpdateCatalogRequest tUpdateCatalogRequest = new TUpdateCatalogRequest();
    tUpdateCatalogRequest.setDb_name(dBName);
    tUpdateCatalogRequest.setTarget_table(tableName);
    tUpdateCatalogRequest.setHeader(new TCatalogServiceRequestHeader());
    tUpdateCatalogRequest.getHeader().setRedacted_sql_stmt(redacted_sql_stmt);
    return tUpdateCatalogRequest;
  }
}