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

package org.apache.impala.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.UUID;

import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TIcebergDmlFinalizeParams;
import org.apache.impala.thrift.TIcebergOperation;
import org.apache.impala.thrift.TIcebergOperationParam;
import org.junit.Test;

public class IcebergRestDmlOperationTest {
  private static final String MISSING_UUID = "Missing Iceberg table UUID";
  private static final String UNSUPPORTED_OPERATION =
      "Unsupported operation for the Iceberg REST finalizer";

  @Test
  public void testRestFinalizerAcceptsInsert() throws Exception {
    TIcebergOperationParam operation = restOperation(TIcebergOperation.INSERT);
    IcebergDmlFinalizer.validateRestDmlOperation(operation);

    operation.unsetTable_uuid();
    assertRejected(operation, MISSING_UUID);
  }

  @Test
  public void testRestFinalizerAcceptsInsertOverwrite() throws Exception {
    TIcebergOperationParam operation = restOperation(TIcebergOperation.INSERT);
    operation.setIs_overwrite(true);
    IcebergDmlFinalizer.validateRestDmlOperation(operation);

    operation.unsetTable_uuid();
    assertRejected(operation, MISSING_UUID);
  }

  @Test
  public void testRestFinalizerAcceptsUpdate() throws Exception {
    TIcebergOperationParam operation = restOperation(TIcebergOperation.UPDATE);
    IcebergDmlFinalizer.validateRestDmlOperation(operation);

    operation.unsetTable_uuid();
    assertRejected(operation, MISSING_UUID);
  }

  @Test
  public void testRestFinalizerAcceptsDelete() throws Exception {
    TIcebergOperationParam operation = restOperation(TIcebergOperation.DELETE);
    IcebergDmlFinalizer.validateRestDmlOperation(operation);

    operation.unsetTable_uuid();
    assertRejected(operation, MISSING_UUID);
  }

  @Test
  public void testRestFinalizerRejectsMerge() {
    assertRejected(restOperation(TIcebergOperation.MERGE), UNSUPPORTED_OPERATION);
  }

  @Test
  public void testPlanningCarriesRestCatalogName() {
    FeIcebergTable table = mock(FeIcebergTable.class);
    org.apache.iceberg.Table apiTable = mock(org.apache.iceberg.Table.class);
    UUID uuid = UUID.randomUUID();
    when(table.getIcebergDmlCatalogName()).thenReturn("rest-1");
    when(table.getIcebergApiTable()).thenReturn(apiTable);
    when(apiTable.uuid()).thenReturn(uuid);

    TIcebergDmlFinalizeParams params =
        Frontend.addFinalizationParamsForIcebergDml(
            table, TIcebergOperation.INSERT);

    assertTrue(params.isSetRest_catalog_name());
    assertEquals("rest-1", params.getRest_catalog_name());
    assertEquals(uuid.toString(), params.getTable_uuid());
  }

  @Test
  public void testPlanningKeepsCatalogdRouteByDefault() {
    FeIcebergTable table = mock(FeIcebergTable.class);

    TIcebergDmlFinalizeParams params =
        Frontend.addFinalizationParamsForIcebergDml(
            table, TIcebergOperation.INSERT);

    assertFalse(params.isSetRest_catalog_name());
  }

  /** Returns a valid REST DML operation, with a table UUID and no overwrite. */
  private static TIcebergOperationParam restOperation(TIcebergOperation op) {
    TIcebergOperationParam operation = new TIcebergOperationParam();
    operation.setOperation(op);
    operation.setIs_overwrite(false);
    operation.setTable_uuid(UUID.randomUUID().toString());
    return operation;
  }

  private static void assertRejected(
      TIcebergOperationParam operation, String expectedMessage) {
    ImpalaRuntimeException e = assertThrows(ImpalaRuntimeException.class,
        () -> IcebergDmlFinalizer.validateRestDmlOperation(operation));
    assertTrue(e.getMessage(), e.getMessage().contains(expectedMessage));
  }
}
