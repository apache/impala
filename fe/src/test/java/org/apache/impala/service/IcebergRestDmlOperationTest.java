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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
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
  @Test
  public void testStandaloneFinalizerAcceptsOnlyInsertInto() throws Exception {
    TIcebergOperationParam operation = new TIcebergOperationParam();
    operation.setOperation(TIcebergOperation.INSERT);
    operation.setIs_overwrite(false);
    assertMissingUuid(operation);
    operation.setTable_uuid(UUID.randomUUID().toString());
    IcebergDmlFinalizer.validateRestDmlOperation(operation);

    operation.setIs_overwrite(true);
    assertUnsupported(operation);

    operation.setIs_overwrite(false);
    operation.setOperation(TIcebergOperation.DELETE);
    assertUnsupported(operation);
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

  private static void assertUnsupported(TIcebergOperationParam operation)
      throws Exception {
    try {
      IcebergDmlFinalizer.validateRestDmlOperation(operation);
      fail("Expected unsupported REST DML operation to be rejected");
    } catch (ImpalaRuntimeException e) {
      assertTrue(e.getMessage().contains("only supports INSERT INTO"));
    }
  }

  private static void assertMissingUuid(TIcebergOperationParam operation)
      throws Exception {
    try {
      IcebergDmlFinalizer.validateRestDmlOperation(operation);
      fail("Expected a missing UUID to be rejected");
    } catch (ImpalaRuntimeException e) {
      assertTrue(e.getMessage().contains("Missing Iceberg table UUID"));
    }
  }
}
