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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.UUID;

import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TIcebergDmlFinalizeParams;
import org.apache.impala.thrift.TIcebergOperation;
import org.apache.impala.thrift.TIcebergOperationParam;
import org.junit.Before;
import org.junit.Test;

public class IcebergCatalogOpExecutorTest {
  private FeIcebergTable table_;
  private org.apache.iceberg.Table icebergApiTable_;
  private TIcebergOperationParam operation_;

  @Before
  public void setUp() {
    table_ = mock(FeIcebergTable.class);
    icebergApiTable_ = mock(org.apache.iceberg.Table.class);
    operation_ = new TIcebergOperationParam();
  }

  @Test
  public void testUuidIsOptionalForCompatibility() throws Exception {
    IcebergCatalogOpExecutor.validateTableUuid(table_, operation_);

    verify(table_, never()).getIcebergApiTable();
  }

  @Test
  public void testPlanningCapturesUuid() {
    UUID uuid = UUID.randomUUID();
    when(table_.getDefaultPartitionSpecId()).thenReturn(7);
    when(table_.snapshotId()).thenReturn(11L);
    when(table_.getIcebergApiTable()).thenReturn(icebergApiTable_);
    when(icebergApiTable_.uuid()).thenReturn(uuid);

    TIcebergDmlFinalizeParams params = Frontend.addFinalizationParamsForIcebergDml(
        table_, TIcebergOperation.MERGE);

    assertEquals(TIcebergOperation.MERGE, params.getOperation());
    assertEquals(7, params.getSpec_id());
    assertEquals(11L, params.getInitial_snapshot_id());
    assertEquals(uuid.toString(), params.getTable_uuid());
  }

  @Test
  public void testPlanningWithoutApiTableKeepsUuidUnset() {
    TIcebergDmlFinalizeParams params = Frontend.addFinalizationParamsForIcebergDml(
        table_, TIcebergOperation.INSERT);

    assertFalse(params.isSetTable_uuid());
  }

  @Test
  public void testPlanningWithoutUuidSupportKeepsUuidUnset() {
    when(table_.getIcebergApiTable()).thenReturn(icebergApiTable_);
    when(icebergApiTable_.uuid()).thenThrow(new UnsupportedOperationException());

    TIcebergDmlFinalizeParams params = Frontend.addFinalizationParamsForIcebergDml(
        table_, TIcebergOperation.INSERT);

    assertFalse(params.isSetTable_uuid());
  }

  @Test
  public void testMatchingUuid() throws Exception {
    UUID uuid = UUID.randomUUID();
    operation_.setTable_uuid(uuid.toString().toUpperCase());
    when(table_.getIcebergApiTable()).thenReturn(icebergApiTable_);
    when(icebergApiTable_.uuid()).thenReturn(uuid);

    IcebergCatalogOpExecutor.validateTableUuid(table_, operation_);
  }

  @Test
  public void testMalformedUuid() throws Exception {
    operation_.setTable_uuid("not-a-uuid");
    when(table_.getFullName()).thenReturn("db.table");

    try {
      IcebergCatalogOpExecutor.validateTableUuid(table_, operation_);
      fail("Expected a malformed Iceberg table UUID to be rejected");
    } catch (ImpalaRuntimeException e) {
      assertTrue(e.getMessage().contains("Invalid Iceberg table UUID"));
      assertTrue(e.getMessage().contains("db.table"));
    }
  }

  @Test
  public void testUuidCannotBeVerified() throws Exception {
    operation_.setTable_uuid(UUID.randomUUID().toString());
    when(table_.getFullName()).thenReturn("db.table");

    try {
      IcebergCatalogOpExecutor.validateTableUuid(table_, operation_);
      fail("Expected a missing Iceberg API table to be rejected");
    } catch (ImpalaRuntimeException e) {
      assertTrue(e.getMessage().contains("Unable to verify"));
      assertTrue(e.getMessage().contains("db.table"));
    }
  }

  @Test
  public void testMismatchedUuid() throws Exception {
    UUID expectedUuid = UUID.randomUUID();
    UUID currentUuid = UUID.randomUUID();
    operation_.setTable_uuid(expectedUuid.toString());
    when(table_.getFullName()).thenReturn("db.table");
    when(table_.getIcebergApiTable()).thenReturn(icebergApiTable_);
    when(icebergApiTable_.uuid()).thenReturn(currentUuid);

    try {
      IcebergCatalogOpExecutor.validateTableUuid(table_, operation_);
      fail("Expected a changed Iceberg table to be rejected");
    } catch (ImpalaRuntimeException e) {
      assertTrue(e.getMessage().contains("db.table"));
      assertTrue(e.getMessage().contains(expectedUuid.toString()));
      assertTrue(e.getMessage().contains(currentUuid.toString()));
    }
  }
}
