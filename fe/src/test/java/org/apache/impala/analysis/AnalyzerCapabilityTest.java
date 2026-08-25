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

package org.apache.impala.analysis;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.UUID;

import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.compat.MetastoreShim;
import org.junit.Test;

public class AnalyzerCapabilityTest {
  @Test
  public void testInsertCapabilityOnlyBypassesHmsAccessType() throws Exception {
    FeIcebergTable table = mock(FeIcebergTable.class);
    Table msTable = new Table();
    StorageDescriptor storageDescriptor = new StorageDescriptor();
    storageDescriptor.setNumBuckets(0);
    msTable.setSd(storageDescriptor);
    msTable.setParameters(new java.util.HashMap<>());
    MetastoreShim.setTableAccessType(msTable, Analyzer.ACCESSTYPE_READ);
    when(table.getMetaStoreTable()).thenReturn(msTable);
    when(table.getFullName()).thenReturn("rest_db.rest_table");
    when(table.hasProviderDerivedCapabilities()).thenReturn(true);
    when(table.supportsInsertInto()).thenReturn(true);
    org.apache.iceberg.Table apiTable = mock(org.apache.iceberg.Table.class);
    when(apiTable.uuid()).thenReturn(UUID.randomUUID());
    when(table.getIcebergApiTable()).thenReturn(apiTable);

    Analyzer.ensureTableWriteSupported(table, true);

    try {
      Analyzer.ensureNonInsertDmlSupported(table);
      fail("Expected non-INSERT DML to be rejected");
    } catch (AnalysisException e) {
      assertTrue(e.getMessage(),
          e.getMessage().contains("Only INSERT INTO is supported"));
    }

    storageDescriptor.setNumBuckets(1);
    assertInsertRejected(table,
        "Only read operations are supported on such tables");
  }

  @Test
  public void testRestTableWithoutUniqueFinalizerIsReadOnly() throws Exception {
    FeIcebergTable table = mock(FeIcebergTable.class);
    when(table.getFullName()).thenReturn("rest_db.rest_table");
    when(table.hasProviderDerivedCapabilities()).thenReturn(true);
    when(table.supportsInsertInto()).thenReturn(false);

    assertInsertRejected(table,
        "REST catalog does not have a unique name");
    assertRejectedNonInsertDml(table, "Only INSERT INTO is supported");
  }

  @Test
  public void testRestTableWithoutUuidIsReadOnly() throws Exception {
    FeIcebergTable table = mock(FeIcebergTable.class);
    when(table.getFullName()).thenReturn("rest_db.rest_table");
    when(table.hasProviderDerivedCapabilities()).thenReturn(true);
    when(table.supportsInsertInto()).thenReturn(true);

    assertInsertRejected(table,
        "Iceberg table UUID is unavailable");
    assertRejectedNonInsertDml(table, "Only INSERT INTO is supported");
  }

  private static void assertRejectedNonInsertDml(
      FeTable table, String expectedMessage) throws Exception {
    try {
      Analyzer.ensureNonInsertDmlSupported(table);
      fail("Expected non-INSERT DML to be rejected");
    } catch (AnalysisException e) {
      assertTrue(e.getMessage(), e.getMessage().contains(expectedMessage));
    }
  }

  private static void assertInsertRejected(FeTable table, String expectedMessage)
      throws Exception {
    try {
      Analyzer.ensureTableWriteSupported(table, true);
      fail("Expected table operation to be rejected");
    } catch (AnalysisException e) {
      assertTrue(e.getMessage(), e.getMessage().contains(expectedMessage));
    }
  }
}
