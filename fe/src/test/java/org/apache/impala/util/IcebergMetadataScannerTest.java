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

package org.apache.impala.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.IncompleteTable;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TImpalaTableType;
import org.junit.Test;

/**
 * Tests for IcebergMetadataScanner.
 */
public class IcebergMetadataScannerTest extends FrontendTestBase {

  /**
   * IMPALA-14778: The backend passes the base table that it got again at execution time
   * to IcebergMetadataScanner. It may be an incomplete or a non-Iceberg table.
   */
  @Test
  public void testCheckIcebergTable() throws ImpalaException {
    FeTable iceTbl =
        catalog_.getOrLoadTable("functional_parquet", "iceberg_query_metadata");
    assertSame(iceTbl, IcebergMetadataScanner.checkIcebergTable(iceTbl, "SNAPSHOTS"));

    FeTable hdfsTbl = catalog_.getOrLoadTable("functional", "alltypes");
    try {
      IcebergMetadataScanner.checkIcebergTable(hdfsTbl, "SNAPSHOTS");
      fail("Expected an ImpalaRuntimeException");
    } catch (ImpalaRuntimeException e) {
      assertEquals("Cannot scan metadata table 'functional.alltypes.snapshots': "
          + "table 'functional.alltypes' is not an Iceberg table", e.getMessage());
    }

    // A table whose metadata failed to load. The load failure is kept as the cause.
    TableLoadingException cause = new TableLoadingException("Injected load failure");
    FeTable failedTbl = IncompleteTable.createFailedMetadataLoadTable(
        catalog_.getDb("functional"), "failed_tbl", cause);
    try {
      IcebergMetadataScanner.checkIcebergTable(failedTbl, "SNAPSHOTS");
      fail("Expected a TableLoadingException");
    } catch (TableLoadingException e) {
      assertEquals("Cannot scan metadata table 'functional.failed_tbl.snapshots': "
          + "failed to load table 'functional.failed_tbl'", e.getMessage());
      assertSame(cause, e.getCause());
    }

    // A table whose metadata is not loaded (legacy catalog mode).
    FeTable unloadedTbl = IncompleteTable.createUninitializedTable(
        catalog_.getDb("functional"), "unloaded_tbl", TImpalaTableType.TABLE, null, -1L);
    try {
      IcebergMetadataScanner.checkIcebergTable(unloadedTbl, "SNAPSHOTS");
      fail("Expected a TableLoadingException");
    } catch (TableLoadingException e) {
      assertEquals("Cannot scan metadata table 'functional.unloaded_tbl.snapshots': "
          + "the metadata of table 'functional.unloaded_tbl' is not loaded",
          e.getMessage());
      assertNull(e.getCause());
    }
  }

  /**
   * IMPALA-14778: close() may be called before the scan, after a partial or a full scan,
   * and more than once. Closing the iterator of each task does not lose rows.
   */
  @Test
  public void testClose() throws ImpalaException {
    FeTable iceTbl =
        catalog_.getOrLoadTable("functional_parquet", "iceberg_query_metadata");
    new IcebergMetadataScanner(iceTbl, "ENTRIES").close();

    // ENTRIES rows are read from the manifests, one manifest reader per task.
    try (IcebergMetadataScanner scanner = new IcebergMetadataScanner(iceTbl, "ENTRIES")) {
      scanner.ScanMetadataTable();
      int numRows = 0;
      while (scanner.GetNext() != null) ++numRows;
      assertEquals(4, numRows);
    }

    IcebergMetadataScanner scanner = new IcebergMetadataScanner(iceTbl, "ENTRIES");
    scanner.ScanMetadataTable();
    assertNotNull(scanner.GetNext());
    scanner.close();
    scanner.close();
    assertNull(scanner.GetNext());
  }
}
