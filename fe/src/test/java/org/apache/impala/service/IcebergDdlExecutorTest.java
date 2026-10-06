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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.impala.catalog.DatabaseNotFoundException;
import org.apache.impala.catalog.TableNotFoundException;
import org.apache.impala.catalog.iceberg.IcebergRESTCatalog;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.service.catalogmanager.FeCatalogManager;
import org.apache.impala.thrift.TBackendGflags;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlExecResponse;
import org.apache.impala.thrift.TDdlQueryOptions;
import org.apache.impala.thrift.TDdlType;
import org.apache.impala.thrift.TDropTableOrViewParams;
import org.apache.impala.thrift.TErrorCode;
import org.apache.impala.thrift.TIcebergDdlExecRequest;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.util.CatalogBlacklistUtilsTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Strings;

/**
 * Unit tests for executing DDL on the coordinator through an Iceberg REST catalog,
 * without a live catalog. Routing is covered by MultiMetaProviderTest.
 */
public class IcebergDdlExecutorTest {
  private static final String CATALOG_NAME = "rest-1";
  private static final String DB_NAME = "db";
  private static final String TBL_NAME = "tbl";

  private static TBackendGflags origFlags_;

  private FeCatalogManager catalogManager_;
  private IcebergRESTCatalog restCatalog_;

  @BeforeClass
  public static void setUpClass() {
    // The blacklist check needs a BackendConfig. Save the original one so that it can
    // be restored for other tests.
    if (BackendConfig.INSTANCE == null) BackendConfig.create(new TBackendGflags());
    origFlags_ = BackendConfig.INSTANCE.getBackendCfg();
  }

  @AfterClass
  public static void tearDownClass() {
    CatalogBlacklistUtilsTest.setBlacklist(
        Strings.nullToEmpty(origFlags_.getBlacklisted_dbs()),
        Strings.nullToEmpty(origFlags_.getBlacklisted_tables()),
        origFlags_.isEnable_workload_mgmt());
    BackendConfig.create(origFlags_, false);
  }

  @Before
  public void setUp() throws Exception {
    CatalogBlacklistUtilsTest.setBlacklist("", "");
    catalogManager_ = mock(FeCatalogManager.class);
    restCatalog_ = mock(IcebergRESTCatalog.class);
    when(catalogManager_.getIcebergCatalog(CATALOG_NAME)).thenReturn(restCatalog_);
  }

  @Test
  public void testDropTable() throws Exception {
    when(restCatalog_.dropTable(DB_NAME, TBL_NAME, true)).thenReturn(true);

    TDdlExecResponse response = IcebergDdlExecutor.execDdl(
        catalogManager_, dropTableRequest(/*ifExists=*/false, /*purge=*/true));

    verify(restCatalog_).dropTable(DB_NAME, TBL_NAME, true);
    assertEquals("Table has been dropped.", summary(response));
    assertEquals(TErrorCode.OK, response.getResult().getStatus().getStatus_code());
    assertEquals(0, response.getResult().getVersion());
  }

  @Test
  public void testDropTableWithoutPurge() throws Exception {
    when(restCatalog_.dropTable(DB_NAME, TBL_NAME, false)).thenReturn(true);

    TDdlExecResponse response = IcebergDdlExecutor.execDdl(
        catalogManager_, dropTableRequest(/*ifExists=*/true, /*purge=*/false));

    verify(restCatalog_).dropTable(DB_NAME, TBL_NAME, false);
    assertEquals("Table has been dropped.", summary(response));
  }

  @Test
  public void testDropTableWithUpperCaseNames() throws Exception {
    // The names arrive as typed, but go to the REST catalog in lower case, as analysis
    // and routing use them.
    when(restCatalog_.dropTable(DB_NAME, TBL_NAME, false)).thenReturn(true);
    TIcebergDdlExecRequest request =
        dropTableRequest(/*ifExists=*/false, /*purge=*/false);
    request.getDdl_params().getDrop_table_or_view_params().setTable_name(
        new TTableName("Db", "TBL"));

    TDdlExecResponse response = IcebergDdlExecutor.execDdl(catalogManager_, request);

    verify(restCatalog_).dropTable(DB_NAME, TBL_NAME, false);
    assertEquals("Table has been dropped.", summary(response));
  }

  @Test
  public void testDropMissingTable() throws Exception {
    when(restCatalog_.dropTable(DB_NAME, TBL_NAME, false)).thenReturn(false);

    TDdlExecResponse response = IcebergDdlExecutor.execDdl(
        catalogManager_, dropTableRequest(/*ifExists=*/true, /*purge=*/false));
    assertEquals("Table does not exist.", summary(response));

    try {
      IcebergDdlExecutor.execDdl(
          catalogManager_, dropTableRequest(/*ifExists=*/false, /*purge=*/false));
      fail("Expected a missing table to be rejected without IF EXISTS");
    } catch (TableNotFoundException e) {
      assertTrue(e.getMessage().contains("Table does not exist"));
    }
  }

  @Test
  public void testDropTableInMissingDatabase() throws Exception {
    when(restCatalog_.dropTable(DB_NAME, TBL_NAME, false))
        .thenThrow(new NoSuchNamespaceException("Namespace does not exist: " + DB_NAME));

    TDdlExecResponse response = IcebergDdlExecutor.execDdl(
        catalogManager_, dropTableRequest(/*ifExists=*/true, /*purge=*/false));
    assertEquals("Database does not exist: " + DB_NAME, summary(response));

    try {
      IcebergDdlExecutor.execDdl(
          catalogManager_, dropTableRequest(/*ifExists=*/false, /*purge=*/false));
      fail("Expected a missing database to be rejected without IF EXISTS");
    } catch (DatabaseNotFoundException e) {
      assertTrue(e.getMessage().contains("Database does not exist"));
    }
  }

  @Test
  public void testDropView() throws Exception {
    // REST catalogs serve tables only, so the view cannot exist there.
    TIcebergDdlExecRequest request = dropTableRequest(/*ifExists=*/true, /*purge=*/false);
    request.getDdl_params().setDdl_type(TDdlType.DROP_VIEW);
    request.getDdl_params().getDrop_table_or_view_params().setIs_table(false);
    TDdlExecResponse response = IcebergDdlExecutor.execDdl(catalogManager_, request);
    assertEquals("View does not exist.", summary(response));

    request.getDdl_params().getDrop_table_or_view_params().setIf_exists(false);
    try {
      IcebergDdlExecutor.execDdl(catalogManager_, request);
      fail("Expected DROP VIEW to be rejected without IF EXISTS");
    } catch (TableNotFoundException e) {
      assertTrue(e.getMessage().contains("View does not exist"));
    }
    verify(restCatalog_, never()).dropTable(anyString(), anyString(), anyBoolean());
  }

  @Test
  public void testDropBlacklistedTable() throws Exception {
    // Like catalogd, a blacklisted table is never dropped, not even with PURGE. The
    // analyzer hides it and answers IF EXISTS on it as a no-op, so this only guards
    // against a request that bypassed analysis.
    CatalogBlacklistUtilsTest.setBlacklist("", DB_NAME + "." + TBL_NAME);
    assertDropBlacklistedIsRejected(DB_NAME, TBL_NAME);
    assertDropBlacklistedIsRejected("DB", "Tbl");
  }

  @Test
  public void testDropTableInBlacklistedDatabase() throws Exception {
    CatalogBlacklistUtilsTest.setBlacklist(DB_NAME, "");
    assertDropBlacklistedIsRejected(DB_NAME, TBL_NAME);
    assertDropBlacklistedIsRejected("DB", "Tbl");
  }

  /**
   * Checks that dropping 'dbName'.'tblName', which is db.tbl in some case, is rejected
   * as blacklisted.
   */
  private void assertDropBlacklistedIsRejected(String dbName, String tblName)
      throws Exception {
    TIcebergDdlExecRequest request = dropTableRequest(/*ifExists=*/true, /*purge=*/true);
    TDropTableOrViewParams params =
        request.getDdl_params().getDrop_table_or_view_params();
    params.setTable_name(new TTableName(dbName, tblName));
    TDdlExecResponse response = IcebergDdlExecutor.execDdl(catalogManager_, request);
    assertEquals("Can't drop blacklisted table: db.tbl", summary(response));

    params.setIf_exists(false);
    try {
      IcebergDdlExecutor.execDdl(catalogManager_, request);
      fail("Expected a blacklisted table to be rejected without IF EXISTS");
    } catch (ImpalaRuntimeException e) {
      assertTrue(e.getMessage().contains("Can't drop blacklisted table: db.tbl"));
    }
    verify(restCatalog_, never()).dropTable(anyString(), anyString(), anyBoolean());
  }

  @Test
  public void testUnknownCatalog() throws Exception {
    when(catalogManager_.getIcebergCatalog(CATALOG_NAME))
        .thenThrow(new ImpalaRuntimeException("Unknown Iceberg REST catalog: rest-1"));
    try {
      IcebergDdlExecutor.execDdl(
          catalogManager_, dropTableRequest(/*ifExists=*/true, /*purge=*/false));
      fail("Expected an unknown catalog to be rejected");
    } catch (ImpalaRuntimeException e) {
      assertTrue(e.getMessage().contains("Unknown Iceberg REST catalog"));
    }
  }

  @Test
  public void testUnsupportedDdlType() throws Exception {
    TIcebergDdlExecRequest request = dropTableRequest(false, false);
    request.getDdl_params().setDdl_type(TDdlType.ALTER_TABLE);
    try {
      IcebergDdlExecutor.execDdl(catalogManager_, request);
      fail("Expected ALTER TABLE to be rejected");
    } catch (ImpalaRuntimeException e) {
      assertTrue(e.getMessage().contains("ALTER_TABLE is not supported"));
    }
  }

  private static TIcebergDdlExecRequest dropTableRequest(boolean ifExists,
      boolean purge) {
    TDropTableOrViewParams params = new TDropTableOrViewParams();
    params.setTable_name(new TTableName(DB_NAME, TBL_NAME));
    params.setIf_exists(ifExists);
    params.setPurge(purge);
    params.setIs_table(true);
    TDdlExecRequest ddl = new TDdlExecRequest();
    ddl.setDdl_type(TDdlType.DROP_TABLE);
    ddl.setDrop_table_or_view_params(params);
    ddl.setQuery_options(new TDdlQueryOptions(false));
    TIcebergDdlExecRequest request = new TIcebergDdlExecRequest();
    request.setRest_catalog_name(CATALOG_NAME);
    request.setDdl_params(ddl);
    return request;
  }

  private static String summary(TDdlExecResponse response) {
    return response.getResult_set().getRows().get(0).getColVals().get(0)
        .getString_val();
  }
}
