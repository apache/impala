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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.impala.catalog.DatabaseNotFoundException;
import org.apache.impala.catalog.TableNotFoundException;
import org.apache.impala.catalog.iceberg.IcebergRESTCatalog;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.service.catalogmanager.FeCatalogManager;
import org.apache.impala.thrift.TBackendGflags;
import org.apache.impala.thrift.TCreateDbParams;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlExecResponse;
import org.apache.impala.thrift.TDdlQueryOptions;
import org.apache.impala.thrift.TDdlType;
import org.apache.impala.thrift.TDropDbParams;
import org.apache.impala.thrift.TDropTableOrViewParams;
import org.apache.impala.thrift.TErrorCode;
import org.apache.impala.thrift.TIcebergDdlExecRequest;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.util.CatalogBlacklistUtilsTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

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
    CatalogBlacklistUtilsTest.restoreBackendConfig(origFlags_, false);
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

    TableNotFoundException e = assertThrows(TableNotFoundException.class,
        () -> IcebergDdlExecutor.execDdl(
            catalogManager_, dropTableRequest(/*ifExists=*/false, /*purge=*/false)));
    assertTrue(e.getMessage(), e.getMessage().contains("Table does not exist"));
  }

  @Test
  public void testDropTableInMissingDatabase() throws Exception {
    when(restCatalog_.dropTable(DB_NAME, TBL_NAME, false))
        .thenThrow(new NoSuchNamespaceException("Namespace does not exist: " + DB_NAME));

    TDdlExecResponse response = IcebergDdlExecutor.execDdl(
        catalogManager_, dropTableRequest(/*ifExists=*/true, /*purge=*/false));
    assertEquals("Database does not exist: " + DB_NAME, summary(response));

    DatabaseNotFoundException e = assertThrows(DatabaseNotFoundException.class,
        () -> IcebergDdlExecutor.execDdl(
            catalogManager_, dropTableRequest(/*ifExists=*/false, /*purge=*/false)));
    assertTrue(e.getMessage(), e.getMessage().contains("Database does not exist"));
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
    TableNotFoundException e = assertThrows(TableNotFoundException.class,
        () -> IcebergDdlExecutor.execDdl(catalogManager_, request));
    assertTrue(e.getMessage(), e.getMessage().contains("View does not exist"));
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
    ImpalaRuntimeException e = assertThrows(ImpalaRuntimeException.class,
        () -> IcebergDdlExecutor.execDdl(catalogManager_, request));
    assertTrue(e.getMessage(),
        e.getMessage().contains("Can't drop blacklisted table: db.tbl"));
    verify(restCatalog_, never()).dropTable(anyString(), anyString(), anyBoolean());
  }

  @Test
  public void testDropDatabase() throws Exception {
    when(restCatalog_.dropNamespace(DB_NAME)).thenReturn(true);

    TDdlExecResponse response = IcebergDdlExecutor.execDdl(
        catalogManager_, dropDbRequest("DB", /*ifExists=*/false, /*cascade=*/false));

    verify(restCatalog_).dropNamespace(DB_NAME);
    assertEquals("Database has been dropped.", summary(response));
    assertEquals(0, response.getResult().getVersion());
  }

  @Test
  public void testDropMissingDatabase() throws Exception {
    when(restCatalog_.dropNamespace(DB_NAME)).thenReturn(false);

    TDdlExecResponse response = IcebergDdlExecutor.execDdl(
        catalogManager_, dropDbRequest(DB_NAME, /*ifExists=*/true, /*cascade=*/false));
    assertEquals("Database does not exist.", summary(response));

    DatabaseNotFoundException e = assertThrows(DatabaseNotFoundException.class,
        () -> IcebergDdlExecutor.execDdl(catalogManager_,
            dropDbRequest(DB_NAME, /*ifExists=*/false, /*cascade=*/false)));
    assertTrue(e.getMessage(),
        e.getMessage().contains("Database does not exist: " + DB_NAME));
  }

  @Test
  public void testDropNonEmptyDatabase() throws Exception {
    // The server answers HTTP 409, which Iceberg clients report as either exception.
    for (RuntimeException notEmpty : new RuntimeException[] {
        new NamespaceNotEmptyException("Namespace not empty: %s", DB_NAME),
        new AlreadyExistsException("Namespace not empty: %s", DB_NAME)}) {
      doThrow(notEmpty).when(restCatalog_).dropNamespace(DB_NAME);
      ImpalaRuntimeException e = assertThrows(ImpalaRuntimeException.class,
          () -> IcebergDdlExecutor.execDdl(catalogManager_,
              dropDbRequest(DB_NAME, /*ifExists=*/true, /*cascade=*/false)));
      assertTrue(e.getMessage(),
          e.getMessage().contains("Cannot drop non-empty database: " + DB_NAME));
    }
  }

  @Test
  public void testDropDatabaseCascade() throws Exception {
    ImpalaRuntimeException e = assertThrows(ImpalaRuntimeException.class,
        () -> IcebergDdlExecutor.execDdl(catalogManager_,
            dropDbRequest(DB_NAME, /*ifExists=*/true, /*cascade=*/true)));
    assertTrue(e.getMessage(),
        e.getMessage().contains("DROP DATABASE CASCADE is not supported"));
    verify(restCatalog_, never()).dropNamespace(anyString());
  }

  @Test
  public void testDropBlacklistedDatabase() throws Exception {
    CatalogBlacklistUtilsTest.setBlacklist(DB_NAME, "");
    TDdlExecResponse response = IcebergDdlExecutor.execDdl(
        catalogManager_, dropDbRequest("DB", /*ifExists=*/true, /*cascade=*/false));
    assertEquals("Can't drop blacklisted database: " + DB_NAME, summary(response));
    ImpalaRuntimeException e = assertThrows(ImpalaRuntimeException.class,
        () -> IcebergDdlExecutor.execDdl(catalogManager_,
            dropDbRequest(DB_NAME, /*ifExists=*/false, /*cascade=*/false)));
    assertTrue(e.getMessage(),
        e.getMessage().contains("Can't drop blacklisted database"));
    verify(restCatalog_, never()).dropNamespace(anyString());
  }

  @Test
  public void testCreateDatabase() throws Exception {
    TIcebergDdlExecRequest request = createDbRequest("DB", /*ifNotExists=*/false);
    TCreateDbParams params = request.getDdl_params().getCreate_db_params();
    params.setComment("c");
    params.setLocation("/loc");
    params.setProperties(ImmutableMap.of("k", "v"));

    TDdlExecResponse response = IcebergDdlExecutor.execDdl(catalogManager_, request);

    verify(restCatalog_).createNamespace(DB_NAME,
        ImmutableMap.of("k", "v", "comment", "c", "location", "/loc"));
    assertEquals("Database has been created.", summary(response));
    assertEquals(0, response.getResult().getVersion());
  }

  @Test
  public void testCreateDatabaseWithReservedProperty() throws Exception {
    // Analysis rejects these; this guards against a request that bypassed it.
    for (String property : new String[] {"owner", "OWNER", "ownerType", "Location"}) {
      TIcebergDdlExecRequest request = createDbRequest(DB_NAME, /*ifNotExists=*/false);
      request.getDdl_params().getCreate_db_params().setProperties(
          ImmutableMap.of(property, "x"));
      ImpalaRuntimeException e = assertThrows(ImpalaRuntimeException.class,
          () -> IcebergDdlExecutor.execDdl(catalogManager_, request));
      assertTrue(e.getMessage(), e.getMessage().contains(
          "Database property '" + property + "' cannot be set"));
    }
    verify(restCatalog_, never()).createNamespace(anyString(), any());
  }

  @Test
  public void testCreateExistingDatabase() throws Exception {
    doThrow(new AlreadyExistsException("Namespace already exists: %s", DB_NAME))
        .when(restCatalog_).createNamespace(DB_NAME, ImmutableMap.of());

    TDdlExecResponse response = IcebergDdlExecutor.execDdl(
        catalogManager_, createDbRequest(DB_NAME, /*ifNotExists=*/true));
    assertEquals("Database already exists.", summary(response));

    ImpalaRuntimeException e = assertThrows(ImpalaRuntimeException.class,
        () -> IcebergDdlExecutor.execDdl(
            catalogManager_, createDbRequest(DB_NAME, /*ifNotExists=*/false)));
    assertTrue(e.getMessage(),
        e.getMessage().contains("Database already exists: " + DB_NAME));
  }

  @Test
  public void testUnknownCatalog() throws Exception {
    when(catalogManager_.getIcebergCatalog(CATALOG_NAME))
        .thenThrow(new ImpalaRuntimeException("Unknown Iceberg REST catalog: rest-1"));
    ImpalaRuntimeException e = assertThrows(ImpalaRuntimeException.class,
        () -> IcebergDdlExecutor.execDdl(
            catalogManager_, dropTableRequest(/*ifExists=*/true, /*purge=*/false)));
    assertTrue(e.getMessage(), e.getMessage().contains("Unknown Iceberg REST catalog"));
  }

  @Test
  public void testUnsupportedDdlType() throws Exception {
    TIcebergDdlExecRequest request = dropTableRequest(false, false);
    request.getDdl_params().setDdl_type(TDdlType.ALTER_TABLE);
    ImpalaRuntimeException e = assertThrows(ImpalaRuntimeException.class,
        () -> IcebergDdlExecutor.execDdl(catalogManager_, request));
    assertTrue(e.getMessage(), e.getMessage().contains("ALTER_TABLE is not supported"));
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

  private static TIcebergDdlExecRequest createDbRequest(String dbName,
      boolean ifNotExists) {
    TCreateDbParams params = new TCreateDbParams();
    params.setDb(dbName);
    params.setIf_not_exists(ifNotExists);
    TDdlExecRequest ddl = new TDdlExecRequest();
    ddl.setDdl_type(TDdlType.CREATE_DATABASE);
    ddl.setCreate_db_params(params);
    ddl.setQuery_options(new TDdlQueryOptions(false));
    TIcebergDdlExecRequest request = new TIcebergDdlExecRequest();
    request.setRest_catalog_name(CATALOG_NAME);
    request.setDdl_params(ddl);
    return request;
  }

  private static TIcebergDdlExecRequest dropDbRequest(String dbName, boolean ifExists,
      boolean cascade) {
    TDropDbParams params = new TDropDbParams();
    params.setDb(dbName);
    params.setIf_exists(ifExists);
    params.setCascade(cascade);
    TDdlExecRequest ddl = new TDdlExecRequest();
    ddl.setDdl_type(TDdlType.DROP_DATABASE);
    ddl.setDrop_db_params(params);
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
