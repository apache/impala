// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.Set;

import org.apache.hive.service.cli.thrift.TGetCatalogsReq;
import org.apache.hive.service.cli.thrift.TGetColumnsReq;
import org.apache.hive.service.cli.thrift.TGetFunctionsReq;
import org.apache.hive.service.cli.thrift.TGetInfoReq;
import org.apache.hive.service.cli.thrift.TGetSchemasReq;
import org.apache.hive.service.cli.thrift.TGetTablesReq;
import org.junit.Test;

import com.cloudera.impala.authorization.AuthorizationConfig;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.testutil.ImpaladTestCatalog;
import com.cloudera.impala.testutil.TestUtils;
import com.cloudera.impala.thrift.TMetadataOpRequest;
import com.cloudera.impala.thrift.TMetadataOpcode;
import com.cloudera.impala.thrift.TQueryCtx;
import com.cloudera.impala.thrift.TResultRow;
import com.cloudera.impala.thrift.TResultSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Unit test for Frontend.execHiveServer2MetadataOp, which executes a HiveServer2
 * metadata operation and returns a TMetadataOpResponse (which contains a result set and
 * the schema of the result set). HiveServer2 API specifies the result set schema for
 * each of the operation. Each test will validate the result set schema and the expected
 * result set.
 *
 */
public class FrontendTest {
  private static Frontend fe_ = new Frontend(
      AuthorizationConfig.createAuthDisabledConfig(), new ImpaladTestCatalog());

  @Test
  public void TestCatalogNotReady() throws ImpalaException {
    Frontend fe = new Frontend(AuthorizationConfig.createAuthDisabledConfig());
    TQueryCtx queryCtx = TestUtils.createQueryContext("default", "fake_user");

    // Queries that do not touch catalog objects should succeed.
    queryCtx.request.setStmt("select 1");
    fe.createExecRequest(queryCtx, new StringBuilder());

    // A query that touches a catalog object should fail.
    queryCtx.request.setStmt("show tables");
    try {
      fe.createExecRequest(queryCtx, new StringBuilder());
      fail("Expected failure to due uninitialized catalog.");
    } catch (AnalysisException e) {
      assertEquals("This Impala daemon is not ready to accept user requests. " +
          "Status: Waiting for catalog update from the StateStore.", e.getMessage());
    }
  }

  @Test
  public void TestGetTypeInfo() throws ImpalaException {
    // Verify that the correct number of types are returned.
    TMetadataOpRequest getInfoReq = new TMetadataOpRequest();
    getInfoReq.opcode = TMetadataOpcode.GET_TYPE_INFO;
    getInfoReq.get_info_req = new TGetInfoReq();
    TResultSet resp = execMetadataOp(getInfoReq);
    // DatabaseMetaData.getTypeInfo has 18 columns.
    assertEquals(18, resp.schema.columns.size());
    assertEquals(18, resp.rows.get(0).colVals.size());
    // All primitives types, except INVALID_TYPE, DATE, DATETIME, DECIMAL, CHAR,
    // and VARCHAR should be returned.
    // Therefore #supported types =  PrimitiveType.values().length - 6.
    assertEquals(PrimitiveType.values().length - 6, resp.rows.size());
  }

  @Test
  public void TestGetSchema() throws ImpalaException {
    // "default%" should return schema "default"
    TMetadataOpRequest req = new TMetadataOpRequest();
    req.opcode = TMetadataOpcode.GET_SCHEMAS;
    req.get_schemas_req = new TGetSchemasReq();
    req.get_schemas_req.setSchemaName("default%");
    TResultSet resp = execMetadataOp(req);
    // HiveServer2 GetSchemas has 2 columns.
    assertEquals(2, resp.schema.columns.size());
    assertEquals(2, resp.rows.get(0).colVals.size());
    assertEquals(1, resp.rows.size());
    assertEquals("default", resp.rows.get(0).colVals.get(0).string_val.toLowerCase());
  }

  @Test
  public void TestGetTables() throws ImpalaException {
    // It should return two tables: one in "default"db and one in "testdb1".
    TMetadataOpRequest req = new TMetadataOpRequest();
    req.opcode = TMetadataOpcode.GET_TABLES;
    req.get_tables_req = new TGetTablesReq();
    req.get_tables_req.setSchemaName("functional");
    req.get_tables_req.setTableName("all_ypes");
    TResultSet resp = execMetadataOp(req);
    // HiveServer2 GetTables has 5 columns.
    assertEquals(5, resp.schema.columns.size());
    assertEquals(5, resp.rows.get(0).colVals.size());
    assertEquals(1, resp.rows.size());
    assertEquals("alltypes", resp.rows.get(0).colVals.get(2).string_val.toLowerCase());
  }

  @Test
  public void TestGetColumns() throws ImpalaException {
    // It should return one column: default.alltypes.string_col
    TMetadataOpRequest req = new TMetadataOpRequest();
    req.opcode = TMetadataOpcode.GET_COLUMNS;
    req.get_columns_req = new TGetColumnsReq();
    req.get_columns_req.setSchemaName("functional");
    req.get_columns_req.setTableName("alltypes");
    req.get_columns_req.setColumnName("stri%");
    TResultSet resp = execMetadataOp(req);
    // TODO: HiveServer2 thrift says the result set columns should be the
    // same as ODBC SQLColumns, which has 18 columns. But the HS2 implementation has
    // 23 columns. Follow the HS2 implementation for now because the HS2 thrift comment
    // could be outdated.
    assertEquals(23, resp.schema.columns.size());
    assertEquals(23, resp.rows.get(0).colVals.size());
    assertEquals(1, resp.rows.size());
    TResultRow row = resp.rows.get(0);
    assertEquals("functional", row.colVals.get(1).string_val.toLowerCase());
    assertEquals("alltypes", row.colVals.get(2).string_val.toLowerCase());
    assertEquals("string_col", row.colVals.get(3).string_val.toLowerCase());
  }

  @Test
  public void TestGetCatalogs() throws ImpalaException {
    // Hive/Impala does not have catalog concept. Should return zero rows.
    TMetadataOpRequest req = new TMetadataOpRequest();
    req.opcode = TMetadataOpcode.GET_CATALOGS;
    req.get_catalogs_req = new TGetCatalogsReq();
    TResultSet resp = execMetadataOp(req);

    // HiveServer2 GetCatalogs() has 1 column.
    assertEquals(1, resp.schema.columns.size());
    assertEquals(0, resp.rows.size());
  }

  @Test
  public void TestGetTableTypes() throws ImpalaException {
    // Impala should only return TABLE as the only table type.
    TMetadataOpRequest req = new TMetadataOpRequest();
    req.opcode = TMetadataOpcode.GET_TABLE_TYPES;
    TResultSet resp = execMetadataOp(req);
    // HiveServer2 GetTableTypes() has 1 column.
    assertEquals(1, resp.schema.columns.size());
    assertEquals(1, resp.rows.get(0).colVals.size());
    assertEquals(1, resp.rows.size());
    assertEquals("TABLE", resp.rows.get(0).getColVals().get(0).string_val);
  }

  @Test
  public void TestGetFunctions() throws ImpalaException {
    // "sub%" should return functions subdate, substr and substring.
    TMetadataOpRequest req = new TMetadataOpRequest();
    req.opcode = TMetadataOpcode.GET_FUNCTIONS;
    req.get_functions_req = new TGetFunctionsReq();
    req.get_functions_req.setFunctionName("sub%");
    TResultSet resp = execMetadataOp(req);

    // HiveServer2 GetFunctions() has 6 columns.
    assertEquals(6, resp.schema.columns.size());
    assertEquals(6, resp.rows.get(0).colVals.size());

    Set<String> fns = Sets.newHashSet();
    for (TResultRow row: resp.rows) {
      String fn = row.colVals.get(2).string_val.toLowerCase();
      fns.add(fn);
    }
    assertEquals(3, fns.size());

    List<String> expectedResult = Lists.newArrayList();
    expectedResult.add("subdate");
    expectedResult.add("substr");
    expectedResult.add("substring");
    for (String fn: fns) {
      assertTrue(fn + " not found", expectedResult.remove(fn));
    }
  }

  private TResultSet execMetadataOp(TMetadataOpRequest req)
      throws ImpalaException {
    return fe_.execHiveServer2MetadataOp(req);
  }
}
