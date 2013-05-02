// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.hive.service.cli.thrift.TGetCatalogsReq;
import org.apache.hive.service.cli.thrift.TGetColumnsReq;
import org.apache.hive.service.cli.thrift.TGetFunctionsReq;
import org.apache.hive.service.cli.thrift.TGetInfoReq;
import org.apache.hive.service.cli.thrift.TGetSchemasReq;
import org.apache.hive.service.cli.thrift.TGetTablesReq;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cloudera.impala.authorization.AuthorizationConfig;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.thrift.TMetadataOpRequest;
import com.cloudera.impala.thrift.TMetadataOpResponse;
import com.cloudera.impala.thrift.TMetadataOpcode;
import com.cloudera.impala.thrift.TResultRow;
import com.google.common.collect.Lists;

/**
 * Unit test for Frontend.execHiveServer2MetadataOp, which executes a HiveServer2
 * metadata operation and returns a TMetadataOpResponse (which contains a result set and
 * the schema of the result set). HiveServer2 API specifies the result set schema for
 * each of the operation. Each test will validate the result set schema and the expected
 * result set.
 *
 */
public class FrontendTest {
  private static Frontend fe = new Frontend(true,
      AuthorizationConfig.createAuthDisabledConfig());

  @BeforeClass
  public static void setUp() throws Exception {
    fe = new Frontend(true, AuthorizationConfig.createAuthDisabledConfig());
  }

  @AfterClass
  public static void cleanUp() {
    fe.close();
  }

  @Test
  public void TestGetTypeInfo() throws ImpalaException {
    // Verify that the correct number of types are returned.
    TMetadataOpRequest getInfoReq = new TMetadataOpRequest();
    getInfoReq.opcode = TMetadataOpcode.GET_TYPE_INFO;
    getInfoReq.get_info_req = new TGetInfoReq();
    TMetadataOpResponse resp = execMetadataOp(getInfoReq);
    // DatabaseMetaData.getTypeInfo has 18 columns.
    assertEquals(18, resp.result_set_metadata.columnDescs.size());
    assertEquals(18, resp.results.get(0).colVals.size());
    // All primitives types, except INVALID_TYPE, DATE and DATETIME, should be returned.
    // Therefore #supported types =  PrimitiveType.values().length - 3.
    assertEquals(PrimitiveType.values().length - 3, resp.results.size());
  }

  @Test
  public void TestGetSchema() throws ImpalaException {
    // "default%" should return schema "default"
    TMetadataOpRequest req = new TMetadataOpRequest();
    req.opcode = TMetadataOpcode.GET_SCHEMAS;
    req.get_schemas_req = new TGetSchemasReq();
    req.get_schemas_req.setSchemaName("default%");
    TMetadataOpResponse resp = execMetadataOp(req);
    // HiveServer2 GetSchemas has 2 columns.
    assertEquals(2, resp.result_set_metadata.columnDescs.size());
    assertEquals(2, resp.results.get(0).colVals.size());
    assertEquals(1, resp.results.size());
    assertEquals("default", resp.results.get(0).colVals.get(0).stringVal.toLowerCase());
  }

  @Test
  public void TestGetTables() throws ImpalaException {
    // It should return two tables: one in "default"db and one in "testdb1".
    TMetadataOpRequest req = new TMetadataOpRequest();
    req.opcode = TMetadataOpcode.GET_TABLES;
    req.get_tables_req = new TGetTablesReq();
    req.get_tables_req.setSchemaName("functional");
    req.get_tables_req.setTableName("all_ypes");
    TMetadataOpResponse resp = execMetadataOp(req);
    // HiveServer2 GetTables has 5 columns.
    assertEquals(5, resp.result_set_metadata.columnDescs.size());
    assertEquals(5, resp.results.get(0).colVals.size());
    assertEquals(1, resp.results.size());
    assertEquals("alltypes", resp.results.get(0).colVals.get(2).stringVal.toLowerCase());
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
    TMetadataOpResponse resp = execMetadataOp(req);
    // TODO: HiveServer2 thrift says the result set columns should be the
    // same as ODBC SQLColumns, which has 18 columns. But the HS2 implementation has
    // 23 columns. Follow the HS2 implementation for now because the HS2 thrift comment
    // could be outdated.
    assertEquals(23, resp.result_set_metadata.columnDescs.size());
    assertEquals(23, resp.results.get(0).colVals.size());
    assertEquals(1, resp.results.size());
    TResultRow row = resp.results.get(0);
    assertEquals("functional", row.colVals.get(1).stringVal.toLowerCase());
    assertEquals("alltypes", row.colVals.get(2).stringVal.toLowerCase());
    assertEquals("string_col", row.colVals.get(3).stringVal.toLowerCase());
  }

  @Test
  public void TestGetCatalogs() throws ImpalaException {
    // Hive/Impala does not have catalog concept. Should return zero rows.
    TMetadataOpRequest req = new TMetadataOpRequest();
    req.opcode = TMetadataOpcode.GET_CATALOGS;
    req.get_catalogs_req = new TGetCatalogsReq();
    TMetadataOpResponse resp = execMetadataOp(req);

    // HiveServer2 GetCatalogs() has 1 column.
    assertEquals(1, resp.result_set_metadata.columnDescs.size());
    assertEquals(0, resp.results.size());
  }

  @Test
  public void TestGetTableTypes() throws ImpalaException {
    // Impala should only return TABLE as the only table type.
    TMetadataOpRequest req = new TMetadataOpRequest();
    req.opcode = TMetadataOpcode.GET_TABLE_TYPES;
    TMetadataOpResponse resp = execMetadataOp(req);
    // HiveServer2 GetTableTypes() has 1 column.
    assertEquals(1, resp.result_set_metadata.columnDescs.size());
    assertEquals(1, resp.results.get(0).colVals.size());
    assertEquals(1, resp.results.size());
    assertEquals("TABLE", resp.results.get(0).getColVals().get(0).stringVal);
  }

  @Test
  public void TestGetFunctions() throws ImpalaException {
    // "sub%" should return functions subdate, substr and substring.
    TMetadataOpRequest req = new TMetadataOpRequest();
    req.opcode = TMetadataOpcode.GET_FUNCTIONS;
    req.get_functions_req = new TGetFunctionsReq();
    req.get_functions_req.setFunctionName("sub%");
    TMetadataOpResponse resp = execMetadataOp(req);

    // HiveServer2 GetFunctions() has 6 columns.
    assertEquals(6, resp.result_set_metadata.columnDescs.size());
    assertEquals(6, resp.results.get(0).colVals.size());

    assertEquals(3, resp.results.size());

    List<String> expectedResult = Lists.newArrayList();
    expectedResult.add("subdate");
    expectedResult.add("substr");
    expectedResult.add("substring");
    for (TResultRow row: resp.results) {
      String fn = row.colVals.get(2).stringVal.toLowerCase();
      assertTrue(fn + " not found", expectedResult.remove(fn));
    }
  }

  private TMetadataOpResponse execMetadataOp(TMetadataOpRequest req)
      throws ImpalaException {
    return fe.execHiveServer2MetadataOp(req);
  }
}
