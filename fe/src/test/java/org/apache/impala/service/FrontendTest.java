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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hive.service.rpc.thrift.TGetCatalogsReq;
import org.apache.hive.service.rpc.thrift.TGetColumnsReq;
import org.apache.hive.service.rpc.thrift.TGetFunctionsReq;
import org.apache.hive.service.rpc.thrift.TGetInfoReq;
import org.apache.hive.service.rpc.thrift.TGetSchemasReq;
import org.apache.hive.service.rpc.thrift.TGetTablesReq;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.testutil.TestUtils;
import org.apache.impala.thrift.TMetadataOpRequest;
import org.apache.impala.thrift.TMetadataOpcode;
import org.apache.impala.thrift.TResultRow;
import org.apache.impala.thrift.TResultSet;
import org.junit.Assume;
import org.junit.Test;

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
public class FrontendTest extends FrontendTestBase {

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
    // Validate the supported types
    List<String> supportedTypes = new ArrayList<>();
    for (ScalarType stype : Type.getSupportedTypes()) {
      if (stype.isSupported() && !stype.isInternalType()) {
        supportedTypes.add(stype.getPrimitiveType().name());
      }
    }
    supportedTypes.add("ARRAY");
    supportedTypes.add("MAP");
    supportedTypes.add("STRUCT");
    assertEquals(supportedTypes.size(), resp.rows.size());
    for (TResultRow row : resp.rows) {
      boolean foundType = false;
      String typeName = row.colVals.get(0).getString_val();
      if (supportedTypes.contains(typeName)) {
        supportedTypes.remove(typeName);
        foundType = true;
      }
      assertTrue(foundType);
    }
    assertEquals(supportedTypes.size(), 0);
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
  public void TestGetTablesTypeTable() throws ImpalaException {
    // Make sure these views are loaded so they can be distinguished from tables.
    AnalyzesOk("select * from functional.alltypes_hive_view");
    AnalyzesOk("select * from functional.alltypes_parens");
    AnalyzesOk("select * from functional.alltypes_view");
    AnalyzesOk("select * from functional.alltypes_view_sub");
    AnalyzesOk("select * from functional.alltypes_dp_2_view_1");
    AnalyzesOk("select * from functional.alltypes_dp_2_view_2");

    TMetadataOpRequest req = new TMetadataOpRequest();
    req.opcode = TMetadataOpcode.GET_TABLES;
    req.get_tables_req = new TGetTablesReq();
    req.get_tables_req.setSchemaName("functional");
    req.get_tables_req.setTableName("alltypes\\_%");
    req.get_tables_req.setTableTypes(Arrays.asList("TABLE"));
    TResultSet resp = execMetadataOp(req);
    // HiveServer2 GetTables has 5 columns.
    assertEquals(5, resp.schema.columns.size());
    assertEquals(5, resp.rows.get(0).colVals.size());
    assertEquals(6, resp.rows.size());
    assertEquals("alltypes_datasource",
        resp.rows.get(0).colVals.get(2).string_val.toLowerCase());
    assertEquals("alltypes_date_partition",
        resp.rows.get(1).colVals.get(2).string_val.toLowerCase());
    assertEquals("alltypes_date_partition_2",
        resp.rows.get(2).colVals.get(2).string_val.toLowerCase());
    assertEquals("alltypes_jdbc_datasource",
        resp.rows.get(3).colVals.get(2).string_val.toLowerCase());
    assertEquals("alltypes_jdbc_datasource_2",
        resp.rows.get(4).colVals.get(2).string_val.toLowerCase());
    assertEquals("alltypes_with_date",
        resp.rows.get(5).colVals.get(2).string_val.toLowerCase());
  }

  @Test
  public void TestGetTablesTypeView() throws ImpalaException {
    // Make sure these views are loaded so they can be distinguished from tables.
    AnalyzesOk("select * from functional.alltypes_hive_view");
    AnalyzesOk("select * from functional.alltypes_parens");
    AnalyzesOk("select * from functional.alltypes_view");
    AnalyzesOk("select * from functional.alltypes_view_sub");
    AnalyzesOk("select * from functional.alltypes_dp_2_view_1");
    AnalyzesOk("select * from functional.alltypes_dp_2_view_2");

    TMetadataOpRequest req = new TMetadataOpRequest();
    req.opcode = TMetadataOpcode.GET_TABLES;
    req.get_tables_req = new TGetTablesReq();
    req.get_tables_req.setSchemaName("functional");
    req.get_tables_req.setTableName("alltypes%");
    req.get_tables_req.setTableTypes(Arrays.asList("VIEW"));
    TResultSet resp = execMetadataOp(req);
    // HiveServer2 GetTables has 5 columns.
    assertEquals(5, resp.schema.columns.size());
    assertEquals(5, resp.rows.get(0).colVals.size());
    assertEquals(6, resp.rows.size());
    assertEquals("alltypes_dp_2_view_1",
        resp.rows.get(0).colVals.get(2).string_val.toLowerCase());
    assertEquals("alltypes_dp_2_view_2",
        resp.rows.get(1).colVals.get(2).string_val.toLowerCase());
    assertEquals("alltypes_hive_view",
        resp.rows.get(2).colVals.get(2).string_val.toLowerCase());
    assertEquals("alltypes_parens",
        resp.rows.get(3).colVals.get(2).string_val.toLowerCase());
    assertEquals("alltypes_view",
        resp.rows.get(4).colVals.get(2).string_val.toLowerCase());
    assertEquals("alltypes_view_sub",
        resp.rows.get(5).colVals.get(2).string_val.toLowerCase());
  }

  @Test
  public void TestGetTablesWithComments() throws ImpalaException {
    // Add test db and test tables with comments
    final String dbName = "tbls_with_comments_test_db";
    Db testDb = addTestDb(dbName, "Stores tables with comments");
    assertNotNull(testDb);
    final String tableComment = "this table has a comment";
    final String columnComment = "this column has a comment";
    final String columnWithCommentName = "column_with_comment";
    final String columnWithoutCommentName = "column_without_comment";
    final String tableWithCommentsStmt = String.format(
        "create table %s.tbl_with_comments (%s int comment '%s', %s int) comment '%s'",
         dbName, columnWithCommentName, columnComment, columnWithoutCommentName,
         tableComment);
    Table tbl = addTestTable(tableWithCommentsStmt);
    assertNotNull(tbl);
    final String tableWithoutCommentsStmt = String.format(
        "create table %s.tbl_without_comments (a int)", dbName);
    tbl = addTestTable(tableWithoutCommentsStmt);
    assertNotNull(tbl);

    // Prepare and perform the GetTables request
    TMetadataOpRequest req = new TMetadataOpRequest();
    req.opcode = TMetadataOpcode.GET_TABLES;
    req.get_tables_req = new TGetTablesReq();
    req.get_tables_req.setSchemaName(dbName);
    TResultSet resp = execMetadataOp(req);
    assertEquals(2, resp.rows.size());
    for (TResultRow row: resp.rows) {
      if (row.colVals.get(2).string_val.toLowerCase().equals("tbl_with_comments")) {
        assertEquals(tableComment, row.colVals.get(4).string_val.toLowerCase());
      } else {
        assertEquals("", row.colVals.get(4).string_val);
      }
    }

    // Test column comments
    req = new TMetadataOpRequest();
    req.opcode = TMetadataOpcode.GET_COLUMNS;
    req.get_columns_req = new TGetColumnsReq();
    req.get_columns_req.setSchemaName(dbName);
    req.get_columns_req.setTableName("tbl_with_comments");
    resp = execMetadataOp(req);
    assertEquals(2, resp.rows.size());
    for (TResultRow row: resp.rows) {
      if (row.colVals.get(3).string_val.equals(columnWithCommentName)) {
        assertEquals(columnComment, row.colVals.get(11).string_val);
      } else {
        assertEquals(null, row.colVals.get(11).string_val);
      }
    }
  }

  @Test
  public void TestGetTablesWithCommentsOnHive2() throws ImpalaException {
    // run the test only when it is running against Hive-2 since index tables are
    // skipped during data-load against Hive-3
    Assume.assumeTrue(
        "Skipping this test since it is only supported when running against Hive-2",
        TestUtils.getHiveMajorVersion() == 2);

    // IMPALA-5579: GetTables() should succeed and display the available information for
    // tables that cannot be loaded.
    TMetadataOpRequest req = new TMetadataOpRequest();
    req = new TMetadataOpRequest();
    req.opcode = TMetadataOpcode.GET_TABLES;
    req.get_tables_req = new TGetTablesReq();
    req.get_tables_req.setSchemaName("functional");
    req.get_tables_req.setTableName("hive_index_tbl");
    TResultSet resp = execMetadataOp(req);
    assertEquals(1, resp.rows.size());
  }

  @Test
  public void TestUnloadedView() throws ImpalaException {
    final String dbName = "tbls_for_views_test_db";
    Db testDb = addTestDb(dbName, "Stores views");
    assertNotNull(testDb);
    Table view = addTestView(String.format(
        "create view %s.test_view as select * from functional.alltypes", dbName));
    assertNotNull(view);

    // Prepare and perform the GetTables request
    TMetadataOpRequest req = new TMetadataOpRequest();
    req.opcode = TMetadataOpcode.GET_TABLES;
    req.get_tables_req = new TGetTablesReq();
    req.get_tables_req.setSchemaName(dbName);
    TResultSet resp = execMetadataOp(req);
    assertEquals(1, resp.rows.size());
    for (TResultRow row : resp.rows) {
      assertEquals(row.colVals.get(2).string_val.toLowerCase(), "test_view");
      assertEquals("table", row.colVals.get(3).string_val.toLowerCase());
    }
  }

  @Test
  public void TestLoadedView() throws ImpalaException {
    // Issue the statement to make sure the view is loaded
    AnalyzesOk("select * from functional.alltypes_view");
    // Prepare and perform the GetTables request
    TMetadataOpRequest req = new TMetadataOpRequest();
    req.opcode = TMetadataOpcode.GET_TABLES;
    req.get_tables_req = new TGetTablesReq();
    req.get_tables_req.setSchemaName("functional");
    req.get_tables_req.setTableName("alltypes_view");

    TResultSet resp = execMetadataOp(req);
    assertEquals(1, resp.rows.size());
    for (TResultRow row : resp.rows) {
      assertEquals("VIEW", row.colVals.get(3).string_val);
    }
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
    TMetadataOpRequest req = new TMetadataOpRequest();
    req.opcode = TMetadataOpcode.GET_TABLE_TYPES;
    TResultSet resp = execMetadataOp(req);
    // HiveServer2 GetTableTypes() has 1 column.
    assertEquals(2, resp.rows.size());
    assertEquals(1, resp.schema.columns.size());
    assertEquals(1, resp.rows.get(0).colVals.size());
    assertEquals("TABLE", resp.rows.get(0).getColVals().get(0).string_val);
    assertEquals("VIEW", resp.rows.get(1).getColVals().get(0).string_val);
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
    return frontend_.execHiveServer2MetadataOp(req);
  }
}
