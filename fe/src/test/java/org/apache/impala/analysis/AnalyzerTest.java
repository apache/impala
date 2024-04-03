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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.util.FunctionUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import static org.junit.Assert.assertEquals;

public class AnalyzerTest extends FrontendTestBase {
  protected final static Logger LOG = LoggerFactory.getLogger(AnalyzerTest.class);

  // maps from type to string that will result in literal of that type
  protected static Map<ScalarType, String> typeToLiteralValue_ =
      new HashMap<>();
  static {
    typeToLiteralValue_.put(Type.BOOLEAN, "true");
    typeToLiteralValue_.put(Type.TINYINT, "1");
    typeToLiteralValue_.put(Type.SMALLINT, (Byte.MAX_VALUE + 1) + "");
    typeToLiteralValue_.put(Type.INT, (Short.MAX_VALUE + 1) + "");
    typeToLiteralValue_.put(Type.BIGINT,
        ((long) Integer.MAX_VALUE + 1) + "");
    typeToLiteralValue_.put(Type.FLOAT, "cast(1.0 as float)");
    typeToLiteralValue_.put(Type.DOUBLE,
        "cast(" + (Float.MAX_VALUE + 1) + " as double)");
    typeToLiteralValue_.put(Type.TIMESTAMP,
        "cast('2012-12-21 00:00:00.000' as timestamp)");
    typeToLiteralValue_.put(Type.DATE, "cast('2012-12-21' as date)");
    typeToLiteralValue_.put(Type.STRING, "'Hello, World!'");
    typeToLiteralValue_.put(Type.NULL, "NULL");
  }

  @Before
  public void setUpTest() throws Exception {
    // Reset the RuntimeEnv - individual tests may change it.
    RuntimeEnv.INSTANCE.reset();
    RuntimeEnv.INSTANCE.setTestEnv(true);
  }

  @AfterClass
  public static void cleanUpClass() throws Exception {
    // Reset the RuntimeEnv - individual tests may have changed it.
    RuntimeEnv.INSTANCE.reset();
  }

  /**
   * Generates and analyzes two variants of the given query by replacing all occurrences
   * of "$TBL" in the query string with the unqualified and fully-qualified version of
   * the given table name. The unqualified variant is analyzed using an analyzer that has
   * tbl's db set as the default database.
   * Example:
   * query = "select id from $TBL, $TBL"
   * tbl = "functional.alltypes"
   * Variants generated and analyzed:
   * select id from alltypes, alltypes (default db is "functional")
   * select id from functional.alltypes, functional.alltypes (default db is "default")
   */
  protected void TblsAnalyzeOk(String query, TableName tbl) {
    Preconditions.checkState(tbl.isFullyQualified());
    Preconditions.checkState(query.contains("$TBL"));
    String uqQuery = query.replace("$TBL", tbl.getTbl());
    AnalyzesOk(uqQuery, createAnalysisCtx(tbl.getDb()));
    String fqQuery = query.replace("$TBL", tbl.toString());
    AnalyzesOk(fqQuery);
  }

  /**
   * Same as TblsAnalyzeOk(), except that analysis of all variants is expected
   * to fail with the given error message.
   */
  protected void TblsAnalysisError(String query, TableName tbl,
      String expectedError) {
    Preconditions.checkState(tbl.isFullyQualified());
    Preconditions.checkState(query.contains("$TBL"));
    String uqQuery = query.replace("$TBL", tbl.getTbl());
    AnalysisError(uqQuery, createAnalysisCtx(tbl.getDb()), expectedError);
    String fqQuery = query.replace("$TBL", tbl.toString());
    AnalysisError(fqQuery, expectedError);
  }

  @Test
  public void TestCompressedText() throws AnalysisException {
    AnalyzesOk("SELECT count(*) FROM functional_text_bzip.tinyinttable");
    AnalyzesOk("SELECT count(*) FROM functional_text_def.tinyinttable");
    AnalyzesOk("SELECT count(*) FROM functional_text_gzip.tinyinttable");
    AnalyzesOk("SELECT count(*) FROM functional_text_snap.tinyinttable");
  }

  @Test
  public void TestMemLayout() throws AnalysisException {
    testSelectStar();
    testNonNullable();
    testMixedNullable();
    testNonMaterializedSlots();
  }

  private void testSelectStar() throws AnalysisException {
    SelectStmt stmt = (SelectStmt) AnalyzesOk(
        "select * from functional.AllTypes, functional.date_tbl");
    Analyzer analyzer = stmt.getAnalyzer();
    DescriptorTable descTbl = analyzer.getDescTbl();
    TupleDescriptor tupleDesc = descTbl.getTupleDesc(new TupleId(0));
    tupleDesc.materializeSlots();
    TupleDescriptor dateTblTupleDesc = descTbl.getTupleDesc(new TupleId(1));
    dateTblTupleDesc.materializeSlots();
    descTbl.computeMemLayout();

    assertEquals(89.0f, tupleDesc.getAvgSerializedSize(), 0.0);
    checkLayoutParams("functional.alltypes.timestamp_col", 16, 0, 80, 0, analyzer);
    checkLayoutParams("functional.alltypes.date_string_col", 12, 16, 80, 1, analyzer);
    checkLayoutParams("functional.alltypes.string_col", 12, 28, 80, 2, analyzer);
    checkLayoutParams("functional.alltypes.bigint_col", 8, 40, 80, 3, analyzer);
    checkLayoutParams("functional.alltypes.double_col", 8, 48, 80, 4, analyzer);
    checkLayoutParams("functional.alltypes.id", 4, 56, 80, 5, analyzer);
    checkLayoutParams("functional.alltypes.int_col", 4, 60, 80, 6, analyzer);
    checkLayoutParams("functional.alltypes.float_col", 4, 64, 80, 7, analyzer);
    checkLayoutParams("functional.alltypes.year", 4, 68, 81, 0, analyzer);
    checkLayoutParams("functional.alltypes.month", 4, 72, 81, 1, analyzer);
    checkLayoutParams("functional.alltypes.smallint_col", 2, 76, 81, 2, analyzer);
    checkLayoutParams("functional.alltypes.bool_col", 1, 78, 81, 3, analyzer);
    checkLayoutParams("functional.alltypes.tinyint_col", 1, 79, 81, 4, analyzer);

    Assert.assertEquals(12, dateTblTupleDesc.getAvgSerializedSize(), 0.0);
    checkLayoutParams("functional.date_tbl.id_col", 4, 0, 12, 0, analyzer);
    checkLayoutParams("functional.date_tbl.date_col", 4, 4, 12, 1, analyzer);
    checkLayoutParams("functional.date_tbl.date_part", 4, 8, 12, 2, analyzer);
  }

  private void testNonNullable() throws AnalysisException {
    // both slots are non-nullable bigints. The layout should look like:
    // (byte range : data)
    // 0 - 7: count(int_col)
    // 8 - 15: count(*)
    SelectStmt stmt = (SelectStmt) AnalyzesOk(
        "select count(int_col), count(*) from functional.AllTypes");
    DescriptorTable descTbl = stmt.getAnalyzer().getDescTbl();
    TupleDescriptor aggDesc = descTbl.getTupleDesc(new TupleId(1));
    aggDesc.materializeSlots();
    descTbl.computeMemLayout();
    assertEquals(16.0f, aggDesc.getAvgSerializedSize(), 0.0);
    assertEquals(16, aggDesc.getByteSize());
    checkLayoutParams(aggDesc.getSlots().get(0), 8, 0, 0, -1);
    checkLayoutParams(aggDesc.getSlots().get(1), 8, 8, 0, -1);
  }

  private void testMixedNullable() throws AnalysisException {
    // one slot is nullable, one is not. The layout should look like:
    // (byte range : data)
    // 0 - 7: sum(int_col)
    // 8 - 15: count(*)
    // 16 - 17: nullable-byte (only 1 bit used)
    SelectStmt stmt = (SelectStmt) AnalyzesOk(
        "select sum(int_col), count(*) from functional.AllTypes");
    DescriptorTable descTbl = stmt.getAnalyzer().getDescTbl();
    TupleDescriptor aggDesc = descTbl.getTupleDesc(new TupleId(1));
    aggDesc.materializeSlots();
    descTbl.computeMemLayout();
    assertEquals(16.0f, aggDesc.getAvgSerializedSize(), 0.0);
    assertEquals(17, aggDesc.getByteSize());
    checkLayoutParams(aggDesc.getSlots().get(0), 8, 0, 16, 0);
    checkLayoutParams(aggDesc.getSlots().get(1), 8, 8, 0, -1);
  }

  /**
   * Tests that computeMemLayout() ignores non-materialized slots.
   */
  private void testNonMaterializedSlots() throws AnalysisException {
    SelectStmt stmt = (SelectStmt) AnalyzesOk(
        "select * from functional.alltypes, functional.date_tbl");
    Analyzer analyzer = stmt.getAnalyzer();
    DescriptorTable descTbl = analyzer.getDescTbl();
    TupleDescriptor tupleDesc = descTbl.getTupleDesc(new TupleId(0));
    tupleDesc.materializeSlots();
    // Mark slots 0 (id), 7 (double_col), 9 (string_col) as non-materialized.
    List<SlotDescriptor> slots = tupleDesc.getSlots();
    slots.get(0).setIsMaterialized(false);
    slots.get(7).setIsMaterialized(false);
    slots.get(9).setIsMaterialized(false);

    TupleDescriptor dateTblTupleDesc = descTbl.getTupleDesc(new TupleId(1));
    dateTblTupleDesc.materializeSlots();
    // Mark slots 0 and 1 (id_col and date_col) non-materialized.
    slots = dateTblTupleDesc.getSlots();
    slots.get(0).setIsMaterialized(false);
    slots.get(1).setIsMaterialized(false);

    descTbl.computeMemLayout();

    assertEquals(64.0f, tupleDesc.getAvgSerializedSize(), 0.0);
    // Check non-materialized slots.
    checkLayoutParams("functional.alltypes.id", 0, -1, 0, 0, analyzer);
    checkLayoutParams("functional.alltypes.double_col", 0, -1, 0, 0, analyzer);
    checkLayoutParams("functional.alltypes.string_col", 0, -1, 0, 0, analyzer);
    // Check materialized slots.
    checkLayoutParams("functional.alltypes.timestamp_col", 16, 0, 56, 0, analyzer);
    checkLayoutParams("functional.alltypes.date_string_col", 12, 16, 56, 1, analyzer);
    checkLayoutParams("functional.alltypes.bigint_col", 8, 28, 56, 2, analyzer);
    checkLayoutParams("functional.alltypes.int_col", 4, 36, 56, 3, analyzer);
    checkLayoutParams("functional.alltypes.float_col", 4, 40, 56, 4, analyzer);
    checkLayoutParams("functional.alltypes.year", 4, 44, 56, 5, analyzer);
    checkLayoutParams("functional.alltypes.month", 4, 48, 56, 6, analyzer);
    checkLayoutParams("functional.alltypes.smallint_col", 2, 52, 56, 7, analyzer);
    checkLayoutParams("functional.alltypes.bool_col", 1, 54, 57, 0, analyzer);
    checkLayoutParams("functional.alltypes.tinyint_col", 1, 55, 57, 1, analyzer);

    Assert.assertEquals(4, dateTblTupleDesc.getAvgSerializedSize(), 0.0);
    // Non-materialized slots.
    checkLayoutParams("functional.date_tbl.id_col", 0, -1, 0, 0, analyzer);
    checkLayoutParams("functional.date_tbl.date_col", 0, -1, 0, 0, analyzer);
    // Materialized slot.
    checkLayoutParams("functional.date_tbl.date_part", 4, 0, 4, 0, analyzer);
  }

  private void checkLayoutParams(SlotDescriptor d, int byteSize, int byteOffset,
      int nullIndicatorByte, int nullIndicatorBit) {
    assertEquals(byteSize, d.getByteSize());
    assertEquals(byteOffset, d.getByteOffset());
    assertEquals(nullIndicatorByte, d.getNullIndicatorByte());
    assertEquals(nullIndicatorBit, d.getNullIndicatorBit());
  }

  private void checkLayoutParams(String colAlias, int byteSize, int byteOffset,
      int nullIndicatorByte, int nullIndicatorBit, Analyzer analyzer) {
    List<String> colAliasRawPath = Arrays.asList(colAlias.split("\\."));
    SlotDescriptor d = analyzer.getSlotDescriptor(colAliasRawPath);
    checkLayoutParams(d, byteSize, byteOffset, nullIndicatorByte, nullIndicatorBit);
  }

  // Analyzes query and asserts that the first result expr returns the given type.
  // Requires query to parse to a SelectStmt.
  protected void checkExprType(String query, Type type) {
    SelectStmt select = (SelectStmt) AnalyzesOk(query);
    assertEquals(select.getResultExprs().get(0).getType(), type);
  }

  /**
   * We distinguish between three classes of unsupported types:
   * 1. Complex types, e.g., map
   *    For tables with such types we prevent loading the table metadata.
   * 2. Primitive types
   *    For tables with unsupported primitive types (e.g. datetime)
   *    we can run queries as long as the unsupported columns are not referenced.
   *    We fail analysis if a query references an unsupported primitive column.
   * 3. Partition-column types
   *    We do not support table partitioning on timestamp columns
   */
  @Test
  public void TestUnsupportedTypes() {
    // With DATE and BINARY support Impala can now handle the same scalar types as Hive.
    // There is still some gap in supported types for partition columns.

    // Unsupported partition-column type.
    AnalysisError("select * from functional.unsupported_timestamp_partition",
        "Failed to load metadata for table: " +
        "'functional.unsupported_timestamp_partition'");
    AnalysisError("select * from functional.unsupported_binary_partition",
        "Failed to load metadata for table: 'functional.unsupported_binary_partition'");
    // Try with hbase
    AnalyzesOk("describe functional_hbase.allcomplextypes");

    for (ScalarType t: Type.getUnsupportedTypes()) {
      // Create/Alter table.
      AnalysisError(String.format("create table new_table (new_col %s)", t.toSql()),
          String.format("Unsupported data type: %s", t.toSql()));
      AnalysisError(String.format(
          "create table new_table (new_col int) PARTITIONED BY (p_col %s)", t.toSql()),
          String.format("Unsupported data type: %s", t.toSql()));
      AnalysisError(String.format(
          "alter table functional.alltypes add columns (new_col %s)", t.toSql()),
          String.format("Unsupported data type: %s", t.toSql()));
      AnalysisError(String.format(
          "alter table functional.alltypes change column int_col new_col %s", t.toSql()),
          String.format("Unsupported data type: %s", t.toSql()));

      // UDFs.
      final String udfSuffix = " LOCATION '/test-warehouse/libTestUdfs.so' " +
          "SYMBOL='_Z8IdentityPN10impala_udf15FunctionContextERKNS_10BooleanValE'";
      AnalysisError(String.format(
          "create function foo(VARCHAR(5)) RETURNS %s" + udfSuffix, t.toSql()),
          String.format("Unsupported data type: %s", t.toSql()));
      AnalysisError(String.format(
          "create function foo(%s) RETURNS int" + udfSuffix, t.toSql()),
          String.format("Unsupported data type: %s", t.toSql()));

      // UDAs.
      final String udaSuffix = " LOCATION '/test-warehouse/libTestUdas.so' " +
          "UPDATE_FN='AggUpdate'";
      AnalysisError(String.format("create aggregate function foo(string, double) " +
          "RETURNS %s" + udaSuffix, t.toSql()),
          String.format("Unsupported data type: %s", t.toSql()));
      AnalysisError(String.format("create aggregate function foo(%s, double) " +
          "RETURNS int" + udaSuffix, t.toSql()),
          String.format("Unsupported data type: %s", t.toSql()));

      // Cast,
      AnalysisError(String.format("select cast('abc' as %s)", t.toSql()),
          String.format("Unsupported data type: %s", t.toSql()));

    }
  }

  @Test
  public void TestVirtualColumnInputFileName() {
    // Select virtual columns.
    AnalyzesOk("select input__file__name from functional.alltypes");
    AnalyzesOk("select input__file__name, id from functional_parquet.alltypes");
    AnalyzesOk("select input__file__name, * from functional_orc_def.alltypes");
    AnalyzesOk("select input__file__name, * from " +
        "(select input__file__name, * from functional_avro.alltypes) v");
    AnalyzesOk(
        "select id, input__file__name from functional_parquet.iceberg_partitioned");
    AnalyzesOk(
            "select input__file__name, * from functional_parquet.complextypestbl c, " +
            "c.int_array");
    AnalyzesOk(
        "select c.input__file__name, c.int_array.* " +
        "from functional_parquet.complextypestbl c, c.int_array");

    // Error cases:
    AnalysisError(
        "select id, nested_struct.input__file__name " +
        "from functional_parquet.complextypestbl",
        "Could not resolve column/field reference: 'nested_struct.input__file__name'");
    AnalysisError(
        "select c.int_array.input__file__name, c.int_array.* " +
        "from functional_parquet.complextypestbl c, c.int_array",
        "Could not resolve column/field reference: 'c.int_array.input__file__name'");
    AnalysisError(
        "select id, nested_struct.input__file__name " +
        "from functional_parquet.complextypestbl",
        "Could not resolve column/field reference: 'nested_struct.input__file__name'");
    AnalysisError("select input__file__name from functional_kudu.alltypes",
        "Could not resolve column/field reference: 'input__file__name'");
    AnalysisError("select input__file__name from functional_hbase.alltypes",
        "Could not resolve column/field reference: 'input__file__name'");
  }

  @Test
  public void TestCopyTestCase() {
    AnalyzesOk("copy testcase to 'hdfs:///tmp' select * from functional.alltypes");
    AnalyzesOk("copy testcase to 'hdfs:///tmp' select * from functional.alltypes union " +
        "select * from functional.alltypes");
    // Containing views
    AnalyzesOk("copy testcase to 'hdfs:///tmp' select * from functional.alltypes_view");
    // Mix of view and table
    AnalyzesOk("copy testcase to 'hdfs:///tmp' select * from functional.alltypes_view " +
        "union all select * from functional.alltypes");
    AnalyzesOk("copy testcase to 'hdfs:///tmp' with v as (select 1) select * from v");
    // Target directory does not exist
    AnalysisError("copy testcase to 'hdfs:///foo' select 1", "Path does not exist: " +
        "hdfs://localhost:20500/foo");
    // Testcase file does not exist
    AnalysisError("copy testcase from 'hdfs:///tmp/file-doesnot-exist'", "Path does not" +
        " exist");
  }

  @Test
  public void TestBinaryHBaseTable() {
    AnalyzesOk("select * from functional_hbase.alltypessmallbinary");
  }

  @Test
  public void TestUnsupportedSerde() {
    AnalysisError("select * from functional.bad_serde",
        "Failed to load metadata for table: 'functional.bad_serde'");
  }

  @Test
  public void TestResetMetadata() {
    BiConsumer<ParseNode, ResetMetadataStmt.Action> assertAction =
        (parseNode, action) -> {
          Preconditions.checkArgument(parseNode instanceof ResetMetadataStmt);
          assertEquals(action, ((ResetMetadataStmt) parseNode).getAction());
        };

    assertAction.accept(AnalyzesOk("invalidate metadata"),
        ResetMetadataStmt.Action.INVALIDATE_METADATA_ALL);
    assertAction.accept(AnalyzesOk("invalidate metadata functional.alltypessmall"),
        ResetMetadataStmt.Action.INVALIDATE_METADATA_TABLE);
    assertAction.accept(AnalyzesOk("invalidate metadata functional.alltypes_view"),
        ResetMetadataStmt.Action.INVALIDATE_METADATA_TABLE);
    assertAction.accept(AnalyzesOk("invalidate metadata functional.bad_serde"),
        ResetMetadataStmt.Action.INVALIDATE_METADATA_TABLE);
    assertAction.accept(AnalyzesOk("refresh functional.alltypessmall"),
        ResetMetadataStmt.Action.REFRESH_TABLE);
    assertAction.accept(AnalyzesOk("refresh functional.alltypes_view"),
        ResetMetadataStmt.Action.REFRESH_TABLE);
    assertAction.accept(AnalyzesOk("refresh functional.bad_serde"),
        ResetMetadataStmt.Action.REFRESH_TABLE);
    assertAction.accept(AnalyzesOk(
        "refresh functional.alltypessmall partition (year=2009, month=1)"),
        ResetMetadataStmt.Action.REFRESH_PARTITION);
    assertAction.accept(AnalyzesOk(
        "refresh functional.alltypessmall partition (year=2009, month=NULL)"),
        ResetMetadataStmt.Action.REFRESH_PARTITION);
    assertAction.accept(AnalyzesOk(
        "refresh authorization", createAnalysisCtx(createAuthorizationFactory())),
        ResetMetadataStmt.Action.REFRESH_AUTHORIZATION);

    // invalidate metadata <table name> checks the Hive Metastore for table existence
    // and should not throw an AnalysisError if the table or db does not exist.
    assertAction.accept(AnalyzesOk("invalidate metadata functional.unknown_table"),
        ResetMetadataStmt.Action.INVALIDATE_METADATA_TABLE);
    assertAction.accept(AnalyzesOk("invalidate metadata unknown_db.unknown_table"),
        ResetMetadataStmt.Action.INVALIDATE_METADATA_TABLE);

    AnalysisError("refresh functional.unknown_table",
        "Table does not exist: functional.unknown_table");
    AnalysisError("refresh unknown_db.unknown_table",
        "Database does not exist: unknown_db");
    AnalysisError("refresh functional.alltypessmall partition (year=2009, int_col=10)",
        "Column 'int_col' is not a partition column in table: functional.alltypessmall");
    AnalysisError("refresh functional.alltypessmall partition (year=2009)",
        "Items in partition spec must exactly match the partition columns in "
            + "the table definition: functional.alltypessmall (1 vs 2)");
    AnalysisError("refresh functional.alltypessmall partition (year=2009, year=2009)",
        "Duplicate partition key name: year");
    AnalysisError(
        "refresh functional.alltypessmall partition (year=2009, month='foo')",
        "Value of partition spec (column=month) has incompatible type: 'STRING'. "
            + "Expected type: 'INT'");
    AnalysisError("refresh functional.zipcode_incomes partition (year=2009, month=1)",
        "Table is not partitioned: functional.zipcode_incomes");
  }

  @Test
  public void TestExplain() {
    // Analysis error from explain insert: too many partitioning columns.
    AnalysisError("explain insert into table functional.alltypessmall " +
        "partition (year=2009, month=4, year=10)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, " +
        "float_col, double_col, date_string_col, string_col, timestamp_col " +
        "from functional.alltypes",
        "Duplicate column 'year' in partition clause");

    // Analysis error from explain query
    AnalysisError("explain " +
        "select id from (select id+2 from functional_hbase.alltypessmall) a",
        "Could not resolve column/field reference: 'id'");

    // Analysis error from explain upsert
    AnalysisError("explain upsert into table functional.alltypes select * from " +
        "functional.alltypes", "UPSERT is only supported for Kudu tables");

    // Positive test for explain query
    AnalyzesOk("explain select * from functional.AllTypes");

    // Positive test for explain insert
    AnalyzesOk("explain insert into table functional.alltypessmall " +
        "partition (year=2009, month=4)" +
        "select id, bool_col, tinyint_col, smallint_col, int_col, int_col, " +
        "float_col, float_col, date_string_col, string_col, timestamp_col " +
        "from functional.alltypes");

    // Positive test for explain upsert
    AnalyzesOk("explain upsert into table functional_kudu.testtbl select * from " +
        "functional_kudu.testtbl");
  }

  @Test
  public void TestLimitAndOffset() {
    // Arithmetic expressions that result in a positive, integral value are OK
    AnalyzesOk("select * from functional.AllTypes limit 10 * 10 + 10 - 10 % 10");
    AnalyzesOk("select * from functional.AllTypes limit 1 ^ 0 | 3 & 3");
    // Test offset, requires order by and limit
    AnalyzesOk("select * from functional.AllTypes order by id limit 10 offset 1+2*3%4");
    // Test offset within an inline view and with-clause view
    AnalyzesOk("select t5.id from (select id from functional.AllTypes order by id " +
        "limit 10 offset 2) t5");
    AnalyzesOk("with t5 as (select id from functional.AllTypes order by id limit 10 " +
        "offset 2) select * from t5");

    // Casting to int is fine
    AnalyzesOk("select id, bool_col from functional.AllTypes limit CAST(10.0 AS INT)");
    AnalyzesOk("select id, bool_col from functional.AllTypes limit " +
        "CAST(NOT FALSE AS INT)");
    AnalyzesOk("select * from functional.AllTypes order by id limit 10 " +
        "offset CAST(1.0 AS INT)");

    // Analysis error from negative values
    AnalysisError("select * from functional.AllTypes limit 10 - 20",
        "LIMIT must be a non-negative integer: 10 - 20 = -10");
    AnalysisError("select * from functional.AllTypes order by id limit 10 " +
        "offset 10 - 20",
        "OFFSET must be a non-negative integer: 10 - 20 = -10");

    // Analysis error from non-integral values
    AnalysisError("select * from functional.AllTypes limit 10.0",
        "LIMIT expression must be an integer type but is 'DECIMAL(3,1)': 10.0");
    AnalysisError("select * from functional.AllTypes limit NOT FALSE",
        "LIMIT expression must be an integer type but is 'BOOLEAN': NOT FALSE");
    AnalysisError("select * from functional.AllTypes limit CAST(\"asdf\" AS INT)",
        "LIMIT expression evaluates to NULL: CAST('asdf' AS INT)");
    AnalysisError("select * from functional.AllTypes order by id limit 10 " +
        "OFFSET 10.0",
        "OFFSET expression must be an integer type but is 'DECIMAL(3,1)': 10.0");
    AnalysisError("select * from functional.AllTypes order by id limit 10 " +
        "offset CAST('asdf' AS INT)",
        "OFFSET expression evaluates to NULL: CAST('asdf' AS INT)");

    // Analysis error from non-constant expressions
    AnalysisError("select id, bool_col from functional.AllTypes limit id < 10",
        "LIMIT expression must be a constant expression: id < 10");
    AnalysisError("select id, bool_col from functional.AllTypes order by id limit 10 " +
        "offset id < 10",
        "OFFSET expression must be a constant expression: id < 10");
    AnalysisError("select id, bool_col from functional.AllTypes limit count(*)",
        "LIMIT expression must be a constant expression: count(*)");
    AnalysisError("select id, bool_col from functional.AllTypes order by id limit 10 " +
        "offset count(*)",
        "OFFSET expression must be a constant expression: count(*)");

    // Offset is only valid with an order by
    AnalysisError("SELECT a FROM test LIMIT 10 OFFSET 5",
        "OFFSET requires an ORDER BY clause: LIMIT 10 OFFSET 5");
    AnalysisError("SELECT x.id FROM (SELECT id FROM alltypesagg LIMIT 5 OFFSET 5) x " +
        "ORDER BY x.id LIMIT 100 OFFSET 4",
        "OFFSET requires an ORDER BY clause: LIMIT 5 OFFSET 5");
    AnalysisError("SELECT a FROM test OFFSET 5",
        "OFFSET requires an ORDER BY clause: OFFSET 5");
    AnalyzesOk("SELECT id FROM functional.Alltypes ORDER BY bool_col OFFSET 5");
  }

  @Test
  public void TestAnalyzeShowCreateTable() {
    AnalyzesOk("show create table functional.AllTypes");
    AnalyzesOk("show create table functional.alltypes_view");
    AnalysisError("show create table functional.not_a_table",
        "Table does not exist: functional.not_a_table");
    AnalysisError("show create table doesnt_exist",
        "Table does not exist: default.doesnt_exist");
  }

  @Test
  public void TestAnalyzeTransactional() {
    Assume.assumeTrue(MetastoreShim.getMajorVersion() > 2);
    String fullAcidErrorMsg = "%s not supported on full " +
        "transactional (ACID) table: functional_orc_def.full_transactional_table";
    String transactionalErrorMsg = "%s not supported on " +
        "transactional (ACID) table: %s";
    String insertOnlyTbl = "functional.insert_only_transactional_table";
    String fullTxnTbl = "functional_orc_def.full_transactional_table";

    AnalyzesOk(
        "create table test as select * from functional_orc_def.full_transactional_table");
    AnalyzesOk(
        "create table test as select * from functional.insert_only_transactional_table");

    AnalyzesOk("create table test like functional_orc_def.full_transactional_table");
    AnalyzesOk("create table test like functional.insert_only_transactional_table");

    AnalyzesOk(
        "insert into functional.testtbl " +
        "select 1,'test',* from functional_orc_def.full_transactional_table");
    AnalyzesOk("insert into functional.testtbl select *,'test',1 " +
            "from functional.insert_only_transactional_table");

    AnalyzesOk("insert into functional.insert_only_transactional_table select * " +
        "from functional.insert_only_transactional_table");


    AnalyzesOk("compute stats functional_orc_def.full_transactional_table");
    AnalyzesOk("compute stats functional.insert_only_transactional_table");

    AnalyzesOk("select * from functional_orc_def.full_transactional_table");
    AnalyzesOk("select * from functional.insert_only_transactional_table");

    AnalyzesOk("drop table functional_orc_def.full_transactional_table");
    AnalyzesOk("drop table functional.insert_only_transactional_table");

    AnalysisError("truncate table functional_orc_def.full_transactional_table",
        String.format(fullAcidErrorMsg, "TRUNCATE"));
    AnalyzesOk("truncate table functional.insert_only_transactional_table");

    AnalysisError(
        "alter table functional_orc_def.full_transactional_table " +
        "add columns (col2 string)",
        String.format(transactionalErrorMsg, "ALTER TABLE", fullTxnTbl));
    AnalysisError(
        "alter table functional.insert_only_transactional_table " +
        "add columns (col2 string)",
        String.format(transactionalErrorMsg, "ALTER TABLE", insertOnlyTbl));

    AnalysisError(
        "drop stats functional_orc_def.full_transactional_table",
        String.format(transactionalErrorMsg, "DROP STATS", fullTxnTbl));
    AnalysisError("drop stats functional.insert_only_transactional_table",
        String.format(transactionalErrorMsg, "DROP STATS", insertOnlyTbl));

    AnalyzesOk("describe functional.insert_only_transactional_table");
    AnalyzesOk("describe functional_orc_def.full_transactional_table");

    AnalyzesOk("show column stats functional_orc_def.full_transactional_table");
    AnalyzesOk("show column stats functional.insert_only_transactional_table");

    AnalyzesOk("refresh functional.insert_only_transactional_table");
    AnalyzesOk("refresh functional_orc_def.full_transactional_table");
    AnalysisError("refresh functional.insert_only_transactional_table partition (j=1)",
        "Refreshing a partition is not allowed on transactional tables. Try to refresh " +
        "the whole table instead.");
    AnalysisError("refresh functional_orc_def.full_transactional_table partition (j=1)",
        "Refreshing a partition is not allowed on transactional tables. Try to refresh " +
        "the whole table instead.");
  }

  @Test
  public void TestAnalyzeMaterializedView() {
    Assume.assumeTrue(MetastoreShim.getMajorVersion() > 2);
    AnalysisError("alter table functional.materialized_view " +
        "set tblproperties ('foo'='bar')",
        "Write not supported. Table functional.materialized_view  " +
        "access type is: READONLY");

    AnalysisError("insert into table functional.materialized_view " +
        "select * from functional.insert_only_transactional_table",
      "Impala does not support INSERTing into views:" +
        " functional.materialized_view");

    AnalysisError("drop table functional.materialized_view ",
        "Write not supported. Table functional.materialized_view  " +
        "access type is: READONLY");

    AnalyzesOk("Select * from functional.materialized_view");
  }

  private Function createFunction(boolean hasVarArgs, Type... args) {
    return new Function(new FunctionName("test"), args, Type.INVALID, hasVarArgs);
  }

  @Test
  // Test matching function signatures.
  public void TestFunctionMatching() {
    Function[] fns = new Function[19];
    // test()
    fns[0] = createFunction(false);

    // test(int)
    fns[1] = createFunction(false, Type.INT);

    // test(int...)
    fns[2] = createFunction(true, Type.INT);

    // test(tinyint)
    fns[3] = createFunction(false, Type.TINYINT);

    // test(tinyint...)
    fns[4] = createFunction(true, Type.TINYINT);

    // test(double)
    fns[5] = createFunction(false, Type.DOUBLE);

    // test(double...)
    fns[6] = createFunction(true, Type.DOUBLE);

    // test(double, double)
    fns[7] = createFunction(false, Type.DOUBLE, Type.DOUBLE);

    // test(double, double...)
    fns[8] = createFunction(true, Type.DOUBLE, Type.DOUBLE);

    // test(smallint, tinyint)
    fns[9] = createFunction(false, Type.SMALLINT, Type.TINYINT);

    // test(int, double, double, double)
    fns[10] = createFunction(false, Type.INT, Type.DOUBLE, Type.DOUBLE, Type.DOUBLE);

    // test(int, string, int...)
    fns[11] = createFunction(true, Type.INT, Type.STRING, Type.INT);

    // test(tinying, string, tinyint, int, tinyint)
    fns[12] = createFunction(false, Type.TINYINT, Type.STRING, Type.TINYINT, Type.INT,
        Type.TINYINT);

    // test(tinying, string, bigint, int, tinyint)
    fns[13] = createFunction(false, Type.TINYINT, Type.STRING, Type.BIGINT, Type.INT,
        Type.TINYINT);

    // test(date, string, date)
    fns[14] = createFunction(false, Type.DATE, Type.STRING, Type.DATE);
    // test(timestamp...)
    fns[15] = createFunction(true, Type.TIMESTAMP);
    // test(date...)
    fns[16] = createFunction(true, Type.DATE);
    // test(string...)
    fns[17] = createFunction(true, Type.STRING);
    // test(binary...)
    fns[18] = createFunction(true, Type.BINARY);

    Assert.assertFalse(fns[1].compare(fns[0], Function.CompareMode.IS_SUPERTYPE_OF));
    Assert.assertTrue(fns[1].compare(fns[2], Function.CompareMode.IS_SUPERTYPE_OF));
    Assert.assertTrue(fns[1].compare(fns[3], Function.CompareMode.IS_SUPERTYPE_OF));
    Assert.assertTrue(fns[1].compare(fns[4], Function.CompareMode.IS_SUPERTYPE_OF));
    Assert.assertFalse(fns[1].compare(fns[5], Function.CompareMode.IS_SUPERTYPE_OF));
    Assert.assertFalse(fns[1].compare(fns[6], Function.CompareMode.IS_SUPERTYPE_OF));
    Assert.assertFalse(fns[1].compare(fns[7], Function.CompareMode.IS_SUPERTYPE_OF));
    Assert.assertFalse(fns[1].compare(fns[8], Function.CompareMode.IS_SUPERTYPE_OF));

    Assert.assertTrue(fns[1].compare(fns[2], Function.CompareMode.IS_INDISTINGUISHABLE));
    Assert.assertTrue(fns[3].compare(fns[4], Function.CompareMode.IS_INDISTINGUISHABLE));
    Assert.assertTrue(fns[5].compare(fns[6], Function.CompareMode.IS_INDISTINGUISHABLE));
    Assert.assertFalse(fns[5].compare(fns[7], Function.CompareMode.IS_INDISTINGUISHABLE));
    Assert.assertFalse(fns[5].compare(fns[8], Function.CompareMode.IS_INDISTINGUISHABLE));
    Assert.assertTrue(fns[6].compare(fns[7], Function.CompareMode.IS_INDISTINGUISHABLE));
    Assert.assertTrue(fns[6].compare(fns[8], Function.CompareMode.IS_INDISTINGUISHABLE));
    Assert.assertTrue(fns[7].compare(fns[8], Function.CompareMode.IS_INDISTINGUISHABLE));
    Assert.assertFalse(fns[1].compare(fns[3], Function.CompareMode.IS_INDISTINGUISHABLE));
    Assert.assertFalse(fns[1].compare(fns[4], Function.CompareMode.IS_INDISTINGUISHABLE));

    Assert.assertFalse(fns[9].compare(fns[4], Function.CompareMode.IS_SUPERTYPE_OF));
    Assert.assertTrue(fns[2].compare(fns[9], Function.CompareMode.IS_SUPERTYPE_OF));

    Assert.assertTrue(fns[8].compare(fns[10], Function.CompareMode.IS_SUPERTYPE_OF));
    Assert.assertFalse(fns[10].compare(fns[8], Function.CompareMode.IS_SUPERTYPE_OF));

    Assert.assertTrue(fns[11].compare(fns[12], Function.CompareMode.IS_SUPERTYPE_OF));
    Assert.assertFalse(fns[11].compare(fns[13], Function.CompareMode.IS_SUPERTYPE_OF));

    Assert.assertFalse(fns[15].compare(fns[14], Function.CompareMode.IS_SUPERTYPE_OF));
    Assert.assertTrue(fns[15].compare(fns[14],
        Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF));
    Assert.assertFalse(fns[15].compare(fns[16],
        Function.CompareMode.IS_SUPERTYPE_OF));
    Assert.assertTrue(fns[15].compare(fns[16],
        Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF));
    Assert.assertFalse(fns[15].compare(fns[17],
        Function.CompareMode.IS_SUPERTYPE_OF));
    Assert.assertTrue(fns[15].compare(fns[17],
        Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF));

    Assert.assertFalse(fns[16].compare(fns[14], Function.CompareMode.IS_SUPERTYPE_OF));
    Assert.assertTrue(fns[16].compare(fns[14],
        Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF));
    Assert.assertFalse(fns[16].compare(fns[15],
        Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF));
    Assert.assertFalse(fns[16].compare(fns[17],
        Function.CompareMode.IS_SUPERTYPE_OF));
    Assert.assertTrue(fns[16].compare(fns[17],
        Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF));

    Assert.assertFalse(fns[17].compare(fns[14],
        Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF));
    Assert.assertFalse(fns[17].compare(fns[15],
        Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF));
    Assert.assertFalse(fns[17].compare(fns[16],
        Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF));
    Assert.assertFalse(fns[18].compare(fns[17],
        Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF));

    for (int i = 0; i < fns.length; ++i) {
      for (int j = 0; j < fns.length; ++j) {
        if (i == j) {
          Assert.assertTrue(
              fns[i].compare(fns[i], Function.CompareMode.IS_IDENTICAL));
          Assert.assertTrue(
              fns[i].compare(fns[i], Function.CompareMode.IS_INDISTINGUISHABLE));
          Assert.assertTrue(
              fns[i].compare(fns[i], Function.CompareMode.IS_SUPERTYPE_OF));
        } else {
          Assert.assertFalse(fns[i].compare(fns[j], Function.CompareMode.IS_IDENTICAL));
          if (fns[i].compare(fns[j], Function.CompareMode.IS_INDISTINGUISHABLE)) {
            // If it's a indistinguishable, at least one of them must be a super type
            // of the other
            Assert.assertTrue(
                fns[i].compare(fns[j], Function.CompareMode.IS_SUPERTYPE_OF) ||
                fns[j].compare(fns[i], Function.CompareMode.IS_SUPERTYPE_OF));
            // This is reflexive
            Assert.assertTrue(
                fns[j].compare(fns[i], Function.CompareMode.IS_INDISTINGUISHABLE));
          } else if (fns[i].compare(fns[j], Function.CompareMode.IS_INDISTINGUISHABLE)) {
          }
        }
      }
    }
  }

  @Test
  public void testFunctionMatchScore() {
    // test(date, date, int)
    Function fnDate = createFunction(false, Type.DATE, Type.DATE, Type.INT);
    // test(timestamp, timestamp, int)
    Function fnTimestamp = createFunction(false, Type.TIMESTAMP, Type.TIMESTAMP,
        Type.INT);

    // test(string, date, tinyint)
    Function fn = createFunction(false, Type.STRING, Type.DATE, Type.TINYINT);
    Assert.assertEquals(-1,
        fnDate.calcMatchScore(fn, Function.CompareMode.IS_SUPERTYPE_OF));
    int fnDateScore = fnDate.calcMatchScore(fn,
        Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
    Assert.assertTrue(fnDateScore >= 0);

    Assert.assertEquals(-1,
        fnTimestamp.calcMatchScore(fn, Function.CompareMode.IS_SUPERTYPE_OF));
    int fnTimestampScore = fnTimestamp.calcMatchScore(fn,
        Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
    Assert.assertTrue(fnTimestampScore >= 0);

    Assert.assertTrue(fnDateScore > fnTimestampScore);

    // test(string, timestamp, tinyint)
    fn = createFunction(false, Type.STRING, Type.TIMESTAMP, Type.TINYINT);
    Assert.assertEquals(-1,
        fnDate.calcMatchScore(fn, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF));

    Assert.assertEquals(-1,
        fnTimestamp.calcMatchScore(fn, Function.CompareMode.IS_SUPERTYPE_OF));
    Assert.assertTrue(
        fnTimestamp.calcMatchScore(fn, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF)
        >= 0);

    // test(string, string, tinyint)
    fn = createFunction(false, Type.STRING, Type.STRING, Type.TINYINT);
    Assert.assertEquals(-1,
        fnDate.calcMatchScore(fn, Function.CompareMode.IS_SUPERTYPE_OF));
    fnDateScore = fnDate.calcMatchScore(fn,
        Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
    Assert.assertTrue(fnDateScore >= 0);

    Assert.assertEquals(-1,
        fnTimestamp.calcMatchScore(fn, Function.CompareMode.IS_SUPERTYPE_OF));
    fnTimestampScore = fnTimestamp.calcMatchScore(fn,
        Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
    Assert.assertTrue(fnTimestampScore >= 0);

    Assert.assertTrue(fnDateScore == fnTimestampScore);
  }

  @Test
  public void testResolveFunction() {
    Function[] fns = new Function[] {
        createFunction(false, Type.TIMESTAMP, Type.TIMESTAMP, Type.INT),
        createFunction(false, Type.DATE, Type.DATE, Type.INT)};

    // fns[0] and fns[1] has the same match score, so fns[0] will be chosen.
    Function fnStr = createFunction(false, Type.STRING, Type.STRING, Type.TINYINT);
    Assert.assertEquals(fns[0],
        FunctionUtils.resolveFunction(Arrays.asList(fns), fnStr,
            Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF));

    // fns[0] is a better match. First argument is an exact match.
    Function fnTimestamp = createFunction(false, Type.TIMESTAMP, Type.STRING,
        Type.TINYINT);
    Assert.assertEquals(fns[0],
        FunctionUtils.resolveFunction(Arrays.asList(fns), fnTimestamp,
            Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF));

    // fns[0] is a better match. First argument is an exact match.
    fnTimestamp = createFunction(false, Type.TIMESTAMP, Type.DATE,
        Type.TINYINT);
    Assert.assertEquals(fns[0],
        FunctionUtils.resolveFunction(Arrays.asList(fns), fnTimestamp,
            Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF));

    // fns[0] is a better match. Second argument is an exact match.
    fnTimestamp = createFunction(false, Type.DATE, Type.TIMESTAMP,
        Type.TINYINT);
    Assert.assertEquals(fns[0],
        FunctionUtils.resolveFunction(Arrays.asList(fns), fnTimestamp,
            Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF));

    // fns[1] is a better match. First argument is an exact match.
    Function fnDate = createFunction(false, Type.DATE, Type.STRING, Type.TINYINT);
    Assert.assertEquals(fns[1],
        FunctionUtils.resolveFunction(Arrays.asList(fns), fnDate,
            Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF));

    // fns[1] is a better match. Second argument is an exact match.
    fnDate = createFunction(false, Type.STRING, Type.DATE, Type.TINYINT);
    Assert.assertEquals(fns[1],
        FunctionUtils.resolveFunction(Arrays.asList(fns), fnDate,
            Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF));
  }

  @Test
  public void testAnalyzeBucketed() {
    AnalyzesOk("select count(*) from functional.bucketed_table");
    AnalyzesOk("select count(*) from functional.bucketed_ext_table");
    AnalyzesOk("drop stats functional.bucketed_table");
    AnalyzesOk("describe functional.bucketed_table");
    AnalyzesOk("show column stats functional.bucketed_table");
    AnalyzesOk("create table test as select * from functional.bucketed_table");
    AnalyzesOk("compute stats functional.bucketed_table");
    AnalyzesOk("drop table functional.bucketed_table");

    String errorMsgBucketed = "functional.bucketed_table " +
        "is a bucketed table. Only read operations are supported on such tables.";
    String errorMsgExtBucketed = "functional.bucketed_ext_table " +
        "is a bucketed table. Only read operations are supported on such tables.";
    String errorMsgInsertOnlyBucketed =
        "functional.insert_only_transactional_bucketed_table " +
        "is a bucketed table. Only read operations are supported on such tables.";
    String errorMsg = "Table bucketed_ext_table write not supported";

    if (MetastoreShim.getMajorVersion() > 2) {
      AnalyzesOk(
          "select count(*) from functional.insert_only_transactional_bucketed_table");
      AnalysisError("insert into functional.insert_only_transactional_bucketed_table " +
          "select * from functional.insert_only_transactional_bucketed_table",
          errorMsgInsertOnlyBucketed);
      // Separates from Hive 2 as the error message may different after Hive
      // provides error message needed information.
      AnalysisError("insert into functional.bucketed_ext_table select * from " +
          "functional.bucketed_ext_table", errorMsgExtBucketed);
    } else {
      AnalysisError("insert into functional.bucketed_ext_table select * from " +
         "functional.bucketed_ext_table", errorMsgExtBucketed);
    }
    AnalysisError("insert into functional.bucketed_table select * from " +
       "functional.bucketed_table", errorMsgBucketed);
    AnalysisError("create table test like functional.bucketed_table", errorMsgBucketed);
    AnalysisError("truncate table functional.bucketed_table", errorMsgBucketed);
    AnalysisError("alter table functional.bucketed_table add columns(col3 int)",
        errorMsgBucketed);
  }
}
