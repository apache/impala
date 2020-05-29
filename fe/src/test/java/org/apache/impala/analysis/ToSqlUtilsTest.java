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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.impala.catalog.AggregateFunction;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.FeView;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.thrift.TFunctionBinaryType;
import org.junit.Test;

import com.google.common.collect.Lists;

public class ToSqlUtilsTest extends FrontendTestBase {

  @Test
  public void testRemoveHiddenProperties() {
    Map<String,String> props = new HashMap<>();
    for (String kw : ToSqlUtils.HIDDEN_TABLE_PROPERTIES) {
      props.put(kw, kw + "-value");
    }
    props.put("foo", "foo-value");
    ToSqlUtils.removeHiddenTableProperties(props);
    assertEquals(1, props.size());
    assertEquals("foo-value", props.get("foo"));
  }

  @Test
  public void testRemoveHiddenKuduProperties() {
    Map<String,String> props = new HashMap<>();
    props.put(KuduTable.KEY_TABLE_NAME, "kudu-value");
    props.put("foo", "foo-value");
    ToSqlUtils.removeHiddenKuduTableProperties(props);
    assertEquals(1, props.size());
    assertEquals("foo-value", props.get("foo"));
  }

  @Test
  public void testGetSortColumns() {
    Map<String,String> props = new HashMap<>();
    props.put("foo", "foo-value");
    // Returns null if no sort cols property
    assertNull(ToSqlUtils.getSortColumns(props));

    // Degenerate case
    props.put(AlterTableSortByStmt.TBL_PROP_SORT_COLUMNS, "");
    List<String> sortCols = ToSqlUtils.getSortColumns(props);
    assertTrue(sortCols.isEmpty());

    // One column
    props.put(AlterTableSortByStmt.TBL_PROP_SORT_COLUMNS, "col1");
    sortCols = ToSqlUtils.getSortColumns(props);
    assertEquals(1, sortCols.size());
    assertEquals("col1", sortCols.get(0));

    // One column with padding
    props.put(AlterTableSortByStmt.TBL_PROP_SORT_COLUMNS, " col1 ");
    sortCols = ToSqlUtils.getSortColumns(props);
    assertEquals(1, sortCols.size());
    assertEquals("col1", sortCols.get(0));

    // One column with spaces in name
    props.put(AlterTableSortByStmt.TBL_PROP_SORT_COLUMNS, " col 1 ");
    sortCols = ToSqlUtils.getSortColumns(props);
    assertEquals(1, sortCols.size());
    assertEquals("col 1", sortCols.get(0));

    // Spurious commas
    props.put(AlterTableSortByStmt.TBL_PROP_SORT_COLUMNS, ",col1,");
    sortCols = ToSqlUtils.getSortColumns(props);
    assertEquals(1, sortCols.size());
    assertEquals("col1", sortCols.get(0));

    // Spurious commas and spaces
    props.put(AlterTableSortByStmt.TBL_PROP_SORT_COLUMNS, " , col1 , ");
    sortCols = ToSqlUtils.getSortColumns(props);
    assertEquals(1, sortCols.size());
    assertEquals("col1", sortCols.get(0));

    // Two columns
    props.put(AlterTableSortByStmt.TBL_PROP_SORT_COLUMNS, "col1,col2");
    sortCols = ToSqlUtils.getSortColumns(props);
    assertEquals(2, sortCols.size());
    assertEquals("col1", sortCols.get(0));
    assertEquals("col2", sortCols.get(1));

    // Two columns with extra commas and spaces
    props.put(AlterTableSortByStmt.TBL_PROP_SORT_COLUMNS, " col1 ,, col2 ");
    sortCols = ToSqlUtils.getSortColumns(props);
    assertEquals(2, sortCols.size());
    assertEquals("col1", sortCols.get(0));
    assertEquals("col2", sortCols.get(1));

    // Three columns
    props.put(AlterTableSortByStmt.TBL_PROP_SORT_COLUMNS, "col1,col2,col3");
    sortCols = ToSqlUtils.getSortColumns(props);
    assertEquals(3, sortCols.size());
    assertEquals("col1", sortCols.get(0));
    assertEquals("col2", sortCols.get(1));
    assertEquals("col3", sortCols.get(2));

    // Note: this method cannot handle a the pathological case of a
    // quoted column with a comma in the name: `foo,bar`.
  }

  @Test
  public void testSortOrder() {
    Map<String,String> props = new HashMap<>();
    props.put("foo", "foo-value");
    // Sorting order is LEXICAL by default
    assertEquals("LEXICAL", ToSqlUtils.getSortingOrder(props));

    props.put(AlterTableSortByStmt.TBL_PROP_SORT_ORDER, "ZORDER");
    // Returns true if zorder property set ZORDER
    assertEquals("ZORDER", ToSqlUtils.getSortingOrder(props));

    props.put(AlterTableSortByStmt.TBL_PROP_SORT_ORDER, "LEXICAL");
    // Returns false if zorder property set to LEXICAL
    assertEquals("LEXICAL", ToSqlUtils.getSortingOrder(props));
  }

  private FeTable getTable(String dbName, String tableName) {
    FeTable table = catalog_.getOrLoadTable(dbName, tableName);
    assertNotNull(table);
    return table;
  }

  private FeView getView(String dbName, String tableName) {
    FeTable table = getTable(dbName, tableName);
    assertTrue(table instanceof FeView);
    return (FeView) table;
  }

  @Test
  public void testHiveNeedsQuotes() {
    // Regular old ident
    assertFalse(ToSqlUtils.hiveNeedsQuotes("foo"));
    // Operator
    assertTrue(ToSqlUtils.hiveNeedsQuotes("+"));
    // Keyword
    assertTrue(ToSqlUtils.hiveNeedsQuotes("SELECT"));
    assertTrue(ToSqlUtils.hiveNeedsQuotes("select"));
    assertTrue(ToSqlUtils.hiveNeedsQuotes("sElEcT"));
    // Two idents
    assertTrue(ToSqlUtils.hiveNeedsQuotes("foo bar"));
    // Expression
    assertTrue(ToSqlUtils.hiveNeedsQuotes("a+b"));
    assertFalse(ToSqlUtils.hiveNeedsQuotes("123ab"));
    assertTrue(ToSqlUtils.hiveNeedsQuotes("123.a"));
  }

  @Test
  public void testImpalaNeedsQuotes() {
    // Regular old ident
    assertFalse(ToSqlUtils.impalaNeedsQuotes("foo"));
    // Keyword
    assertTrue(ToSqlUtils.impalaNeedsQuotes("SELECT"));
    assertTrue(ToSqlUtils.impalaNeedsQuotes("select"));
    assertTrue(ToSqlUtils.impalaNeedsQuotes("sElEcT"));
    // Special case checks for numbers
    assertTrue(ToSqlUtils.impalaNeedsQuotes("123"));
    assertTrue(ToSqlUtils.impalaNeedsQuotes("123a"));
    assertFalse(ToSqlUtils.impalaNeedsQuotes("a123"));

    // Note: the Impala check can't detect multi-part
    // symbols "a b" nor operators. Rely on the Hive
    // version for that.
  }

  @Test
  public void testGetIdentSql() {
    // Hive & Impala keyword
    assertEquals("`create`", ToSqlUtils.getIdentSql("create"));
    // Hive-only keyword
    assertEquals("`month`", ToSqlUtils.getIdentSql("month"));
    // Impala keyword
    assertEquals("`kudu`", ToSqlUtils.getIdentSql("kudu"));
    // Number
    assertEquals("`123`", ToSqlUtils.getIdentSql("123"));
    // Starts with number
    assertEquals("`123a`", ToSqlUtils.getIdentSql("123a"));
    // Contains spaces
    assertEquals("`a b`", ToSqlUtils.getIdentSql("a b"));
    // Operator
    assertEquals("`+`", ToSqlUtils.getIdentSql("+"));
    // Simple identifier
    assertEquals("foo", ToSqlUtils.getIdentSql("foo"));
    // Comment characters in name
    assertEquals("`foo#`", ToSqlUtils.getIdentSql("foo#"));
    assertEquals("`foo#bar`", ToSqlUtils.getIdentSql("foo#bar"));
    assertEquals("`foo--bar`", ToSqlUtils.getIdentSql("foo--bar"));

    List<String> in = Lists.newArrayList("create", "foo");
    List<String> out = ToSqlUtils.getIdentSqlList(in);
    assertEquals(2, out.size());
    assertEquals("`create`", out.get(0));
    assertEquals("foo", out.get(1));

    assertEquals("`create`.foo", ToSqlUtils.getPathSql(in));
  }

  @Test
  public void tesToIdentSql() {
    // Normal quoting
    assertEquals("`create`", ToSqlUtils.identSql("create"));
    assertEquals("foo", ToSqlUtils.identSql("foo"));
    // Wildcard is special in test cases
    assertEquals("*", ToSqlUtils.identSql("*"));
    // Multi-part names
    assertEquals("foo.`create`.*", ToSqlUtils.identSql("foo.create.*"));
  }

  @Test
  public void testCreateViewSql() {
    {
      FeView view = getView("functional", "view_view");
      String sql = ToSqlUtils.getCreateViewSql(view);
      String expected =
          "CREATE VIEW functional.view_view AS\n" +
          "SELECT * FROM functional.alltypes_view";
      assertEquals(expected, sql);
    }
    {
      FeView view = getView("functional", "complex_view");
      String sql = ToSqlUtils.getCreateViewSql(view);
      String expected =
          "CREATE VIEW functional.complex_view AS\n" +
          "SELECT complex_view.`_c0` abc, complex_view.string_col xyz" +
          " FROM (SELECT count(a.bigint_col), b.string_col" +
          " FROM functional.alltypesagg a" +
          " INNER JOIN functional.alltypestiny b ON a.id = b.id" +
          " WHERE a.bigint_col < 50" +
          " GROUP BY b.string_col" +
          " HAVING count(a.bigint_col) > 1" +
          " ORDER BY b.string_col ASC LIMIT 100) complex_view";
      assertEquals(expected, sql);
    }
  }

  @Test
  public void testScalarFunctionSql() {
    {
      // Can't generate SQL for an unresolved function

      List<Type> args = new ArrayList<>();
      Function fn = Function.createFunction("mydb", "fn1",
          args, Type.INT, false, TFunctionBinaryType.JAVA);
      try {
        ToSqlUtils.getCreateFunctionSql(Lists.newArrayList(fn));
      } catch (UnsupportedOperationException e) {
        // Expected
      }
    }
    {
      // Java function, leave off location and symbol
      List<Type> args = new ArrayList<>();
      Function fn = new ScalarFunction(new FunctionName("mydb", "fn1"),
          args, Type.INT, false);
      fn.setBinaryType(TFunctionBinaryType.JAVA);
      String sql = ToSqlUtils.getCreateFunctionSql(Lists.newArrayList(fn));
      String expected = "CREATE FUNCTION mydb.fn1\n";
      assertEquals(expected, sql);
    }
    {
      // Java function, with location and symbol
      List<Type> args = new ArrayList<>();
      ScalarFunction fn = new ScalarFunction(new FunctionName("mydb", "fn1"),
          args, Type.INT, false);
      fn.setBinaryType(TFunctionBinaryType.JAVA);
      fn.setLocation(new HdfsUri("hdfs://foo:123/fns/myfunc.jar"));
      fn.setSymbolName("MyClass");
      String sql = ToSqlUtils.getCreateFunctionSql(Lists.newArrayList(fn));
      String expected = "CREATE FUNCTION mydb.fn1\n" +
          " LOCATION 'hdfs://foo:123/fns/myfunc.jar'\n" +
          " SYMBOL='MyClass'\n";
      assertEquals(expected, sql);
    }
    {
      // Java function, with location and symbol
      List<Type> args = Lists.newArrayList(Type.VARCHAR, Type.BOOLEAN);
      ScalarFunction fn = new ScalarFunction(new FunctionName("mydb", "fn1"),
          args, Type.INT, false);
      fn.setBinaryType(TFunctionBinaryType.JAVA);
      fn.setLocation(new HdfsUri("hdfs://foo:123/fns/myfunc.jar"));
      fn.setSymbolName("MyClass");
      String sql = ToSqlUtils.getCreateFunctionSql(Lists.newArrayList(fn));
      String expected = "CREATE FUNCTION mydb.fn1\n" +
          " LOCATION 'hdfs://foo:123/fns/myfunc.jar'\n" +
          " SYMBOL='MyClass'\n";
      assertEquals(expected, sql);
    }
    {
      // C++ function, with location and symbol
      List<Type> args = new ArrayList<>();
      ScalarFunction fn = new ScalarFunction(new FunctionName("mydb", "fn1"),
          args, Type.INT, false);
      fn.setBinaryType(TFunctionBinaryType.NATIVE);
      fn.setLocation(new HdfsUri("hdfs://foo:123/fns/myfunc.so"));
      fn.setSymbolName("myClass");
      String sql = ToSqlUtils.getCreateFunctionSql(Lists.newArrayList(fn));
      String expected = "CREATE FUNCTION mydb.fn1()\n" +
          " RETURNS INT\n" +
          " LOCATION 'hdfs://foo:123/fns/myfunc.so'\n" +
          " SYMBOL='myClass'\n";
      assertEquals(expected, sql);
    }
    {
      // C++ function, with location and symbol
      List<Type> args = Lists.newArrayList(Type.VARCHAR, Type.BOOLEAN);
      ScalarFunction fn = new ScalarFunction(new FunctionName("mydb", "fn1"),
          args, Type.INT, false);
      fn.setBinaryType(TFunctionBinaryType.NATIVE);
      fn.setLocation(new HdfsUri("hdfs://foo:123/fns/myfunc.so"));
      fn.setSymbolName("myClass");
      String sql = ToSqlUtils.getCreateFunctionSql(Lists.newArrayList(fn));
      String expected = "CREATE FUNCTION mydb.fn1(VARCHAR(*), BOOLEAN)\n" +
          " RETURNS INT\n" +
          " LOCATION 'hdfs://foo:123/fns/myfunc.so'\n" +
          " SYMBOL='myClass'\n";
      assertEquals(expected, sql);
    }
  }

  @Test
  public void testAggFnSql() {
    {
      // C++ aggregate function, with minimum state
      List<Type> args = Lists.newArrayList(Type.INT, Type.BOOLEAN);
      AggregateFunction fn = new AggregateFunction(new FunctionName("mydb", "fn1"),
          args, Type.BIGINT, false);
      fn.setBinaryType(TFunctionBinaryType.NATIVE);
      fn.setLocation(new HdfsUri("hdfs://foo:123/fns/myfunc.so"));
      fn.setUpdateFnSymbol("Update");
      fn.setInitFnSymbol("Init");
      fn.setMergeFnSymbol("Merge");
      String sql = ToSqlUtils.getCreateFunctionSql(Lists.newArrayList(fn));
      String expected = "CREATE AGGREGATE FUNCTION mydb.fn1(INT, BOOLEAN)\n" +
          " RETURNS BIGINT\n" +
          " LOCATION 'hdfs://foo:123/fns/myfunc.so'\n" +
          " UPDATE_FN='Update'\n" +
          " INIT_FN='Init'\n" +
          " MERGE_FN='Merge'\n";
      assertEquals(expected, sql);
    }
    {
      // C++ aggregate function, with full state
      List<Type> args = Lists.newArrayList(Type.INT, Type.BOOLEAN);
      AggregateFunction fn = new AggregateFunction(new FunctionName("mydb", "fn1"),
          args, Type.BIGINT, false);
      fn.setBinaryType(TFunctionBinaryType.NATIVE);
      fn.setLocation(new HdfsUri("hdfs://foo:123/fns/myfunc.so"));
      fn.setUpdateFnSymbol("Update");
      fn.setInitFnSymbol("Init");
      fn.setMergeFnSymbol("Merge");
      fn.setFinalizeFnSymbol("Finalize");
      fn.setSerializeFnSymbol("Serialize");
      fn.setIntermediateType(Type.INT);
      String sql = ToSqlUtils.getCreateFunctionSql(Lists.newArrayList(fn));
      String expected = "CREATE AGGREGATE FUNCTION mydb.fn1(INT, BOOLEAN)\n" +
          " RETURNS BIGINT\n" +
          " INTERMEDIATE INT\n" +
          " LOCATION 'hdfs://foo:123/fns/myfunc.so'\n" +
          " UPDATE_FN='Update'\n" +
          " INIT_FN='Init'\n" +
          " MERGE_FN='Merge'\n" +
          " SERIALIZE_FN='Serialize'\n" +
          " FINALIZE_FN='Finalize'\n";
      assertEquals(expected, sql);
    }
  }

  @Test
  public void testCreateFunctionSql() {
    {
      // Two functions, one C++, one Java
      ScalarFunction fn1 = new ScalarFunction(new FunctionName("mydb", "fn1"),
          new ArrayList<>(), Type.INT, false);
      fn1.setBinaryType(TFunctionBinaryType.JAVA);
      fn1.setLocation(new HdfsUri("hdfs://foo:123/fns/myfunc.jar"));
      fn1.setSymbolName("MyClass");

      List<Type> args = Lists.newArrayList(Type.VARCHAR, Type.BOOLEAN);
      ScalarFunction fn2 = new ScalarFunction(new FunctionName("mydb", "fn2"),
          args, Type.INT, false);
      fn2.setBinaryType(TFunctionBinaryType.NATIVE);
      fn2.setLocation(new HdfsUri("hdfs://foo:123/fns/myfunc.so"));
      fn2.setSymbolName("myClass");

      String sql = ToSqlUtils.getCreateFunctionSql(Lists.newArrayList(fn1, fn2));
      String expected =
          "CREATE FUNCTION mydb.fn1\n" +
          " LOCATION 'hdfs://foo:123/fns/myfunc.jar'\n" +
          " SYMBOL='MyClass';\n"  +
          "CREATE FUNCTION mydb.fn2(VARCHAR(*), BOOLEAN)\n" +
          " RETURNS INT\n" +
          " LOCATION 'hdfs://foo:123/fns/myfunc.so'\n" +
          " SYMBOL='myClass'\n";
      assertEquals(expected, sql);
    }
  }
}
