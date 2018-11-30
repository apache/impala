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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.impala.catalog.AggregateFunction;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.FeView;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.thrift.TFunctionBinaryType;
import org.junit.Test;

import com.google.common.collect.Lists;

public class ToSqlUtilsTest extends FrontendTestBase {

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
