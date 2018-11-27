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

import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.FeView;
import org.apache.impala.common.FrontendTestBase;
import org.junit.Test;

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

}
