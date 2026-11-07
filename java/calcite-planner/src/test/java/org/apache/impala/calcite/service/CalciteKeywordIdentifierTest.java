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

package org.apache.impala.calcite.service;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.impala.common.ImpalaException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CalciteKeywordIdentifierTest {
  private SqlNode expression(String sql) throws ImpalaException {
    return ((SqlSelect) new CalciteQueryParser(sql).parse()).getSelectList().get(0);
  }

  private void assertColumn(String word, boolean qualified) throws ImpalaException {
    String sql = qualified ? "select t." + word
        + " from (select 1 as `" + word + "`) t" : "select " + word;
    SqlIdentifier id = (SqlIdentifier) expression(sql);
    assertEquals(word, id.names.get(id.names.size() - 1));
  }

  private void assertAlias(String word) throws ImpalaException {
    SqlCall call = (SqlCall) expression("select 1 as " + word);
    assertEquals(SqlKind.AS, call.getKind());
    assertEquals(word, ((SqlIdentifier) call.operand(1)).getSimple());
  }

  @Test
  public void testKeywordColumns() throws ImpalaException {
    for (String word : new String[] {"RESULT", "RETURN", "TIME", "USER", "ABS"}) {
      assertColumn(word, false);
      assertColumn(word, true);
      assertAlias(word);
    }
  }

  private void assertCoalesceColumn(String column, String expected)
      throws ImpalaException {
    SqlCall call = (SqlCall) expression("select coalesce(" + column
        + ", 0) from (select 1 as `time`) t");
    assertEquals("COALESCE", call.getOperator().getName());
    SqlIdentifier identifier = call.operand(0);
    assertEquals(expected, identifier.names.get(identifier.names.size() - 1));
  }

  @Test
  public void testTimeCoalesceColumn() throws ImpalaException {
    assertCoalesceColumn("time", "TIME");
  }

  @Test
  public void testTimeQualifiedCoalesceColumn() throws ImpalaException {
    assertCoalesceColumn("t.time", "TIME");
  }

  @Test
  public void testQuotedTimeCoalesceColumn() throws ImpalaException {
    assertCoalesceColumn("`time`", "time");
  }

  @Test
  public void testQuotedQualifiedTimeCoalesceColumn() throws ImpalaException {
    assertCoalesceColumn("t.`time`", "time");
  }

  private void assertUserQualifier(SqlNode node) {
    SqlIdentifier id = (SqlIdentifier) node;
    assertEquals(2, id.names.size());
    assertEquals("USER", id.names.get(0));
    assertEquals("A", id.names.get(1));
  }

  @Test
  public void testUserQualifier() throws ImpalaException {
    assertUserQualifier(expression("select user.a from (select 1 as a) user"));
  }

  @Test
  public void testUserGroupByQualifier() throws ImpalaException {
    SqlSelect select = (SqlSelect) new CalciteQueryParser(
        "select user.a from (select 1 as a) user group by user.a").parse();
    assertUserQualifier(select.getGroup().get(0));
  }

  @Test
  public void testUserCteQualifier() throws ImpalaException {
    SqlWith with = (SqlWith) new CalciteQueryParser(
        "with user as (select 1 as a) select user.a from user").parse();
    SqlSelect select = (SqlSelect) with.body;
    assertUserQualifier(select.getSelectList().get(0));
  }

  @Test
  public void testQuotedIdentifiers() throws ImpalaException {
    for (String word : new String[] {"result", "return", "time", "user"}) {
      SqlIdentifier id = (SqlIdentifier) expression("select `" + word + "`");
      assertEquals(word, id.getSimple());
      SqlCall alias = (SqlCall) expression("select 1 as `" + word + "`");
      assertEquals(word, ((SqlIdentifier) alias.operand(1)).getSimple());
    }
  }

  @Test
  public void testOrdinaryIdentifier() throws ImpalaException {
    assertColumn("SUM_VALUE", false);
    assertAlias("SUM_VALUE");
  }

  private void assertLiteral(String type, String value) throws ImpalaException {
    SqlNode literal = expression("select " + type + " '" + value + "'");
    assertEquals(SqlKind.LITERAL, literal.getKind());
    assertEquals(type + " '" + value + "'", literal.toString());
  }

  @Test
  public void testTimeLiteral() throws ImpalaException {
    assertLiteral("TIME", "12:34:56");
  }

  @Test
  public void testDateLiteral() throws ImpalaException {
    assertLiteral("DATE", "2020-01-02");
  }

  @Test
  public void testTimestampLiteral() throws ImpalaException {
    assertLiteral("TIMESTAMP", "2020-01-02 12:34:56");
  }

  private void assertCast(String type) throws ImpalaException {
    SqlCall call = (SqlCall) expression("select cast(null as " + type + ")");
    assertEquals(SqlKind.CAST, call.getKind());
    SqlDataTypeSpec spec = call.operand(1);
    assertEquals(type, spec.getTypeName().getSimple());
  }

  @Test
  public void testTimeCast() throws ImpalaException {
    assertCast("TIME");
  }

  @Test
  public void testDateCast() throws ImpalaException {
    assertCast("DATE");
  }

  @Test
  public void testTimestampCast() throws ImpalaException {
    assertCast("TIMESTAMP");
  }

  @Test
  public void testUserFunction() throws ImpalaException {
    SqlCall call = (SqlCall) expression("select user()");
    assertEquals("USER", call.getOperator().getName());
  }
}
