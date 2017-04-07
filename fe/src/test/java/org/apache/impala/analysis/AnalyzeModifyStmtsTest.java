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

import org.apache.impala.testutil.TestUtils;
import org.junit.Test;

/**
 * Tests analysis phase of the ModifyStmt and its sub-classes.
 *
 * Most of the tests are on UpdateStmt only as it exercises all important code paths while
 * DeleteStmt uses only a subset of the functionality of the UpdateStmt for now.
 */
public class AnalyzeModifyStmtsTest extends AnalyzerTest {

  @Test
  public void TestFromListAliases() {
    TestUtils.assumeKuduIsSupported();
    AnalysisError("update a.name set a.name = 'Oskar' from functional_kudu.testtbl a",
        "'a.name' is not a table alias. Using the FROM clause requires the target table" +
            " to be a table alias.");
    AnalysisError("update a.name set a.name = 'Oskar' from functional_kudu.testtbl",
        "'a.name' is not a valid table alias or reference.");
    AnalysisError("Update functional_kudu.testtbl.c set name='Oskar'",
        "Could not resolve table reference: 'functional_kudu.testtbl.c'");
    AnalyzesOk("update a set a.name = 'values' from functional_kudu.testtbl a " +
        "where a.zip in (select zip from functional.testtbl limit 10)");
    AnalyzesOk("update functional_kudu.dimtbl set name = 'Oskar' FROM dimtbl",
        createAnalysisCtx("functional_kudu"));
    AnalysisError("update a set b.name = 'Oskar' FROM dimtbl b",
        createAnalysisCtx("functional_kudu"),
        "'a' is not a valid table alias or reference.");
    AnalyzesOk("update a set a.name = 'Oskar' FROM dimtbl a",
        createAnalysisCtx("functional_kudu"));
    // Table name is an implicit alias
    AnalyzesOk(
        "update functional_kudu.dimtbl set name = 'Oskar' FROM functional_kudu.dimtbl");
    // If explicit alias is set, it has to be used
    AnalysisError(
        "update functional_kudu.dimtbl set name = 'Oskar' FROM functional_kudu" +
            ".dimtbl foo",
        "'functional_kudu.dimtbl' is not a valid table alias or reference.");
    // Implicit alias is ok
    AnalyzesOk("update dimtbl set name = 'Oskar' FROM functional_kudu.dimtbl");
    // Duplicate aliases are illegal
    AnalysisError(
        "update dimtbl set name = 'Oskar' FROM functional_kudu.dimtbl, functional" +
            ".alltypes dimtbl", "Duplicate table alias: 'dimtbl'");
    // Location of the kudu table doesnt matter
    AnalyzesOk(
        "update a set a.name = 'Oskar' from functional.testtbl b, dimtbl a where b.id =" +
        " a.id ", createAnalysisCtx("functional_kudu"));
    AnalyzesOk("update a set name = 'Oskar' from functional_kudu.testtbl a");
    AnalysisError(
        "update functional_kudu.testtbl set name = 'Oskar' from functional_kudu.dimtbl",
        "'functional_kudu.testtbl' is not a valid table alias or reference.");
    // Order of target reference is not important
    AnalyzesOk(
        "update a set a.name = b.name FROM functional.testtbl b join functional_kudu" +
        ".testtbl a on a.id = b.id where a.id = 10");

    AnalyzesOk("delete from functional_kudu.testtbl");
    AnalyzesOk("delete functional_kudu.testtbl from functional_kudu.testtbl");
    AnalyzesOk("delete a from functional_kudu.testtbl a");
    AnalyzesOk("delete a from functional_kudu.testtbl a join functional.testtbl b " +
        "on a.zip = b.zip");
  }

  @Test
  public void TestUpdate() {
    TestUtils.assumeKuduIsSupported();
    AnalyzesOk("update functional_kudu.dimtbl set name = 'Oskar'");
    // Correct default database resolution
    AnalyzesOk("update dimtbl set name = 'Oskar'",
        createAnalysisCtx("functional_kudu"));
    // Correct table alias resolution
    AnalyzesOk("update functional_kudu.dimtbl set name = '10'");
    // Check type compatibility, zip is int, 4711 is smallint
    AnalyzesOk("update functional_kudu.testtbl set zip = 4711");
    // Non-compatible types
    AnalysisError("update functional_kudu.dimtbl set name = name < '10'",
        "Target table 'functional_kudu.dimtbl' is incompatible with source expressions.\n"
            + "Expression 'name < '10'' (type: BOOLEAN) is not compatible with column "
            + "'name' (type: STRING)");
    AnalysisError("update functional_kudu.dimtbl set name = name < 10",
        "operands of type STRING and TINYINT are not comparable: name < 10");
    // Expression with joined slot ref overflows type
    AnalysisError(
        "update a set a.zip = b.zip * a.zip from functional_kudu.testtbl a, functional" +
            ".testtbl b",
        "Possible loss of precision for target table 'functional_kudu.testtbl'.\n"
            + "Expression 'b.zip * a.zip' (type: BIGINT) would need to be cast to INT for"
            + " column 'zip'");
    // Explicit casting the expression
    AnalyzesOk(
        "update a set a.zip = cast(b.zip * a.zip as int) from functional_kudu.testtbl " +
            "a, functional.testtbl b");
    // Smallint is implicitly castable
    AnalyzesOk(
        "update a set a.zip = cast(4 as smallint) from functional_kudu.testtbl " +
            "a, functional.testtbl b");
    // Simple SlotRef
    AnalyzesOk("update functional_kudu.dimtbl set name = name");
    AnalyzesOk("update functional_kudu.dimtbl set name = name, zip = 10");
    // Expressions in set value
    AnalyzesOk("update functional_kudu.dimtbl set name = substr('hallo', 3)");
    // Only Kudu tables can be updated
    AnalysisError("update functional.alltypes set intcol = 99",
        "Impala does not support modifying a non-Kudu table: functional.alltypes");
    // Non existing column in update
    AnalysisError("update functional_kudu.dimtbl set links='10'",
        "Could not resolve column/field reference: 'links'");
    // RHS of the join cannot be right side of a left semi join
    AnalysisError(
        "update foo set foo.name = 'Oskar' from functional.dimtbl a left semi join " +
            "functional_kudu.dimtbl foo on a.zip = foo.zip",
        "Illegal column/field reference 'foo.id' of semi-/anti-joined table 'foo'");
    // No key column update
    AnalysisError("update functional_kudu.dimtbl set id=99 where name = '10'",
        "Key column 'id' cannot be updated.");
    // Duplicate target columns are not allowed
    AnalysisError(
        "update functional_kudu.dimtbl set name = '10', name = cast(4 * 3 as string) " +
            "where name = '9'",
        "Duplicate value assignment to column: 'name'");
  }

  @Test
  public void TestWhereClause() {
    TestUtils.assumeKuduIsSupported();
    // With where clause
    AnalyzesOk("update functional_kudu.dimtbl set name = '10' where name = '11'");
    // Complex where clause
    AnalyzesOk(
        "update functional_kudu.dimtbl set name = '10' where name < '11' and name " +
            "between '1' and '10'");
    AnalyzesOk("delete from functional_kudu.testtbl where id < 9");
    AnalyzesOk("delete from functional_kudu.testtbl where " +
        "id in (select id from functional.testtbl)");
  }

  @Test
  public void TestWithSourceStmtRewrite() {
    TestUtils.assumeKuduIsSupported();
    // No subqueries in set statement as we cannot translate them into subqueries in
    // the select list
    AnalysisError(
        "update a set name = (select name from functional.testtbl b where b.zip = a.zip" +
            " limit 1) from functional_kudu.testtbl a",
        "Subqueries are not supported as update expressions for column 'name'");

    AnalysisError(
        "update functional_kudu.testtbl set zip = 1 + (select count(*) from functional" +
            ".alltypes)",
        "Subqueries are not supported as update expressions for column 'zip'");

    // Subqueries in where condition
    AnalyzesOk("update functional_kudu.dimtbl set name = '10' where " +
        "name in (select name from functional.dimtbl)");
    AnalyzesOk("delete functional_kudu.dimtbl where " +
        "name in (select name from functional.dimtbl)");
  }

  @Test
  public void TestWithJoin() {
    TestUtils.assumeKuduIsSupported();
    // Simple Join
    AnalyzesOk(
        "update a set a.name = b.name FROM functional_kudu.testtbl a join functional" +
        ".testtbl b on a.id = b.id where a.id = 10");
    AnalyzesOk(
        "delete a from functional_kudu.testtbl a join functional.testtbl b on " +
            "a.id = b.id where a.id = 10");
    // Wrong target table
    AnalysisError(
        "update a set b.name = 'Oskar' FROM functional_kudu.testtbl a join functional" +
            ".testtbl b on a.id = b.id where a.id = 10",
        "Left-hand side column 'b.name' in assignment expression 'b.name='Oskar'' does " +
            "not belong to target table 'functional_kudu.testtbl'");
    AnalysisError(
        "update a set b.name = b.other  FROM functional_kudu.testtbl a join functional" +
            ".testtbl b on a.id = b.id where a.id = 10",
        "Could not resolve column/field reference: 'b.other'");
    // Join with values clause as data
    AnalyzesOk(
        "update a set a.name = 'values' FROM functional_kudu.testtbl a join (values (1 " +
            "as ids,2,3) ) b on a.id = b.ids");
    AnalyzesOk(
        "delete a FROM functional_kudu.testtbl a join (values (1 " +
            "as ids,2,3) ) b on a.id = b.ids");
    AnalysisError(
        "update a set b.name =" +
            " 'Oskar' FROM functional.testtbl a join functional_kudu.testtbl b",
        "Impala does not support modifying a non-Kudu table: functional.testtbl");
    AnalysisError(
        "delete a FROM functional.testtbl a join functional_kudu.testtbl b",
        "Impala does not support modifying a non-Kudu table: functional.testtbl");
  }

  @Test
  public void TestNoViewModification() {
    TestUtils.assumeKuduIsSupported();
    AnalysisError("update functional.alltypes_view set id = 10", "Cannot modify view");
    AnalysisError("delete functional.alltypes_view", "Cannot modify view");
  }

  @Test
  public void TestNoNestedTypes() {
    TestUtils.assumeKuduIsSupported();
    AnalysisError(
        "update a set c.item = 10 FROM functional_kudu.testtbl a, functional" +
        ".allcomplextypes b, b.int_array_col c",
        "Left-hand side column 'c.item' in assignment expression 'c.item=10' does not " +
            "belong to target table 'functional_kudu.testtbl'");

    AnalysisError(
        "update a set a.zip = b.int_array_col FROM functional_kudu.testtbl a, " +
            "functional.allcomplextypes b, b.int_array_col c",
        "Target table 'functional_kudu.testtbl' is incompatible with source "
            + "expressions.\nExpression 'b.int_array_col' (type: ARRAY<INT>) is not "
            + "compatible with column 'zip' (type: INT)");

    AnalysisError("update functional.allcomplextypes.int_array_col set item = 10",
        "'functional.allcomplextypes.int_array_col' is not a valid table alias or " +
        "reference.");
  }
}
