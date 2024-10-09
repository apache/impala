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
        "Impala only supports modifying Kudu and Iceberg tables, but the following "+
        "table is neither: functional.alltypes");
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
        "Impala only supports modifying Kudu and Iceberg tables, but the following " +
        "table is neither: functional.testtbl");
    AnalysisError(
        "delete a FROM functional.testtbl a join functional_kudu.testtbl b",
        "Impala only supports modifying Kudu and Iceberg tables, but the " +
        "following table is neither: functional.testtbl");
  }

  @Test
  public void TestNoViewModification() {
    AnalysisError("update functional.alltypes_view set id = 10", "Cannot modify view");
    AnalysisError("delete functional.alltypes_view", "Cannot modify view");
  }

  @Test
  public void TestNoNestedTypes() {
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

  @Test
  public void TestIcebergMerge() {
    // UPDATE
    AnalyzesOk("merge into "
        + "functional_parquet.iceberg_v2_partitioned_position_deletes target "
        + "using (select * from functional_parquet.iceberg_non_partitioned) "
        + "source "
        + "on target.id = source.id "
        + "when matched and target.id > 10 then update set user = source.user");
    AnalyzesOk("merge into "
        + "functional_parquet.iceberg_v2_partitioned_position_deletes "
        + "using "
        + "(select * from functional_parquet.iceberg_non_partitioned) source "
        + "on functional_parquet.iceberg_v2_partitioned_position_deletes.id = source.id "
        + "when matched and "
        + "functional_parquet.iceberg_v2_partitioned_position_deletes.id > 10 "
        + "then update set user = source.user, action = source.action");
    // LHS expression targets source
    AnalysisError("merge into "
            + "functional_parquet.iceberg_v2_partitioned_position_deletes target "
            + "using "
            + "(select * from functional_parquet.iceberg_non_partitioned) "
            + "source "
            + "on target.id = source.id when matched and target.id > 10 "
            + "then update set source.user = target.user;",
        "Left-hand side column 'source.`user`' in assignment "
            + "expression 'source.`user`=target.`user`' does not belong to "
            + "target table "
            + "'functional_parquet.iceberg_v2_partitioned_position_deletes'");
    // DELETE
    AnalyzesOk("merge into "
        + "functional_parquet.iceberg_v2_partitioned_position_deletes target "
        + "using "
        + "(select * from functional_parquet.iceberg_non_partitioned) source "
        + "on target.id = source.id "
        + "when matched and target.id > 10 then delete");
    AnalyzesOk("merge into "
        + "functional_parquet.iceberg_v2_partitioned_position_deletes target "
        + "using "
        + "(select * from functional_parquet.iceberg_non_partitioned) source "
        + "on target.id = source.id when matched then delete");
    AnalyzesOk("merge into "
        + "functional_parquet.iceberg_v2_partitioned_position_deletes target "
        + "using functional_parquet.iceberg_non_partitioned "
        + "on target.id = functional_parquet.iceberg_non_partitioned.id "
        + "when matched then delete");
    // INSERT
    // Inserting values originated from the target table (NULL values)
    AnalyzesOk("merge into "
        + "functional_parquet.iceberg_v2_partitioned_position_deletes t "
        + "using (select * from functional_parquet.iceberg_non_partitioned) s "
        + "on t.id = s.id "
        + "when not matched "
        + "then insert (id, user, action) values(t.id, t.user, t.action)");
    // Regular cases
    AnalyzesOk("merge into "
        + "functional_parquet.iceberg_v2_partitioned_position_deletes t "
        + "using (select * from functional_parquet.iceberg_non_partitioned) s "
        + "on t.id = s.id "
        + "when not matched "
        + "then insert (id, user, action) values(s.id, s.user, s.action)");
    AnalyzesOk("merge into "
        + "functional_parquet.iceberg_v2_partitioned_position_deletes t "
        + "using (select * from functional_parquet.iceberg_non_partitioned) s "
        + "on t.id = s.id "
        + "when not matched then "
        + "insert values(s.id, s.user, s.action, s.event_time)");
    AnalyzesOk("merge into "
        + "functional_parquet.iceberg_v2_partitioned_position_deletes t "
        + "using (select * from functional_parquet.iceberg_non_partitioned) s "
        + "on t.id = s.id "
        + "when not matched then "
        + "insert values(s.id, s.user, s.action, s.event_time) "
        + "when not matched and t.id = 1 "
        + "then insert values(s.id, s.user, s.action, s.event_time)");
    AnalyzesOk("merge into "
        + "functional_parquet.iceberg_v2_partitioned_position_deletes t "
        + "using (select * from functional_parquet.iceberg_non_partitioned) s "
        + "on t.id = s.id and t.user = s.user "
        + "when not matched then "
        + "insert values(s.id, s.user, s.action, s.event_time) "
        + "when not matched and t.id = 1 then "
        + "insert values(s.id, s.user, s.action, s.event_time)");
    AnalyzesOk("merge into "
        + "functional_parquet.iceberg_v2_partitioned_position_deletes t "
        + "using (select * from functional_parquet.iceberg_non_partitioned) s "
        + "on t.id = s.id and t.user <> s.user "
        + "when not matched then "
        + "insert values(s.id, s.user, s.action, s.event_time) "
        + "when not matched and t.id = 1 "
        + "then insert values(s.id, s.user, s.action, s.event_time)");
    // Multiple MATCHED and NOT MATCHED cases
    AnalyzesOk("merge into "
        + "functional_parquet.iceberg_v2_partitioned_position_deletes target "
        + "using functional_parquet.iceberg_non_partitioned s "
        + "on target.id = s.id "
        + "when matched and target.id = 10 then update set user = 'b' "
        + "when matched and target.id = 11 then delete "
        + "when not matched and s.id = 13 then "
        + "insert values(s.id, s.user, target.user, target.event_time) "
        + "when matched and target.id = 14 then update set user = 'a' "
        + "when matched then delete "
        + "when not matched and s.id = 14 "
        + "then insert (id, user) values (s.id, concat(s.user, 'string'))"
        + "when not matched then "
        + "insert values (s.id, s.user, 'ab', s.event_time);");
    // NOT MATCHED BY TARGET
    AnalyzesOk("merge into "
        + "functional_parquet.iceberg_v2_partitioned_position_deletes target "
        + "using functional_parquet.iceberg_non_partitioned s "
        + "on target.id = s.id "
        + "when not matched by target and s.id <= 12 "
        + "then insert (id, user) values (s.id, 'user')");
    // NOT MATCHED BY SOURCE
    AnalyzesOk("merge into "
        + "functional_parquet.iceberg_v2_partitioned_position_deletes target "
        + "using functional_parquet.iceberg_non_partitioned s "
        + "on target.id = s.id "
        + "when not matched by source "
        + "then update set user = 'updated'");
    AnalyzesOk("merge into "
        + "functional_parquet.iceberg_v2_partitioned_position_deletes target "
        + "using functional_parquet.iceberg_non_partitioned s "
        + "on target.id = s.id "
        + "when not matched by source and target.user = 'something' then delete");
    // INSERT *
    AnalyzesOk("merge into "
        + "functional_parquet.iceberg_v2_partitioned_position_deletes target "
        + "using functional_parquet.iceberg_non_partitioned s "
        + "on target.id = s.id when not matched then insert *");
    AnalyzesOk("merge into "
        + "functional_parquet.iceberg_v2_partitioned_position_deletes target "
        + "using functional_parquet.iceberg_non_partitioned s "
        + "on target.id = s.id when not matched by target then insert *");
    // UPDATE SET *
    AnalyzesOk("merge into "
        + "functional_parquet.iceberg_v2_partitioned_position_deletes target "
        + "using functional_parquet.iceberg_non_partitioned s "
        + "on target.id = s.id when matched then update set *");
    AnalyzesOk("merge into "
        + "functional_parquet.iceberg_v2_partitioned_position_deletes target "
        + "using functional_parquet.iceberg_non_partitioned s "
        + "on target.id = s.id when not matched by source then update set *");
    // Query rewrite in the source subquery
    AnalyzesOk("merge into functional_parquet.iceberg_partition_evolution t "
        + "using (select * from functional_parquet.iceberg_non_partitioned where "
        + "id in (select max(id) from functional_parquet.iceberg_non_partitioned)) s "
        + "on t.id = s.id "
        + "when matched and s.id > 2 then delete");

    // Inline view as target
    AnalysisError("merge into "
            + "(select * from "
            + "functional_parquet.iceberg_v2_partitioned_position_deletes) t "
            + "using "
            + "(select * from functional_parquet.iceberg_non_partitioned) s "
            + "on t.id = s.id "
            + "when not matched then "
            + "insert (id, user, action) values(id, user, action)",
        "Cannot modify view: "
            + "(SELECT * FROM "
            + "functional_parquet.iceberg_v2_partitioned_position_deletes) t");
    // Referencing source as target
    AnalysisError("merge into source_alias target_alias "
            + "using functional_parquet.iceberg_non_partitioned source_alias "
            + "on target_alias.id = source_alias.id "
            + "when not matched then "
            + "insert (id, user, action) values(id, user, action)",
        "Could not resolve table reference: 'source_alias'");
    // Ambiguous column reference
    AnalysisError("merge into "
            + "functional_parquet.iceberg_v2_partitioned_position_deletes t "
            + "using "
            + "(select * from functional_parquet.iceberg_non_partitioned) s "
            + "on t.id = s.id "
            + "when not matched then "
            + "insert (id, user, action) values(id, user, action)",
        "Column/field reference is ambiguous: 'id'");
    // Ambiguous table reference
    AnalysisError("merge into "
            + "functional_parquet.iceberg_v2_partitioned_position_deletes "
            + "using functional_parquet.iceberg_v2_partitioned_position_deletes "
            + "on iceberg_v2_partitioned_position_deletes.id = "
            + "iceberg_v2_partitioned_position_deletes.id "
            + "when not matched then "
            + "insert (id, user, action) values(id, user, action)",
        "Duplicate table alias: "
            + "'functional_parquet.iceberg_v2_partitioned_position_deletes'");
    // Star in values clause
    AnalysisError("merge into "
            + "functional_parquet.iceberg_v2_partitioned_position_deletes t "
            + "using "
            + "(select * from functional_parquet.iceberg_non_partitioned) s "
            + "on t.id = s.id "
            + "when not matched then insert (id, user, action) values(t.*)",
        "Invalid expression: t.*");
    // Fewer columns in VALUES
    AnalysisError("merge into "
            + "functional_parquet.iceberg_v2_partitioned_position_deletes t "
            + "using "
            + "(select * from functional_parquet.iceberg_non_partitioned) s "
            + "on t.id = s.id "
            + "when not matched then insert (id, user, action) values(t.user)",
        "Column permutation mentions more columns (3) than the VALUES clause"
            + " returns (1): WHEN NOT MATCHED BY TARGET THEN INSERT (id, user, action)"
            + " VALUES (t.`user`)");
    // More columns in VALUES
    AnalysisError("merge into "
            + "functional_parquet.iceberg_v2_partitioned_position_deletes t "
            + "using "
            + "(select * from functional_parquet.iceberg_non_partitioned) s "
            + "on t.id = s.id "
            + "when not matched then "
            + "insert (id) values(t.id, t.user, t.action)",
        "Column permutation mentions fewer columns (1) than the VALUES clause"
            + " returns (3): WHEN NOT MATCHED BY TARGET THEN INSERT (id) "
            + "VALUES (t.id, t.`user`, t.action)");
    AnalysisError("merge into "
            + "functional_parquet.iceberg_v2_partitioned_position_deletes t "
            + "using "
            + "(select * from functional_parquet.iceberg_non_partitioned) s "
            + "on t.id = s.id "
            + "when not matched then insert (id, user, action) "
            + "values(t.id, t.action, t.user, t.user)",
        "Column permutation mentions fewer columns (3) than the VALUES clause"
            + " returns (4): WHEN NOT MATCHED BY TARGET THEN INSERT (id, user, action) "
            + "VALUES (t.id, t.action, t.`user`, t.`user`)");
    // Unresolved reference in WHEN NOT MATCHED clause
    AnalysisError("merge into "
            + "functional_parquet.iceberg_v2_partitioned_position_deletes t "
            + "using (select * from functional_parquet.iceberg_non_partitioned) s "
            + "on t.id = s.id "
            + "when not matched then "
            + "insert values(s.id, s.user, s.action, s.event_time) "
            + "when not matched and a.id = 1 then "
            + "insert values(s.id, s.user, s.action)",
        "Could not resolve column/field reference: 'a.id'");
    AnalysisError("merge into "
            + "functional_parquet.iceberg_v2_partitioned_position_deletes t "
            + "using (select * from functional_parquet.iceberg_non_partitioned "
            + "where id = t.id) s "
            + "on t.id = s.id "
            + "when matched then delete",
        "Could not resolve column/field reference: 't.id'");
    // Integer as filter expression
    AnalysisError("merge into functional_parquet.iceberg_partition_evolution t "
            + "using functional_parquet.iceberg_non_partitioned s "
            + "on t.id = s.id "
            + "when matched and s.id then delete",
        "Filter expression requires return type 'BOOLEAN'. Actual type is 'INT'");
    // UPDATE SET * with different column lists
    AnalysisError("merge into functional_parquet.iceberg_partition_evolution t "
            + "using functional_parquet.iceberg_non_partitioned s "
            + "on t.id = s.id when matched and s.id > 2 then update set *",
        "Target table has more columns (6) than the source expression (4): "
            + "WHEN MATCHED AND s.id > 2 THEN UPDATE SET id = id, int_col = user, "
            + "string_col = action, date_string_col = event_time");
    AnalysisError("merge into functional_parquet.iceberg_timestamp_part t "
            + "using functional_parquet.iceberg_partition_evolution s "
            + "on t.i = s.id when matched and s.id > 2 then update set *",
        "Target table has fewer columns (2) than the source expression (6): "
            + "WHEN MATCHED AND s.id > 2 THEN UPDATE SET i = id, ts = int_col");
    // UPDATE SET * with different column types
    AnalysisError("merge into functional_parquet.iceberg_partition_evolution t "
            + "using (select int_col, id, date_string_col, month, string_col, year from "
            + "functional_parquet.iceberg_partition_evolution) s "
            + "on t.id = s.id when matched and s.id > 2 then update set *",
        "Target table 'functional_parquet.iceberg_partition_evolution' is incompatible"
            + " with source expressions.\nExpression 's.`month`' (type: INT) is not "
            + "compatible with column 'date_string_col' (type: STRING)");
    // INSERT * with different column lists
    AnalysisError("merge into functional_parquet.iceberg_partition_evolution t "
            + "using functional_parquet.iceberg_non_partitioned s "
            + "on t.id = s.id when not matched and s.id > 2 then insert *",
        "Column permutation mentions more columns (6) than the source "
            + "expression (4): WHEN NOT MATCHED BY TARGET AND s.id > 2 "
            + "THEN INSERT (id, int_col, string_col, date_string_col, year, month) "
            + "VALUES (s.id, s.`user`, s.action, s.event_time)");
    AnalysisError("merge into functional_parquet.iceberg_timestamp_part t "
            + "using functional_parquet.iceberg_non_partitioned s "
            + "on t.i = s.id when not matched and s.id > 2 then insert *",
        "Column permutation mentions fewer columns (2) than the source "
            + "expression (4): WHEN NOT MATCHED BY TARGET AND s.id > 2 THEN INSERT "
            + "(i, ts) VALUES (s.id, s.`user`, s.action, s.event_time)");
    // INSERT * with different column types
    AnalysisError("merge into functional_parquet.iceberg_partition_evolution t "
            + "using (select int_col, id, date_string_col, month, string_col, year from "
            + "functional_parquet.iceberg_partition_evolution) s "
            + "on t.id = s.id when not matched then insert *",
        "Target table 'functional_parquet.iceberg_partition_evolution' is incompatible"
            + " with source expressions.\nExpression 's.`month`' (type: INT) is not "
            + "compatible with column 'date_string_col' (type: STRING)");
    // Target table contains equality delete files
    AnalysisError("merge into functional_parquet.iceberg_v2_delete_equality t "
            + "using functional_parquet.iceberg_v2_delete_equality s "
            + "on t.id = s.id when not matched then insert *",
        "MERGE statement is not supported for Iceberg tables "
            + "containing equality deletes.");
  }
}
