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

public class AnalyzeUpsertStmtTest extends AnalyzerTest {
  @Test
  public void TestUpsert() {
    TestUtils.assumeKuduIsSupported();
    // VALUES clause
    AnalyzesOk("upsert into table functional_kudu.testtbl values(1, 'a', 1)");
    AnalyzesOk("upsert into table functional_kudu.testtbl(id) values(1)");
    AnalyzesOk("upsert into table functional_kudu.testtbl values(1, 'a', 1), " +
        "(2, 'b', 2), (3, 'c', 3)");
    // SELECT clause
    AnalyzesOk("upsert into functional_kudu.testtbl select bigint_col, string_col, " +
        "int_col from functional.alltypes");
    // Permutation lists
    AnalyzesOk("upsert into table functional_kudu.testtbl(id) select bigint_col " +
        "from functional.alltypes");
    AnalyzesOk("upsert into table functional_kudu.testtbl(id, name) select bigint_col, " +
        "string_col from functional.alltypes");
    AnalyzesOk("upsert into table functional_kudu.testtbl(name, zip, id) select " +
        "string_col, int_col, bigint_col from functional.alltypes");
    // WITH clause
    AnalyzesOk("with t1 as (select 1, 'a', 2) upsert into functional_kudu.testtbl " +
        "select * from t1");
    AnalyzesOk("with t1 as (select * from functional.alltypes) upsert into " +
        "functional_kudu.testtbl select bigint_col, string_col, int_col from t1");
    // WITH belonging to the select clause
    AnalyzesOk("upsert into functional_kudu.testtbl with t1 as (select * from " +
        "functional.alltypes) select bigint_col, string_col, int_col from t1");
    AnalyzesOk("upsert into functional_kudu.testtbl(id) with t1 as (select * from " +
        "functional.alltypes) select bigint_col from t1");
    // Multiple WITH clauses
    AnalyzesOk("with t1 as (select * from functional.alltypestiny) " +
        "upsert into functional_kudu.testtbl with t2 as (select * from " +
        "functional.alltypessmall) select bigint_col, string_col, int_col from t1");
    // Correlated inline view
    AnalyzesOk("upsert into table functional_kudu.testtbl " +
        "select a.id, string_col, b.month " +
        "from functional.alltypes a, functional.allcomplextypes b, " +
        "(select item from b.int_array_col) v1 " +
        "where a.id = b.id");
    // Hint
    AnalyzesOk("upsert into table functional_kudu.testtbl [clustered] select * from " +
        "functional_kudu.testtbl");

    // Key columns missing from permutation
    AnalysisError("upsert into functional_kudu.testtbl(zip) values(1)",
        "All primary key columns must be specified for UPSERTing into Kudu tables. " +
        "Missing columns are: id");
    // SELECT clause with wrong number of columns
    AnalysisError("upsert into functional_kudu.testtbl select * from functional.alltypes",
        "Target table 'functional_kudu.testtbl' has fewer columns (3) than the SELECT " +
        "/ VALUES clause returns (13)");
    // VALUES clause with wrong number of columns
    AnalysisError("upsert into functional_kudu.testtbl values(1)", "Target table " +
        "'functional_kudu.testtbl' has more columns (3) than the SELECT / VALUES " +
        "clause returns (1)");
    // Permutation with wrong number of columns
    AnalysisError("upsert into functional_kudu.testtbl(id, name, zip) values(1)",
        "Column permutation mentions more columns (3) than the SELECT / VALUES " +
        "clause returns (1)");
    // Type mismatch
    AnalysisError("upsert into functional_kudu.testtbl values(1, 1, 1)",
        "Target table 'functional_kudu.testtbl' is incompatible with source " +
        "expressions.\nExpression '1' (type: TINYINT) is not compatible with column " +
        "'name' (type: STRING)");
    // Permutation with type mismatch
    AnalysisError("upsert into functional_kudu.testtbl(zip, id, name) " +
        "values('a', 'a', 'a')", "Target table 'functional_kudu.testtbl' is " +
        "incompatible with source expressions.\nExpression ''a'' (type: STRING) is not " +
        "compatible with column 'zip' (type: INT)");
    // Permutation with invalid column name
    AnalysisError("upsert into functional_kudu.testtbl (id, name, invalid) values " +
        "(1, 'a', 1)", "Unknown column 'invalid' in column permutation");
    // Permutation with repeated column
    AnalysisError("upsert into functional_kudu.testtbl (id, name, zip, id) values " +
        "(1, 'a', 1, 1)", "Duplicate column 'id' in column permutation");
    // UPSERT into non-Kudu table
    AnalysisError("upsert into functional.alltypes select * from functional.alltypes",
        "UPSERT is only supported for Kudu tables");
    // Unknown target DB
    AnalysisError("upsert into UNKNOWNDB.testtbl select * " +
        "from functional.alltypesnopart",
        "Database does not exist: UNKNOWNDB");
    // WITH-clause tables cannot be upserted into
    AnalysisError("with t1 as (select 'a' x) upsert into t1 values('b' x)",
        "Table does not exist: default.t1");
    // Cannot upsert into a view
    AnalysisError("upsert into functional.alltypes_view select * from " +
        "functional.alltypes",
        "Impala does not support UPSERTing into views: functional.alltypes_view");
    // Upsert with uncorrelated inline view
    AnalysisError("upsert into table functional_kudu.testtbl " +
        "select a.id, a.string_col, b.month " +
        "from functional.alltypes a, functional.allcomplextypes b, " +
        "(select item from b.int_array_col, functional.alltypestiny) v1 " +
        "where a.id = b.id",
        "Nested query is illegal because it contains a table reference " +
        "'b.int_array_col' correlated with an outer block as well as an " +
        "uncorrelated one 'functional.alltypestiny':\n" +
        "SELECT item FROM b.int_array_col, functional.alltypestiny");
    // Illegal complex-typed expr
    AnalysisError("upsert into functional_kudu.testtbl " +
        "select int_struct_col from functional.allcomplextypes",
        "Expr 'int_struct_col' in select list returns a " +
        "complex type 'STRUCT<f1:INT,f2:INT>'.\n" +
        "Only scalar types are allowed in the select list.");
  }
}
