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

#include "observe/otel.h"

#include <string>
#include <string_view>

#include <boost/algorithm/string/replace.hpp>
#include <gtest/gtest.h>
#include "gutil/strings/substitute.h"

#include "gen-cpp/Query_types.h"
#include "testutil/scoped-flag-setter.h"

using namespace std;
using namespace impala;

DECLARE_bool(otel_trace_beeswax);

TEST(OtelTest, QueriesTraced) {
  const auto runtest = [](const string_view sql_str) -> void {
    string formatted_sql(sql_str);
    boost::replace_all(formatted_sql, "\n", "\\n");

    EXPECT_TRUE(should_otel_trace_query(sql_str, TSessionType::HIVESERVER2))
        << "Query Not Traced: " << formatted_sql;
  };

  runtest("SELECT * FROM foo");
  runtest("WITH alltypes_tiny1 AS (SELECT * FROM functional.alltypes)");
  runtest("INSERT INTO functional.alltypes (id) VALUES (99999)");
  runtest("CREATE TABLE foo.bar (id int, string_col string)");
  runtest("UPDATE foo.bar SET string_col='a'");
  runtest("ALTER TABLE foo.bar ADD COLUMNS (new_col string)");
  runtest("DELETE FROM foo.bar WHERE id=1");
  runtest("COMPUTE STATS foo.bar");
  runtest("COMPUTE INCREMENTAL STATS foo.bar PARTITION (month=1)");
  runtest("INVALIDATE METADATA foo.bar");
  runtest("DROP TABLE foo.bar PURGE");
  runtest("-- comment1\nSELECT 1");
  runtest("-- comment1\n SELECT 1");
  runtest("-- comment1/*comment2*/\nSELECT 1");
  runtest("-- comment1\n/*comment2*/SELECT 1");
  runtest("--comment1\n--comment2\nSELECT 1");
  runtest("--comment1\n  --comment2\nSELECT 1");
  runtest("--comment1  \n  --comment2\nSELECT 1");
  runtest("--comment1  \n  --comment2  \n   SELECT 1");
  runtest("/*comment1*/SELECT 1");
  runtest("/*comment1*/ SELECT 1");
  runtest("/*comment1*/\nSELECT 1");
  runtest("/*comment1*/  \n  SELECT 1");
  runtest("/*comment1*/  \n /* comment 2 */  \n SELECT 1");
  runtest("/*comment1*/ /*comment2*/SELECT 1");
  runtest("/*comment1*/   /*comment2*/SELECT 1");
  runtest("/*comment1*/ SELECT /* inline */ 1");
  runtest("/*comment1*/   SELECT /* inline */ 1");
  runtest("/*comment1*/SELECT /* inline */ 1 /* ending */");
  runtest("/*comment1*/ SELECT /* inline */ 1 /* ending */");
  runtest("/*comment1*/   SELECT /* inline */ 1 /* ending */");
  runtest("/*comment1*/  --comment2 \n  SELECT 1");
  runtest("--comment1\nSELECT /* inline */ 1 /* ending */");
  runtest("--comment1 \n SELECT /* inline */ 1 /* ending */");
  runtest("--comment1  \n  SELECT /* inline */ 1 /* ending */");
  runtest("--comment1 /*inline*/\nSELECT /* inline */ 1 /* ending */");
  runtest("--comment1 /*inline*/  \n SELECT /* inline */ 1 /* ending */");
  runtest("SELECT 'SELECT'");
  runtest("SELECT `SELECT` from tbl");
  runtest("-- comment1  \n  SELECT `SELECT` from tbl");
  runtest("-- comment1  \n  --comment2\nSELECT `SELECT` from tbl");
  runtest("/*comment1*/SELECT `SELECT` from tbl");
  runtest("/*comment1*/  \n SELECT `SELECT` from tbl");
  runtest("/*comment1*/  --comment2 \n  SELECT `SELECT` from tbl");

  auto run_newline_test = [&runtest](const string keyword, const string rest) -> void {
    runtest(strings::Substitute("$0\n$1", keyword, rest));
    runtest(strings::Substitute("$0  \n$1", keyword, rest));
    runtest(strings::Substitute("$0\n  $1", keyword, rest));
    runtest(strings::Substitute("$0  \n  $1", keyword, rest));
    runtest(strings::Substitute("/*/ comment */$0  \n  $1", keyword, rest));
    runtest(strings::Substitute("$0/* inline comment*/$1", keyword, rest));
    runtest(strings::Substitute("--comment\n$0/* inline comment*/$1", keyword, rest));
    runtest(strings::Substitute("/*comment 1*/\n$0/* inline comment*/$1", keyword, rest));
    runtest(strings::Substitute("--comment\n$0  \n$1", keyword, rest));
    runtest(strings::Substitute("/*comment1*/ --comment2\n$0\n$1", keyword, rest));
    runtest(strings::Substitute("/*comment1*/ --comment2\n$0  \n$1", keyword, rest));
    runtest(strings::Substitute("/*comment1*/ --comment2\n  $0\n$1", keyword, rest));
    runtest(strings::Substitute("/*comment1*/ --comment2\n  $0  \n$1", keyword, rest));
    runtest(strings::Substitute("/*comm1*/ --comm2\n$0/*comm3*/\n$1", keyword, rest));
  };

  run_newline_test("SELECT", "* FROM FOO");
  run_newline_test("ALTER", "TABLE FOO");
  run_newline_test("COMPUTE", "STATS FOO");
  run_newline_test("CREATE", "TABLE FOO");
  run_newline_test("DELETE", "TABLE FOO");
  run_newline_test("DROP", "TABLE FOO");
  run_newline_test("INSERT", "INTO TABLE FOO");
  run_newline_test("INVALIDATE", "METADATA FOO");
  run_newline_test("WITH", "T1 AS SELECT * FROM FOO");

  // Beeswax queries are traced when the otel_trace_beeswax flag is set.
  {
    auto trace_beeswax_setter =
        ScopedFlagSetter<bool>::Make(&FLAGS_otel_trace_beeswax,true);

    EXPECT_TRUE(should_otel_trace_query("SELECT * FROM foo", TSessionType::BEESWAX));
  }
}

TEST(OtelTest, QueriesNotTraced) {
  const auto runtest = [](string sql_str) -> void {
    string formatted_sql(sql_str);
    boost::replace_all(formatted_sql, "\n", "\\n");

    EXPECT_FALSE(should_otel_trace_query(sql_str, TSessionType::HIVESERVER2))
        << "Query traced but should not be: " << formatted_sql;
  };

  runtest("COMMENT ON DATABASE {} IS 'test'");
  runtest("DESCRIBE {}");
  runtest("EXPLAIN SELECT * FROM {}");
  runtest("REFRESH FUNCTIONS functional");
  runtest("REFRESH\nFUNCTIONS functional");
  runtest("REFRESH  \nFUNCTIONS functional");
  runtest("REFRESH\n  FUNCTIONS functional");
  runtest("REFRESH  \n  FUNCTIONS functional");
  runtest("REFRESH TABLE functional");
  runtest("REFRESH\nTABLE functional");
  runtest("REFRESH  \nTABLE functional");
  runtest("REFRESH\n  TABLE functional");
  runtest("REFRESH  \n  TABLE functional");
  runtest("SET ALL");
  runtest("SHOW TABLES IN {}");
  runtest("SHOW DATABASES");
  runtest("TRUNCATE TABLE {}");
  runtest("USE functional");
  runtest("VALUES (1, 2, 3)");
  runtest("KILL QUERY '1234:5678'");
  runtest("/*comment1*/SET EXPLAIN_LEVEL=0");
  runtest("/*comment1*/   SET EXPLAIN_LEVEL=0");
  runtest("--comment1\nSET EXPLAIN_LEVEL=0");
  runtest("--comment1  \n  SET EXPLAIN_LEVEL=0");
  runtest("/* comment1 */--comment1  \n  SET EXPLAIN_LEVEL=0");
  runtest("/* comment1 */  --comment1  \n  SET EXPLAIN_LEVEL=0");
  runtest("REFRESH AUTHORIZATION");
  runtest("REFRESH\nAUTHORIZATION");
  runtest("REFRESH  \nAUTHORIZATION");
  runtest("REFRESH\n  AUTHORIZATION");
  runtest("REFRESH  \n  AUTHORIZATION");
  runtest("/*comment not terminated select 1");
  runtest("/*comment1*/ /*comment 2 not terminated select 1");
  runtest("/*comment only*/");
  runtest("--comment only");
  runtest("--comment only\n");
  runtest("--comment only\n--comment only 2");
  runtest("--comment only\n--comment only 2\n");
  // TODO: Move to the QueriesTraced test case one IMPALA-14370 is fixed.
  runtest(strings::Substitute("/*/ comment */select * from tbl"));

  // Beeswax queries are not traced unless the otel_trace_beeswax flag is set.
  {
    auto trace_beeswax_setter =
        ScopedFlagSetter<bool>::Make(&FLAGS_otel_trace_beeswax, false);

    EXPECT_FALSE(should_otel_trace_query("SELECT * FROM foo", TSessionType::BEESWAX));
  }
}
