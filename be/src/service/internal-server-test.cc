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

#include <iostream>
#include <list>
#include <memory>

#include "catalog/catalog-util.h"
#include "codegen/llvm-codegen.h"
#include "common/status.h"
#include "common/thread-debug-info.h"
#include "gtest/gtest.h"
#include "gutil/strings/strcat.h"
#include "gutil/walltime.h"
#include "rapidjson/rapidjson.h"
#include "runtime/exec-env.h"
#include "runtime/query-driver.h"
#include "service/client-request-state.h"
#include "service/fe-support.h"
#include "service/frontend.h"
#include "service/internal-server.h"
#include "service/impala-server.h"
#include "testutil/http-util.h"
#include "testutil/gtest-util.h"
#include "testutil/scoped-flag-setter.h"
#include "util/debug-util.h"
#include "util/jni-util.h"

DECLARE_string(log_dir);
DECLARE_string(debug_actions);
DECLARE_string(jni_frontend_class);
DECLARE_string(hostname);
DECLARE_string(state_store_host);
DECLARE_int32(state_store_port);
DECLARE_string(catalog_service_host);
DECLARE_int32(catalog_service_port);
DECLARE_bool(enable_webserver);
DECLARE_int32(beeswax_port);
DECLARE_int32(hs2_port);
DECLARE_int32(hs2_http_port);
DECLARE_int32(krpc_port);
DECLARE_int32(state_store_subscriber_port);
DECLARE_int32(webserver_port);

using namespace std;
using namespace impala;
using namespace rapidjson;

shared_ptr<ImpalaServer> impala_server_;

const static string QUERY_STATE_SUCCESS = "FINISHED";
const static string QUERY_STATE_FAILED  = "EXCEPTION";

namespace impala {
namespace internalservertest {

// Retrieves the json representation of the coordinator's queries and asserts the state
// of the specified query matches the provided expected state.
void assertQueryState(const TUniqueId& query_id, const string expected_state) {
  // Give the Impala web server a second to refresh its completed queries list.
  SleepForMs(1000);

  // Retrieve the list of queries from the test's coordinator. Results contain the raw
  // http response including headers.
  Document queries;
  stringstream contents;
  ASSERT_OK(HttpGet("localhost", FLAGS_webserver_port, "/queries?json", &contents));
  string contents_str = contents.str();

  // Locate the start of the response body.
  size_t header_end = contents_str.find("\r\n\r\n", 0);
  ASSERT_NE(string::npos, header_end);

  // Parse the json queries response.
  ASSERT_FALSE(queries.Parse(contents_str.substr(header_end+2,
      contents_str.length()).c_str()).HasParseError());
  ASSERT_TRUE(queries.IsObject());

  // Locate the completed query matching the provided query_id.
  const Value& completed_queries = queries["completed_queries"];
  const string query_id_str = PrintId(query_id);
  string found_state;
  ASSERT_TRUE(completed_queries.IsArray());
  for (SizeType i=0; i<completed_queries.Size(); i++) {
    const Value& query = completed_queries[i];
    ASSERT_TRUE(query.IsObject());
    if (query_id_str == query["query_id"].GetString()) {
      found_state = query["state"].GetString();
      break;
    }
  }

  // Assert the completed query has the expected state. If expected_state is empty, then
  // the query was not found.
  ASSERT_EQ(expected_state, found_state);
} // assertQueryState

// Helper class to set up a uniquely named database. Not every test will need its own
// database, thus an instance of this class must be instantiated in every that that needs
// its own database.
//
// Upon construction, instances of this class create a database consisting of the
// name_prefix parameter with the current time appended. Upon destruction, instances of
// this class drop cascade the database that was created during construction.
//
// DO NOT provide a value for record_count that is less than 5.
class DatabaseTest {
  public:
    DatabaseTest(const shared_ptr<ImpalaServer> impala_server, const string name_prefix,
        const bool create_table = false, const int record_count = 5000) {
      // See the warning on the category_count_ class member definition.
      EXPECT_LE(category_count_, 11);

      impala_server_ = impala_server;
      database_name_ = StrCat(name_prefix, "_", GetCurrentTimeMicros());
      TUniqueId query_id;
      EXPECT_OK(impala_server_->ExecuteIgnoreResults("impala", StrCat("create database ",
          database_name_, " comment 'Temporary database created and managed by "
          "internal-server-test'"), {}, false, &query_id));
      assertQueryState(query_id, QUERY_STATE_SUCCESS);

      if (create_table) {
        table_name_ = StrCat(database_name_, ".", "products");
        EXPECT_OK(impala_server_->ExecuteIgnoreResults("impala", StrCat("create table ",
            table_name_, "(id INT, name STRING, first_sold TIMESTAMP, "
            "last_sold TIMESTAMP, price DECIMAL(30, 2)) partitioned by (category INT)"),
            {}, false, &query_id));
        assertQueryState(query_id, QUERY_STATE_SUCCESS);

        // Insert some products that have a last_sold time.
        string sql1 = StrCat("insert into ", table_name_ , "(id,category,name,"
            "first_sold,last_sold,price) values ");

        const int secs_per_year = 31536000; // Seconds in 1 year.
        for (int i=0; i<record_count; i+=2) {
          int cat = (i % category_count_) + 1;
          double price = cat * .01;
          // Calculate a first sold offset.
          uint32_t first_sold = secs_per_year * (cat * cat);
          // Calculate a last sold offset that is a minimum 1 year after first_sold.
          uint32_t last_sold = first_sold - (secs_per_year * cat);
          sql1 += StrCat("(", i, ",", cat, ",'prod_", i,
              "',seconds_sub(now(),", first_sold, "),seconds_sub(now(),", last_sold,
              "),cast(", price, " as DECIMAL(30, 2)))");

          if (i < record_count - 2) {
            sql1 += ",";
          }
        }

        EXPECT_OK(impala_server_->ExecuteIgnoreResults("impala", sql1, {},
            false, &query_id));
        assertQueryState(query_id, QUERY_STATE_SUCCESS);

        // Insert some products that do not have a last_sold time.
        string sql2 = StrCat("insert into ", table_name_, "(id, category,name,first_sold,"
            "price) values ");

        for (int i=1; i<record_count; i+=2) {
          int cat = (i % category_count_) + 1;
          double price = cat * .01;
          // Calculate a sold offset.
          uint32_t sold = secs_per_year * (cat * cat);
          sql2 += StrCat("(", i, ",", cat, ",'prod_", i,
              "',seconds_sub(now(),", sold, "),cast(", price, " as DECIMAL(30, 2)))");

          if (i < record_count - 2) {
            sql2 += ",";
          }
        }

        EXPECT_OK(impala_server_->ExecuteIgnoreResults("impala", sql2, {},
            false, &query_id));
        assertQueryState(query_id, QUERY_STATE_SUCCESS);
      }
    }

    ~DatabaseTest() {
      RETURN_VOID_IF_ERROR(impala_server_->ExecuteIgnoreResults("impala",
          "drop database if exists " + database_name_ + " cascade"));
    }

    const string GetDbName() const {
        return database_name_;
    }

    const string GetTableName() const {
        return table_name_;
    }

    int GetCategoryCount() const {
      return category_count_;
    }

  private:
    // WARNING: Values greater than 11 will overflow the uint32_t variables.
    const int category_count_ = 10;
    mutable string database_name_;
    mutable string table_name_;
    shared_ptr<ImpalaServer> impala_server_;
}; // class DatabaseTest

// Asserts that a timeout error is returned if the query times out during execution.
// This test must select from a table because selecting directly from the sleep() function
// does not cause a timeout.
TEST(InternalServerTest, QueryTimeout) {
  DatabaseTest db_test = DatabaseTest(impala_server_, "query_timeout", true, 5);
  InternalServer* fixture = impala_server_.get();

  TUniqueId session_id;
  TUniqueId query_id;

  ASSERT_OK(fixture->OpenSession("impala", session_id,
      {{TImpalaQueryOptions::FETCH_ROWS_TIMEOUT_MS, "1"}}));

  // Run a query that will execute for longer than the configured exec timeout.
  ASSERT_OK(fixture->SubmitQuery(StrCat("select * from ", db_test.GetTableName(),
      " where id=sleep(2000000)"), session_id, query_id));

  Status stat = fixture->WaitForResults(query_id);

  // Assert the expected timeout error was returned.
  EXPECT_FALSE(stat.ok());
  EXPECT_EQ(TErrorCode::GENERAL, stat.code());
  EXPECT_EQ("query timed out waiting for results", stat.msg().msg());

  fixture->CloseQuery(query_id);
  fixture->CloseSession(session_id);

  assertQueryState(query_id, QUERY_STATE_FAILED);
} // TEST QueryTimeout

// Asserts the expected error is returned when a query option is set to an invalid value.
TEST(InternalServerTest, InvalidQueryOption) {
  InternalServer* fixture = impala_server_.get();

  TUniqueId session_id;
  Status stat = fixture->OpenSession("impala", session_id,
      {{TImpalaQueryOptions::MEM_LIMIT_EXECUTORS, "-2"}});

  ASSERT_FALSE(stat.ok());
  ASSERT_EQ("Failed to parse query option 'MEM_LIMIT_EXECUTORS': -2", stat.msg().msg());
} // TEST InvalidQueryOption

// Asserts that executing multiple queries over multiple sessions against the same
// internal server instance works correctly.
TEST(InternalServerTest, MultipleQueriesMultipleSessions) {
  query_results results = make_shared<vector<string>>();
  InternalServer* fixture = impala_server_.get();
  DatabaseTest db_test = DatabaseTest(impala_server_,
      "multiple_queries_multiple_sessions");
  const string test_table_name = StrCat(db_test.GetDbName(), ".test_table_a");

  // Set up a test table using a new session.
  TUniqueId query_id;
  ASSERT_OK(fixture->ExecuteAndFetchAllText("impala",
      StrCat("create table if not exists ", test_table_name,
      "(id INT, first_name STRING, last_name STRING)"), results, nullptr, &query_id));
  ASSERT_EQ(1, results->size());
  ASSERT_EQ(results->at(0), "Table has been created.");
  assertQueryState(query_id, QUERY_STATE_SUCCESS);

  // Insert a record into the test table using a new session.
  ASSERT_OK(fixture->ExecuteIgnoreResults("impala", StrCat("insert into ",
      test_table_name, "(id,first_name,last_name) VALUES (1,'test','person1')"),
      {}, false, &query_id));
  assertQueryState(query_id, QUERY_STATE_SUCCESS);

  // Select a record from the test table using a new session.
  results->clear();
  results_columns columns;
  ASSERT_OK(fixture->ExecuteAndFetchAllText("impala", StrCat("select id,first_name,"
      "last_name FROM ", test_table_name), results, &columns, &query_id));
  assertQueryState(query_id, QUERY_STATE_SUCCESS);

  ASSERT_EQ(3, columns.size());
  EXPECT_EQ("id", columns.at(0).first);
  EXPECT_EQ("int", columns.at(0).second);
  EXPECT_EQ("first_name", columns.at(1).first);
  EXPECT_EQ("string", columns.at(1).second);
  EXPECT_EQ("last_name", columns.at(2).first);
  EXPECT_EQ("string", columns.at(2).second);

  ASSERT_EQ(1, results->size());
  EXPECT_EQ(results->at(0), "1\ttest\tperson1");
} // MultipleQueriesMultipleSessions

// Simulates an RPC failure which causes the coordinator to automatically retry the query.
TEST(InternalServerTest, RetryFailedQuery) {
  InternalServer* fixture = impala_server_.get();

  TUniqueId session_id;
  TUniqueId query_id;
  TUniqueId orig_query_id;

  // Simulate a krpc failure which will cause the coordinator to automatically retry
  // the query.
  const ScopedFlagSetter sfs = ScopedFlagSetter<string>::Make(&FLAGS_debug_actions,
      StrCat("IMPALA_SERVICE_POOL:127.0.0.1:",FLAGS_krpc_port,
      ":ExecQueryFInstances:FAIL"));

  ASSERT_OK(fixture->OpenSession("impala", session_id,
      {{TImpalaQueryOptions::RETRY_FAILED_QUERIES, "true"}}));

  // Run a query that will fail and get automatically retried.
  ASSERT_OK(fixture->SubmitQuery("select 1", session_id, query_id));
  orig_query_id = query_id;

  Status wait_status = fixture->WaitForResults(query_id);
  ASSERT_OK(wait_status);

  EXPECT_TRUE(orig_query_id != query_id);

  fixture->CloseQuery(query_id);
  fixture->CloseSession(session_id);

  assertQueryState(query_id, QUERY_STATE_FAILED);
} // TEST RetryFailedQuery

// Asserts that executing multiple queries in one session against the same internal
// server instance works correctly.
TEST(InternalServerTest, MultipleQueriesOneSession) {
  query_results results = make_shared<vector<string>>();
  InternalServer* fixture = impala_server_.get();
  DatabaseTest db_test = DatabaseTest(impala_server_, "multiple_queries_one_session");
  const string test_table_name = StrCat(db_test.GetDbName(), ".test_table_1");

  // Open a new session that will be used to run all queries.
  TUniqueId session_id;
  ASSERT_OK(fixture->OpenSession("impala", session_id));

  // Create a test table.
  TUniqueId query_id1;
  ASSERT_OK(fixture->SubmitQuery(StrCat("create table if not exists ", test_table_name,
      "(id INT,name STRING)"), session_id, query_id1));
  ASSERT_OK(fixture->WaitForResults(query_id1));
  ASSERT_OK(fixture->FetchAllRows(query_id1, results));

  // Assert the test table was created.
  ASSERT_EQ(1, results->size());
  EXPECT_EQ(results->at(0), "Table has been created.");
  results->clear();
  fixture->CloseQuery(query_id1);

  // In the same session, insert into the newly created test table.
  TUniqueId query_id2;
  ASSERT_OK(fixture->SubmitQuery(StrCat("insert into ", test_table_name,
      " (id, name) VALUES (1, 'one'), (2, 'two')"), session_id, query_id2));
  ASSERT_OK(fixture->WaitForResults(query_id2));
  ASSERT_OK(fixture->FetchAllRows(query_id2, results));

  // Assert the insert succeeded.
  ASSERT_EQ(0, results->size());
  fixture->CloseQuery(query_id2);

  // Still in the same session, select from the test table.
  TUniqueId query_id3;
  results_columns columns;

  ASSERT_OK(fixture->SubmitQuery(StrCat("select name,id,name from ", test_table_name,
      " order by id asc"), session_id, query_id3));
  ASSERT_OK(fixture->WaitForResults(query_id3));
  ASSERT_OK(fixture->FetchAllRows(query_id3, results, &columns));

  // Assert the expected number of columns were returned from the select statement.
  ASSERT_EQ(3, columns.size());
  EXPECT_EQ("name", columns.at(0).first);
  EXPECT_EQ("string", columns.at(0).second);
  EXPECT_EQ("id", columns.at(1).first);
  EXPECT_EQ("int", columns.at(1).second);
  EXPECT_EQ("name", columns.at(2).first);
  EXPECT_EQ("string", columns.at(2).second);

  // Assert the expected number of rows were returned from the select statement.
  ASSERT_EQ(2, results->size());
  EXPECT_EQ(results->at(0), "one\t1\tone");
  EXPECT_EQ(results->at(1), "two\t2\ttwo");
  fixture->CloseQuery(query_id3);
  assertQueryState(query_id3, QUERY_STATE_SUCCESS);

  fixture->CloseSession(session_id);

  // Assert the session was properly closed.
  TUniqueId query_id4;
  Status expect_fail = fixture->SubmitQuery( "select 1", session_id, query_id4);
  EXPECT_EQ(PrintId(session_id), expect_fail.msg().msg());
  EXPECT_EQ(2, expect_fail.code());
} // TEST MultipleQueriesOneSession

TEST(InternalServerTest, MissingClosingQuote) {
  InternalServer* fixture = impala_server_.get();
  DatabaseTest db_test = DatabaseTest(impala_server_, "missing_quote", true, 5);
  TUniqueId query_id;
  Status res;

  const string expected_msg = "ParseException: Unmatched string literal";
  res = fixture->ExecuteIgnoreResults("impala",StrCat( "select * from ",
      db_test.GetTableName(), " where name = 'foo"), {}, false, &query_id);
  EXPECT_EQ(TErrorCode::GENERAL, res.code());
  EXPECT_EQ(expected_msg, res.msg().msg().substr(0, expected_msg.length()));
  EXPECT_EQ(TUniqueId(), query_id);
} // TEST MissingClosingQuote

TEST(InternalServerTest, SyntaxError) {
  InternalServer* fixture = impala_server_.get();
  DatabaseTest db_test = DatabaseTest(impala_server_, "syntax_error", true, 5);
  TUniqueId query_id;
  Status res;

  const string expected_msg = "ParseException: Syntax error in line 1";
  res = fixture->ExecuteIgnoreResults("impala", StrCat("select * from ",
      db_test.GetTableName(), "; select"), {}, false, &query_id);
  EXPECT_EQ(TErrorCode::GENERAL, res.code());
  EXPECT_EQ(expected_msg, res.msg().msg().substr(0, expected_msg.length()));
  EXPECT_EQ(TUniqueId(), query_id);
} // TEST SyntaxError

TEST(InternalServerTest, UnclosedComment) {
  InternalServer* fixture = impala_server_.get();
  TUniqueId query_id;
  Status res;

  const string expected_msg = "ParseException: Syntax error in line 1";
  res = fixture->ExecuteIgnoreResults("impala", "select 1 /*foo", {}, false,
      &query_id);
  EXPECT_EQ(TErrorCode::GENERAL, res.code());
  EXPECT_EQ(expected_msg, res.msg().msg().substr(0, expected_msg.length()));
  EXPECT_EQ(TUniqueId(), query_id);
} // TEST UnclosedComment

TEST(InternalServerTest, TableNotExist) {
  InternalServer* fixture = impala_server_.get();
  DatabaseTest db_test = DatabaseTest(impala_server_, "table_not_exist", true, 5);
  TUniqueId query_id;
  Status res;

  const string expected_msg = "AnalysisException: Could not resolve table reference:"
      " '" + db_test.GetTableName() + "'";
      ASSERT_OK(fixture->ExecuteIgnoreResults("impala", StrCat("drop table ",
      db_test.GetTableName(), " purge")));
  res = fixture->ExecuteIgnoreResults("impala", StrCat("select * from ",
      db_test.GetTableName()), {}, false, &query_id);
  EXPECT_EQ(TErrorCode::GENERAL, res.code());
  EXPECT_EQ(expected_msg, res.msg().msg().substr(0, expected_msg.length()));
  EXPECT_EQ(TUniqueId(), query_id);
} // TEST TableNotExist

// Helper function for the SimultaneousMultipleQueriesOneSession test. Intended to be run
// as part of a separate thread.
void runTestQueries(const int thread_num, InternalServer* server,
    const DatabaseTest* const db_test, const TUniqueId* session_id) {
  const int queries_to_run = 100;

  TUniqueId query_id;

  for (int i=0; i<queries_to_run; i++) {
    int cat = i % db_test->GetCategoryCount();
    ASSERT_OK(server->SubmitQuery(StrCat("select * from ", db_test->GetTableName(),
        " where category=", cat, " --thread '", thread_num, "' query '", i, "'"),
        *session_id, query_id));
    ASSERT_OK(server->WaitForResults(query_id));
    server->CloseQuery(query_id);

    assertQueryState(query_id, QUERY_STATE_SUCCESS);
  }
} // runTestQueries

TEST(InternalServerTest, SimultaneousMultipleQueriesOneSession) {
  const int test_threads = 10;
  list<unique_ptr<Thread>> threads;
  TUniqueId session_id;

  InternalServer* fixture = impala_server_.get();
  DatabaseTest db_test = DatabaseTest(impala_server_, "smqos", true);

  // Open a new session for all the select queries.
  ASSERT_OK(fixture->OpenSession("impala", session_id));

  // Execute select queries across multiple threads.
  for (int i=0; i<test_threads; i++) {
    threads.emplace_back(unique_ptr<Thread>());
    ABORT_IF_ERROR(Thread::Create("internal-server-test", "thread",
        &runTestQueries, i, fixture, &db_test, &session_id, &(threads.back())));
  }

  for (auto iter = threads.cbegin(); iter != threads.cend(); iter++) {
    iter->get()->Join();
  }

  fixture->CloseSession(session_id);
} // TEST SimultaneousMultipleQueriesOneSession

} // namespace internalservertest
} // namespace impala

int main(int argc, char** argv) {
  FLAGS_jni_frontend_class = "org/apache/impala/service/JniFrontend";

  FLAGS_hostname = "localhost";
  FLAGS_state_store_host = FLAGS_hostname;
  FLAGS_catalog_service_host = FLAGS_hostname;
  FLAGS_enable_webserver = true;

  FLAGS_beeswax_port = 21005;
  FLAGS_hs2_port = 21055;
  FLAGS_hs2_http_port = 28005;
  FLAGS_krpc_port = 27005;
  FLAGS_state_store_subscriber_port = 23005;
  FLAGS_webserver_port = 25005;

  // Provides information about the current thread in a minidump if one is generated.
  ThreadDebugInfo debugInfo;

  testing::InitGoogleTest(&argc, argv);
  InitCommonRuntime(argc, argv, true, TestInfo::BE_CLUSTER_TEST);
  ABORT_IF_ERROR(LlvmCodeGen::InitializeLlvm(argv[0]));
  JniUtil::InitLibhdfs();
  ABORT_IF_ERROR(JniCatalogCacheUpdateIterator::InitJNI());
  InitFeSupport();

  ExecEnv exec_env;
  ABORT_IF_ERROR(exec_env.Init());

  impala_server_ = make_shared<ImpalaServer>(&exec_env);
  ABORT_IF_ERROR(impala_server_->Start(FLAGS_beeswax_port, FLAGS_hs2_port,
      FLAGS_hs2_http_port, 0));

  EXPECT_TRUE(impala_server_->IsCoordinator());
  EXPECT_TRUE(impala_server_->IsExecutor());
  exec_env.frontend()->WaitForCatalog();

  ABORT_IF_ERROR(WaitForServer(FLAGS_hostname, FLAGS_beeswax_port, 10, 100));

  int test_ret_val = RUN_ALL_TESTS();

  ShutdownStatusPB shutdown_status;
  ABORT_IF_ERROR(impala_server_->StartShutdown(5, &shutdown_status));
  impala_server_->Join();

  return test_ret_val;
}
