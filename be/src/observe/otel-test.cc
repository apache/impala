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

#include <chrono>
#include <string>
#include <string_view>

#include <boost/algorithm/string/replace.hpp>
#include <gtest/gtest.h>
#include "gutil/strings/substitute.h"
#include <opentelemetry/exporters/otlp/otlp_file_exporter.h>
#include <opentelemetry/exporters/otlp/otlp_http.h>
#include <opentelemetry/exporters/otlp/otlp_http_exporter.h>
#include <opentelemetry/exporters/otlp/otlp_http_exporter_options.h>
#include <opentelemetry/sdk/common/global_log_handler.h>
#include <opentelemetry/sdk/trace/batch_span_processor.h>
#include <opentelemetry/sdk/trace/batch_span_processor_options.h>
#include <opentelemetry/sdk/trace/simple_processor.h>
#include "gen-cpp/Query_types.h"
#include "observe/otel-log-handler.h"
#include "testutil/scoped-flag-setter.h"

using namespace std;
using namespace impala;
using namespace opentelemetry::sdk::common::internal_log;
using namespace opentelemetry::sdk::trace;
using namespace opentelemetry::exporter::otlp;

DECLARE_bool(otel_debug);
DECLARE_string(otel_trace_additional_headers);
DECLARE_int32(otel_trace_batch_queue_size);
DECLARE_int32(otel_trace_batch_max_batch_size);
DECLARE_int32(otel_trace_batch_schedule_delay_ms);
DECLARE_string(otel_trace_ca_cert_path);
DECLARE_string(otel_trace_ca_cert_string);
DECLARE_string(otel_trace_collector_url);
DECLARE_bool(otel_trace_compression);
DECLARE_string(otel_trace_exporter);
DECLARE_double(otel_trace_retry_policy_backoff_multiplier);
DECLARE_double(otel_trace_retry_policy_initial_backoff_s);
DECLARE_int32(otel_trace_retry_policy_max_attempts);
DECLARE_int32(otel_trace_retry_policy_max_backoff_s);
DECLARE_string(otel_trace_span_processor);
DECLARE_string(otel_trace_ssl_ciphers);
DECLARE_int32(otel_trace_timeout_s);
DECLARE_string(otel_trace_tls_cipher_suites);
DECLARE_bool(otel_trace_tls_insecure_skip_verify);
DECLARE_string(otel_trace_tls_minimum_version);
DECLARE_string(ssl_cipher_list);
DECLARE_string(ssl_minimum_version);
DECLARE_string(tls_ciphersuites);

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
  // TODO: Move to the QueriesTraced test case once IMPALA-14370 is fixed.
  runtest(strings::Substitute("/*/ comment */select * from tbl"));

  // Beeswax queries are not traced.
  EXPECT_FALSE(should_otel_trace_query("SELECT * FROM foo", TSessionType::BEESWAX));
}

TEST(OtelTest, TLSEnabled) {
  {
    auto ca_cert_path_setter =
      ScopedFlagSetter<string>::Make(&FLAGS_otel_trace_collector_url, "https://foo.com");
    // NOLINTNEXTLINE(clang-diagnostic-error-undeclared-identifier)
    EXPECT_TRUE(test::otel_tls_enabled_for_testing());
  }

  {
    auto ca_cert_path_setter =
      ScopedFlagSetter<string>::Make(&FLAGS_otel_trace_collector_url, "HTTPS://foo.com");
    // NOLINTNEXTLINE(clang-diagnostic-error-undeclared-identifier)
    EXPECT_TRUE(test::otel_tls_enabled_for_testing());
  }
}

TEST(OtelTest, TLSNotEnabled) {
  {
    auto ca_cert_path_setter =
      ScopedFlagSetter<string>::Make(&FLAGS_otel_trace_collector_url, "");
    // NOLINTNEXTLINE(clang-diagnostic-error-undeclared-identifier)
    EXPECT_FALSE(test::otel_tls_enabled_for_testing());
  }

  {
    auto ca_cert_path_setter =
      ScopedFlagSetter<string>::Make(&FLAGS_otel_trace_collector_url, "http://foo.com");
    // NOLINTNEXTLINE(clang-diagnostic-error-undeclared-identifier)
    EXPECT_FALSE(test::otel_tls_enabled_for_testing());
  }

  {
    auto ca_cert_path_setter =
      ScopedFlagSetter<string>::Make(&FLAGS_otel_trace_collector_url, "HTTP://foo.com");
    // NOLINTNEXTLINE(clang-diagnostic-error-undeclared-identifier)
    EXPECT_FALSE(test::otel_tls_enabled_for_testing());
  }
}

// Assert the default values of the OtlpHttpExporterOptions struct used to configure the
// OtlpHttpExporter.
TEST(OtelTest, InitHttpDefaults) {
  FLAGS_otel_trace_collector_url = "https://foo.com";
  FLAGS_ssl_minimum_version = "tlsv1.0";
  FLAGS_ssl_cipher_list = "ssl_ciphers";
  FLAGS_tls_ciphersuites = "tls_ciphers";

  OtlpHttpExporterOptions actual = test::get_http_exporter_config();

  EXPECT_EQ("https://foo.com", actual.url);
  EXPECT_EQ(HttpRequestContentType::kJson, actual.content_type);
  EXPECT_EQ(false, actual.console_debug);
  EXPECT_EQ(chrono::seconds(10), actual.timeout);
  EXPECT_EQ(5, actual.retry_policy_max_attempts);
  EXPECT_EQ(chrono::seconds(1), actual.retry_policy_initial_backoff);
  EXPECT_EQ(chrono::duration<float>(5.0), actual.retry_policy_max_backoff);
  EXPECT_EQ(2.0, actual.retry_policy_backoff_multiplier);
  EXPECT_EQ("zlib", actual.compression);
  EXPECT_EQ("1.0", actual.ssl_min_tls);
  EXPECT_EQ("1.3", actual.ssl_max_tls);
  EXPECT_EQ("ssl_ciphers", actual.ssl_cipher);
  EXPECT_EQ("tls_ciphers", actual.ssl_cipher_suite);
  EXPECT_EQ(false, actual.ssl_insecure_skip_verify);
  EXPECT_EQ("", actual.ssl_ca_cert_path);
  EXPECT_EQ("", actual.ssl_ca_cert_string);
  EXPECT_EQ(0, actual.http_headers.size());
  EXPECT_TRUE(actual.http_headers.empty());
}

// Assert the flags that customize the values of the OtlpHttpExporterOptions struct used
// to configure the OtlpHttpExporter.
TEST(OtelTest, InitHttpOverrides) {
  FLAGS_otel_trace_collector_url = "https://foo.com";
  FLAGS_otel_trace_tls_minimum_version = "tlsv1.3";
  FLAGS_otel_trace_timeout_s = 9;
  FLAGS_otel_debug = true;
  FLAGS_otel_trace_retry_policy_max_attempts = 8;
  FLAGS_otel_trace_retry_policy_initial_backoff_s = 7.0;
  FLAGS_otel_trace_retry_policy_max_backoff_s = 6;
  FLAGS_otel_trace_retry_policy_backoff_multiplier = 42.0;
  FLAGS_otel_trace_ssl_ciphers = "override_ssl_ciphers";
  FLAGS_otel_trace_tls_cipher_suites = "override_tls_ciphers";
  FLAGS_otel_trace_tls_insecure_skip_verify = true;
  FLAGS_otel_trace_ca_cert_path = "ca_cert_path";
  FLAGS_otel_trace_ca_cert_string = "ca_cert_string";
  FLAGS_otel_trace_compression = false;

  OtlpHttpExporterOptions actual = test::get_http_exporter_config();

  EXPECT_EQ("https://foo.com", actual.url);
  EXPECT_EQ(true, actual.console_debug);
  EXPECT_EQ(chrono::seconds(9), actual.timeout);
  EXPECT_EQ(8, actual.retry_policy_max_attempts);
  EXPECT_EQ(chrono::seconds(7), actual.retry_policy_initial_backoff);
  EXPECT_EQ(chrono::seconds(6), actual.retry_policy_max_backoff);
  EXPECT_EQ(42.0, actual.retry_policy_backoff_multiplier);
  EXPECT_EQ("1.3", actual.ssl_min_tls);
  EXPECT_EQ("override_ssl_ciphers", actual.ssl_cipher);
  EXPECT_EQ("override_tls_ciphers", actual.ssl_cipher_suite);
  EXPECT_EQ(true, actual.ssl_insecure_skip_verify);
  EXPECT_EQ("ca_cert_path", actual.ssl_ca_cert_path);
  EXPECT_EQ("ca_cert_string", actual.ssl_ca_cert_string);
  EXPECT_EQ("none", actual.compression);
  EXPECT_TRUE(actual.http_headers.empty());
}

// The otel_trace_additional_headers flag allows for specifying arbitrary HTTP headers
// that are added to each HTTP request to the OTel collector. Assert one additional header
// is correctly parsed.
TEST(OtelTest, InitOneHttpHeader) {
  FLAGS_otel_trace_additional_headers = "foo=bar";
  OtlpHttpExporterOptions actual = test::get_http_exporter_config();

  EXPECT_EQ(1, actual.http_headers.size());
  const auto val = actual.http_headers.find("foo");
  ASSERT_NE(actual.http_headers.cend(), val) << "Could not find header with key 'foo'";
  EXPECT_EQ("bar", val->second);
}

// The otel_trace_additional_headers flag allows for specifying arbitrary HTTP headers
// that are added to each HTTP request to the OTel collector. Assert multiple additional
// headers (including the same header specified twice) are correctly parsed.
TEST(OtelTest, InitMultipleHttpHeaders) {
  FLAGS_otel_trace_additional_headers = "foo=bar1:::foo2=bar3:::foo=bar2:::foo3=bar4";
  OtlpHttpExporterOptions actual = test::get_http_exporter_config();

  EXPECT_EQ(4, actual.http_headers.size());

  const auto val2 = actual.http_headers.find("foo2");
  ASSERT_NE(actual.http_headers.cend(), val2) << "Could not find header with key 'foo2'";
  EXPECT_EQ("bar3", val2->second);

  const auto val3 = actual.http_headers.find("foo3");
  ASSERT_NE(actual.http_headers.cend(), val3) << "Could not find header with key 'foo3'";
  EXPECT_EQ("bar4", val3->second);

  bool val1_found = false;
  bool val2_found = false;

  for (auto iter : actual.http_headers) {
    if (iter.first == "foo") {
      if (iter.second == "bar1") {
        val1_found = true;
      } else if (iter.second == "bar2") {
        val2_found = true;
      }
    }
  }

  EXPECT_TRUE(val1_found) << "Did not find header with key 'foo' and value 'bar1'";
  EXPECT_TRUE(val2_found) << "Did not find header with key 'foo' and value 'bar2'";
}

// Assert the default values of the BatchSpanProcessorOptions struct used to configure
// the BatchSpanProcessor.
TEST(OtelTest, BatchSpanProcessorDefaults) {
  const BatchSpanProcessorOptions actual = test::get_batch_processor_config();

  // Defaults come from the BatchSpanProcessorOptions struct definition.
  EXPECT_EQ(512, actual.max_export_batch_size);
  EXPECT_EQ(2048, actual.max_queue_size);
  EXPECT_EQ(chrono::milliseconds(5000), actual.schedule_delay_millis);
}

// Assert the flags that customize the values of the BatchSpanProcessorOptions struct
// used to configure the BatchSpanProcessor.
TEST(OtelTest, BatchSpanProcessorOverrides) {
  FLAGS_otel_trace_batch_max_batch_size = 1;
  FLAGS_otel_trace_batch_queue_size = 2;
  FLAGS_otel_trace_batch_schedule_delay_ms = 3;

  const BatchSpanProcessorOptions actual = test::get_batch_processor_config();

  // Defaults come from the BatchSpanProcessorOptions struct definition.
  EXPECT_EQ(1, actual.max_export_batch_size);
  EXPECT_EQ(2, actual.max_queue_size);
  EXPECT_EQ(chrono::milliseconds(3), actual.schedule_delay_millis);
}

// Assert an OtlpHttpExporter is created based on the default value of the
// otel_trace_exporter flag and assert the default value of that flag.
TEST(OtelTest, InitExporterHttp) {
  EXPECT_EQ("otlp_http", FLAGS_otel_trace_exporter);
  unique_ptr<SpanExporter> exporter = test::get_exporter();

  ASSERT_NE(nullptr, exporter);
  EXPECT_NE(nullptr, dynamic_cast<OtlpHttpExporter*>(exporter.get()));
}

// Assert an OtlpFileExporter is created when the otel_trace_exporter flag is set to
// "file".
TEST(OtelTest, InitExporterFile) {
  FLAGS_otel_trace_exporter = "file";
  unique_ptr<SpanExporter> exporter = test::get_exporter();

  ASSERT_NE(nullptr, exporter);
  EXPECT_NE(nullptr, dynamic_cast<OtlpFileExporter*>(exporter.get()));
}

// Assert a BatchSpanProcessor is created based on the default value of the
// otel_trace_span_processor flag and assert the default value of that flag.
TEST(OtelTest, InitSpanProcessorBatch) {
  EXPECT_EQ("batch", FLAGS_otel_trace_span_processor);
  unique_ptr<SpanProcessor> processor = test::get_span_processor();

  ASSERT_NE(nullptr, processor);
  EXPECT_NE(nullptr, dynamic_cast<BatchSpanProcessor*>(processor.get()));
}

// Assert a SimpleSpanProcessor is created when the otel_trace_span_processor flag is set
// to "simple".
TEST(OtelTest, InitSpanProcessorSimple) {
  FLAGS_otel_trace_span_processor = "simple";
  unique_ptr<SpanProcessor> processor = test::get_span_processor();

  ASSERT_NE(nullptr, processor);
  EXPECT_NE(nullptr, dynamic_cast<SimpleSpanProcessor*>(processor.get()));
}

// Assert that when otel_debug is true, the GlobalLogHandler is set to an OtelLogHandler
// instance and the OTel global log level is set to Debug.
// Asserts the OTel global log handler is correctly initialized.
TEST(OtelTest, InitLogHandlerDebug) {
  FLAGS_otel_debug = true;
  init_otel_tracer();

  ASSERT_NE(nullptr, GlobalLogHandler::GetLogHandler().get());
  EXPECT_NE(nullptr, dynamic_cast<OtelLogHandler*>(
      GlobalLogHandler::GetLogHandler().get()));
  EXPECT_EQ(LogLevel::Debug, GlobalLogHandler::GetLogLevel());

  shutdown_otel_tracer();
}

// Assert that when otel_debug is false but VLOG(1) is enabled, the OTel global log level
// is set to Info.
TEST(OtelTest, InitLogHandlerInfoV1) {
  FLAGS_otel_debug = false;
  auto vlog_setter = ScopedFlagSetter<int32_t>::Make(&FLAGS_v, 1);
  init_otel_tracer();

  EXPECT_EQ(LogLevel::Info, GlobalLogHandler::GetLogLevel());

  shutdown_otel_tracer();
}

// Assert that when otel_debug is false but VLOG(2) is enabled, the OTel global log level
// is set to Info.
TEST(OtelTest, InitLogHandlerInfoV2) {
  FLAGS_otel_debug = false;
  auto vlog_setter = ScopedFlagSetter<int32_t>::Make(&FLAGS_v, 2);
  init_otel_tracer();

  EXPECT_EQ(LogLevel::Info, GlobalLogHandler::GetLogLevel());

  shutdown_otel_tracer();
}

// Assert that when otel_debug is false and VLOG(1) is disabled, the OTel global log level
// is set to None.
TEST(OtelTest, InitLogHandlerNone) {
  FLAGS_otel_debug = false;
  auto vlog_setter = ScopedFlagSetter<int32_t>::Make(&FLAGS_v, 0);
  init_otel_tracer();

  EXPECT_EQ(LogLevel::None, GlobalLogHandler::GetLogLevel());

  shutdown_otel_tracer();
}
