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

#include <string>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>
#include <gutil/strings/substitute.h>
#include <openssl/crypto.h>
#include <openssl/ssl.h>
#include <regex>

#include "common/init.h"
#include "testutil/gtest-util.h"
#include "testutil/scoped-flag-setter.h"

#include "util/default-path-handlers.h"
#include "util/kudu-status-util.h"
#include "util/metrics.h"
#include "util/os-util.h"
#include "util/webserver.h"

#include "kudu/security/test/mini_kdc.h"

DECLARE_bool(webserver_require_spnego);
DECLARE_int32(webserver_port);
DECLARE_string(webserver_password_file);
DECLARE_string(webserver_certificate_file);
DECLARE_string(webserver_private_key_file);
DECLARE_string(webserver_private_key_password_cmd);
DECLARE_string(webserver_x_frame_options);
DECLARE_string(ssl_cipher_list);
DECLARE_string(ssl_minimum_version);
DECLARE_string(tls_ciphersuites);
DECLARE_bool(webserver_ldap_passwords_in_clear_ok);
DECLARE_bool(cookie_require_secure);

#include "common/names.h"

using boost::asio::ip::tcp;
namespace filesystem = boost::filesystem;
using namespace impala;
using namespace rapidjson;
using namespace strings;

const string TEST_ARG = "test-arg";
const string SALUTATION_KEY = "Salutation";
const string SALUTATION_VALUE = "Hello!";
const string TO_ESCAPE_KEY = "ToEscape";
const string TO_ESCAPE_VALUE = "<script language='javascript'>";
const string ESCAPED_VALUE = "&lt;script language=&apos;javascript&apos;&gt;";

// Adapted from:
// http://stackoverflow.com/questions/10982717/get-html-without-header-with-boostasio
Status HttpGet(const string& host, const int32_t& port, const string& url_path,
    ostream* out, int expected_code = 200, const string& method = "GET") {
  try {
    tcp::iostream request_stream;
    request_stream.connect(host, lexical_cast<string>(port));
    if (!request_stream) return Status("Could not connect request_stream");

    request_stream << method << " " << url_path << " HTTP/1.1\r\n";
    request_stream << "Host: " << host << ":" << port <<  "\r\n";
    request_stream << "Accept: */*\r\n";
    request_stream << "Cache-Control: no-cache\r\n";

    request_stream << "Connection: close\r\n\r\n";
    request_stream.flush();

    string line1;
    getline(request_stream, line1);
    if (!request_stream) return Status("No response");

    stringstream response_stream(line1);
    string http_version;
    response_stream >> http_version;

    unsigned int status_code;
    response_stream >> status_code;

    string status_message;
    getline(response_stream,status_message);
    if (!response_stream || http_version.substr(0,5) != "HTTP/") {
      return Status("Malformed response");
    }

    if (status_code != expected_code) {
      return Status(Substitute("Unexpected status code: $0", status_code));
    }

    (*out) << request_stream.rdbuf();
    return Status::OK();
  } catch (const std::exception& e){
    return Status(e.what());
  }
}

TEST(Webserver, SmokeTest) {
  MetricGroup metrics("webserver-test");
  Webserver webserver("", FLAGS_webserver_port, &metrics);
  ASSERT_OK(webserver.Start());
  AddDefaultUrlCallbacks(&webserver);

  stringstream contents;
  ASSERT_OK(HttpGet("localhost", FLAGS_webserver_port, "/", &contents));
}

void AssertArgsCallback(bool* success, const Webserver::WebRequest& req,
    Document* document) {
  const auto& args = req.parsed_args;
  *success = args.find(TEST_ARG) != args.end();
}

TEST(Webserver, ArgsTest) {
  MetricGroup metrics("webserver-test");
  Webserver webserver("", FLAGS_webserver_port, &metrics);

  const string ARGS_TEST_PATH = "/args-test";
  bool success = false;
  Webserver::UrlCallback callback = bind<void>(AssertArgsCallback, &success , _1, _2);
  webserver.RegisterUrlCallback(ARGS_TEST_PATH, "json-test.tmpl", callback, true);

  ASSERT_OK(webserver.Start());
  stringstream contents;
  ASSERT_OK(HttpGet("localhost", FLAGS_webserver_port, ARGS_TEST_PATH, &contents));
  ASSERT_FALSE(success) << "Unexpectedly found " << TEST_ARG;

  ASSERT_OK(HttpGet("localhost", FLAGS_webserver_port,
      Substitute("$0?$1", ARGS_TEST_PATH, TEST_ARG), &contents));
  ASSERT_TRUE(success) << "Did not find " << TEST_ARG;
}

void JsonCallback(bool always_text, const Webserver::WebRequest& req,
    Document* document) {
  document->AddMember(rapidjson::StringRef(SALUTATION_KEY.c_str()),
      StringRef(SALUTATION_VALUE.c_str()), document->GetAllocator());
  document->AddMember(rapidjson::StringRef(TO_ESCAPE_KEY.c_str()),
      StringRef(TO_ESCAPE_VALUE.c_str()), document->GetAllocator());
  if (always_text) {
    document->AddMember(rapidjson::StringRef(Webserver::ENABLE_RAW_HTML_KEY), true,
        document->GetAllocator());
  }
}

TEST(Webserver, JsonTest) {
  MetricGroup metrics("webserver-test");
  Webserver webserver("", FLAGS_webserver_port, &metrics);

  const string JSON_TEST_PATH = "/json-test";
  const string RAW_TEXT_PATH = "/text";
  const string NO_TEMPLATE_PATH = "/no-template";
  Webserver::UrlCallback callback = bind<void>(JsonCallback, false, _1, _2);
  webserver.RegisterUrlCallback(JSON_TEST_PATH, "json-test.tmpl", callback, true);
  webserver.RegisterUrlCallback(NO_TEMPLATE_PATH, "doesnt-exist.tmpl", callback, true);

  Webserver::UrlCallback text_callback = bind<void>(JsonCallback, true, _1, _2);
  webserver.RegisterUrlCallback(RAW_TEXT_PATH, "json-test.tmpl", text_callback, true);
  ASSERT_OK(webserver.Start());

  stringstream contents;
  ASSERT_OK(HttpGet("localhost", FLAGS_webserver_port, JSON_TEST_PATH, &contents));
  ASSERT_TRUE(contents.str().find(SALUTATION_VALUE) != string::npos);
  ASSERT_TRUE(contents.str().find(SALUTATION_KEY) == string::npos);

  stringstream json_contents;
  ASSERT_OK(HttpGet("localhost", FLAGS_webserver_port,
      Substitute("$0?json", JSON_TEST_PATH), &json_contents));
  ASSERT_TRUE(json_contents.str().find("\"Salutation\": \"Hello!\"") != string::npos);

  stringstream error_contents;
  ASSERT_OK(
      HttpGet("localhost", FLAGS_webserver_port, NO_TEMPLATE_PATH, &error_contents));
  ASSERT_TRUE(error_contents.str().find("Could not open template: ") != string::npos);

  // Adding ?raw should send text
  stringstream raw_contents;
  ASSERT_OK(HttpGet("localhost", FLAGS_webserver_port,
          Substitute("$0?raw", JSON_TEST_PATH), &raw_contents));
  ASSERT_TRUE(raw_contents.str().find("text/plain") != string::npos);

  // Any callback that includes ENABLE_RAW_HTML_KEY should always return text.
  stringstream raw_cb_contents;
  ASSERT_OK(HttpGet("localhost", FLAGS_webserver_port, RAW_TEXT_PATH,
      &raw_cb_contents));
  ASSERT_TRUE(raw_cb_contents.str().find("text/plain") != string::npos);
}

TEST(Webserver, EscapingTest) {
  MetricGroup metrics("webserver-test");
  Webserver webserver("", FLAGS_webserver_port, &metrics);

  const string JSON_TEST_PATH = "/json-test";
  Webserver::UrlCallback callback = bind<void>(JsonCallback, false, _1, _2);
  webserver.RegisterUrlCallback(JSON_TEST_PATH, "json-test.tmpl", callback, true);
  ASSERT_OK(webserver.Start());
  stringstream contents;
  ASSERT_OK(HttpGet("localhost", FLAGS_webserver_port, JSON_TEST_PATH, &contents));
  ASSERT_TRUE(contents.str().find(ESCAPED_VALUE) != string::npos);
  ASSERT_TRUE(contents.str().find(TO_ESCAPE_VALUE) == string::npos);
}

TEST(Webserver, EscapeErrorUriTest) {
  MetricGroup metrics("webserver-test");
  Webserver webserver("", FLAGS_webserver_port, &metrics);
  ASSERT_OK(webserver.Start());
  stringstream contents;
  ASSERT_OK(HttpGet("localhost", FLAGS_webserver_port,
          "/dont-exist<script>alert(42);</script>", &contents, 404));
  ASSERT_EQ(contents.str().find("<script>alert(42);</script>"), string::npos);
  ASSERT_TRUE(contents.str().find("dont-exist&lt;script&gt;alert(42);&lt;/script&gt;") !=
      string::npos);
}

TEST(Webserver, SslTest) {
  auto cert = ScopedFlagSetter<string>::Make(&FLAGS_webserver_certificate_file,
      Substitute("$0/be/src/testutil/server-cert.pem", getenv("IMPALA_HOME")));
  auto key = ScopedFlagSetter<string>::Make(&FLAGS_webserver_private_key_file,
      Substitute("$0/be/src/testutil/server-key.pem", getenv("IMPALA_HOME")));

  MetricGroup metrics("webserver-test");
  Webserver webserver("", FLAGS_webserver_port, &metrics);
  ASSERT_OK(webserver.Start());
}

TEST(Webserver, SslBadCertTest) {
  auto cert = ScopedFlagSetter<string>::Make(&FLAGS_webserver_certificate_file,
      Substitute("$0/be/src/testutil/invalid-server-cert.pem", getenv("IMPALA_HOME")));
  auto key = ScopedFlagSetter<string>::Make(&FLAGS_webserver_private_key_file,
      Substitute("$0/be/src/testutil/server-key.pem", getenv("IMPALA_HOME")));

  MetricGroup metrics("webserver-test");
  Webserver webserver("", FLAGS_webserver_port, &metrics);
  ASSERT_FALSE(webserver.Start().ok());
}

TEST(Webserver, SslWithPrivateKeyPasswordTest) {
  auto cert = ScopedFlagSetter<string>::Make(&FLAGS_webserver_certificate_file,
      Substitute("$0/be/src/testutil/server-cert.pem", getenv("IMPALA_HOME")));
  auto key = ScopedFlagSetter<string>::Make(&FLAGS_webserver_private_key_file,
      Substitute("$0/be/src/testutil/server-key-password.pem", getenv("IMPALA_HOME")));
  auto cmd = ScopedFlagSetter<string>::Make(
      &FLAGS_webserver_private_key_password_cmd, "echo password");

  MetricGroup metrics("webserver-test");
  Webserver webserver("", FLAGS_webserver_port, &metrics);
  ASSERT_OK(webserver.Start());
}

TEST(Webserver, SslBadPrivateKeyPasswordTest) {
  auto cert = ScopedFlagSetter<string>::Make(&FLAGS_webserver_certificate_file,
      Substitute("$0/be/src/testutil/server-cert.pem", getenv("IMPALA_HOME")));
  auto key = ScopedFlagSetter<string>::Make(&FLAGS_webserver_private_key_file,
      Substitute("$0/be/src/testutil/server-key-password.pem", getenv("IMPALA_HOME")));
  auto cmd = ScopedFlagSetter<string>::Make(
      &FLAGS_webserver_private_key_password_cmd, "echo wrongpassword");

  MetricGroup metrics("webserver-test");
  Webserver webserver("", FLAGS_webserver_port, &metrics);
  ASSERT_FALSE(webserver.Start().ok());
}

TEST(Webserver, SslCipherSuite) {
  auto cert = ScopedFlagSetter<string>::Make(&FLAGS_webserver_certificate_file,
      Substitute("$0/be/src/testutil/server-cert.pem", getenv("IMPALA_HOME")));
  auto key = ScopedFlagSetter<string>::Make(&FLAGS_webserver_private_key_file,
      Substitute("$0/be/src/testutil/server-key-password.pem", getenv("IMPALA_HOME")));
  auto cmd = ScopedFlagSetter<string>::Make(
      &FLAGS_webserver_private_key_password_cmd, "echo password");
  {
    auto ciphers = ScopedFlagSetter<string>::Make(
        &FLAGS_ssl_cipher_list, "not_a_cipher");
    auto ciphersuites = ScopedFlagSetter<string>::Make(
        &FLAGS_tls_ciphersuites, "");
    MetricGroup metrics("webserver-test");
    Webserver webserver("", FLAGS_webserver_port, &metrics);
    ASSERT_FALSE(webserver.Start().ok());
  }
  {
    auto ciphers = ScopedFlagSetter<string>::Make(
        &FLAGS_ssl_cipher_list, "AES128-SHA");
    auto ciphersuites = ScopedFlagSetter<string>::Make(
        &FLAGS_tls_ciphersuites, "");
    MetricGroup metrics("webserver-test");
    Webserver webserver("", FLAGS_webserver_port, &metrics);
    ASSERT_OK(webserver.Start());
  }
}

#if OPENSSL_VERSION_NUMBER >= 0x10101000L

TEST(Webserver, TlsCiphersuite) {
  auto cert = ScopedFlagSetter<string>::Make(&FLAGS_webserver_certificate_file,
      Substitute("$0/be/src/testutil/server-cert.pem", getenv("IMPALA_HOME")));
  auto key = ScopedFlagSetter<string>::Make(&FLAGS_webserver_private_key_file,
      Substitute("$0/be/src/testutil/server-key-password.pem", getenv("IMPALA_HOME")));
  auto cmd = ScopedFlagSetter<string>::Make(
      &FLAGS_webserver_private_key_password_cmd, "echo password");
  {
    auto ciphers = ScopedFlagSetter<string>::Make(
        &FLAGS_ssl_minimum_version, "tlsv1.3");
    auto ciphersuites = ScopedFlagSetter<string>::Make(
        &FLAGS_tls_ciphersuites, "not_a_ciphersuite");
    MetricGroup metrics("webserver-test");
    Webserver webserver("", FLAGS_webserver_port, &metrics);
    ASSERT_FALSE(webserver.Start().ok());
  }
  {
    auto ciphers = ScopedFlagSetter<string>::Make(
        &FLAGS_ssl_minimum_version, "tlsv1.3");
    auto ciphersuites = ScopedFlagSetter<string>::Make(
        &FLAGS_tls_ciphersuites, "TLS_AES_256_GCM_SHA384");
    MetricGroup metrics("webserver-test");
    Webserver webserver("", FLAGS_webserver_port, &metrics);
    ASSERT_OK(webserver.Start());
  }
}

#endif // OPENSSL_VERSION_NUMBER

TEST(Webserver, SslBadTlsVersion) {
  auto cert = ScopedFlagSetter<string>::Make(&FLAGS_webserver_certificate_file,
      Substitute("$0/be/src/testutil/server-cert.pem", getenv("IMPALA_HOME")));
  auto key = ScopedFlagSetter<string>::Make(&FLAGS_webserver_private_key_file,
      Substitute("$0/be/src/testutil/server-key-password.pem", getenv("IMPALA_HOME")));
  auto cmd = ScopedFlagSetter<string>::Make(
      &FLAGS_webserver_private_key_password_cmd, "echo password");

  auto ssl_version = ScopedFlagSetter<string>::Make(
      &FLAGS_ssl_minimum_version, "not_a_version");

  MetricGroup metrics("webserver-test");
  Webserver webserver("", FLAGS_webserver_port, &metrics);
  ASSERT_FALSE(webserver.Start().ok());
}

TEST(Webserver, SslGoodTlsVersion) {
  auto cert = ScopedFlagSetter<string>::Make(&FLAGS_webserver_certificate_file,
      Substitute("$0/be/src/testutil/server-cert.pem", getenv("IMPALA_HOME")));
  auto key = ScopedFlagSetter<string>::Make(&FLAGS_webserver_private_key_file,
      Substitute("$0/be/src/testutil/server-key-password.pem", getenv("IMPALA_HOME")));
  auto cmd = ScopedFlagSetter<string>::Make(
      &FLAGS_webserver_private_key_password_cmd, "echo password");
#if OPENSSL_VERSION_NUMBER >= 0x10101000L
  auto versions = {"tlsv1", "tlsv1.1", "tlsv1.2", "tlsv1.3"};
  vector <string> unsupported_versions = {};
#elif OPENSSL_VERSION_NUMBER >= 0x10001000L
  auto versions = {"tlsv1", "tlsv1.1", "tlsv1.2"};
  vector<string> unsupported_versions = {};
#else
  auto versions = {"tlsv1"};
  auto unsupported_versions = {"tlsv1.1", "tlsv1.2"};
#endif
  for (auto v: versions) {
    auto ssl_version = ScopedFlagSetter<string>::Make(
        &FLAGS_ssl_minimum_version, v);

    MetricGroup metrics("webserver-test");
    Webserver webserver("", FLAGS_webserver_port, &metrics);
    ASSERT_OK(webserver.Start());
  }

  for (auto v : unsupported_versions) {
    auto ssl_version = ScopedFlagSetter<string>::Make(&FLAGS_ssl_minimum_version, v);

    MetricGroup metrics("webserver-test");
    Webserver webserver("", FLAGS_webserver_port, &metrics);
    EXPECT_FALSE(webserver.Start().ok()) << "Version: " << v;
  }
}

using kudu::MiniKdc;
using kudu::MiniKdcOptions;

void CheckAuthMetrics(MetricGroup* metrics, int num_negotiate_success,
    int num_negotiate_failure, int num_cookie_success, int num_cookie_failure) {
  IntCounter* negotiate_success_metric = metrics->FindMetricForTesting<IntCounter>(
      "impala.webserver.total-negotiate-auth-success");
  ASSERT_EQ(negotiate_success_metric->GetValue(), num_negotiate_success);
  IntCounter* negotiate_failure_metric = metrics->FindMetricForTesting<IntCounter>(
      "impala.webserver.total-negotiate-auth-failure");
  ASSERT_EQ(negotiate_failure_metric->GetValue(), num_negotiate_failure);
  IntCounter* cookie_success_metric = metrics->FindMetricForTesting<IntCounter>(
      "impala.webserver.total-cookie-auth-success");
  ASSERT_EQ(cookie_success_metric->GetValue(), num_cookie_success);
  IntCounter* cookie_failure_metric = metrics->FindMetricForTesting<IntCounter>(
      "impala.webserver.total-cookie-auth-failure");
  ASSERT_EQ(cookie_failure_metric->GetValue(), num_cookie_failure);
}

TEST(Webserver, TestWithSpnego) {
  MiniKdc kdc(MiniKdcOptions{});
  KUDU_ASSERT_OK(kdc.Start());
  kdc.SetKrb5Environment();

  string kt_path;
  KUDU_ASSERT_OK(kdc.CreateServiceKeytab("HTTP/127.0.0.1", &kt_path));
  CHECK_ERR(setenv("KRB5_KTNAME", kt_path.c_str(), 1));
  KUDU_ASSERT_OK(kdc.CreateUserPrincipal("alice"));

  gflags::FlagSaver saver;
  FLAGS_webserver_require_spnego = true;
  FLAGS_webserver_ldap_passwords_in_clear_ok = true;
  FLAGS_cookie_require_secure = false;

  MetricGroup metrics("webserver-test");
  Webserver webserver("", FLAGS_webserver_port, &metrics);
  ASSERT_OK(webserver.Start());

  // Don't expect HTTP requests to work without Kerberos credentials.
  stringstream contents;
  ASSERT_ERROR_MSG(HttpGet("localhost", FLAGS_webserver_port, "/", &contents),
      "Unexpected status code: 401");
  // There should be one failed auth attempt.
  CheckAuthMetrics(&metrics, 0, 1, 0, 0);

  // TODO(todd) IMPALA-8987: import curl into native-toolchain and test this with
  // authentication.
  string curl_output;
  RunShellProcess("curl --version", &curl_output);

  // Detect curl version. We only care about the major and minor.
  std::regex curl_version_regex = std::regex("curl ([0-9]+)\\.([0-9]+)\\.[0-9]+");
  std::smatch match_result;
  ASSERT_TRUE(std::regex_search(curl_output, match_result, curl_version_regex));
  ASSERT_EQ(match_result.size(), 3);

  int curl_major_version = std::stoi(match_result[1]);
  int curl_minor_version = std::stoi(match_result[2]);
  bool curl_7_64_or_above = true;
  if (curl_major_version < 7 ||
      (curl_major_version == 7 && curl_minor_version < 64)) {
    curl_7_64_or_above = false;
  }
  LOG(INFO) << "Detected curl version " << std::to_string(curl_major_version) << "."
            << std::to_string(curl_minor_version) << " "
            << (curl_7_64_or_above ? "is at least 7.64" : "is below 7.64");

  if (curl_output.find("GSS-API") != string::npos
      && curl_output.find("SPNEGO") != string::npos) {
    // Test that OPTIONS works with and without having kinit-ed.
    string options_cmd =
        Substitute("curl -X OPTIONS -v --negotiate -u : 'http://127.0.0.1:$0'",
            FLAGS_webserver_port);
    system(options_cmd.c_str());
    KUDU_ASSERT_OK(kdc.Kinit("alice"));
    system(options_cmd.c_str());

    // Test that GET works with cookies.
    filesystem::path cookie_dir = filesystem::unique_path();
    filesystem::create_directories(cookie_dir);
    filesystem::path cookie_path = cookie_dir / "cookiejar";
    LOG(INFO) << "Storing cookies in " << cookie_path;
    string curl_cmd =
        Substitute("curl -c $0 -b $0 -X GET -v --negotiate -u : 'http://127.0.0.1:$1'",
            cookie_path.string(), FLAGS_webserver_port);
    // Run the command twice, the first time we should authenticate with SPNEGO, the
    // second time with a cookie.
    system(Substitute("$0 && $0", curl_cmd).c_str());
    // curl behavior changed in 7.64.0. Before 7.64.0, there would be one more failed
    // auth attempt, when curl first tries to connect without authentication, then
    // one successful attempt, then a successful cookie auth. Starting with 7.64,
    // curl does not do the initial attempt without authentication, so there is no
    // additional failed auth attempt.
    CheckAuthMetrics(&metrics, 1, (curl_7_64_or_above ? 1 : 2), 1, 0);

    webserver.Stop();
    MetricGroup metrics2("webserver-test");
    Webserver webserver2("", FLAGS_webserver_port, &metrics2);
    ASSERT_OK(webserver2.Start());
    // Run the command again. We should get a failed cookie attempt because the new
    // webserver uses a different HMAC key. See above note about curl 7.64.0 or above.
    system(curl_cmd.c_str());
    CheckAuthMetrics(&metrics2, 1, (curl_7_64_or_above ? 0 : 1), 0, 1);

    filesystem::remove_all(cookie_dir);
  } else {
    LOG(INFO) << "Skipping test, curl was not present or did not have the required "
              << "features: " << curl_output;
  }
}

TEST(Webserver, StartWithPasswordFileTest) {
  stringstream password_file;
  password_file << getenv("IMPALA_HOME") << "/be/src/testutil/htpasswd";
  auto password =
      ScopedFlagSetter<string>::Make(&FLAGS_webserver_password_file, password_file.str());

  MetricGroup metrics("webserver-test");
  Webserver webserver("", FLAGS_webserver_port, &metrics);
  if (FIPS_mode()) {
    ASSERT_FALSE(webserver.Start().ok());
  } else {
    ASSERT_OK(webserver.Start());

    // Don't expect HTTP requests to work without a password
    stringstream contents;
    ASSERT_ERROR_MSG(HttpGet("localhost", FLAGS_webserver_port, "/", &contents),
        "Unexpected status code: 401");
  }
}

TEST(Webserver, StartWithMissingPasswordFileTest) {
  if (FIPS_mode()) return;
  stringstream password_file;
  password_file << getenv("IMPALA_HOME") << "/be/src/testutil/doesntexist";
  auto password =
      ScopedFlagSetter<string>::Make(&FLAGS_webserver_password_file, password_file.str());

  MetricGroup metrics("webserver-test");
  Webserver webserver("", FLAGS_webserver_port, &metrics);
  ASSERT_FALSE(webserver.Start().ok());
}

TEST(Webserver, DirectoryListingDisabledTest) {
  MetricGroup metrics("webserver-test");
  Webserver webserver("", FLAGS_webserver_port, &metrics);
  ASSERT_OK(webserver.Start());
  stringstream contents;
  ASSERT_OK(HttpGet("localhost", FLAGS_webserver_port,
      "/www/bootstrap/", &contents, 403));
  ASSERT_TRUE(contents.str().find("Directory listing denied") != string::npos);
}

void FrameCallback(const Webserver::WebRequest& req, Document* document) {
  const string contents = "<frameset cols='50%,50%'><frame src='/metrics'></frameset>";
  Value value(contents.c_str(), document->GetAllocator());
  document->AddMember("contents", value, document->GetAllocator());
}

TEST(Webserver, NoFrameEmbeddingTest) {
  const string FRAME_TEST_PATH = "/frames_test";
  MetricGroup metrics("webserver-test");
  Webserver webserver("", FLAGS_webserver_port, &metrics);
  Webserver::UrlCallback callback = bind<void>(FrameCallback, _1, _2);
  webserver.RegisterUrlCallback(FRAME_TEST_PATH, "raw_text.tmpl", callback, true);
  ASSERT_OK(webserver.Start());
  stringstream contents;
  ASSERT_OK(HttpGet("localhost", FLAGS_webserver_port,
      FRAME_TEST_PATH, &contents, 200));

  // Confirm that there is an HTTP header to deny framing
  ASSERT_FALSE(contents.str().find("X-Frame-Options: DENY") == string::npos);
}
TEST(Webserver, FrameAllowEmbeddingTest) {
  const string FRAME_TEST_PATH = "/frames_test";
  auto x_frame_opt =
      ScopedFlagSetter<string>::Make(&FLAGS_webserver_x_frame_options, "ALLOWALL");
  MetricGroup metrics("webserver-test");
  Webserver webserver("", FLAGS_webserver_port, &metrics);
  Webserver::UrlCallback callback = bind<void>(FrameCallback, _1, _2);
  webserver.RegisterUrlCallback(FRAME_TEST_PATH, "raw_text.tmpl", callback, true);
  ASSERT_OK(webserver.Start());
  stringstream contents;
  ASSERT_OK(HttpGet("localhost", FLAGS_webserver_port,
      FRAME_TEST_PATH, &contents, 200));

  // Confirm that there is an HTTP header to allow framing
  ASSERT_FALSE(contents.str().find("X-Frame-Options: ALLOWALL") == string::npos);
}

const string STRING_WITH_NULL = "123456789\0ABCDE";

void NullCharCallback(const Webserver::WebRequest& req, stringstream* out,
    kudu::HttpStatusCode* response) {
  (*out) << STRING_WITH_NULL;
}

TEST(Webserver, NullCharTest) {
  const string NULL_CHAR_TEST_PATH = "/null-char-test";
  MetricGroup metrics("webserver-test");
  Webserver webserver("", FLAGS_webserver_port, &metrics);
  webserver.RegisterUrlCallback(NULL_CHAR_TEST_PATH, NullCharCallback);
  ASSERT_OK(webserver.Start());
  stringstream contents;
  ASSERT_OK(
      HttpGet("localhost", FLAGS_webserver_port, NULL_CHAR_TEST_PATH, &contents));
  ASSERT_TRUE(contents.str().find(STRING_WITH_NULL) != string::npos);
}

TEST(Webserver, Options) {
  MetricGroup metrics("webserver-test");
  Webserver webserver("", FLAGS_webserver_port, &metrics);
  ASSERT_OK(webserver.Start());
  stringstream contents;
  ASSERT_OK(HttpGet("localhost", FLAGS_webserver_port, "/", &contents, 200, "OPTIONS"));
  ASSERT_FALSE(contents.str().find("Allow: GET, POST, HEAD, OPTIONS")
      == string::npos);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  InitCommonRuntime(argc, argv, false, TestInfo::BE_TEST);
  FLAGS_webserver_port = 27890;
  return RUN_ALL_TESTS();
}
