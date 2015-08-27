// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "util/webserver.h"
#include "common/init.h"

#include <gtest/gtest.h>
#include <string>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>
#include <gutil/strings/substitute.h>

DECLARE_int32(webserver_port);
DECLARE_string(webserver_password_file);
DECLARE_string(webserver_certificate_file);
DECLARE_string(webserver_private_key_file);
DECLARE_string(webserver_private_key_password_cmd);

#include "common/names.h"

using boost::asio::ip::tcp;
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
    ostream* out, int expected_code = 200) {
  try {
    tcp::iostream request_stream;
    request_stream.connect(host, lexical_cast<string>(port));
    if (!request_stream) return Status("Could not connect request_stream");

    request_stream << "GET " << url_path << " HTTP/1.1\r\n";
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
  Webserver webserver(FLAGS_webserver_port);
  ASSERT_TRUE(webserver.Start().ok());

  stringstream contents;
  ASSERT_TRUE(HttpGet("localhost", FLAGS_webserver_port, "/", &contents).ok());
}

void AssertArgsCallback(bool* success, const Webserver::ArgumentMap& args,
    Document* document) {
  *success = args.find(TEST_ARG) != args.end();
}

TEST(Webserver, ArgsTest) {
  Webserver webserver(FLAGS_webserver_port);

  const string ARGS_TEST_PATH = "/args-test";
  bool success = false;
  Webserver::UrlCallback callback = bind<void>(AssertArgsCallback, &success , _1, _2);
  webserver.RegisterUrlCallback(ARGS_TEST_PATH, "json-test.tmpl", callback);

  ASSERT_TRUE(webserver.Start().ok());
  stringstream contents;
  ASSERT_TRUE(HttpGet("localhost", FLAGS_webserver_port, ARGS_TEST_PATH, &contents).ok());
  ASSERT_FALSE(success) << "Unexpectedly found " << TEST_ARG;

  ASSERT_TRUE(HttpGet("localhost", FLAGS_webserver_port,
      Substitute("$0?$1", ARGS_TEST_PATH, TEST_ARG), &contents).ok());
  ASSERT_TRUE(success) << "Did not find " << TEST_ARG;
}

void JsonCallback(bool always_text, const Webserver::ArgumentMap& args,
    Document* document) {
  document->AddMember(SALUTATION_KEY.c_str(), SALUTATION_VALUE.c_str(),
      document->GetAllocator());
  document->AddMember(TO_ESCAPE_KEY.c_str(), TO_ESCAPE_VALUE.c_str(),
      document->GetAllocator());
  if (always_text) {
    document->AddMember(Webserver::ENABLE_RAW_JSON_KEY, true, document->GetAllocator());
  }
}

TEST(Webserver, JsonTest) {
  Webserver webserver(FLAGS_webserver_port);

  const string JSON_TEST_PATH = "/json-test";
  const string RAW_TEXT_PATH = "/text";
  const string NO_TEMPLATE_PATH = "/no-template";
  Webserver::UrlCallback callback = bind<void>(JsonCallback, false, _1, _2);
  webserver.RegisterUrlCallback(JSON_TEST_PATH, "json-test.tmpl", callback);
  webserver.RegisterUrlCallback(NO_TEMPLATE_PATH, "doesnt-exist.tmpl", callback);

  Webserver::UrlCallback text_callback = bind<void>(JsonCallback, true, _1, _2);
  webserver.RegisterUrlCallback(RAW_TEXT_PATH, "json-test.tmpl", text_callback);
  ASSERT_TRUE(webserver.Start().ok());

  stringstream contents;
  ASSERT_TRUE(HttpGet("localhost", FLAGS_webserver_port, JSON_TEST_PATH, &contents).ok());
  ASSERT_TRUE(contents.str().find(SALUTATION_VALUE) != string::npos);
  ASSERT_TRUE(contents.str().find(SALUTATION_KEY) == string::npos);

  stringstream json_contents;
  ASSERT_TRUE(HttpGet("localhost", FLAGS_webserver_port,
      Substitute("$0?json", JSON_TEST_PATH), &json_contents).ok());
  ASSERT_TRUE(json_contents.str().find("\"Salutation\": \"Hello!\"") != string::npos);

  stringstream error_contents;
  ASSERT_TRUE(
      HttpGet("localhost", FLAGS_webserver_port, NO_TEMPLATE_PATH, &error_contents).ok());
  ASSERT_TRUE(error_contents.str().find("Could not open template: ") != string::npos);

  // Adding ?raw should send text
  stringstream raw_contents;
  ASSERT_TRUE(HttpGet("localhost", FLAGS_webserver_port,
          Substitute("$0?raw", JSON_TEST_PATH), &raw_contents).ok());
  ASSERT_TRUE(raw_contents.str().find("text/plain") != string::npos);

  // Any callback that includes ENABLE_RAW_JSON_KEY should always return text.
  stringstream raw_cb_contents;
  ASSERT_TRUE(HttpGet("localhost", FLAGS_webserver_port, RAW_TEXT_PATH,
      &raw_cb_contents).ok());
  ASSERT_TRUE(raw_cb_contents.str().find("text/plain") != string::npos);
}

TEST(Webserver, EscapingTest) {
  Webserver webserver(FLAGS_webserver_port);

  const string JSON_TEST_PATH = "/json-test";
  Webserver::UrlCallback callback = bind<void>(JsonCallback, false, _1, _2);
  webserver.RegisterUrlCallback(JSON_TEST_PATH, "json-test.tmpl", callback);
  ASSERT_TRUE(webserver.Start().ok());
  stringstream contents;
  ASSERT_TRUE(HttpGet("localhost", FLAGS_webserver_port, JSON_TEST_PATH, &contents).ok());
  ASSERT_TRUE(contents.str().find(ESCAPED_VALUE) != string::npos);
  ASSERT_TRUE(contents.str().find(TO_ESCAPE_VALUE) == string::npos);
}

TEST(Webserver, EscapeErrorUriTest) {
  Webserver webserver(FLAGS_webserver_port);
  ASSERT_TRUE(webserver.Start().ok());
  stringstream contents;
  ASSERT_TRUE(HttpGet("localhost", FLAGS_webserver_port,
          "/dont-exist<script>alert(42);</script>", &contents, 404).ok());
  ASSERT_EQ(contents.str().find("<script>alert(42);</script>"), string::npos);
  ASSERT_TRUE(contents.str().find("dont-exist&lt;script&gt;alert(42);&lt;/script&gt;") !=
      string::npos);
}

template<typename T>
struct ScopedFlagSetter {
  T* flag;
  T old_val;
  ScopedFlagSetter(T* f, T new_val) {
    flag = f;
    old_val = *f;
    *f = new_val;
  }

  ~ScopedFlagSetter() {
    *flag = old_val;
  }
};

TEST(Webserver, SslTest) {
  ScopedFlagSetter<string> certificate(&FLAGS_webserver_certificate_file,
      Substitute("$0/be/src/testutil/server-cert.pem", getenv("IMPALA_HOME")));
  ScopedFlagSetter<string> private_key(&FLAGS_webserver_private_key_file,
      Substitute("$0/be/src/testutil/server-key.pem", getenv("IMPALA_HOME")));

  Webserver webserver(FLAGS_webserver_port);
  ASSERT_TRUE(webserver.Start().ok());
}

TEST(Webserver, SslBadCertTest) {
  ScopedFlagSetter<string> certificate(&FLAGS_webserver_certificate_file,
      Substitute("$0/be/src/testutil/invalid-server-cert.pem", getenv("IMPALA_HOME")));
  ScopedFlagSetter<string> private_key(&FLAGS_webserver_private_key_file,
      Substitute("$0/be/src/testutil/server-key.pem", getenv("IMPALA_HOME")));

  Webserver webserver(FLAGS_webserver_port);
  ASSERT_FALSE(webserver.Start().ok());
}

TEST(Webserver, SslWithPrivateKeyPasswordTest) {
  ScopedFlagSetter<string> certificate(&FLAGS_webserver_certificate_file,
      Substitute("$0/be/src/testutil/server-cert.pem", getenv("IMPALA_HOME")));
  ScopedFlagSetter<string> private_key(&FLAGS_webserver_private_key_file,
      Substitute("$0/be/src/testutil/server-key-password.pem", getenv("IMPALA_HOME")));
  ScopedFlagSetter<string> password_cmd(
      &FLAGS_webserver_private_key_password_cmd, "echo password");

  Webserver webserver(FLAGS_webserver_port);
  ASSERT_TRUE(webserver.Start().ok());
}

TEST(Webserver, SslBadPrivateKeyPasswordTest) {
  ScopedFlagSetter<string> certificate(&FLAGS_webserver_certificate_file,
      Substitute("$0/be/src/testutil/server-cert.pem", getenv("IMPALA_HOME")));
  ScopedFlagSetter<string> private_key(&FLAGS_webserver_private_key_file,
      Substitute("$0/be/src/testutil/server-key-password.pem", getenv("IMPALA_HOME")));
  ScopedFlagSetter<string> password_cmd(
      &FLAGS_webserver_private_key_password_cmd, "echo wrongpassword");

  Webserver webserver(FLAGS_webserver_port);
  ASSERT_FALSE(webserver.Start().ok());
}

TEST(Webserver, StartWithPasswordFileTest) {
  stringstream password_file;
  password_file << getenv("IMPALA_HOME") << "/be/src/testutil/htpasswd";
  ScopedFlagSetter<string> password_flag(&FLAGS_webserver_password_file,
      password_file.str());

  Webserver webserver(FLAGS_webserver_port);
  ASSERT_TRUE(webserver.Start().ok());

  // Don't expect HTTP requests to work without a password
  stringstream contents;
  ASSERT_FALSE(HttpGet("localhost", FLAGS_webserver_port, "/", &contents).ok());
}

TEST(Webserver, StartWithMissingPasswordFileTest) {
  stringstream password_file;
  password_file << getenv("IMPALA_HOME") << "/be/src/testutil/doesntexist";
  ScopedFlagSetter<string> password_flag(&FLAGS_webserver_password_file,
      password_file.str());

  Webserver webserver(FLAGS_webserver_port);
  ASSERT_FALSE(webserver.Start().ok());
}

TEST(Webserver, DirectoryListingDisabledTest) {
  Webserver webserver(FLAGS_webserver_port);
  ASSERT_TRUE(webserver.Start().ok());
  stringstream contents;
  ASSERT_TRUE(HttpGet("localhost", FLAGS_webserver_port,
      "/www/bootstrap/", &contents, 403).ok());
  ASSERT_TRUE(contents.str().find("Directory listing denied") != string::npos);
}

void FrameCallback(const Webserver::ArgumentMap& args, Document* document) {
  const string& contents = "<frameset cols='50%,50%'><frame src='/metrics'></frameset>";
  document->AddMember("contents", contents.c_str(), document->GetAllocator());
}

TEST(Webserver, NoFrameEmbeddingTest) {
  const string FRAME_TEST_PATH = "/frames_test";
  Webserver webserver(FLAGS_webserver_port);
  Webserver::UrlCallback callback = bind<void>(FrameCallback, _1, _2);
  webserver.RegisterUrlCallback(FRAME_TEST_PATH, "raw-text.tmpl", callback);
  ASSERT_TRUE(webserver.Start().ok());
  stringstream contents;
  ASSERT_TRUE(HttpGet("localhost", FLAGS_webserver_port,
      FRAME_TEST_PATH, &contents, 200).ok());

  // Confirm that the embedded frame isn't rendered
  ASSERT_TRUE(contents.str().find("Metrics") == string::npos);
}

const string STRING_WITH_NULL = "123456789\0ABCDE";

void NullCharCallback(const Webserver::ArgumentMap& args, stringstream* out) {
  (*out) << STRING_WITH_NULL;
}

TEST(Webserver, NullCharTest) {
  const string NULL_CHAR_TEST_PATH = "/null-char-test";
  Webserver webserver(FLAGS_webserver_port);
  webserver.RegisterUrlCallback(NULL_CHAR_TEST_PATH, NullCharCallback);
  ASSERT_TRUE(webserver.Start().ok());
  stringstream contents;
  ASSERT_TRUE(
      HttpGet("localhost", FLAGS_webserver_port, NULL_CHAR_TEST_PATH, &contents).ok());
  ASSERT_TRUE(contents.str().find(STRING_WITH_NULL) != string::npos);
}


int main(int argc, char **argv) {
  InitCommonRuntime(argc, argv, false, TestInfo::BE_TEST);
  ::testing::InitGoogleTest(&argc, argv);
  FLAGS_webserver_port = 27890;
  return RUN_ALL_TESTS();
}
