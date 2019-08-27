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

#include "util/webserver.h"

#include <signal.h>
#include <stdio.h>
#include <fstream>
#include <map>
#include <string>
#include <boost/algorithm/string.hpp>
#include <boost/bind.hpp>
#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/mem_fn.hpp>
#include <boost/thread/locks.hpp>
#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include "common/logging.h"
#include "gutil/endian.h"
#include "gutil/strings/substitute.h"
#include "gutil/strings/strip.h"
#include "kudu/util/env.h"
#include "kudu/util/logging.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/security/gssapi.h"
#include "rpc/thrift-util.h"
#include "runtime/exec-env.h"
#include "service/impala-server.h"
#include "thirdparty/mustache/mustache.h"
#include "util/asan.h"
#include "util/coding-util.h"
#include "util/cpu-info.h"
#include "util/debug-util.h"
#include "util/disk-info.h"
#include "util/mem-info.h"
#include "util/os-info.h"
#include "util/os-util.h"
#include "util/pretty-printer.h"
#include "util/process-state-info.h"
#include "util/stopwatch.h"

#include "common/names.h"

#ifdef __APPLE__
typedef sig_t sighandler_t;
#endif

using boost::algorithm::is_any_of;
using boost::algorithm::join;
using boost::algorithm::split;
using boost::algorithm::trim_right;
using boost::algorithm::to_lower;
using boost::filesystem::exists;
using boost::upgrade_to_unique_lock;
using kudu::HttpStatusCode;
using namespace google;
using namespace strings;
using namespace rapidjson;
using namespace mustache;

const char* GetDefaultDocumentRoot();

DEFINE_int32(webserver_port, 25000, "Port to start debug webserver on");
DEFINE_string(webserver_interface, "",
    "Interface to start debug webserver on. If blank, webserver binds to 0.0.0.0");
DEFINE_string(webserver_doc_root, GetDefaultDocumentRoot(),
    "Files under <webserver_doc_root>/www are accessible via the debug webserver. "
    "Defaults to $IMPALA_HOME, or if $IMPALA_HOME is not set, disables the document "
    "root");
DEFINE_bool(enable_webserver_doc_root, true,
    "If true, webserver may serve static files from the webserver_doc_root");

DEFINE_string(webserver_certificate_file, "",
    "The location of the debug webserver's SSL certificate file, in .pem format. If "
    "empty, webserver SSL support is not enabled");
DEFINE_string(webserver_private_key_file, "", "The full path to the private key used as a"
    " counterpart to the public key contained in --webserver_certificate_file. If "
    "--webserver_certificate_file is set, this option must be set as well.");
DEFINE_string(webserver_private_key_password_cmd, "", "A Unix command whose output "
    "returns the password used to decrypt the Webserver's certificate private key file "
    "specified in --webserver_private_key_file. If the .PEM key file is not "
    "password-protected, this command will not be invoked. The output of the command "
    "will be truncated to 1024 bytes, and then all trailing whitespace will be trimmed "
    "before it is used to decrypt the private key");
DEFINE_string(webserver_authentication_domain, "",
    "Domain used for debug webserver authentication");
DEFINE_string(webserver_password_file, "",
    "(Optional) Location of .htpasswd file containing user names and hashed passwords for"
    " debug webserver authentication");

DEFINE_string(webserver_x_frame_options, "DENY",
    "webserver will add X-Frame-Options HTTP header with this value");
DEFINE_int32(webserver_max_post_length_bytes, 1024 * 1024,
             "The maximum length of a POST request that will be accepted by "
             "the embedded web server.");

DEFINE_bool(webserver_require_spnego, false,
            "Require connections to the web server to authenticate via Kerberos "
            "using SPNEGO.");

DECLARE_bool(is_coordinator);
DECLARE_string(ssl_minimum_version);
DECLARE_string(ssl_cipher_list);

static const char* DOC_FOLDER = "/www/";
static const int DOC_FOLDER_LEN = strlen(DOC_FOLDER);

// Standard key in the json document sent to templates for rendering. Must be kept in
// sync with the templates themselves.
static const char* COMMON_JSON_KEY = "__common__";

// Standard key used to add errors to the argument map passed to the webserver's error
// handler.
static const char* ERROR_KEY = "__error_msg__";

// Returns $IMPALA_HOME if set, otherwise /tmp/impala_www
const char* GetDefaultDocumentRoot() {
  stringstream ss;
  char* impala_home = getenv("IMPALA_HOME");
  if (impala_home == nullptr) {
    return ""; // Empty document root means don't serve static files
  } else {
    ss << impala_home;
  }

  // Deliberate memory leak, but this should be called exactly once.
  string* str = new string(ss.str());
  IGNORE_LEAKING_OBJECT(str);
  return str->c_str();
}

namespace impala {

const char* Webserver::ENABLE_RAW_HTML_KEY = "__raw__";

const char* Webserver::ENABLE_PLAIN_JSON_KEY = "__json__";

namespace {
string HttpStatusCodeToString(HttpStatusCode code) {
  switch (code) {
    case HttpStatusCode::Ok:
      return "200 OK";
    case HttpStatusCode::BadRequest:
      return "400 Bad Request";
    case HttpStatusCode::NotFound:
      return "404 Not Found";
    case HttpStatusCode::LengthRequired:
      return "411 Length Required";
    case HttpStatusCode::RequestEntityTooLarge:
      return "413 Request Entity Too Large";
    case HttpStatusCode::InternalServerError:
      return "500 Internal Server Error";
    case HttpStatusCode::ServiceUnavailable:
      return "503 Service Unavailable";
  }
  LOG(FATAL) << "Unexpected HTTP response code";
  return "";
}


void SendPlainResponse(struct sq_connection* connection,
                       const string& response_code_line,
                       const string& content,
                       const vector<string>& header_lines) {
  sq_printf(connection, "HTTP/1.1 %s\r\n", response_code_line.c_str());
  for (const auto& h : header_lines) {
    sq_printf(connection, "%s\r\n", h.c_str());
  }
  sq_printf(connection, "Content-Type: text/plain\r\n");
  sq_printf(connection, "Content-Length: %zd\r\n\r\n", content.size());
  sq_printf(connection, "%s", content.c_str());
}

// Return the address of the remote user from the squeasel request info.
kudu::Sockaddr GetRemoteAddress(const struct sq_request_info* req) {
  struct sockaddr_in addr;
  addr.sin_family = AF_INET;
  addr.sin_port = NetworkByteOrder::FromHost16(req->remote_port);
  addr.sin_addr.s_addr = NetworkByteOrder::FromHost32(req->remote_ip);
  return kudu::Sockaddr(addr);
}


// Performs a step of SPNEGO authorization by parsing the HTTP Authorization header
// 'authz_header' and running it through GSSAPI. If authentication fails or the header
// is invalid, a bad Status will be returned (and the other out-parameters left
// untouched).
kudu::Status RunSpnegoStep(const char* authz_header, string* resp_header,
                     string* authn_user) {
  string neg_token;
  if (authz_header && !TryStripPrefixString(authz_header, "Negotiate ", &neg_token)) {
    return kudu::Status::InvalidArgument("bad Negotiate header");
  }

  string resp_token_b64;
  bool is_complete;
  RETURN_NOT_OK(kudu::gssapi::SpnegoStep(
      neg_token, &resp_token_b64, &is_complete, authn_user));

  if (!resp_token_b64.empty()) {
    *resp_header = Substitute("WWW-Authenticate: Negotiate $0", resp_token_b64);
  }
   return is_complete ? kudu::Status::OK() : kudu::Status::Incomplete("authn incomplete");
}

} // anonymous namespace

// Builds a valid HTTP header given the response code and a content type.
string BuildHeaderString(HttpStatusCode response, ContentType content_type) {
  static const string RESPONSE_TEMPLATE = "HTTP/1.1 $0\r\n"
      "Content-Type: $1\r\n"
      "Content-Length: %d\r\n"
      "X-Frame-Options: $2\r\n"
      "\r\n";

  return Substitute(RESPONSE_TEMPLATE, HttpStatusCodeToString(response),
      Webserver::GetMimeType(content_type), FLAGS_webserver_x_frame_options.c_str());
}

Webserver::Webserver()
    : context_(nullptr),
      error_handler_(UrlHandler(bind<void>(&Webserver::ErrorHandler, this, _1, _2),
          "error.tmpl", false)) {
  http_address_ = MakeNetworkAddress(
      FLAGS_webserver_interface.empty() ? "0.0.0.0" : FLAGS_webserver_interface,
      FLAGS_webserver_port);
}

Webserver::Webserver(const int port)
    : context_(nullptr),
      error_handler_(UrlHandler(bind<void>(&Webserver::ErrorHandler, this, _1, _2),
          "error.tmpl", false)) {
  http_address_ = MakeNetworkAddress("0.0.0.0", port);
}

Webserver::~Webserver() {
  Stop();
}

void Webserver::ErrorHandler(const WebRequest& req, Document* document) {
  ArgumentMap::const_iterator it = req.parsed_args.find(ERROR_KEY);
  if (it == req.parsed_args.end()) return;

  Value error(it->second.c_str(), document->GetAllocator());
  document->AddMember("error", error, document->GetAllocator());
}

void Webserver::BuildArgumentMap(const string& args, ArgumentMap* output) {
  vector<string> arg_pairs;
  split(arg_pairs, args, is_any_of("&"));

  for (const string& arg_pair: arg_pairs) {
    vector<string> key_value;
    split(key_value, arg_pair, is_any_of("="));
    if (key_value.empty()) continue;

    string key;
    if (!UrlDecode(key_value[0], &key)) continue;
    string value;
    if (!UrlDecode((key_value.size() >= 2 ? key_value[1] : ""), &value)) continue;
    to_lower(key);
    (*output)[key] = value;
  }
}

bool Webserver::IsSecure() const {
  return !FLAGS_webserver_certificate_file.empty();
}

string Webserver::Url() {
  string hostname = http_address_.hostname;
  if (IsWildcardAddress(http_address_.hostname)) {
    if (!GetHostname(&hostname).ok()) {
      hostname = http_address_.hostname;
    }
  }
  return Substitute("$0://$1:$2", IsSecure() ? "https" : "http",
      hostname, http_address_.port);
}

Status Webserver::Start() {
  LOG(INFO) << "Starting webserver on " << TNetworkAddressToString(http_address_);

  stringstream listening_spec;
  listening_spec << TNetworkAddressToString(http_address_);

  if (IsSecure()) {
    LOG(INFO) << "Webserver: Enabling HTTPS support";
    // Squeasel makes sockets with 's' suffixes accept SSL traffic only
    listening_spec << "s";
  }
  string listening_str = listening_spec.str();
  vector<const char*> options;

  if (!FLAGS_webserver_doc_root.empty() && FLAGS_enable_webserver_doc_root) {
    LOG(INFO) << "Document root: " << FLAGS_webserver_doc_root;
    options.push_back("document_root");
    options.push_back(FLAGS_webserver_doc_root.c_str());
  } else {
    LOG(INFO)<< "Document root disabled";
  }

  string key_password;
  if (IsSecure()) {
    // Impala initializes OpenSSL (see authentication.h).
    options.push_back("ssl_global_init");
    options.push_back("false");

    options.push_back("ssl_certificate");
    options.push_back(FLAGS_webserver_certificate_file.c_str());

    if (!FLAGS_webserver_private_key_file.empty()) {
      options.push_back("ssl_private_key");
      options.push_back(FLAGS_webserver_private_key_file.c_str());

      const string& password_cmd = FLAGS_webserver_private_key_password_cmd;
      if (!password_cmd.empty()) {
        if (!RunShellProcess(password_cmd, &key_password, true)) {
          return Status(TErrorCode::SSL_PASSWORD_CMD_FAILED, password_cmd, key_password,
              {"JAVA_TOOL_OPTIONS"});
        }
        options.push_back("ssl_private_key_password");
        options.push_back(key_password.c_str());
      }
    }

    options.push_back("ssl_min_version");
    options.push_back(FLAGS_ssl_minimum_version.c_str());
    if (!FLAGS_ssl_cipher_list.empty()) {
      options.push_back("ssl_ciphers");
      options.push_back(FLAGS_ssl_cipher_list.c_str());
    }
  }

  if (!FLAGS_webserver_authentication_domain.empty()) {
    options.push_back("authentication_domain");
    options.push_back(FLAGS_webserver_authentication_domain.c_str());
  }

  if (!FLAGS_webserver_password_file.empty()) {
    // Squeasel doesn't log anything if it can't stat the password file (but will if it
    // can't open it, which it tries to do during a request)
    if (!exists(FLAGS_webserver_password_file)) {
      stringstream ss;
      ss << "Webserver: Password file does not exist: " << FLAGS_webserver_password_file;
      return Status(ss.str());
    }
    LOG(INFO) << "Webserver: Password file is " << FLAGS_webserver_password_file;
    options.push_back("global_auth_file");
    options.push_back(FLAGS_webserver_password_file.c_str());
  }

  if (FLAGS_webserver_require_spnego) {
    // If Kerberos has been configured, security::InitKerberosForServer() will
    // already have been called, ensuring that the keytab path has been
    // propagated into this environment variable where the GSSAPI calls will
    // pick it up. In other words, we aren't expecting users to pass in this
    // environment variable specifically.
    const char* kt_file = getenv("KRB5_KTNAME");
    if (!kt_file || !kudu::Env::Default()->FileExists(kt_file)) {
      return Status("Unable to configure web server for SPNEGO authentication: "
                    "must configure a keytab file for the server");
    }
  }

  options.push_back("listening_ports");
  options.push_back(listening_str.c_str());

  options.push_back("enable_directory_listing");
  options.push_back("no");

  options.push_back("enable_keep_alive");
  options.push_back("yes");

  // Options must be a NULL-terminated list
  options.push_back(nullptr);

  // squeasel ignores SIGCHLD and we need it to run kinit. This means that since
  // squeasel does not reap its own children CGI programs must be avoided.
  // Save the signal handler so we can restore it after squeasel sets it to be ignored.
  sighandler_t sig_chld = signal(SIGCHLD, SIG_DFL);

  sq_callbacks callbacks;
  memset(&callbacks, 0, sizeof(callbacks));
  callbacks.begin_request = &Webserver::BeginRequestCallbackStatic;
  callbacks.log_message = &Webserver::LogMessageCallbackStatic;

  // To work around not being able to pass member functions as C callbacks, we store a
  // pointer to this server in the per-server state, and register a static method as the
  // default callback. That method unpacks the pointer to this and calls the real
  // callback.
  context_ = sq_start(&callbacks, reinterpret_cast<void*>(this), options.data());

  // Restore the child signal handler so wait() works properly.
  signal(SIGCHLD, sig_chld);

  if (context_ == nullptr) {
    stringstream error_msg;
    error_msg << "Webserver: Could not start on address "
              << TNetworkAddressToString(http_address_);
    return Status(error_msg.str());
  }

  LOG(INFO) << "Webserver started";
  return Status::OK();
}

void Webserver::Stop() {
  if (context_ != nullptr) {
    sq_stop(context_);
    context_ = nullptr;
  }
}

void Webserver::GetCommonJson(Document* document) {
  DCHECK(document != nullptr);
  Value obj(kObjectType);
  obj.AddMember("process-name",
      rapidjson::StringRef(google::ProgramInvocationShortName()),
      document->GetAllocator());

  Value lst(kArrayType);
  for (const UrlHandlerMap::value_type& handler: url_handlers_) {
    if (handler.second.is_on_nav_bar()) {
      Value hdl(kObjectType);
      // Though we set link and title the same value, be careful with RapidJSON's MOVE
      // semantic. We create the values by deep-copy here.
      Value link(handler.first.c_str(), document->GetAllocator());
      Value title(handler.first.c_str(), document->GetAllocator());
      hdl.AddMember("link", link, document->GetAllocator());
      hdl.AddMember("title", title, document->GetAllocator());
      lst.PushBack(hdl, document->GetAllocator());
    }
  }

  obj.AddMember("navbar", lst, document->GetAllocator());
  document->AddMember(rapidjson::StringRef(COMMON_JSON_KEY), obj,
      document->GetAllocator());
}

int Webserver::LogMessageCallbackStatic(const struct sq_connection* connection,
    const char* message) {
  if (message != nullptr) {
    LOG(INFO) << "Webserver: " << message;
  }
  return SQ_HANDLED_OK;
}

sq_callback_result_t Webserver::BeginRequestCallbackStatic(
    struct sq_connection* connection) {
  struct sq_request_info* request_info = sq_get_request_info(connection);
  Webserver* instance = reinterpret_cast<Webserver*>(request_info->user_data);
  return instance->BeginRequestCallback(connection, request_info);
}

sq_callback_result_t Webserver::BeginRequestCallback(struct sq_connection* connection,
    struct sq_request_info* request_info) {
  if (FLAGS_webserver_require_spnego){
    sq_callback_result_t spnego_result = HandleSpnego(connection, request_info);
    if (spnego_result != SQ_CONTINUE_HANDLING) {
      return spnego_result;
    }
  }

  if (!FLAGS_webserver_doc_root.empty() && FLAGS_enable_webserver_doc_root) {
    if (strncmp(DOC_FOLDER, request_info->uri, DOC_FOLDER_LEN) == 0) {
      VLOG(2) << "HTTP File access: " << request_info->uri;
      // Let Squeasel deal with this request; returning NULL will fall through
      // to the default handler which will serve files.
      return SQ_CONTINUE_HANDLING;
    }
  }

  WebRequest req;
  if (request_info->query_string != nullptr) {
    req.query_string = request_info->query_string;
    BuildArgumentMap(request_info->query_string, &req.parsed_args);
  }

  HttpStatusCode response = HttpStatusCode::Ok;
  ContentType content_type = HTML;
  const UrlHandler* url_handler = nullptr;
  {
    shared_lock<shared_mutex> lock(url_handlers_lock_);
    UrlHandlerMap::const_iterator it = url_handlers_.find(request_info->uri);
    if (it == url_handlers_.end()) {
      response = HttpStatusCode::NotFound;
      req.parsed_args[ERROR_KEY] = Substitute("No URI handler for '$0'",
          request_info->uri);
      url_handler = &error_handler_;
    } else {
      url_handler = &it->second;
    }
  }

  MonotonicStopWatch sw;
  sw.Start();

  req.request_method = request_info->request_method;
  if (req.request_method == "POST") {
    const char* content_len_str = sq_get_header(connection, "Content-Length");
    int32_t content_len = 0;
    if (content_len_str == nullptr ||
        !safe_strto32(content_len_str, &content_len)) {
      sq_printf(connection,
                "HTTP/1.1 %s\r\n",
                HttpStatusCodeToString(HttpStatusCode::LengthRequired).c_str());
      return SQ_HANDLED_OK;
    }
    if (content_len > FLAGS_webserver_max_post_length_bytes) {
      // TODO: for this and other HTTP requests, we should log the
      // remote IP, etc.
      LOG(WARNING) << "Rejected POST with content length " << content_len;
      sq_printf(connection,
                "HTTP/1.1 %s\r\n",
                HttpStatusCodeToString(HttpStatusCode::RequestEntityTooLarge).c_str());
      return SQ_HANDLED_CLOSE_CONNECTION;
    }

    char buf[8192];
    int rem = content_len;
    while (rem > 0) {
      int n = sq_read(connection, buf, std::min<int>(sizeof(buf), rem));
      if (n <= 0) {
        LOG(WARNING) << "error reading POST data: expected "
                     << content_len << " bytes but only read "
                     << req.post_data.size();
        sq_printf(connection,
                  "HTTP/1.1 %s\r\n",
                  HttpStatusCodeToString(HttpStatusCode::InternalServerError).c_str());
        return SQ_HANDLED_CLOSE_CONNECTION;
      }

      req.post_data.append(buf, n);
      rem -= n;
    }
  }

  // The output of this page is accumulated into this stringstream.
  stringstream output;
  if (!url_handler->use_templates()) {
    content_type = PLAIN;
    url_handler->raw_callback()(req, &output, &response);
  } else {
    RenderUrlWithTemplate(req, *url_handler, &output, &content_type);
  }

  VLOG(3) << "Rendering page " << request_info->uri << " took "
          << PrettyPrinter::Print(sw.ElapsedTime(), TUnit::TIME_NS);

  const string& str = output.str();

  const string& headers = BuildHeaderString(response, content_type);

  // printf with a non-literal format string is a security concern, but BuildHeaderString
  // returns a limited set of strings and all members of that set are safe.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wformat-nonliteral"
  sq_printf(connection, headers.c_str(), (int)str.length());
#pragma clang diagnostic pop

  // Make sure to use sq_write for printing the body; sq_printf truncates at 8kb
  sq_write(connection, str.c_str(), str.length());
  return SQ_HANDLED_OK;
}

sq_callback_result_t Webserver::HandleSpnego(
    struct sq_connection* connection,
    struct sq_request_info* request_info) {
  const char* authz_header = sq_get_header(connection, "Authorization");
  string resp_header, authn_princ;
  kudu::Status s = RunSpnegoStep(authz_header, &resp_header, &authn_princ);
  if (s.IsIncomplete()) {
    SendPlainResponse(connection, "401 Authentication Required",
                      "Must authenticate with SPNEGO.",
                      { resp_header });
    return SQ_HANDLED_OK;
  }
  if (s.ok() && authn_princ.empty()) {
    s = kudu::Status::RuntimeError("SPNEGO indicated complete, but got empty principal");
    // Crash in debug builds, but fall through to treating as an error 500 in
    // release.
    LOG(DFATAL) << "Got no authenticated principal for SPNEGO-authenticated "
                << " connection from "
                << GetRemoteAddress(request_info).ToString()
                << ": " << s.ToString();
  }
  if (!s.ok()) {
    LOG(WARNING) << "Failed to authenticate request from "
                 << GetRemoteAddress(request_info).ToString()
                 << " via SPNEGO: " << s.ToString();
    const char* http_status = s.IsNotAuthorized() ? "401 Authentication Required" :
        "500 Internal Server Error";

    SendPlainResponse(connection, http_status, s.ToString(), {});
    return SQ_HANDLED_OK;
  }


  request_info->remote_user = strdup(authn_princ.c_str());

  // NOTE: According to the SPNEGO RFC (https://tools.ietf.org/html/rfc4559) it
  // is possible that a non-empty token will be returned along with the HTTP 200
  // response:
  //
  //     A status code 200 status response can also carry a "WWW-Authenticate"
  //     response header containing the final leg of an authentication.  In
  //     this case, the gssapi-data will be present.  Before using the
  //     contents of the response, the gssapi-data should be processed by
  //     gss_init_security_context to determine the state of the security
  //     context.  If this function indicates success, the response can be
  //     used by the application.  Otherwise, an appropriate action, based on
  //     the authentication status, should be taken.
  //
  //     For example, the authentication could have failed on the final leg if
  //     mutual authentication was requested and the server was not able to
  //     prove its identity.  In this case, the returned results are suspect.
  //     It is not always possible to mutually authenticate the server before
  //     the HTTP operation.  POST methods are in this category.
  //
  // In fact, from inspecting the MIT krb5 source code, it appears that this
  // only happens when the client requests mutual authentication by passing
  // 'GSS_C_MUTUAL_FLAG' when establishing its side of the protocol. In practice,
  // this seems to be widely unimplemented:
  //
  // - curl has some source code to support GSS_C_MUTUAL_FLAG, but in order to
  //   enable it, you have to modify a FALSE constant to TRUE and recompile curl.
  //   In fact, it was broken for all of 2015 without anyone noticing (see curl
  //   commit 73f1096335d468b5be7c3cc99045479c3314f433)
  //
  // - Chrome doesn't support mutual auth at all -- see DelegationTypeToFlag(...)
  //   in src/net/http/http_auth_gssapi_posix.cc.
  //
  // In practice, users depend on TLS to authenticate the server, and SPNEGO
  // is used to authenticate the client.
  //
  // Given this, and because actually sending back the token on an OK response
  // would require significant code restructuring (eg buffering the header until
  // after the response handler has run) we just ignore any response token, but
  // log a periodic warning just in case it turns out we're wrong about the above.
  if (!resp_header.empty()) {
    KLOG_EVERY_N_SECS(WARNING, 5) << "ignoring SPNEGO token on HTTP 200 response "
                                  << "for user " << authn_princ << " at host "
                                  << GetRemoteAddress(request_info).ToString();
  }
  return SQ_CONTINUE_HANDLING;
}

void Webserver::RenderUrlWithTemplate(const WebRequest& req,
    const UrlHandler& url_handler, stringstream* output, ContentType* content_type) {
  Document document;
  document.SetObject();
  GetCommonJson(&document);

  const auto& arguments = req.parsed_args;
  url_handler.callback()(req, &document);
  bool plain_json = (arguments.find("json") != arguments.end())
      || document.HasMember(ENABLE_PLAIN_JSON_KEY);
  if (plain_json) {
    // Callbacks may optionally be rendered as a text-only, pretty-printed Json document
    // (mostly for debugging or integration with third-party tools).
    StringBuffer strbuf;
    PrettyWriter<StringBuffer> writer(strbuf);
    document.Accept(writer);
    (*output) << strbuf.GetString();
    *content_type = JSON;
  } else {
    if (arguments.find("raw") != arguments.end()) {
      document.AddMember(rapidjson::StringRef(ENABLE_RAW_HTML_KEY), "true",
          document.GetAllocator());
    }
    if (document.HasMember(ENABLE_RAW_HTML_KEY)) {
      *content_type = PLAIN;
    }

    const string& full_template_path =
        Substitute("$0/$1/$2", FLAGS_webserver_doc_root, DOC_FOLDER,
            url_handler.template_filename());
    ifstream tmpl(full_template_path.c_str());
    if (!tmpl.is_open()) {
      (*output) << "Could not open template: " << full_template_path;
      *content_type = PLAIN;
    } else {
      stringstream buffer;
      buffer << tmpl.rdbuf();
      bool success = RenderTemplate(buffer.str(),
          Substitute("$0/", FLAGS_webserver_doc_root), document,
          output);
      LOG_IF(WARNING, !success) << "could not render template " << full_template_path;
    }
  }
}

void Webserver::RegisterUrlCallback(const string& path,
    const string& template_filename, const UrlCallback& callback, bool is_on_nav_bar) {
  upgrade_lock<shared_mutex> lock(url_handlers_lock_);
  upgrade_to_unique_lock<shared_mutex> writer_lock(lock);
  DCHECK(url_handlers_.find(path) == url_handlers_.end())
      << "Duplicate Url handler for: " << path;

  url_handlers_.insert(
      make_pair(path, UrlHandler(callback, template_filename, is_on_nav_bar)));
}

void Webserver::RegisterUrlCallback(const string& path, const RawUrlCallback& callback) {
  upgrade_lock<shared_mutex> lock(url_handlers_lock_);
  upgrade_to_unique_lock<shared_mutex> writer_lock(lock);
  DCHECK(url_handlers_.find(path) == url_handlers_.end())
      << "Duplicate Url handler for: " << path;

  url_handlers_.insert(make_pair(path, UrlHandler(callback)));
}

const string Webserver::GetMimeType(const ContentType& content_type) {
  switch (content_type) {
    case HTML: return "text/html; charset=UTF-8";
    case PLAIN: return "text/plain; charset=UTF-8";
    case JSON: return "application/json";
    default:
      DCHECK(false) << "Invalid content_type: " << content_type;
      return "";
  }
}
}
