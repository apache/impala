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
#include <boost/thread/shared_mutex.hpp>
#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include "common/logging.h"
#include "gutil/endian.h"
#include "gutil/strings/escaping.h"
#include "gutil/strings/strip.h"
#include "gutil/strings/substitute.h"
#include "kudu/security/gssapi.h"
#include "kudu/util/env.h"
#include "kudu/util/logging.h"
#include "kudu/util/net/sockaddr.h"
#include "rpc/authentication.h"
#include "rpc/cookie-util.h"
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
#include "util/metrics.h"
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
    " debug webserver authentication. Cannot be used with --webserver_require_ldap or "
    "--webserver_require_spnego.");

DEFINE_string(webserver_x_frame_options, "DENY",
    "webserver will add X-Frame-Options HTTP header with this value");
DEFINE_int32(webserver_max_post_length_bytes, 1024 * 1024,
             "The maximum length of a POST request that will be accepted by "
             "the embedded web server.");

DEFINE_bool(webserver_require_spnego, false,
    "Require connections to the web server to authenticate via Kerberos using SPNEGO. "
    "Cannot be used with --webserver_require_ldap or --webserver_password_file.");

DEFINE_bool(webserver_require_ldap, false,
    "Require connections to the web server to authenticate via LDAP using HTTP Basic "
    "authentication. Cannot be used with --webserver_require_spnego or "
    "--webserver_password_file.");
DEFINE_string(webserver_ldap_user_filter, "",
    "Comma separated list of usernames. If specified, users must be on this list for "
    "LDAP athentication to the webserver to succeed.");
DEFINE_string(webserver_ldap_group_filter, "",
    "Comma separated list of groups. If specified, users must belong to one of these "
    "groups for LDAP authentication to the webserver to succeed.");

DECLARE_bool(enable_ldap_auth);
DECLARE_string(hostname);
DECLARE_bool(is_coordinator);
DECLARE_bool(ldap_passwords_in_clear_ok);
DECLARE_int64(max_cookie_lifetime_s);
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

static const char* CRLF = "\r\n";

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
    case HttpStatusCode::AuthenticationRequired:
      return "401 Authentication Required";
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

void SendResponse(struct sq_connection* connection, const string& response_code_line,
    const string& content_type, const string& content,
    const vector<string>& header_lines) {
  // Buffer the output and send it in a single call to sq_write in order to avoid
  // triggering an interaction between Nagle's algorithm and TCP delayed acks.
  std::ostringstream oss;
  oss << "HTTP/1.1 " << response_code_line << CRLF;
  for (const auto& h : header_lines) {
    oss << h << CRLF;
  }
  oss << "X-Frame-Options: " << FLAGS_webserver_x_frame_options << CRLF;
  oss << "Content-Type: " << content_type << CRLF;
  oss << "Content-Length: " << content.size() << CRLF;
  oss << CRLF;
  oss << content;

  // Make sure to use sq_write for printing the body; sq_printf truncates at 8kb
  string output = oss.str();
  sq_write(connection, output.c_str(), output.length());
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
kudu::Status RunSpnegoStep(
    const char* authz_header, vector<string>* response_headers, string* authn_user) {
  string neg_token;
  if (authz_header && !TryStripPrefixString(authz_header, "Negotiate ", &neg_token)) {
    return kudu::Status::InvalidArgument("bad Negotiate header");
  }

  if (!authz_header) {
    response_headers->push_back("WWW-Authenticate: Negotiate");
    return kudu::Status::Incomplete("authn incomplete");
  }

  string resp_token_b64;
  bool is_complete;
  KUDU_RETURN_NOT_OK(kudu::gssapi::SpnegoStep(
      neg_token, &resp_token_b64, &is_complete, authn_user));

  if (!resp_token_b64.empty()) {
    response_headers->push_back(
        Substitute("WWW-Authenticate: Negotiate $0", resp_token_b64));
  }
   return is_complete ? kudu::Status::OK() : kudu::Status::Incomplete("authn incomplete");
}

} // anonymous namespace

Webserver::Webserver(MetricGroup* metrics)
  : Webserver(FLAGS_webserver_interface, FLAGS_webserver_port, metrics,
        GetConfiguredAuthMode()) {}

Webserver::Webserver(const string& interface, const int port, MetricGroup* metrics,
    const AuthMode& auth_mode)
  : auth_mode_(auth_mode),
    context_(nullptr),
    error_handler_(UrlHandler(
        bind<void>(&Webserver::ErrorHandler, this, _1, _2), "error.tmpl", false)),
    use_cookies_(FLAGS_max_cookie_lifetime_s > 0) {
  http_address_ = MakeNetworkAddress(interface.empty() ? "0.0.0.0" : interface, port);
  Init();

  if (auth_mode_ == AuthMode::SPNEGO) {
    total_negotiate_auth_success_ =
        metrics->AddCounter("impala.webserver.total-negotiate-auth-success", 0);
    total_negotiate_auth_failure_ =
        metrics->AddCounter("impala.webserver.total-negotiate-auth-failure", 0);
  }
  if (auth_mode_ == AuthMode::LDAP) {
    total_basic_auth_success_ =
        metrics->AddCounter("impala.webserver.total-basic-auth-success", 0);
    total_basic_auth_failure_ =
        metrics->AddCounter("impala.webserver.total-basic-auth-failure", 0);
  }
  if (use_cookies_ && (auth_mode_ == AuthMode::SPNEGO || auth_mode_ == AuthMode::LDAP)) {
    total_cookie_auth_success_ =
        metrics->AddCounter("impala.webserver.total-cookie-auth-success", 0);
    total_cookie_auth_failure_ =
        metrics->AddCounter("impala.webserver.total-cookie-auth-failure", 0);
  }
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

Status Webserver::Start() {
  LOG(INFO) << "Starting webserver on " << TNetworkAddressToString(http_address_);

  IpAddr ip;
  RETURN_IF_ERROR(HostnameToIpAddr(http_address_.hostname, &ip));
  stringstream listening_spec;
  listening_spec << ip << ":" << http_address_.port;

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

  if (auth_mode_ == AuthMode::SPNEGO) {
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
    LOG(INFO) << "Webserver: secured with SPNEGO authentication.";
  }

  if (auth_mode_ == AuthMode::LDAP) {
    if (!FLAGS_enable_ldap_auth) {
      return Status("Unable to secure web server with LDAP: LDAP authentication must be "
                    "configured for this daemon.");
    }
    if (!IsSecure() && !FLAGS_ldap_passwords_in_clear_ok) {
      return Status("Unable to secure web server with LDAP: must either enable SSL or "
                    "set --ldap_passwords_in_clear_ok=true");
    }

    ldap_.reset(new ImpalaLdap());
    RETURN_IF_ERROR(
        ldap_->Init(FLAGS_webserver_ldap_user_filter, FLAGS_webserver_ldap_group_filter));
    LOG(INFO) << "Webserver: secured with LDAP authentication.";
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

void Webserver::Init() {
  hostname_ = http_address_.hostname;
  if (IsWildcardAddress(http_address_.hostname)) {
    if (!FLAGS_hostname.empty()) {
      hostname_ = FLAGS_hostname;
    } else if (!GetHostname(&hostname_).ok()) {
      hostname_ = http_address_.hostname;
    }
  }
  url_ = Substitute(
      "$0://$1:$2", IsSecure() ? "https" : "http", hostname_, http_address_.port);
}

void Webserver::GetCommonJson(
    Document* document, const struct sq_connection* connection, const WebRequest& req) {
  DCHECK(document != nullptr);
  Value obj(kObjectType);
  obj.AddMember("process-name",
      rapidjson::StringRef(google::ProgramInvocationShortName()),
      document->GetAllocator());

  // If Apacke Knox is being used to proxy connections to the webserver, the
  // 'x-forwarded-context' header will be present.
  if (sq_get_header(connection, "x-forwarded-context")) {
    // When proxying connections through Apache Knox, we make all links on the webui
    // absolute, which allows Knox to rewrite the links to point to the Knox host while
    // including 'scheme', 'host', and 'port' parameters which tell Knox where do forward
    // the request to.
    Value url_value(url().c_str(), document->GetAllocator());
    obj.AddMember("host-url", url_value, document->GetAllocator());

    // These are used to add hidden form fields when Knox is being used to add the 'host'
    // and related parameters to the form's request.
    Value scheme_value(IsSecure() ? "https" : "http", document->GetAllocator());
    obj.AddMember("scheme", scheme_value, document->GetAllocator());
    Value hostname_value(hostname_.c_str(), document->GetAllocator());
    obj.AddMember("hostname", hostname_value, document->GetAllocator());
    Value port_value;
    port_value.SetInt(http_address_.port);
    obj.AddMember("port", port_value, document->GetAllocator());
  }

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
  if (VLOG_IS_ON(4)) {
    VLOG(4) << request_info->request_method << " " << request_info->uri << " "
            << request_info->http_version;
    for (int i = 0; i < request_info->num_headers; ++i) {
      VLOG(4) << "  " << request_info->http_headers[i].name << ": "
              << request_info->http_headers[i].value;
    }
  }

  if (strncmp("OPTIONS", request_info->request_method, 7) == 0) {
    // Let Squeasel deal with the request. OPTIONS requests should not require
    // authentication, so do this before doing SPNEGO.
    return SQ_CONTINUE_HANDLING;
  }

  vector<string> response_headers;
  bool authenticated = auth_mode_ != AuthMode::SPNEGO && auth_mode_ != AuthMode::LDAP;
  // Try authenticating with a cookie first, if enabled.
  if (!authenticated && use_cookies_) {
    const char* cookie_header = sq_get_header(connection, "Cookie");
    string username;
    if (cookie_header != nullptr) {
      Status cookie_status = AuthenticateCookie(hash_, cookie_header, &username);
      if (cookie_status.ok()) {
        authenticated = true;
        request_info->remote_user = strdup(username.c_str());
        total_cookie_auth_success_->Increment(1);
      } else {
        LOG(INFO) << "Invalid cookie provided: " << cookie_header << ": "
                  << cookie_status.GetDetail();
        response_headers.push_back(Substitute("Set-Cookie: $0", GetDeleteCookie()));
        total_cookie_auth_failure_->Increment(1);
      }
    }
  }

  if (!authenticated) {
    if (auth_mode_ == AuthMode::SPNEGO) {
      sq_callback_result_t spnego_result =
          HandleSpnego(connection, request_info, &response_headers);
      if (spnego_result == SQ_CONTINUE_HANDLING) {
        // Spnego negotiation was successful.
        AddCookie(request_info, &response_headers);
      } else {
        // Spnego negotiation is incomplete or failed, stop processing the request.
        return spnego_result;
      }
    } else {
      DCHECK(auth_mode_ == AuthMode::LDAP);
      Status basic_status = HandleBasic(connection, request_info, &response_headers);
      if (basic_status.ok()) {
        // Basic auth was successful.
        total_basic_auth_success_->Increment(1);
        AddCookie(request_info, &response_headers);
      } else {
        total_basic_auth_failure_->Increment(1);
        if (!sq_get_header(connection, "Authorization")) {
          // This case is expected, as some clients will always initially try to connect
          // without an 'Authorization' and only provide one after getting the
          // 'WWW-Authenticate' back, so we don't log it as an error.
          VLOG(2) << "Not authenticated: no Authorization header provided.";
        } else {
          LOG(ERROR) << "Failed to authenticate: " << basic_status.GetDetail();
        }
        response_headers.push_back("WWW-Authenticate: Basic");
        SendResponse(connection, "401 Authentication Required", "text/plain",
            "Must authenticate with Basic authentication.", response_headers);
        return SQ_HANDLED_OK;
      }
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
  req.source_socket = GetRemoteAddress(request_info).ToString();
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
    RenderUrlWithTemplate(connection, req, *url_handler, &output, &content_type);
  }

  VLOG(3) << "Rendering page " << request_info->uri << " took "
          << PrettyPrinter::Print(sw.ElapsedTime(), TUnit::TIME_NS);

  SendResponse(connection, HttpStatusCodeToString(response),
      Webserver::GetMimeType(content_type), output.str(), response_headers);

  return SQ_HANDLED_OK;
}

sq_callback_result_t Webserver::HandleSpnego(struct sq_connection* connection,
    struct sq_request_info* request_info, vector<string>* response_headers) {
  const char* authz_header = sq_get_header(connection, "Authorization");
  string authn_princ;
  kudu::Status s = RunSpnegoStep(authz_header, response_headers, &authn_princ);
  if (s.IsIncomplete()) {
    SendResponse(connection, "401 Authentication Required", "text/plain",
        "Must authenticate with SPNEGO.", *response_headers);
    total_negotiate_auth_failure_->Increment(1);
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

    SendResponse(connection, http_status, "text/plain", s.ToString(), *response_headers);
    total_negotiate_auth_failure_->Increment(1);
    return SQ_HANDLED_OK;
  }

  request_info->remote_user = strdup(authn_princ.c_str());

  total_negotiate_auth_success_->Increment(1);
  return SQ_CONTINUE_HANDLING;
}

Status Webserver::HandleBasic(struct sq_connection* connection,
    struct sq_request_info* request_info, vector<string>* response_headers) {
  const char* authz_header = sq_get_header(connection, "Authorization");
  if (!authz_header) {
    return Status::Expected("No Authorization header provided.");
  }

  string base64;
  if (!TryStripPrefixString(authz_header, "Basic ", &base64)) {
    return Status::Expected("No Basic authentication provided.");
  }

  string decoded;
  if (!Base64Unescape(base64, &decoded)) {
    return Status::Expected("Failed to decode base64 basic authentication token.");
  }

  std::size_t colon = decoded.find(':');
  if (colon == std::string::npos) {
    return Status::Expected("Invalid basic authentication token format, missing ':'.");
  }
  string username = decoded.substr(0, colon);
  string password = decoded.substr(colon + 1);
  if (ldap_->LdapCheckPass(username.c_str(), password.c_str(), password.length())
      && ldap_->LdapCheckFilters(username)) {
    request_info->remote_user = strdup(username.c_str());
    return Status::OK();
  }

  return Status::Expected("Failed to authenticate to LDAP.");
}

void Webserver::AddCookie(
    struct sq_request_info* request_info, vector<string>* response_headers) {
  if (use_cookies_) {
    // If cookie auth failed and we generated a 'delete cookie' header, remove it.
    auto eq = [](const string& header) { return header.rfind("Set-Cookie", 0) == 0; };
    auto it = find_if(response_headers->begin(), response_headers->end(), eq);
    if (it != response_headers->end()) {
      response_headers->erase(it);
    }
    // Generate a cookie to return.
    response_headers->push_back(
        Substitute("Set-Cookie: $0", GenerateCookie(request_info->remote_user, hash_)));
  }
}

void Webserver::RenderUrlWithTemplate(const struct sq_connection* connection,
    const WebRequest& req, const UrlHandler& url_handler, stringstream* output,
    ContentType* content_type) {
  Document document;
  document.SetObject();
  GetCommonJson(&document, connection, req);

  const auto& arguments = req.parsed_args;
  url_handler.callback()(req, &document);
  bool plain_json = (arguments.find("json") != arguments.end())
      || document.HasMember(ENABLE_PLAIN_JSON_KEY);
  if (plain_json) {
    // Callbacks may optionally be rendered as a text-only, pretty-printed Json document
    // (mostly for debugging or integration with third-party tools).
    StringBuffer strbuf;
    // Write the JSON out with human-readable formatting. The settings are tweaked to
    // reduce extraneous whitespace characters, compared to the default formatting.
    PrettyWriter<StringBuffer> writer(strbuf);
    writer.SetIndent('\t', 1);
    writer.SetFormatOptions(kFormatSingleLineArray);
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

Webserver::AuthMode Webserver::GetConfiguredAuthMode() {
  if (!FLAGS_webserver_password_file.empty()) {
    return AuthMode::HTPASSWD;
  } else if (FLAGS_webserver_require_spnego) {
    return AuthMode::SPNEGO;
  } else if (FLAGS_webserver_require_ldap) {
    return AuthMode::LDAP;
  }
  return AuthMode::NONE;
}
}
