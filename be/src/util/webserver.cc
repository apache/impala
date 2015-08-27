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

#include <boost/algorithm/string.hpp>
#include <boost/bind.hpp>
#include <boost/filesystem.hpp>
#include <boost/foreach.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/mem_fn.hpp>
#include <boost/thread/locks.hpp>
#include <gutil/strings/substitute.h>
#include <map>
#include <fstream>
#include <stdio.h>
#include <signal.h>
#include <string>
#include <mustache/mustache.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/prettywriter.h>

#include "common/logging.h"
#include "util/cpu-info.h"
#include "util/disk-info.h"
#include "util/mem-info.h"
#include "util/os-info.h"
#include "util/os-util.h"
#include "util/process-state-info.h"
#include "util/url-coding.h"
#include "util/debug-util.h"
#include "util/pretty-printer.h"
#include "util/stopwatch.h"
#include "rpc/thrift-util.h"

#include "common/names.h"

using boost::algorithm::is_any_of;
using boost::algorithm::split;
using boost::algorithm::trim_right;
using boost::algorithm::to_lower;
using boost::filesystem::exists;
using boost::upgrade_to_unique_lock;
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
    " counterpart to the public key contained in --ssl_server_certificate. If "
    "--ssl_server_certificate is set, this option must be set as well.");
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

static const char* DOC_FOLDER = "/www/";
static const int DOC_FOLDER_LEN = strlen(DOC_FOLDER);

// Easy-to-read constants for Squeasel return codes
static const uint32_t PROCESSING_COMPLETE = 1;
static const uint32_t NOT_PROCESSED = 0;

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
  if (impala_home == NULL) {
    return ""; // Empty document root means don't serve static files
  } else {
    ss << impala_home;
  }

  // Deliberate memory leak, but this should be called exactly once.
  string* str = new string(ss.str());
  return str->c_str();
}

namespace impala {

const char* Webserver::ENABLE_RAW_JSON_KEY = "__raw__";

// Supported HTTP response codes
enum ResponseCode {
  OK = 200,
  NOT_FOUND = 404
};

// Builds a valid HTTP header given the response code and a content type.
string BuildHeaderString(ResponseCode response, ContentType content_type) {
  static const string RESPONSE_TEMPLATE = "HTTP/1.1 $0 $1\r\n"
      "Content-Type: text/$2\r\n"
      "Content-Length: %d\r\n"
      "X-Frame-Options: DENY\r\n"
      "\r\n";

  return Substitute(RESPONSE_TEMPLATE, response, response == OK ? "OK" : "Not found",
      content_type == HTML ? "html" : "plain");
}

Webserver::Webserver()
    : context_(NULL),
      error_handler_(UrlHandler(bind<void>(&Webserver::ErrorHandler, this, _1, _2),
          "error.tmpl", false)) {
  http_address_ = MakeNetworkAddress(
      FLAGS_webserver_interface.empty() ? "0.0.0.0" : FLAGS_webserver_interface,
      FLAGS_webserver_port);
}

Webserver::Webserver(const int port)
    : context_(NULL),
      error_handler_(UrlHandler(bind<void>(&Webserver::ErrorHandler, this, _1, _2),
          "error.tmpl", false)) {
  http_address_ = MakeNetworkAddress("0.0.0.0", port);
}

Webserver::~Webserver() {
  Stop();
}

void Webserver::RootHandler(const ArgumentMap& args, Document* document) {
  Value version(GetVersionString().c_str(), document->GetAllocator());
  document->AddMember("version", version, document->GetAllocator());
  Value cpu_info(CpuInfo::DebugString().c_str(), document->GetAllocator());
  document->AddMember("cpu_info", cpu_info, document->GetAllocator());
  Value mem_info(MemInfo::DebugString().c_str(), document->GetAllocator());
  document->AddMember("mem_info", mem_info, document->GetAllocator());
  Value disk_info(DiskInfo::DebugString().c_str(), document->GetAllocator());
  document->AddMember("disk_info", disk_info, document->GetAllocator());
  Value os_info(OsInfo::DebugString().c_str(), document->GetAllocator());
  document->AddMember("os_info", os_info, document->GetAllocator());
  Value process_state_info(ProcessStateInfo().DebugString().c_str(),
    document->GetAllocator());
  document->AddMember("process_state_info", process_state_info,
    document->GetAllocator());
}

void Webserver::ErrorHandler(const ArgumentMap& args, Document* document) {
  ArgumentMap::const_iterator it = args.find(ERROR_KEY);
  if (it == args.end()) return;

  Value error(it->second.c_str(), document->GetAllocator());
  document->AddMember("error", error, document->GetAllocator());
}

void Webserver::BuildArgumentMap(const string& args, ArgumentMap* output) {
  vector<string> arg_pairs;
  split(arg_pairs, args, is_any_of("&"));

  BOOST_FOREACH(const string& arg_pair, arg_pairs) {
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
  LOG(INFO) << "Starting webserver on " << http_address_;

  stringstream listening_spec;
  listening_spec << http_address_;

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

  if (IsSecure()) {
    options.push_back("ssl_certificate");
    options.push_back(FLAGS_webserver_certificate_file.c_str());

    if (!FLAGS_webserver_private_key_file.empty()) {
      options.push_back("ssl_private_key");
      options.push_back(FLAGS_webserver_private_key_file.c_str());

      if (!FLAGS_webserver_private_key_password_cmd.empty()) {
        string key_password;
        if (!RunShellProcess(FLAGS_webserver_private_key_password_cmd, &key_password)) {
          return Status(TErrorCode::SSL_PASSWORD_CMD_FAILED,
              FLAGS_webserver_private_key_password_cmd, key_password);
        }
        trim_right(key_password);
        options.push_back("ssl_private_key_password");
        options.push_back(key_password.c_str());
      }
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

  options.push_back("listening_ports");
  options.push_back(listening_str.c_str());

  options.push_back("enable_directory_listing");
  options.push_back("no");

  // Options must be a NULL-terminated list
  options.push_back(NULL);

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
  context_ = sq_start(&callbacks, reinterpret_cast<void*>(this), &options[0]);

  // Restore the child signal handler so wait() works properly.
  signal(SIGCHLD, sig_chld);

  if (context_ == NULL) {
    stringstream error_msg;
    error_msg << "Webserver: Could not start on address " << http_address_;
    return Status(error_msg.str());
  }

  UrlCallback default_callback =
      bind<void>(mem_fn(&Webserver::RootHandler), this, _1, _2);

  RegisterUrlCallback("/", "root.tmpl", default_callback, false);

  LOG(INFO) << "Webserver started";
  return Status::OK();
}

void Webserver::Stop() {
  if (context_ != NULL) {
    sq_stop(context_);
    context_ = NULL;
  }
}

void Webserver::GetCommonJson(Document* document) {
  DCHECK(document != NULL);
  Value obj(kObjectType);
  obj.AddMember("process-name", google::ProgramInvocationShortName(),
      document->GetAllocator());

  Value lst(kArrayType);
  BOOST_FOREACH(const UrlHandlerMap::value_type& handler, url_handlers_) {
    if (handler.second.is_on_nav_bar()) {
      Value obj(kObjectType);
      obj.AddMember("link", handler.first.c_str(), document->GetAllocator());
      obj.AddMember("title", handler.first.c_str(), document->GetAllocator());
      lst.PushBack(obj, document->GetAllocator());
    }
  }

  obj.AddMember("navbar", lst, document->GetAllocator());
  document->AddMember(COMMON_JSON_KEY, obj, document->GetAllocator());
}

int Webserver::LogMessageCallbackStatic(const struct sq_connection* connection,
    const char* message) {
  if (message != NULL) {
    LOG(INFO) << "Webserver: " << message;
  }
  return PROCESSING_COMPLETE;
}

int Webserver::BeginRequestCallbackStatic(struct sq_connection* connection) {
  struct sq_request_info* request_info = sq_get_request_info(connection);
  Webserver* instance = reinterpret_cast<Webserver*>(request_info->user_data);
  return instance->BeginRequestCallback(connection, request_info);
}

int Webserver::BeginRequestCallback(struct sq_connection* connection,
    struct sq_request_info* request_info) {
  if (!FLAGS_webserver_doc_root.empty() && FLAGS_enable_webserver_doc_root) {
    if (strncmp(DOC_FOLDER, request_info->uri, DOC_FOLDER_LEN) == 0) {
      VLOG(2) << "HTTP File access: " << request_info->uri;
      // Let Squeasel deal with this request; returning NULL will fall through
      // to the default handler which will serve files.
      return NOT_PROCESSED;
    }
  }

  map<string, string> arguments;
  if (request_info->query_string != NULL) {
    BuildArgumentMap(request_info->query_string, &arguments);
  }

  shared_lock<shared_mutex> lock(url_handlers_lock_);
  UrlHandlerMap::const_iterator it = url_handlers_.find(request_info->uri);
  ResponseCode response = OK;
  ContentType content_type = HTML;
  const UrlHandler* url_handler = NULL;
  if (it == url_handlers_.end()) {
    response = NOT_FOUND;
    arguments[ERROR_KEY] = Substitute("No URI handler for '$0'", request_info->uri);
    url_handler = &error_handler_;
  } else {
    url_handler = &it->second;
  }

  MonotonicStopWatch sw;
  sw.Start();

  // The output of this page is accumulated into this stringstream.
  stringstream output;
  if (!url_handler->use_templates()) {
    content_type = PLAIN;
    url_handler->raw_callback()(arguments, &output);
  } else {
    RenderUrlWithTemplate(arguments, *url_handler, &output, &content_type);
  }

  VLOG(3) << "Rendering page " << request_info->uri << " took "
          << PrettyPrinter::Print(sw.ElapsedTime(), TUnit::CPU_TICKS);

  const string& str = output.str();

  const string& headers = BuildHeaderString(response, content_type);
  sq_printf(connection, headers.c_str(), (int)str.length());

  // Make sure to use sq_write for printing the body; sq_printf truncates at 8kb
  sq_write(connection, str.c_str(), str.length());
  return PROCESSING_COMPLETE;
}

void Webserver::RenderUrlWithTemplate(const ArgumentMap& arguments,
    const UrlHandler& url_handler, stringstream* output, ContentType* content_type) {
  Document document;
  document.SetObject();
  GetCommonJson(&document);

  bool raw_json = (arguments.find("json") != arguments.end());
  url_handler.callback()(arguments, &document);
  if (raw_json) {
    // Callbacks may optionally be rendered as a text-only, pretty-printed Json document
    // (mostly for debugging or integration with third-party tools).
    StringBuffer strbuf;
    PrettyWriter<StringBuffer> writer(strbuf);
    document.Accept(writer);
    (*output) << strbuf.GetString();
    *content_type = PLAIN;
  } else {
    if (arguments.find("raw") != arguments.end()) {
      document.AddMember(ENABLE_RAW_JSON_KEY, "true", document.GetAllocator());
    }
    if (document.HasMember(ENABLE_RAW_JSON_KEY)) {
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
      RenderTemplate(buffer.str(), Substitute("$0/", FLAGS_webserver_doc_root), document,
          output);
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

}
