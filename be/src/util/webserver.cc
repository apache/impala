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

#include <stdio.h>
#include <signal.h>
#include <string>
#include <map>
#include <boost/lexical_cast.hpp>
#include <boost/foreach.hpp>
#include <boost/bind.hpp>
#include <boost/mem_fn.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>

#include "common/logging.h"
#include "util/cpu-info.h"
#include "util/disk-info.h"
#include "util/mem-info.h"
#include "util/url-coding.h"
#include "util/logging.h"
#include "util/debug-util.h"
#include "util/thrift-util.h"

using namespace std;
using namespace boost;
using namespace boost::filesystem;
using namespace google;

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
DEFINE_string(webserver_authentication_domain, "",
    "Domain used for debug webserver authentication");
DEFINE_string(webserver_password_file, "",
    "(Optional) Location of .htpasswd file containing user names and hashed passwords for"
    " debug webserver authentication");

static const char* DOC_FOLDER = "/www/";
static const int DOC_FOLDER_LEN = strlen(DOC_FOLDER);

// Easy-to-read constants for Mongoose return codes
static const uint32_t PROCESSING_COMPLETE = 1;
static const uint32_t NOT_PROCESSED = 0;

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

Webserver::Webserver() : context_(NULL) {
  http_address_ = MakeNetworkAddress(
      FLAGS_webserver_interface.empty() ? "0.0.0.0" : FLAGS_webserver_interface,
      FLAGS_webserver_port);
}

Webserver::Webserver(const int port) : context_(NULL) {
  http_address_ = MakeNetworkAddress("0.0.0.0", port);
}

Webserver::~Webserver() {
  Stop();
}

void Webserver::RootHandler(const Webserver::ArgumentMap& args, stringstream* output) {
  // path_handler_lock_ already held by MongooseCallback
  (*output) << "<h2>Version</h2>";
  (*output) << "<pre>" << GetVersionString() << "</pre>" << endl;
  (*output) << "<h2>Hardware Info</h2>";
  (*output) << "<pre>";
  (*output) << CpuInfo::DebugString();
  (*output) << MemInfo::DebugString();
  (*output) << DiskInfo::DebugString();
  (*output) << "</pre>";

  (*output) << "<h2>Status Pages</h2>";
  BOOST_FOREACH(const PathHandlerMap::value_type& handler, path_handlers_) {
    if (handler.second.is_on_nav_bar()) {
      (*output) << "<a href=\"" << handler.first << "\">" << handler.first << "</a><br/>";
    }
  }
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
    // Mongoose makes sockets with 's' suffixes accept SSL traffic only
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
  }

  if (!FLAGS_webserver_authentication_domain.empty()) {
    options.push_back("authentication_domain");
    options.push_back(FLAGS_webserver_authentication_domain.c_str());
  }

  if (!FLAGS_webserver_password_file.empty()) {
    // Mongoose doesn't log anything if it can't stat the password file (but will if it
    // can't open it, which it tries to do during a request)
    if (!exists(FLAGS_webserver_password_file)) {
      stringstream ss;
      ss << "Webserver: Password file does not exist: " << FLAGS_webserver_password_file;
      return Status(ss.str());
    }
    LOG(INFO) << "Webserver: Password file is " << FLAGS_webserver_password_file;
    options.push_back("global_passwords_file");
    options.push_back(FLAGS_webserver_password_file.c_str());
  }

  options.push_back("listening_ports");
  options.push_back(listening_str.c_str());

  // Options must be a NULL-terminated list
  options.push_back(NULL);

  // mongoose ignores SIGCHLD and we need it to run kinit. This means that since
  // mongoose does not reap its own children CGI programs must be avoided.
  // Save the signal handler so we can restore it after mongoose sets it to be ignored.
  sighandler_t sig_chld = signal(SIGCHLD, SIG_DFL);

  mg_callbacks callbacks;
  memset(&callbacks, 0, sizeof(callbacks));
  callbacks.begin_request = &Webserver::BeginRequestCallbackStatic;
  callbacks.log_message = &Webserver::LogMessageCallbackStatic;

  // To work around not being able to pass member functions as C callbacks, we store a
  // pointer to this server in the per-server state, and register a static method as the
  // default callback. That method unpacks the pointer to this and calls the real
  // callback.
  context_ = mg_start(&callbacks, reinterpret_cast<void*>(this), &options[0]);

  // Restore the child signal handler so wait() works properly.
  signal(SIGCHLD, sig_chld);

  if (context_ == NULL) {
    stringstream error_msg;
    error_msg << "Webserver: Could not start on address " << http_address_;
    return Status(error_msg.str());
  }

  PathHandlerCallback default_callback =
      bind<void>(mem_fn(&Webserver::RootHandler), this, _1, _2);

  RegisterPathHandler("/", default_callback);

  LOG(INFO) << "Webserver started";
  return Status::OK;
}

void Webserver::Stop() {
  if (context_ != NULL) {
    mg_stop(context_);
    context_ = NULL;
  }
}

int Webserver::LogMessageCallbackStatic(const struct mg_connection* connection,
    const char* message) {
  if (message != NULL) {
    LOG(INFO) << "Webserver: " << message;
  }
  return PROCESSING_COMPLETE;
}

int Webserver::BeginRequestCallbackStatic(struct mg_connection* connection) {
  struct mg_request_info* request_info = mg_get_request_info(connection);
  Webserver* instance = reinterpret_cast<Webserver*>(request_info->user_data);
  return instance->BeginRequestCallback(connection, request_info);
}

int Webserver::BeginRequestCallback(struct mg_connection* connection,
    struct mg_request_info* request_info) {
  if (!FLAGS_webserver_doc_root.empty() && FLAGS_enable_webserver_doc_root) {
    if (strncmp(DOC_FOLDER, request_info->uri, DOC_FOLDER_LEN) == 0) {
      VLOG(2) << "HTTP File access: " << request_info->uri;
      // Let Mongoose deal with this request; returning NULL will fall through
      // to the default handler which will serve files.
      return NOT_PROCESSED;
    }
  }
  mutex::scoped_lock lock(path_handlers_lock_);
  PathHandlerMap::const_iterator it = path_handlers_.find(request_info->uri);
  if (it == path_handlers_.end()) {
    mg_printf(connection, "HTTP/1.1 404 Not Found\r\n"
        "Content-Type: text/plain\r\n\r\n");
    mg_printf(connection, "No handler for URI %s\r\n\r\n", request_info->uri);
    return PROCESSING_COMPLETE;
  }

  // Should we render with css styles?
  bool use_style = true;

  map<string, string> arguments;
  if (request_info->query_string != NULL) {
    BuildArgumentMap(request_info->query_string, &arguments);
  }
  if (!it->second.is_styled() || arguments.find("raw") != arguments.end()) {
    use_style = false;
  }

  stringstream output;
  if (use_style) BootstrapPageHeader(&output);
  BOOST_FOREACH(const PathHandlerCallback& callback_, it->second.callbacks()) {
    callback_(arguments, &output);
  }
  if (use_style) BootstrapPageFooter(&output);

  string str = output.str();
  // Without styling, render the page as plain text
  if (arguments.find("raw") != arguments.end()) {
    mg_printf(connection, "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/plain\r\n"
        "Content-Length: %d\r\n"
        "\r\n", (int)str.length());
  } else {
    mg_printf(connection, "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/html\r\n"
        "Content-Length: %d\r\n"
        "\r\n", (int)str.length());
  }

  // Make sure to use mg_write for printing the body; mg_printf truncates at 8kb
  mg_write(connection, str.c_str(), str.length());
  return PROCESSING_COMPLETE;
}

void Webserver::RegisterPathHandler(const string& path,
    const PathHandlerCallback& callback, bool is_styled, bool is_on_nav_bar) {
  mutex::scoped_lock lock(path_handlers_lock_);
  PathHandlerMap::iterator it = path_handlers_.find(path);
  if (it == path_handlers_.end()) {
    it = path_handlers_.insert(
        make_pair(path, PathHandler(is_styled, is_on_nav_bar))).first;
  }
  it->second.AddCallback(callback);
}

const string PAGE_HEADER = "<!DOCTYPE html>"
" <html>"
"   <head><title>Cloudera Impala</title>"
" <link href='www/bootstrap/css/bootstrap.min.css' rel='stylesheet' media='screen'>"
"  <style>"
"  body {"
"    padding-top: 60px; "
"  }"
"  </style>"
" </head>"
" <body>";

static const string PAGE_FOOTER = "</div></body></html>";

static const string NAVIGATION_BAR_PREFIX =
"<div class='navbar navbar-inverse navbar-fixed-top'>"
"      <div class='navbar-inner'>"
"        <div class='container'>"
"          <a class='btn btn-navbar' data-toggle='collapse' data-target='.nav-collapse'>"
"            <span class='icon-bar'></span>"
"            <span class='icon-bar'></span>"
"            <span class='icon-bar'></span>"
"          </a>"
"          <a class='brand' href='/'>Impala</a>"
"          <div class='nav-collapse collapse'>"
"            <ul class='nav'>";

static const string NAVIGATION_BAR_SUFFIX =
"            </ul>"
"          </div>"
"        </div>"
"      </div>"
"    </div>"
"    <div class='container'>";

void Webserver::BootstrapPageHeader(stringstream* output) {
  (*output) << PAGE_HEADER;
  (*output) << NAVIGATION_BAR_PREFIX;
  BOOST_FOREACH(const PathHandlerMap::value_type& handler, path_handlers_) {
    if (handler.second.is_on_nav_bar()) {
      (*output) << "<li><a href=\"" << handler.first << "\">" << handler.first
                << "</a></li>";
    }
  }
  (*output) << NAVIGATION_BAR_SUFFIX;
}

void Webserver::BootstrapPageFooter(stringstream* output) {
  (*output) << PAGE_FOOTER;
}

}
