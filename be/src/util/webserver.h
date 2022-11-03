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

#pragma once

#include <map>
#include <string>
#include <boost/function.hpp>
#include <boost/thread/pthread/shared_mutex.hpp>
#include <rapidjson/fwd.h>

#include "common/status.h"
#include "kudu/util/web_callback_registry.h"
#include "thirdparty/squeasel/squeasel.h"
#include "util/ldap-util.h"
#include "util/metrics-fwd.h"
#include "util/network-util.h"
#include "util/openssl-util.h"

namespace impala {

/// Supported HTTP content types
enum ContentType {
  /// Corresponds to text/html content-type and used for rendering HTML webpages.
  HTML,
  /// Following two are used for rendering text-only pages and correspond to text/plain
  /// and application/json content-types respectively. JSON type additionally pretty
  /// prints the underlying JSON content. They are primarily used for debugging and for
  /// integration with third-party tools that can consume the text format easily.
  PLAIN,
  JSON
};

/// Wrapper class for the Squeasel web server library. Clients may register callback
/// methods which produce Json documents which are rendered via a template file to either
/// HTML or text.
///
/// Apache Knox Integration
/// -----------------------
/// In order to be compatible with the Knox 'impalaui' service definition, there are a few
/// requirements that template files served by this webserver have to conform to:
/// - Any relative links to pages on this server must be proceeded by
///   {{ __common__.host-url }} so that they can be made into absolute urls when Knox
///   proxying is being used.
/// - Any forms must contain {{>www/form-hidden-inputs.tmpl}}, which adds some hidden
///   fields when Knox proxying is being used.
/// - Any relative urls that are accessed via javascript should be constructed with the
///   make_url() function in common-header.tmpl.
/// See: https://github.com/apache/knox/tree/master/gateway-service-definitions/
///     src/main/resources/services/impalaui/1.0.0
class Webserver {
 public:
  using ArgumentMap = kudu::WebCallbackRegistry::ArgumentMap;
  using WebRequest = kudu::WebCallbackRegistry::WebRequest;
  using HttpStatusCode = kudu::HttpStatusCode;

  typedef boost::function<void (const WebRequest& req, rapidjson::Document* json)>
      UrlCallback;
  typedef boost::function<void (const WebRequest& req, std::stringstream* output,
      HttpStatusCode* response)> RawUrlCallback;

  /// Any callback may add a member to their Json output with key ENABLE_RAW_HTML_KEY;
  /// this causes the result of the template rendering process to be sent to the browser
  /// as text, not HTML.
  static const char* ENABLE_RAW_HTML_KEY;

  /// Any callback may add a member to their Json output with key ENABLE_PLAIN_JSON_KEY;
  /// this causes the result of the template rendering process to be sent to the browser
  /// as pretty printed JSON plain text.
  static const char* ENABLE_PLAIN_JSON_KEY;

  enum AuthMode { NONE, HTPASSWD, SPNEGO, LDAP };

  /// Using this constructor, the webserver will bind to 'interface', or all available
  /// interfaces if not specified.
  Webserver(const std::string& interface, const int port, MetricGroup* metrics,
      const AuthMode& auth_mode = GetConfiguredAuthMode());

  /// Uses FLAGS_webserver_{port, interface}
  Webserver(MetricGroup* metrics);

  ~Webserver();

  /// Starts a webserver on the port passed to the constructor. The webserver runs in a
  /// separate thread, so this call is non-blocking.
  Status Start();

  /// Stops the webserver synchronously.
  void Stop();

  /// Register a callback for a Url that produces a json document that will be rendered
  /// with the template at 'template_filename'. The URL 'path' should not include the
  /// http://hostname/ prefix. If is_on_nav_bar is true, the page will appear in the
  /// standard navigation bar rendered on all pages.
  //
  /// Only one callback may be registered per URL.
  //
  /// The path of the template file is relative to the webserver's document
  /// root.
  void RegisterUrlCallback(const std::string& path, const std::string& template_filename,
      const UrlCallback& callback, bool is_on_nav_bar);

  /// Register a 'raw' url callback that produces a bytestream as output. This should only
  /// be used for URLs that want to return binary data; non-HTML callbacks that want to
  /// produce text should use UrlCallback.
  void RegisterUrlCallback(const std::string& path, const RawUrlCallback& callback);

  /// True if serving all traffic over SSL, false otherwise
  bool IsSecure() const;

  /// Returns the URL to the webserver as a string.
  std::string url() { return url_; }
  std::string hostname() { return hostname_; }
  int port() { return http_address_.port; }

  /// Returns the appropriate MIME type for a given ContentType.
  static const std::string GetMimeType(const ContentType& content_type);

  /// Returns the authentication mode configured by the startup flags.
  static AuthMode GetConfiguredAuthMode();

  /// Parses form-uri-encoded data and returns key/value pairs.
  static ArgumentMap GetVars(const std::string& data);

 private:
  /// Contains all information relevant to rendering one Url. Each Url has one callback
  /// that produces the output to render. The callback either produces a Json document
  /// which is rendered via a template file, or it produces an HTML string that is embedded
  /// directly into the output.
  class UrlHandler {
   public:
    UrlHandler(const UrlCallback& cb, const std::string& template_filename,
        bool is_on_nav_bar)
        : is_on_nav_bar_(is_on_nav_bar), use_templates_(true), template_callback_(cb),
          template_filename_(template_filename) { }

    UrlHandler(const RawUrlCallback& cb)
        : is_on_nav_bar_(false), use_templates_(false),
          raw_callback_(cb) { }

    bool is_on_nav_bar() const { return is_on_nav_bar_; }
    bool use_templates() const { return use_templates_; }
    const UrlCallback& callback() const { return template_callback_; }
    const RawUrlCallback& raw_callback() const { return raw_callback_; }
    const std::string& template_filename() const { return template_filename_; }

   private:
    /// If true, the page appears in the navigation bar.
    const bool is_on_nav_bar_;

    /// If true, use the template rendering callback, otherwise the 'raw' callback is
    /// used.
    const bool use_templates_;

    /// Callback to produce a Json document to render via a template.
    UrlCallback template_callback_;

    /// Callback to produce a raw bytestream.
    RawUrlCallback raw_callback_;

    /// Path to the file that contains the template to render, relative to the webserver's
    /// document root.
    std::string template_filename_;
  };

  /// Sets the values of 'url_' and 'hostname_'.
  void Init();

  /// Squeasel callback for log events. Returns squeasel success code.
  static int LogMessageCallbackStatic(const struct sq_connection* connection,
      const char* message);

  /// Squeasel callback for HTTP request events. Static so that it can act as a function
  /// pointer, and then call the next method. Returns squeasel success code.
  static sq_callback_result_t BeginRequestCallbackStatic(
      struct sq_connection* connection);

  /// Dispatch point for all incoming requests. Returns squeasel success code.
  sq_callback_result_t BeginRequestCallback(struct sq_connection* connection,
      struct sq_request_info* request_info);

  // Handle SPNEGO authentication for this request. Returns SQ_CONTINUE_HANDLING
  // if authentication was successful, otherwise responds to the request and
  // returns SQ_HANDLED_OK.
  sq_callback_result_t HandleSpnego(struct sq_connection* connection,
      struct sq_request_info* request_info, std::vector<std::string>* response_headers);

  /// Checks and returns true if the connection originated from a trusted domain and has a
  /// valid username set in the request's the Authorization header (using Basic Auth).
  bool TrustedDomainCheck(const std::string& origin, struct sq_connection* connection,
      struct sq_request_info* request_info);

  /// Checks and returns true if the JWT token in Authorization header could be verified
  /// and the token has a valid username.
  bool JWTTokenAuth(const std::string& jwt_token, struct sq_connection* connection,
      struct sq_request_info* request_info);

  // Handle Basic authentication for this request. Returns an error if authentication was
  // unsuccessful.
  Status HandleBasic(struct sq_connection* connection,
      struct sq_request_info* request_info, std::vector<std::string>* response_headers);

  // Adds a 'Set-Cookie' header to 'response_headers', if cookie support is enabled.
  // Returns the random value portion of the cookie in 'rand' for use in CSRF prevention.
  void AddCookie(const char* user, vector<string>* response_headers, string* rand);

  // Get username from Authorization header.
  bool GetUsernameFromAuthHeader(struct sq_connection* connection,
      struct sq_request_info* request_info, string& err_msg);

  /// Renders URLs through the Mustache templating library.
  /// - Default ContentType is HTML.
  /// - Argument 'raw' renders the page with PLAIN ContentType.
  /// - Argument 'json' renders the page with JSON ContentType. The underlying JSON is
  ///   pretty-printed.
  void RenderUrlWithTemplate(const struct sq_connection* connection,
      const WebRequest& arguments, const UrlHandler& url_handler,
      std::stringstream* output, ContentType* content_type,
      const std::string& csrf_token);

  /// Called when an error is encountered, e.g. when a handler for a URI cannot be found.
  void ErrorHandler(const WebRequest& req, rapidjson::Document* document);

  /// Builds a map of argument name to argument value from a typical URL argument
  /// string (that is, "key1=value1&key2=value2.."). If no value is given for a
  /// key, it is entered into the map as (key, "").
  static void BuildArgumentMap(const std::string& args, ArgumentMap* output);

  /// Adds a __common__ object to document with common data that every webpage might want
  /// to read (e.g. the names of links to write to the navbar).
  void GetCommonJson(rapidjson::Document* document,
      const struct sq_connection* connection, const WebRequest& req,
      const std::string& csrf_token);

  /// Lock guarding the path_handlers_ map
  boost::shared_mutex url_handlers_lock_;

  /// Map of path to a UrlHandler containing a list of handlers for that
  /// path. More than one handler may register itself with a path so that many
  /// components may contribute to a single page.
  typedef std::map<std::string, UrlHandler> UrlHandlerMap;
  UrlHandlerMap url_handlers_;

  /// The address of the interface on which to run this webserver.
  TNetworkAddress http_address_;

  /// Formatted string representing 'http_address_'.
  std::string url_;

  /// The resolved hostname from 'http_address_'.
  std::string hostname_;

  AuthMode auth_mode_;

  /// Handle to Squeasel context; owned and freed by Squeasel internally
  struct sq_context* context_;

  /// Catch-all handler for error messages
  UrlHandler error_handler_;

  /// Used to generate and verify signatures for cookies.
  AuthenticationHash hash_;

  /// If true and SPNEGO is in use, cookies will be used for authentication.
  bool use_cookies_;

  /// If true and SPNEGO or LDAP is in use, checks whether an incoming connection can skip
  /// auth if it originates from a trusted domain.
  bool check_trusted_domain_;

  /// If true and SPNEGO or LDAP is in use, checks whether an incoming connection can skip
  /// auth if it has trusted auth header.
  bool check_trusted_auth_header_;

  /// If true, the JWT token in Authorization header will be used for authentication.
  /// An incoming connection will be accepted if the JWT token could be verified.
  bool use_jwt_ = false;

  /// Used to validate usernames/passwords If LDAP authentication is in use.
  std::unique_ptr<ImpalaLdap> ldap_;

  /// If 'FLAGS_webserver_require_spnego' is true, metrics for the number of successful
  /// and failed Negotiate auth attempts.
  IntCounter* total_negotiate_auth_success_ = nullptr;
  IntCounter* total_negotiate_auth_failure_ = nullptr;

  /// If 'FLAGS_webserver_require_ldap' is true, metrics for the number of successful
  /// and failed Basic auth attempts.
  IntCounter* total_basic_auth_success_ = nullptr;
  IntCounter* total_basic_auth_failure_ = nullptr;

  /// If 'use_cookies_' is true, metrics for the number of successful and failed cookie
  /// auth attempts.
  IntCounter* total_cookie_auth_success_ = nullptr;
  IntCounter* total_cookie_auth_failure_ = nullptr;

  /// If 'use_cookies_' is true, metrics for the number of successful
  /// attempts to authorize connections originating from a trusted domain.
  IntCounter* total_trusted_domain_check_success_ = nullptr;

  /// Metrics for the number of successful attempts to authorize connections with a
  /// trusted auth header.
  IntCounter* total_trusted_auth_header_check_success_ = nullptr;

  /// If 'use_jwt_' is true, metrics for the number of successful and failed JWT auth
  /// attempts.
  IntCounter* total_jwt_token_auth_success_ = nullptr;
  IntCounter* total_jwt_token_auth_failure_ = nullptr;
};

}
