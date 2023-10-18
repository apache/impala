/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <cstdlib>
#include <sstream>
#include <iostream>

#include <gutil/strings/strip.h>
#include <gutil/strings/substitute.h>
#include <gutil/strings/util.h>

#include "transport/THttpServer.h"
#include <thrift/transport/TSocket.h>
#ifdef _MSC_VER
#include <Shlwapi.h>
#endif

#include "gen-cpp/Frontend_types.h"
#include "util/metrics.h"
#include "common/logging.h"

DECLARE_bool(trusted_domain_use_xff_header);
DECLARE_bool(trusted_domain_empty_xff_header_use_origin);
DECLARE_bool(saml2_ee_test_mode);
DECLARE_string(trusted_auth_header);

namespace apache {
namespace thrift {
namespace transport {

using namespace std;
using strings::Substitute;

THttpServerTransportFactory::THttpServerTransportFactory(const std::string server_name,
    impala::MetricGroup* metrics, bool has_ldap, bool has_kerberos, bool use_cookies,
    bool check_trusted_domain, bool check_trusted_auth_header, bool has_saml,
    bool has_jwt)
  : has_ldap_(has_ldap),
    has_kerberos_(has_kerberos),
    use_cookies_(use_cookies),
    check_trusted_domain_(check_trusted_domain),
    check_trusted_auth_header_(check_trusted_auth_header),
    has_saml_(has_saml),
    has_jwt_(has_jwt),
    metrics_enabled_(metrics != nullptr) {
  if (metrics_enabled_) {
    if (has_ldap_) {
      http_metrics_.total_basic_auth_success_ =
          metrics->AddCounter(Substitute("$0.total-basic-auth-success", server_name), 0);
      http_metrics_.total_basic_auth_failure_ =
          metrics->AddCounter(Substitute("$0.total-basic-auth-failure", server_name), 0);
    }
    if (has_kerberos_) {
      http_metrics_.total_negotiate_auth_success_ = metrics->AddCounter(
          Substitute("$0.total-negotiate-auth-success", server_name), 0);
      http_metrics_.total_negotiate_auth_failure_ = metrics->AddCounter(
          Substitute("$0.total-negotiate-auth-failure", server_name), 0);
    }
    if (use_cookies_) {
      http_metrics_.total_cookie_auth_success_ =
          metrics->AddCounter(Substitute("$0.total-cookie-auth-success", server_name), 0);
      http_metrics_.total_cookie_auth_failure_ =
          metrics->AddCounter(Substitute("$0.total-cookie-auth-failure", server_name), 0);
    }
    if (check_trusted_domain_) {
      http_metrics_.total_trusted_domain_check_success_ = metrics->AddCounter(
          Substitute("$0.total-trusted-domain-check-success", server_name), 0);
    }
    if (check_trusted_auth_header_) {
      http_metrics_.total_trusted_auth_header_check_success_ = metrics->AddCounter(
          Substitute("$0.total-trusted-auth-header-check-success", server_name), 0);
    }
    if (has_saml_) {
      http_metrics_.total_saml_auth_success_ =
          metrics->AddCounter(Substitute("$0.total-saml-auth-success", server_name), 0);
      http_metrics_.total_saml_auth_failure_ =
          metrics->AddCounter(Substitute("$0.total-saml-auth-failure", server_name), 0);
    }
    if (has_jwt_) {
      http_metrics_.total_jwt_token_auth_success_ = metrics->AddCounter(
          Substitute("$0.total-jwt-token-auth-success", server_name), 0);
      http_metrics_.total_jwt_token_auth_failure_ = metrics->AddCounter(
          Substitute("$0.total-jwt-token-auth-failure", server_name), 0);
    }
  }
}

THttpServer::THttpServer(std::shared_ptr<TTransport> transport, bool has_ldap,
    bool has_kerberos, bool has_saml, bool use_cookies, bool check_trusted_domain,
    bool check_trusted_auth_header, bool has_jwt, bool metrics_enabled,
    HttpMetrics* http_metrics)
  : THttpTransport(transport),
    has_ldap_(has_ldap),
    has_kerberos_(has_kerberos),
    has_saml_(has_saml),
    use_cookies_(use_cookies),
    check_trusted_domain_(check_trusted_domain),
    check_trusted_auth_header_(check_trusted_auth_header),
    has_jwt_(has_jwt),
    metrics_enabled_(metrics_enabled),
    http_metrics_(http_metrics) {}

THttpServer::~THttpServer() {
}

#ifdef _MSC_VER
  #define THRIFT_strncasecmp(str1, str2, len) _strnicmp(str1, str2, len)
  #define THRIFT_strcasestr(haystack, needle) StrStrIA(haystack, needle)
#else
  #define THRIFT_strncasecmp(str1, str2, len) strncasecmp(str1, str2, len)
  #define THRIFT_strcasestr(haystack, needle) strcasestr(haystack, needle)
#endif

const std::string THttpServer::HEADER_REQUEST_ID = "X-Request-Id";
const std::string THttpServer::HEADER_IMPALA_SESSION_ID = "X-Impala-Session-Id";
const std::string THttpServer::HEADER_IMPALA_QUERY_ID = "X-Impala-Query-Id";
const std::string THttpServer::HEADER_SAML2_TOKEN_RESPONSE_PORT = "X-Hive-Token-Response-Port";
const std::string THttpServer::HEADER_SAML2_CLIENT_IDENTIFIER = "X-Hive-Client-Identifier";
const std::string THttpServer::HEADER_TRANSFER_ENCODING = "Transfer-Encoding";
const std::string THttpServer::HEADER_CONTENT_LENGTH = "Content-length";
const std::string THttpServer::HEADER_X_FORWARDED_FOR = "X-Forwarded-For";
const std::string THttpServer::HEADER_AUTHORIZATION = "Authorization";
const std::string THttpServer::HEADER_COOKIE = "Cookie";
const std::string THttpServer::HEADER_EXPECT = "Expect";

// Checks whether the name of the http header given in the 'header' parameter,
// with length 'header_name_len', matches the constant given in 'header_constant_str'
// parameter.
inline bool MatchesHeader(const char* header, const string& header_constant_str,
    const size_t header_name_len) {
  return (header_name_len == header_constant_str.length() &&
    THRIFT_strncasecmp(header, header_constant_str.c_str(), header_name_len) == 0);
}

void THttpServer::parseHeader(char* header) {
  char* colon = strchr(header, ':');
  if (colon == NULL) {
    return;
  }
  size_t sz = colon - header;
  char* value = colon + 1;

  if (MatchesHeader(header, HEADER_TRANSFER_ENCODING, sz)) {
    if (THRIFT_strcasestr(value, "chunked") != NULL) {
      chunked_ = true;
    }
  } else if (MatchesHeader(header, HEADER_CONTENT_LENGTH, sz)) {
    chunked_ = false;
    contentLength_ = atoi(value);
  } else if (MatchesHeader(header, HEADER_X_FORWARDED_FOR, sz)) {
    origin_ = value;
  } else if ((has_ldap_ || has_kerberos_ || has_saml_ || has_jwt_)
      && MatchesHeader(header, HEADER_AUTHORIZATION, sz)) {
    auth_value_ = string(value);
  } else if (use_cookies_ && MatchesHeader(header, HEADER_COOKIE, sz)) {
    cookie_value_ = string(value);
  } else if (MatchesHeader(header, HEADER_EXPECT, sz)) {
    if (THRIFT_strcasestr(value, "100-continue")){
      continue_ = true;
    }
  } else if (has_saml_
        && MatchesHeader(header, HEADER_SAML2_TOKEN_RESPONSE_PORT, sz)) {
    saml_port_ = atoi(value);
    string port_str = string(value);
    StripWhiteSpace(&port_str);
    DCHECK(wrapped_request_ != nullptr);
    wrapped_request_->headers[HEADER_SAML2_TOKEN_RESPONSE_PORT] = port_str;
  } else if (has_saml_
        && MatchesHeader(header, HEADER_SAML2_CLIENT_IDENTIFIER, sz)) {
    DCHECK(wrapped_request_ != nullptr);
    string client_id = string(value);
    StripWhiteSpace(&client_id);
    wrapped_request_->headers[HEADER_SAML2_CLIENT_IDENTIFIER] = client_id;
  } else if (check_trusted_auth_header_
      && MatchesHeader(header, FLAGS_trusted_auth_header, sz)) {
    found_trusted_auth_header_ = true;
  } else if (MatchesHeader(header, HEADER_REQUEST_ID, sz)) {
    header_x_request_id_ = string(value);
    StripWhiteSpace(&header_x_request_id_);
  } else if (MatchesHeader(header, HEADER_IMPALA_SESSION_ID, sz)) {
    header_x_session_id_ = string(value);
    StripWhiteSpace(&header_x_session_id_);
  } else if (MatchesHeader(header, HEADER_IMPALA_QUERY_ID, sz)) {
    header_x_query_id_ = string(value);
    StripWhiteSpace(&header_x_query_id_);
  }
}

bool THttpServer::parseStatusLine(char* status) {
  char* method = status;
  char* path = strchr(method, ' ');
  if (path == NULL) {
    throw TTransportException(string("Bad Status: ") + status);
  }

  *path = '\0';
  while (*(++path) == ' ') {
  };

  char* http = strchr(path, ' ');
  if (http == NULL) {
    throw TTransportException(string("Bad Status: ") + status);
  }
  *http = '\0';

  if (strcmp(method, "POST") == 0) {
    // First time filling a field in wrapped_request_, so initialize
    // it first. Should be only non-null in case of SAML authentication.
    wrapped_request_ = callbacks_.init_wrapped_http_request_fn();
    if (wrapped_request_) wrapped_request_->method = "POST";

    string err_msg;
    if (!callbacks_.path_fn(string(path), &err_msg, &readWholeBodyForAuth_)) {
      throw TTransportException(err_msg);
    }
    // POST method ok, looking for content.
    return true;
  } else if (strcmp(method, "OPTIONS") == 0) {
    // preflight OPTIONS method, we don't need further content.
    // how to graciously close connection?
    uint8_t* buf;
    uint32_t len;
    writeBuffer_.getBuffer(&buf, &len);

    // Construct the HTTP header
    std::ostringstream h;
    h << "HTTP/1.1 200 OK" << CRLF << "Date: " << getTimeRFC1123() << CRLF
      << "Access-Control-Allow-Origin: *" << CRLF << "Access-Control-Allow-Methods: POST, OPTIONS"
      << CRLF << "Access-Control-Allow-Headers: Content-Type" << CRLF << CRLF;
    string header = h.str();

    // Write the header, then the data, then flush
    transport_->write((const uint8_t*)header.c_str(), static_cast<uint32_t>(header.size()));
    transport_->write(buf, len);
    transport_->flush();

    // Reset the buffer and header variables
    writeBuffer_.resetBuffer();
    readHeaders_ = true;
    return true;
  }
  throw TTransportException(string("Bad Status (unsupported method): ") + status);
}

void THttpServer::headersDone() {
  if (!header_x_request_id_.empty() || !header_x_session_id_.empty() ||
      !header_x_query_id_.empty()) {
    VLOG_RPC << "HTTP Connection Tracing Headers"
        << (header_x_request_id_.empty() ? "" : " x-request-id=" + header_x_request_id_)
        << (header_x_session_id_.empty() ? "" : " x-session-id=" + header_x_session_id_)
        << (header_x_query_id_.empty() ? "" : " x-query-id=" + header_x_query_id_);
  }

  if (!has_ldap_ && !has_kerberos_ && !has_saml_ && !has_jwt_) {
    // We don't need to authenticate.
    resetAuthState();
    return;
  }

  if (readWholeBodyForAuth_) {
    DCHECK(has_saml_);
    // 2nd SAML message in browser mode, get authNResponse from IP.
    // Will be handled in bodyDone. Must return before processing
    // cookies, as the cookies from the IdP server can confuse our
    // logic.
    return;
  }

  bool authorized = false;
  // Try authenticating with cookies first.
  if (use_cookies_ && !cookie_value_.empty()) {
    StripWhiteSpace(&cookie_value_);
    // If a 'Cookie' header was provided with an empty value, we ignore it rather than
    // counting it as a failed cookie attempt.
    if (!cookie_value_.empty()) {
      if (callbacks_.cookie_auth_fn(cookie_value_)) {
        authorized = true;
        if (metrics_enabled_) http_metrics_->total_cookie_auth_success_->Increment(1);
      } else if (metrics_enabled_) {
        http_metrics_->total_cookie_auth_failure_->Increment(1);
      }
    }
  }

  if (!authorized && has_jwt_ && !auth_value_.empty()
      && auth_value_.find('.') != string::npos) {
    // Check Authorization header with the Bearer authentication scheme as:
    // Authorization: Bearer <token>
    // JWT contains at least one period ('.'). A well-formed JWT consists of three
    // concatenated Base64url-encoded strings, separated by dots (.).
    StripWhiteSpace(&auth_value_);
    string jwt_token;
    bool got_bearer_auth = TryStripPrefixString(auth_value_, "Bearer ", &jwt_token);
    if (got_bearer_auth) {
      if (callbacks_.jwt_token_auth_fn(jwt_token)) {
        authorized = true;
        if (metrics_enabled_) http_metrics_->total_jwt_token_auth_success_->Increment(1);
      } else {
        if (metrics_enabled_) http_metrics_->total_jwt_token_auth_failure_->Increment(1);
      }
    }
  }

  if (!authorized && has_saml_) {
    bool fallback_to_other_auths = true;
    if (saml_port_ != -1) {
      fallback_to_other_auths = false;
      // 1st SAML message in browser mode, redirect SSO.
      impala::TWrappedHttpResponse* response = callbacks_.get_saml_redirect_fn();
      if (response != nullptr) {
        returnWrappedResponse(*response);
        resetAuthState();
        throw TTransportException("HTTP auth - SAML redirection.");
      }
    } else if (!auth_value_.empty()) {
      StripWhiteSpace(&auth_value_);
      string stripped_bearer_auth_token;
      bool got_bearer_auth =
          TryStripPrefixString(auth_value_, "Bearer ", &stripped_bearer_auth_token);
      if (got_bearer_auth) {
        fallback_to_other_auths = false;
        // Final SAML message in browser mode, check bearer and replace it with a cookie.
        DCHECK(wrapped_request_ != nullptr);
        wrapped_request_->headers[HEADER_AUTHORIZATION] = auth_value_;
        if (callbacks_.validate_saml2_bearer_fn()) {
          // During EE tests it makes things easier to return 401-Unauthorized here.
          // This hack can be removed once there is a Python client that
          // supports SAML (IMPALA-10496).
          if (!FLAGS_saml2_ee_test_mode) authorized = true;
          if (metrics_enabled_) http_metrics_->total_saml_auth_success_->Increment(1);
        }
      }
    }

    if (!authorized && !fallback_to_other_auths) {
      // Do not fallback to other auth mechanisms, as the client probably expects
      // only SAML related respsonses.
      if (metrics_enabled_) http_metrics_->total_saml_auth_failure_->Increment(1);
      resetAuthState();
      returnUnauthorized();
      throw TTransportException("HTTP auth failed.");
    }
  }

  // Bypass auth for connections from trusted domains. Returns a cookie on the first
  // successful auth attempt. This check is performed after checking for cookie to avoid
  // subsequent reverse DNS lookups which can be unpredictably costly.
  // This is also done after SAML related authentication, because it is assumed that if
  // the client started the SAML workflow then it doesn't expect Impala to succeed with
  // another mechanism.
  if (!authorized && check_trusted_domain_) {
    string origin =
        FLAGS_trusted_domain_use_xff_header ? origin_ : transport_->getOrigin();
    StripWhiteSpace(&origin);
    if (origin.empty() && FLAGS_trusted_domain_use_xff_header &&
        FLAGS_trusted_domain_empty_xff_header_use_origin) {
      origin = transport_->getOrigin();
      StripWhiteSpace(&origin);
    }
    if (!origin.empty()) {
      if (callbacks_.trusted_domain_check_fn(origin, auth_value_)) {
        authorized = true;
        if (metrics_enabled_) {
          http_metrics_->total_trusted_domain_check_success_->Increment(1);
        }
      }
    }
  }

  // Bypass auth for connections if trusted auth header was found in connection string.
  if (!authorized && found_trusted_auth_header_ && !auth_value_.empty()) {
    if (callbacks_.trusted_auth_header_handle_fn(auth_value_)) {
      authorized = true;
      if (metrics_enabled_) {
        http_metrics_->total_trusted_auth_header_check_success_->Increment(1);
      }
    }
  }

  // If cookie auth wasn't successful, try to auth with the 'Authorization' header.
  if (!authorized) {
    // Determine what type of auth header we got.
    StripWhiteSpace(&auth_value_);
    string stripped_basic_auth_token;
    bool got_basic_auth =
        TryStripPrefixString(auth_value_, "Basic ", &stripped_basic_auth_token);
    string basic_auth_token = got_basic_auth ? move(stripped_basic_auth_token) : "";
    string stripped_negotiate_auth_token;
    bool got_negotiate_auth =
        TryStripPrefixString(auth_value_, "Negotiate ", &stripped_negotiate_auth_token);
    string negotiate_auth_token =
        got_negotiate_auth ? move(stripped_negotiate_auth_token) : "";
    // We can only have gotten one type of auth header.
    DCHECK(!got_basic_auth || !got_negotiate_auth);

    // For each auth type we support, we call the auth callback if the didn't get a header
    // of the other auth type or if the other auth type isn't supported. This way, if a
    // client select a supported auth method, they'll only get return headers for that
    // method, but if they didn't specify a valid auth method or they didn't provide a
    // 'Authorization' header at all, they'll get back 'WWW-Authenticate' return headers
    // for all supported auth types.
    if (has_ldap_ && (!got_negotiate_auth || !has_kerberos_)) {
      if (callbacks_.basic_auth_fn(basic_auth_token)) {
        authorized = true;
        if (metrics_enabled_) http_metrics_->total_basic_auth_success_->Increment(1);
      } else {
        if (got_basic_auth && metrics_enabled_) {
          http_metrics_->total_basic_auth_failure_->Increment(1);
        }
      }
    }
    if (has_kerberos_ && (!got_basic_auth || !has_ldap_)) {
      bool is_complete;
      if (callbacks_.negotiate_auth_fn(negotiate_auth_token, &is_complete)) {
        // If 'is_complete' is false we want to return a 401.
        authorized = is_complete;
        if (is_complete && metrics_enabled_) {
          http_metrics_->total_negotiate_auth_success_->Increment(1);
        }
      } else {
        if (got_negotiate_auth && metrics_enabled_) {
          http_metrics_->total_negotiate_auth_failure_->Increment(1);
        }
      }
    }
  }

  resetAuthState();
  if (!authorized) {
    returnUnauthorized();
    throw TTransportException("HTTP auth failed.");
  }
}

void THttpServer::bodyDone(uint32_t size) {
  DCHECK(has_saml_);
  // assume a SAML authnResponse
  if (chunked_) {
    returnUnauthorized();
    throw TTransportException("Chunked mode not supported for SAML authnResponse");
  }
  if (size != contentLength_) {
    returnUnauthorized();
    throw TTransportException("HTTP auth failed.");
  }
  if (has_saml_) {
    DCHECK(wrapped_request_ != nullptr);
    string content = readBuffer_.readAsString(size);
    wrapped_request_->__set_content(content);
    impala::TWrappedHttpResponse* response =
        callbacks_.validate_saml2_authn_response_fn();
    if (response == nullptr) {
      returnUnauthorized();
      throw TTransportException("HTTP SAML auth failed.");
    }
    returnWrappedResponse(*response);
    throw TTransportException("HTTP auth - SAML redirection.");
  }
}

void THttpServer::flush() {
  // Fetch the contents of the write buffer
  uint8_t* buf;
  uint32_t len;
  writeBuffer_.getBuffer(&buf, &len);

  // Construct the HTTP header
  std::ostringstream h;
  h << "HTTP/1.1 200 OK" << CRLF << "Date: " << getTimeRFC1123() << CRLF
    << "Server: Thrift/" << PACKAGE_VERSION << CRLF
    << "Access-Control-Allow-Origin: *" << CRLF
    << "Content-Type: application/x-thrift" << CRLF << "Content-Length: " << len << CRLF
    << "Connection: Keep-Alive" << CRLF;
  vector<string> return_headers = callbacks_.return_headers_fn();
  for (const string& header : return_headers) {
    h << header << CRLF;
  }
  h << CRLF;
  string header = h.str();

  // Write the header, then the data, then flush
  // cast should be fine, because none of "header" is under attacker control
  transport_->write((const uint8_t*)header.c_str(), static_cast<uint32_t>(header.size()));
  transport_->write(buf, len);
  transport_->flush();

  // Reset the buffer and header variables
  writeBuffer_.resetBuffer();
  readHeaders_ = true;
}

std::string THttpServer::getTimeRFC1123() {
  static const char* Days[] = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};
  static const char* Months[]
      = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
  char buff[128];
  time_t t = time(NULL);
  tm* broken_t = gmtime(&t);

  sprintf(buff,
          "%s, %d %s %d %d:%d:%d GMT",
          Days[broken_t->tm_wday],
          broken_t->tm_mday,
          Months[broken_t->tm_mon],
          broken_t->tm_year + 1900,
          broken_t->tm_hour,
          broken_t->tm_min,
          broken_t->tm_sec);
  return std::string(buff);
}

void THttpServer::returnUnauthorized() {
  std::ostringstream h;
  h << "HTTP/1.1 401 Unauthorized" << CRLF << "Date: " << getTimeRFC1123() << CRLF
    << "Connection: close" << CRLF;
  vector<string> return_headers = callbacks_.return_headers_fn();
  for (const string& header : return_headers) {
    h << header << CRLF;
  }
  h << CRLF;
  string header = h.str();
  transport_->write((const uint8_t*)header.c_str(), static_cast<uint32_t>(header.size()));
  transport_->flush();
}

void THttpServer::returnWrappedResponse(const impala::TWrappedHttpResponse& response) {
  int code = response.status_code;
  string status = response.status_text;
  std::ostringstream h;
  h << "HTTP/1.1 "<< code << " " << status << CRLF << "Date: " << getTimeRFC1123() << CRLF;
  vector<string> return_headers = callbacks_.return_headers_fn();
  for (const auto& header : response.headers) {
    h << header.first << ": " << header.second << CRLF;
  }
  // TODO: response type is not added
  // TODO: cookies are not added, but are not needed right now
  h << CRLF;
  h << response.content;
  string header = h.str();
  transport_->write((const uint8_t*)header.c_str(), static_cast<uint32_t>(header.size()));
  transport_->flush();
}

void THttpServer::resetAuthState() {
  auth_value_ = "";
  cookie_value_ = "";
  saml_port_ = -1;
}

}
}
} // apache::thrift::transport
