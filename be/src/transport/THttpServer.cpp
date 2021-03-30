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

#include "util/metrics.h"

DECLARE_bool(trusted_domain_use_xff_header);

namespace apache {
namespace thrift {
namespace transport {

using namespace std;
using strings::Substitute;

THttpServerTransportFactory::THttpServerTransportFactory(const std::string server_name,
    impala::MetricGroup* metrics, bool has_ldap, bool has_kerberos, bool use_cookies,
    bool check_trusted_domain)
  : has_ldap_(has_ldap),
    has_kerberos_(has_kerberos),
    use_cookies_(use_cookies),
    check_trusted_domain_(check_trusted_domain),
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
  }
}

THttpServer::THttpServer(boost::shared_ptr<TTransport> transport, bool has_ldap,
    bool has_kerberos, bool use_cookies, bool check_trusted_domain, bool metrics_enabled,
    HttpMetrics* http_metrics)
  : THttpTransport(transport),
    has_ldap_(has_ldap),
    has_kerberos_(has_kerberos),
    use_cookies_(use_cookies),
    check_trusted_domain_(check_trusted_domain),
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

void THttpServer::parseHeader(char* header) {
  char* colon = strchr(header, ':');
  if (colon == NULL) {
    return;
  }
  size_t sz = colon - header;
  char* value = colon + 1;

  if (THRIFT_strncasecmp(header, "Transfer-Encoding", sz) == 0) {
    if (THRIFT_strcasestr(value, "chunked") != NULL) {
      chunked_ = true;
    }
  } else if (THRIFT_strncasecmp(header, "Content-length", sz) == 0) {
    chunked_ = false;
    contentLength_ = atoi(value);
  } else if (strncmp(header, "X-Forwarded-For", sz) == 0) {
    origin_ = value;
  } else if ((has_ldap_ || has_kerberos_)
      && THRIFT_strncasecmp(header, "Authorization", sz) == 0) {
    auth_value_ = string(value);
  } else if (use_cookies_ && THRIFT_strncasecmp(header, "Cookie", sz) == 0) {
    cookie_value_ = string(value);
  } else if (THRIFT_strncasecmp(header, "Expect", sz) == 0) {
    if (THRIFT_strcasestr(value, "100-continue")){
      continue_ = true;
    }
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
    string err_msg;
    if (!callbacks_.path_fn(string(path), &err_msg)) {
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
  if (!has_ldap_ && !has_kerberos_) {
    // We don't need to authenticate.
    auth_value_ = "";
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

  // Bypass auth for connections from trusted domains. Returns a cookie on the first
  // successful auth attempt. This check is performed after checking for cookie to avoid
  // subsequent reverse DNS lookups which can be unpredictably costly.
  if (!authorized && check_trusted_domain_) {
    string origin =
        FLAGS_trusted_domain_use_xff_header ? origin_ : transport_->getOrigin();
    StripWhiteSpace(&origin);
    if (!origin.empty()) {
      if (callbacks_.trusted_domain_check_fn(origin, auth_value_)) {
        authorized = true;
        if (metrics_enabled_) {
          http_metrics_->total_trusted_domain_check_success_->Increment(1);
        }
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

  auth_value_ = "";
  cookie_value_ = "";
  if (!authorized) {
    returnUnauthorized();
    throw TTransportException("HTTP auth failed.");
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
    << "Server: Thrift/" << VERSION << CRLF << "Access-Control-Allow-Origin: *" << CRLF
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
}
}
} // apache::thrift::transport
