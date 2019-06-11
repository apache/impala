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

#ifndef IMPALA_TRANSPORT_THTTPSERVER_H
#define IMPALA_TRANSPORT_THTTPSERVER_H

#include "transport/THttpTransport.h"
#include "util/metrics-fwd.h"

namespace apache {
namespace thrift {
namespace transport {

struct HttpMetrics {
  // If 'has_ldap_' is true, metrics for the number of successful and failed Basic
  // auth attempts.
  impala::IntCounter* total_basic_auth_success_ = nullptr;
  impala::IntCounter* total_basic_auth_failure_ = nullptr;

  // If 'has_kerberos_' is true, metrics for the number of successful and failed Negotiate
  // auth attempts.
  impala::IntCounter* total_negotiate_auth_success_ = nullptr;
  impala::IntCounter* total_negotiate_auth_failure_ = nullptr;

  // If 'use_cookies_' is true, metrics for the number of successful and failed cookie
  // auth attempts.
  impala::IntCounter* total_cookie_auth_success_ = nullptr;
  impala::IntCounter* total_cookie_auth_failure_ = nullptr;
};

/*
 * Implements server side work for http connections, including support for Basic auth,
 * SPNEGO, and cookies.
 */
class THttpServer : public THttpTransport {
public:

  struct HttpCallbacks {
   public:
    // Function that takes the value from a 'Authorization: Basic' header. Returns true
    // if authentication is successful. Must be set if 'has_ldap_' is true.
    std::function<bool(const std::string&)> basic_auth_fn =
        [&](const std::string&) { DCHECK(false); return false; };

    // Function that takes the value from a 'Authorization: Negotiate' header. Returns
    // true if successful and sets 'is_complete' to true if negoation is done. Must be set
    // if 'has_kerberos_' is true.
    std::function<bool(const std::string&, bool* is_complete)> negotiate_auth_fn =
        [&](const std::string&, bool*) { DCHECK(false); return false; };

    // Function that returns a list of headers to return to the client.
    std::function<std::vector<std::string>()> return_headers_fn =
        [&]() { return std::vector<std::string>(); };

    // Function that takes the path component of an HTTP request. Returns false and sets
    // 'err_msg' if an error is encountered.
    std::function<bool(const std::string& path, std::string* err_msg)> path_fn =
        [&](const std::string&, std::string*) { return true; };

    // Function that takes the value from the 'Cookie' header and returns true if
    // authentication is successful.
    std::function<bool(const std::string&)> cookie_auth_fn =
        [&](const std::string&) { return false; };
  };

  THttpServer(boost::shared_ptr<TTransport> transport, bool has_ldap, bool has_kerberos,
      bool use_cookies, bool metrics_enabled, HttpMetrics* http_metrics);

  virtual ~THttpServer();

  virtual void flush();

  void setCallbacks(const HttpCallbacks& callbacks) { callbacks_ = callbacks; }

protected:
  void readHeaders();
  virtual void parseHeader(char* header);
  virtual bool parseStatusLine(char* status);
  virtual void headersDone();
  std::string getTimeRFC1123();
  // Returns a '401 - Unauthorized' to the client.
  void returnUnauthorized();

 private:
  // If either of the following is true, a '401 - Unauthorized' will be returned to the
  // client on requests that do not contain a valid 'Authorization' header. If 'has_ldap_'
  // is true, 'Basic' auth headers will be processed, and if 'has_kerberos_' is true
  // 'Negotiate' auth headers will be processed.
  bool has_ldap_ = false;
  bool has_kerberos_ = false;

  HttpCallbacks callbacks_;

  // The value from the 'Authorization' header.
  std::string auth_value_ = "";

  // If true, the value of any 'Cookie' header will be passed to 'cookie_auth_fn' to
  // attempt to authenticate before calling other auth functions.
  bool use_cookies_ = false;

  // The value from the 'Cookie' header.
  std::string cookie_value_ = "";

  bool metrics_enabled_ = false;
  HttpMetrics* http_metrics_ = nullptr;
};

/**
 * Wraps a transport into HTTP protocol
 */
class THttpServerTransportFactory : public TTransportFactory {
public:
 THttpServerTransportFactory() {}

 THttpServerTransportFactory(const std::string server_name, impala::MetricGroup* metrics,
     bool has_ldap, bool has_kerberos, bool use_cookies);

 virtual ~THttpServerTransportFactory() {}

 virtual boost::shared_ptr<TTransport> getTransport(boost::shared_ptr<TTransport> trans) {
   return boost::shared_ptr<TTransport>(new THttpServer(
       trans, has_ldap_, has_kerberos_, use_cookies_, metrics_enabled_, &http_metrics_));
  }

 private:
  bool has_ldap_ = false;
  bool has_kerberos_ = false;
  bool use_cookies_ = false;

  // Metrics for every transport produced by this factory.
  bool metrics_enabled_ = false;
  HttpMetrics http_metrics_;
};
}
}
} // apache::thrift::transport

#endif // #ifndef IMPALA_TRANSPORT_THTTPSERVER_H
