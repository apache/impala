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

namespace impala {
  class TWrappedHttpRequest;
  class TWrappedHttpResponse;
}

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

  // If 'check_trusted_domain_' is true, metrics for the number of successful
  // attempts to authorize connections originating from a trusted domain.
  impala::IntCounter* total_trusted_domain_check_success_ = nullptr;

  impala::IntCounter* total_saml_auth_success_ = nullptr;
  impala::IntCounter* total_saml_auth_failure_ = nullptr;
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
    // 'err_msg' if an error is encountered. If the final bool parameter is set to true,
    // then the whole body has to be read of authentication purposes, not just the
    // headers.
    std::function<bool(const std::string& path, std::string* err_msg, bool*)> path_fn =
        [&](const std::string&, std::string*, bool*) { return true; };

    // Function that takes the value from the 'Cookie' header and returns true if
    // authentication is successful. Also used by SAML.
    std::function<bool(const std::string&)> cookie_auth_fn =
        [&](const std::string&) { return false; };

    // Function that takes the connection's origin ip/hostname, and 'Authorization: Basic'
    // header respectively and returns true if it determines that the connection
    // originated from a trusted domain and if the basic auth header contains a valid
    // username.
    std::function<bool(const std::string&, std::string)> trusted_domain_check_fn =
        [&](const std::string&, std::string) { return false; };

    // Does the first step of SAML2 SSO browser authenticaton and sets the response to
    // redirect to the SSO service.
    std::function<impala::TWrappedHttpResponse*()> get_saml_redirect_fn =
        [&]() { return (impala::TWrappedHttpResponse*) NULL; };

    // Does the second step of SAML2 SSO browser authenticaton, processing the
    // AuthNResponse and returning an HTML form that will submit to the client's port.
    std::function<impala::TWrappedHttpResponse*()>
        validate_saml2_authn_response_fn =
            [&]() { return (impala::TWrappedHttpResponse*) NULL; };

    // Does the final step of SAML2 SSO browser authenticaton, validating the bearer
    // token and switching to cookie authentication.
    std::function<bool()> validate_saml2_bearer_fn = [&]() { return false; };

    // Initializes a TWrappedHttpRequest object that contains every info about the http
    // request and can be passed to Frontend for proccessing. Currently only used in
    // SAML2 SSO.
    std::function<impala::TWrappedHttpRequest*()> init_wrapped_http_request_fn =
        [&]() { return (impala::TWrappedHttpRequest*) NULL; };
  };

  THttpServer(boost::shared_ptr<TTransport> transport, bool has_ldap, bool has_kerberos,
      bool has_saml, bool use_cookies, bool check_trusted_domain, bool metrics_enabled,
      HttpMetrics* http_metrics);

  virtual ~THttpServer();

  virtual void flush();

  void setCallbacks(const HttpCallbacks& callbacks) { callbacks_ = callbacks; }

protected:
  void readHeaders();
  virtual void parseHeader(char* header);
  virtual bool parseStatusLine(char* status);
  virtual void headersDone();
  // Called if the whole body has to be read for authentication. Only used in SAML SSO.
  virtual void bodyDone(uint32_t size);
  std::string getTimeRFC1123();
  // Returns a '401 - Unauthorized' to the client.
  void returnUnauthorized();
  // Returns a response based on a TWrappedHttpResponse that can come from Frontend code.
  void returnWrappedResponse(const impala::TWrappedHttpResponse& response);
  // Resets members that needs to be set per-http request.
  void resetAuthState();
 private:
  // If either of the following is true, a '401 - Unauthorized' will be returned to the
  // client on requests that do not contain a valid 'Authorization' of SAML SSO related
  // header. If 'has_ldap_' is true, 'Basic' auth headers will be processed, and if
  // 'has_kerberos_' is true 'Negotiate' auth headers will be processed.
  bool has_ldap_ = false;
  bool has_kerberos_ = false;

  // Currently SAML2 SSO browser profile is implemented, which needs 3 different http
  // requests to Impala before switching to cookie authentication.
  //  1. redirecting connection from the client to the SSO provider
  //  2. validating an authNRespone from the SSO provider
  //     - this could be handled on another port than the hs2-http, but handling it on
  //       the same allows supporting SAML without having to expose an extra port.
  //  3. validating the bearer token from the client and returning an auth cookie instead
  //
  // SAML can be used alongside LDAP or Kerberos - if the SAML related path of headers
  // are not detected, Impala fall back to other authentications.
  bool has_saml_ = false;

  HttpCallbacks callbacks_;

  // The value from the 'Authorization' header.
  std::string auth_value_ = "";

  // If true, the value of any 'Cookie' header will be passed to 'cookie_auth_fn' to
  // attempt to authenticate before calling other auth functions.
  bool use_cookies_ = false;

  // The value from the 'Cookie' header.
  std::string cookie_value_ = "";

  // Used in SAML2 SSO browser profile authentication to set the port opened by the client
  // where expects to receive the bearer token.
  // Comes from header 'X-Hive-Token-Response-Port'.
  int saml_port_ = -1;

  // If true, checks whether an incoming connection can skip auth if it originates from a
  // trusted domain.
  bool check_trusted_domain_ = false;

  bool metrics_enabled_ = false;
  HttpMetrics* http_metrics_ = nullptr;

  // Used to collect all information about the http request. Can be passed to the
  // Frontend. Currently only used by SAML SSO.
  impala::TWrappedHttpRequest* wrapped_request_ = nullptr;
};

/**
 * Wraps a transport into HTTP protocol
 */
class THttpServerTransportFactory : public TTransportFactory {
public:
 THttpServerTransportFactory() {}

 THttpServerTransportFactory(const std::string server_name, impala::MetricGroup* metrics,
     bool has_ldap, bool has_kerberos, bool use_cookies, bool check_trusted_domain,
     bool has_saml);

 virtual ~THttpServerTransportFactory() {}

 virtual boost::shared_ptr<TTransport> getTransport(boost::shared_ptr<TTransport> trans) {
   return boost::shared_ptr<TTransport>(new THttpServer(trans, has_ldap_, has_kerberos_,
       has_saml_, use_cookies_, check_trusted_domain_, metrics_enabled_, &http_metrics_));
  }

 private:
  bool has_ldap_ = false;
  bool has_kerberos_ = false;
  bool use_cookies_ = false;
  bool check_trusted_domain_ = false;
  bool has_saml_ = false;

  // Metrics for every transport produced by this factory.
  bool metrics_enabled_ = false;
  HttpMetrics http_metrics_;
};
}
}
} // apache::thrift::transport

#endif // #ifndef IMPALA_TRANSPORT_THTTPSERVER_H
