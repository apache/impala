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

#include "rpc/authentication-util.h"

#include <cstdlib>
#include <ctime>
#include <memory>
#include <gutil/strings/escaping.h>
#include <gutil/strings/split.h>
#include <gutil/strings/strcat.h>
#include <gutil/strings/strip.h>
#include <gutil/strings/util.h>

#include "kudu/util/net/sockaddr.h"
#include "runtime/exec-env.h"
#include "service/frontend.h"
#include "util/coding-util.h"
#include "util/network-util.h"
#include "util/openssl-util.h"
#include "util/string-parser.h"

DEFINE_bool(cookie_require_secure, true,
    "(Advanced) If true, authentication cookies will include the 'Secure' attribute, "
    "indicating to clients that they should only be returned over SSL connections. For "
    "testing only.");
DEFINE_int64(max_cookie_lifetime_s, 24 * 60 * 60,
    "Maximum amount of time in seconds that an authentication cookie will remain valid. "
    "Setting to 0 disables use of cookies. Defaults to 1 day.");
DEFINE_bool(samesite_strict, false,
    "(Advanced) If true, authentication cookies will include SameSite=Strict.");

using namespace strings;

namespace impala {

// Used to separate values in cookies. All generated cookies will be of the form:
// <signature>&u=<username>&t=<timestamp>&r=<random number>
// This format was chosen to imitate the cookie format used by other Hadoop system such as
// Hive in order to facilitate interoperability with systems like Knox. See for example:
// service/src/java/org/apache/hive/service/auth/HttpAuthUtils.java in Hive
static const string COOKIE_SEPARATOR = "&";
static const string USERNAME_KEY = "u=";
static const string TIMESTAMP_KEY = "t=";
static const string RAND_KEY = "r=";
static const string AUTH_MECH_KEY = "a=";

// Cookies generated and processed by the HTTP server will be of the form:
// COOKIE_NAME=<cookie>
static const string COOKIE_NAME = "impala.auth";

// The maximum lenth for the base64 encoding of a SHA256 hash.
static const int SHA256_BASE64_LEN =
    CalculateBase64EscapedLen(AuthenticationHash::HashLen(), /* do_padding */ true);

// Since we only return cookies with a single name, well behaved clients should only ever
// return one cookie to us. To accommodate non-malicious but poorly behaved clients, we
// allow for checking a limited number of cookies, up to MAX_COOKIES_TO_CHECK or until we
// find the first one with COOKIE_NAME.
static const int MAX_COOKIES_TO_CHECK = 5;

Status AuthenticateCookie(
    const AuthenticationHash& hash, const string& cookie_header,
    string* username, string* authMech, string* rand) {
  // The 'Cookie' header allows sending multiple name/value pairs separated by ';'.
  vector<string> cookies = strings::Split(cookie_header, ";");
  if (cookies.size() > MAX_COOKIES_TO_CHECK) {
    LOG(WARNING) << "Received cookie header with large number of cookies: "
                 << cookie_header << ". Only checking the first " << MAX_COOKIES_TO_CHECK
                 << " cookies.";
  }
  for (int i = 0; i < cookies.size() && i < MAX_COOKIES_TO_CHECK; ++i) {
    string cookie_pair = cookies[i];
    StripWhiteSpace(&cookie_pair);
    string cookie;
    if (!TryStripPrefixString(cookie_pair, StrCat(COOKIE_NAME, "="), &cookie)) {
      continue;
    }
    if (cookie[0] == '"' && cookie[cookie.length() - 1] == '"') {
      cookie = cookie.substr(1, cookie.length() - 2);
    }
    // Split the cookie into the signature and the cookie value.
    vector<string> cookie_split = Split(cookie, delimiter::Limit(COOKIE_SEPARATOR, 1));
    if (cookie_split.size() != 2) {
      return Status("The cookie has an invalid format.");
    }
    const string& base64_signature = cookie_split[0];
    const string& cookie_value = cookie_split[1];

    string signature;
    if (!WebSafeBase64Unescape(base64_signature, &signature)) {
      return Status("Unable to decode base64 signature.");
    }
    if (signature.length() != AuthenticationHash::HashLen()) {
      return Status("Signature is an incorrect length.");
    }
    bool verified = hash.Verify(reinterpret_cast<const uint8_t*>(cookie_value.data()),
        cookie_value.length(), reinterpret_cast<const uint8_t*>(signature.data()));
    if (!verified) {
      return Status("The signature is incorrect.");
    }

    // Split the cookie value into username, timestamp, random number and auth mechanism.
    vector<string> cookie_value_split = Split(cookie_value, COOKIE_SEPARATOR);
    if (cookie_value_split.size() != 4) {
      return Status("The cookie value has an invalid format.");
    }
    string timestamp;
    if (!TryStripPrefixString(cookie_value_split[1], TIMESTAMP_KEY, &timestamp)) {
      return Status("The cookie timestamp value has an invalid format.");
    }
    StringParser::ParseResult result;
    int64_t create_time = StringParser::StringToInt<int64_t>(
        timestamp.c_str(), timestamp.length(), &result);
    if (result != StringParser::PARSE_SUCCESS) {
      return Status("Could not parse cookie timestamp.");
    }
    // Check that the timestamp contained in the cookie is recent enough for the cookie
    // to still be valid.
    if (MonotonicMillis() - create_time <= FLAGS_max_cookie_lifetime_s * 1000) {
      if (!TryStripPrefixString(cookie_value_split[0], USERNAME_KEY, username)) {
        return Status("The cookie username value has an invalid format.");
      }
      if (rand != nullptr) {
        if (!TryStripPrefixString(cookie_value_split[2], RAND_KEY, rand)) {
          return Status("The cookie rand value has an invalid format.");
        }
      }
      if (!TryStripPrefixString(cookie_value_split[3], AUTH_MECH_KEY, authMech)) {
        return Status("The cookie authMech value has an invalid format.");
      }
      // We've successfully authenticated.
      return Status::OK();
    } else {
      return Status("Cookie is past its max lifetime.");
    }
  }

  return Status(Substitute("Did not find expected cookie name: $0", COOKIE_NAME));
}

string GenerateCookie(const string& username, const AuthenticationHash& hash,
    const std::string& authMech, std::string* srand) {
  // Its okay to use rand() here even though its a weak RNG because being able to guess
  // the random numbers generated won't help an attacker. The important thing is that
  // we're using a strong RNG to create the key and a strong HMAC function.
  int cookie_rand = rand();
  string cookie_rand_s = std::to_string(cookie_rand);
  if (srand != nullptr) {
    *srand = cookie_rand_s;
  }
  string cookie_value = StrCat(USERNAME_KEY, username, COOKIE_SEPARATOR, TIMESTAMP_KEY,
      MonotonicMillis(), COOKIE_SEPARATOR, RAND_KEY, cookie_rand_s, COOKIE_SEPARATOR,
      AUTH_MECH_KEY, authMech);
  uint8_t signature[AuthenticationHash::HashLen()];
  Status compute_status =
      hash.Compute(reinterpret_cast<const uint8_t*>(cookie_value.data()),
          cookie_value.length(), signature);
  if (!compute_status.ok()) {
    LOG(ERROR) << "Failed to compute cookie signature: " << compute_status;
    return "";
  }
  DCHECK_EQ(SHA256_BASE64_LEN, 44);
  char base64_signature[SHA256_BASE64_LEN + 1];
  WebSafeBase64Escape(signature, AuthenticationHash::HashLen(), base64_signature,
      SHA256_BASE64_LEN, /* do_padding */ true);
  base64_signature[SHA256_BASE64_LEN] = '\0';

  const char* secure_flag = FLAGS_cookie_require_secure ? ";Secure" : "";
  const char* samesite_flag = FLAGS_samesite_strict ? ";SameSite=Strict" : "";
  // Add SameSite=Strict to notify the browser it should avoid sending the cookie with
  // requests from other domains.
  return Substitute("$0=$1$2$3;HttpOnly;Max-Age=$4$5$6", COOKIE_NAME, base64_signature,
      COOKIE_SEPARATOR, cookie_value, FLAGS_max_cookie_lifetime_s, secure_flag,
      samesite_flag);
}

string GetDeleteCookie() {
  return Substitute("$0=;HttpOnly;Max-Age=0", COOKIE_NAME);
}

bool IsTrustedDomain(const std::string& origin, const std::string& trusted_domain,
    bool strict_localhost) {
  if (trusted_domain.empty()) return false;
  vector<string> split = Split(origin, delimiter::Limit(",", 1));
  if (split.empty()) return false;
  kudu::Sockaddr sock_addr = kudu::Sockaddr::Wildcard();
  kudu::Status s = sock_addr.ParseString(split[0], 0);
  string host_name;
  if (!s.ok()) {
    VLOG(2) << "Origin address did not parse as a valid IP address. Assuming it to be a "
               "domain name. Reason: " << s.ToString();
    // Remove port if its a part of the origin.
    vector<string> host_n_port = Split(split[0], delimiter::Limit(":", 1));
    host_name = host_n_port[0];
  } else {
    // If using strict localhost checks, only allow localhost to match 127.0.0.1
    if (trusted_domain == "localhost" && strict_localhost) {
      return sock_addr.host() == "127.0.0.1";
    }
    s = sock_addr.LookupHostname(&host_name);
    if (!s.ok()) {
      LOG(ERROR) << "DNS reverse-lookup failed for " << split[0]
                 << " Error: " << s.ToString();
      return false;
    }
  }
  return HasSuffixString(host_name, trusted_domain);
}

Status GetXFFOriginClientAddress(const std::string_view& xff_addresses,
    std::string& origin) {
  if (xff_addresses.empty()) {
    return Status::Expected("Empty XFF header");
  }
  const int pos = xff_addresses.find_first_of(',');
  if (pos == xff_addresses.npos) {
    origin = xff_addresses;
  } else {
    origin = xff_addresses.substr(0, pos);
  }
  StripWhiteSpace(&origin);
  if (origin.empty()) {
    return Status::Expected("Empty XFF header");
  }
  return Status::OK();
}

Status BasicAuthExtractCredentials(
    const string& token, string& username, string& password) {
  if (token.empty()) {
    return Status::Expected("Empty token");
  }
  string decoded;
  if (!Base64Unescape(token, &decoded)) {
    return Status::Expected("Failed to decode base64 basic authentication token.");
  }
  std::size_t colon = decoded.find(':');
  if (colon == std::string::npos) {
    return Status::Expected("Invalid basic authentication token format, must be in the "
                            "form '<username>:<password>'");
  }
  username = decoded.substr(0, colon);
  password = decoded.substr(colon + 1);
  return Status::OK();
}

Status ParseSamlSpUrl(string* saml_sp_path, const string& url) {
  vector<string> split = Split(url, delimiter::Limit("/", 3));
  if (split.size() != 4 || (split[0] != "http:" && split[0] != "https:") ||
      !split[1].empty() || split[2].empty()) {
    return Status(
        Substitute("Bad saml2_sp_callback_url: $0", url));
  }
  // The port in the url should be the same as FLAGS_hs2_http_port / FLAGS_webserver_port
  // in general, but this is not enforced to allow the use case when the port is mapped,
  // e.g. external_port->http_port. FLAGS_saml2_sp_callback_url has to contain the
  // external_port in this case.
  if (saml_sp_path != nullptr) *saml_sp_path = "/" + split[3];
  return Status::OK();
}

bool ParseParams(std::map<string, string*>& params_to_check, const string& original,
    const string& params_string, string* err_msg) {
  for (auto pair : Split(params_string, "&")) {
    vector<string> key_value = Split(pair, delimiter::Limit("=", 1));
    if (key_value.size() == 2) {
      auto it = params_to_check.find(key_value[0]);
      if (it == params_to_check.end()) continue;
      string decoded;
      if (!UrlDecode(key_value[1], &decoded)) {
        *err_msg =
            Substitute("Could not decode '$0' parameter from HTTP request with path: $1",
                key_value[0], original);
        return false;
      } else {
        *it->second = decoded;
      }
    }
  }
  return true;
}

std::unique_ptr<TWrappedHttpResponse> GetSaml2RedirectInternal(
    const TWrappedHttpRequest& request) {
  auto response = std::make_unique<TWrappedHttpResponse>();
  Status status = ExecEnv::GetInstance()->frontend()->GetSaml2Redirect(request,
      response.get());
  if (!status.ok()) {
    return nullptr;
  }
  return response;
}

std::unique_ptr<TWrappedHttpResponse> ValidateSaml2AuthnResponseInternal(
    TWrappedHttpRequest& request) {
  auto response = std::make_unique<TWrappedHttpResponse>();
  // Parse the body.
  std::map<string, string*> params_to_check;
  string saml_relay_state;
  params_to_check["RelayState"] = &saml_relay_state;
  string error_msg;
  const string& content = request.content;
  if (!ParseParams(params_to_check, content, content, &error_msg)) {
    LOG(ERROR) << "failed to parse SAML response params: " << error_msg;
    return nullptr;
  }
  StripWhiteSpace(&saml_relay_state);
  request.params["RelayState"] = saml_relay_state;
  // We return some html in case of auth error. TODO: Should handle other
  // errors where no response is generated.
  discard_result(
      ExecEnv::GetInstance()->frontend()->ValidateSaml2Response(request, response.get()));
  return response;
}

std::unique_ptr<TWrappedHttpRequest> InitWrappedHttpRequestInternal(
    const std::string& remote_ip, const std::string& server_name) {
  auto request = std::make_unique<TWrappedHttpRequest>();
  request->remote_ip = remote_ip;
  request->server_name = server_name;
  // Intentionally lying to the pac4j lib in the frontend to ensure that non-secure
  // test setups work similarly to secure production systems. SAML must use TLS if
  // FLAGS_saml2_allow_without_tls_debug_only is not true.
  request->secure = true;
  return request;
}

std::string GetTimeRFC1123() {
  char buf[64];
  std::time_t t = std::time(nullptr);
  std::tm tm;
  gmtime_r(&t, &tm);
  std::strftime(buf, sizeof(buf), "%a, %d %b %Y %H:%M:%S GMT", &tm);
  return buf;
}

} // namespace impala
