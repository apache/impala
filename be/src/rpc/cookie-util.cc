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

#include "rpc/cookie-util.h"

#include <gutil/strings/escaping.h>
#include <gutil/strings/split.h>
#include <gutil/strings/strcat.h>
#include <gutil/strings/strip.h>

#include "util/network-util.h"
#include "util/openssl-util.h"
#include "util/string-parser.h"

DECLARE_bool(ldap_passwords_in_clear_ok);
DEFINE_int64(max_cookie_lifetime_s, 24 * 60 * 60,
    "Maximum amount of time in seconds that an authentication cookie will remain valid. "
    "Setting to 0 disables use of cookies. Defaults to 1 day.");

using namespace strings;

namespace impala {

// Used to separate values in cookies. All generated cookies will be of the form:
// <signature>&<username>&<timestamp>&<random number>
static const string COOKIE_SEPARATOR = "&";

// Cookies generated and processed by the HTTP server will be of the form:
// COOKIE_NAME=<cookie>
static const string COOKIE_NAME = "impala.hs2.auth";

// The maximum lenth for the base64 encoding of a SHA256 hash.
static const int SHA256_BASE64_LEN =
    CalculateBase64EscapedLen(AuthenticationHash::HashLen(), /* do_padding */ true);

// Since we only return cookies with a single name, well behaved clients should only ever
// return one cookie to us. To accomodate non-malicious but poorly behaved clients, we
// allow for checking a limited number of cookies, up to MAX_COOKIES_TO_CHECK or until we
// find the first one with COOKIE_NAME.
static const int MAX_COOKIES_TO_CHECK = 5;

bool AuthenticateCookie(ThriftServer::ConnectionContext* connection_context,
    const AuthenticationHash& hash, const string& cookie_header) {
  string error_str = "";
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
      error_str = Substitute("Did not find expected cookie name: $0", COOKIE_NAME);
      continue;
    }
    // Split the cookie into the signature and the cookie value.
    vector<string> cookie_split = Split(cookie, delimiter::Limit(COOKIE_SEPARATOR, 1));
    if (cookie_split.size() != 2) {
      error_str = "The cookie has an invalid format.";
      goto error;
    }
    const string& base64_signature = cookie_split[0];
    const string& cookie_value = cookie_split[1];

    string signature;
    if (!WebSafeBase64Unescape(base64_signature, &signature)) {
      error_str = "Unable to decode base64 signature.";
      goto error;
    }
    if (signature.length() != AuthenticationHash::HashLen()) {
      error_str = "Signature is an incorrect length.";
      goto error;
    }
    bool verified = hash.Verify(reinterpret_cast<const uint8_t*>(cookie_value.data()),
        cookie_value.length(), reinterpret_cast<const uint8_t*>(signature.data()));
    if (!verified) {
      error_str = "The signature is incorrect.";
      goto error;
    }

    // Split the cookie value into username, timestamp, and random number.
    vector<string> cookie_value_split = Split(cookie_value, COOKIE_SEPARATOR);
    if (cookie_value_split.size() != 3) {
      error_str = "The cookie value has an invalid format.";
      goto error;
    }
    StringParser::ParseResult result;
    int64_t create_time = StringParser::StringToInt<int64_t>(
        cookie_value_split[1].c_str(), cookie_value_split[1].length(), &result);
    if (result != StringParser::PARSE_SUCCESS) {
      error_str = "Could not parse cookie timestamp.";
      goto error;
    }
    // Check that the timestamp contained in the cookie is recent enough for the cookie
    // to still be valid.
    if (MonotonicMillis() - create_time <= FLAGS_max_cookie_lifetime_s * 1000) {
      // We've successfully authenticated.
      connection_context->username = cookie_value_split[0];
      return true;
    } else {
      error_str = "Cookie is past its max lifetime.";
      goto error;
    }
  }

error:
  LOG(INFO) << "Invalid cookie provided: " << cookie_header
            << " from: " << TNetworkAddressToString(connection_context->network_address)
            << ": " << error_str;
  return false;
}

string GenerateCookie(const string& username, const AuthenticationHash& hash) {
  // Its okay to use rand() here even though its a weak RNG because being able to guess
  // the random numbers generated won't help an attacker. The important thing is that
  // we're using a strong RNG to create the key and a strong HMAC function.
  string cookie_value =
      StrCat(username, COOKIE_SEPARATOR, MonotonicMillis(), COOKIE_SEPARATOR, rand());
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

  const char* secure_flag = ";Secure";
  if (FLAGS_ldap_passwords_in_clear_ok) {
    // If the user specified password can be sent without TLS/SSL, don't include the
    // 'Secure' flag, which indicates the cookie should only be returned over secured
    // connections. This is for testing only.
    secure_flag = "";
  }
  return Substitute("$0=$1$2$3;HttpOnly;MaxAge=$4$5", COOKIE_NAME, base64_signature,
      COOKIE_SEPARATOR, cookie_value, FLAGS_max_cookie_lifetime_s, secure_flag);
}

} // namespace impala
