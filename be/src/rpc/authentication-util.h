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
// under the License.xb

#pragma once

#include "rpc/thrift-server.h"

namespace impala {

class AuthenticationHash;

// Takes a single 'key=value' pair from a 'Cookie' header and attempts to verify its
// signature with 'hash'. If verification is successful and the cookie is still valid,
// sets 'username' and 'rand' (if specified) to the corresponding values and returns OK.
Status AuthenticateCookie(
    const AuthenticationHash& hash, const std::string& cookie_header,
    std::string* username, std::string* rand = nullptr);

// Generates and returns a cookie containing the username set on 'connection_context' and
// a signature generated with 'hash'. If specified, sets 'rand' to the 'r=' cookie value.
std::string GenerateCookie(const std::string& username, const AuthenticationHash& hash,
    std::string* rand = nullptr);

// Returns a empty cookie. Returned in a 'Set-Cookie' when cookie auth fails to indicate
// to the client that the cookie should be deleted.
std::string GetDeleteCookie();

// Takes a comma separated list of ip address/hostname with or without a port, picks the
// left most on the list (this assumes the list follows the http standard for
// 'X-Forwarded-For' header where the left-most IP address is the IP address of the
// originating client), then checks whether this corresponds to the 'trusted_domain'.
// If the 'trusted_domain' is 'localhost' and 'strict_localhost' is true, this
// returns true if the origin is the 127.0.0.1 IP address. If 'strict_localhost' is
// false or 'trusted_domain' is not 'localhost', then this does a reverse DNS lookup
// if it's a valid ip address and checks if it originates from the 'trusted_domain'.
// Returns true if the origin is from the trusted domain.
bool IsTrustedDomain(const std::string& origin, const std::string& trusted_domain,
    bool strict_localhost);

// Takes in the base64 encoded token and returns the username and password via the input
// arguments. Returns an OK status if the token is a valid base64 encoded string of the
// form <username>:<password>, an error status otherwise.
Status BasicAuthExtractCredentials(
    const string& token, string& username, string& password);

constexpr int RAND_MAX_LENGTH = 10;

} // namespace impala
