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
// sets 'username' to the corresponding username and returns OK.
Status AuthenticateCookie(const AuthenticationHash& hash,
    const std::string& cookie_header, std::string* username);

// Generates and returns a cookie containing the username set on 'connection_context' and
// a signature generated with 'hash'.
std::string GenerateCookie(const std::string& username, const AuthenticationHash& hash);

// Returns a empty cookie. Returned in a 'Set-Cookie' when cookie auth fails to indicate
// to the client that the cookie should be deleted.
std::string GetDeleteCookie();

// Takes a comma separated list of ip address/hostname with or without a port, picks the
// left most on the list (this assumes the list follows the http standard for
// 'X-Forwarded-For' header where the left-most IP address is the IP address of the
// originating client), does a reverse DNS lookup on it if its a valid ip address and
// finally checks if it originates from the 'trusted_domain'. Returns true if the origin
// is from the trusted domain.
bool IsTrustedDomain(const std::string& origin, const std::string& trusted_domain);

// Takes in the base64 encoded token and returns the username and password via the input
// arguments. Returns an OK status if the token is a valid base64 encoded string of the
// form <username>:<password>, an error status otherwise.
Status BasicAuthExtractCredentials(
    const string& token, string& username, string& password);

} // namespace impala
