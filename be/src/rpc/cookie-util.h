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
// sets the corresponding username on 'connection_context' and returns true.
bool AuthenticateCookie(ThriftServer::ConnectionContext* connection_context,
    const AuthenticationHash& hash, const std::string& cookie_header);

// Generates and returns a cookie containing the username set on 'connection_context' and
// a signature generated with 'hash'.
std::string GenerateCookie(const std::string& username, const AuthenticationHash& hash);

} // namespace impala
