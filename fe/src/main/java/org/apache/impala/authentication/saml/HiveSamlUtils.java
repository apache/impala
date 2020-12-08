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

package org.apache.impala.authentication.saml;

public class HiveSamlUtils {
  public static final String SSO_TOKEN_RESPONSE_PORT = "X-Hive-Token-Response-Port";
  public static final String SSO_CLIENT_IDENTIFIER = "X-Hive-Client-Identifier";
  public static final String TOKEN_KEY = "token";
  public static final String STATUS_KEY = "status";
  public static final String MESSAGE_KEY = "message";
}
