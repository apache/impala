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

// based on https://github.com/vihangk1/hive/blob/45863cc1fc94c2f2a848d0f3fc160a4dc0214747/service/src/java/org/apache/hive/service/auth/saml/HiveSamlRelayStateInfo.java
public class HiveSamlRelayStateInfoHS2 implements HiveSamlRelayStateInfo {
  private final int port;
  private final String clientIdentifier;

  HiveSamlRelayStateInfoHS2(int port, String clientIdentifier) {
    this.port = port;
    this.clientIdentifier = clientIdentifier;
  }

  public int getPort() {
    return port;
  }

  public String getClientIdentifier() {
    return clientIdentifier;
  }
}
