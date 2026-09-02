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

import java.util.Optional;
import java.util.UUID;
import org.pac4j.core.context.WebContext;

// based on https://github.com/vihangk1/hive/blob/45863cc1fc94c2f2a848d0f3fc160a4dc0214747/service/src/java/org/apache/hive/service/auth/saml/HiveSamlRelayStateStore.java
/**
 * Relay state generator for the HiveServer2 SAML Request which includes the port number
 * from the request header. This port number is used eventually to redirect the token to
 * the localhost:port from the browser.
 */
public class HiveSamlRelayStateStoreHS2
    extends HiveSamlRelayStateCacheBase<HiveSamlRelayStateInfoHS2> {
  private static final HiveSamlRelayStateStoreHS2 INSTANCE =
      new HiveSamlRelayStateStoreHS2();

  private HiveSamlRelayStateStoreHS2() {
  }

  public static HiveSamlRelayStateStoreHS2 get() {
    return INSTANCE;
  }

  @Override
  protected HiveSamlRelayStateInfoHS2 createRelayStateInfo(WebContext webContext) {
    Optional<String> portNumber = webContext
        .getRequestHeader(HiveSamlUtils.SSO_TOKEN_RESPONSE_PORT);
    if (!portNumber.isPresent()) {
      throw new RuntimeException(
          "SAML response port header " + HiveSamlUtils.SSO_TOKEN_RESPONSE_PORT
              + " is not set ");
    }
    int port = Integer.parseInt(portNumber.get());
    return new HiveSamlRelayStateInfoHS2(port, UUID.randomUUID().toString());
  }

  @Override
  protected String getClientIdentifier(HiveSamlRelayStateInfoHS2 relayStateInfo) {
    return relayStateInfo.getClientIdentifier();
  }
}
