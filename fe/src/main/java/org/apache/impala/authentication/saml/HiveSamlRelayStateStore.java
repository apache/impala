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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.security.SecureRandom;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.pac4j.core.context.WebContext;
import org.pac4j.core.util.generator.ValueGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// slightly modified copy of https://github.com/vihangk1/hive/blob/master_saml/service/src/java/org/apache/hive/service/auth/saml/HiveSamlRelayStateInfo.java
/**
 * Relay state generator for the SAML Request which includes the port number from the
 * request header. This port number is used eventually to redirect the token to the
 * localhost:port from the browser.
 */
public class HiveSamlRelayStateStore implements ValueGenerator {

  private final Cache<String, HiveSamlRelayStateInfo> relayStateCache =
      CacheBuilder.newBuilder()
          //TODO(Vihang) make this configurable
          .expireAfterWrite(5, TimeUnit.MINUTES)
          .build();
  private static final Random randGenerator = new SecureRandom();
  private static final Logger LOG = LoggerFactory
      .getLogger(HiveSamlRelayStateStore.class);

  private static final HiveSamlRelayStateStore INSTANCE = new HiveSamlRelayStateStore();

  private HiveSamlRelayStateStore() {
  }

  public static HiveSamlRelayStateStore get() {
    return INSTANCE;
  }

  @Override
  public String generateValue(WebContext webContext) {
    Optional<String> portNumber = webContext
        .getRequestHeader(HiveSamlUtils.SSO_TOKEN_RESPONSE_PORT);
    if (!portNumber.isPresent()) {
      throw new RuntimeException(
          "SAML response port header " + HiveSamlUtils.SSO_TOKEN_RESPONSE_PORT
              + " is not set ");
    }
    int port = Integer.parseInt(portNumber.get());
    String relayState = UUID.randomUUID().toString();
    HiveSamlRelayStateInfo relayStateInfo = new HiveSamlRelayStateInfo(port,
        UUID.randomUUID().toString());
    webContext.setResponseHeader(HiveSamlUtils.SSO_CLIENT_IDENTIFIER,
        relayStateInfo.getClientIdentifier());
    relayStateCache.put(relayState, relayStateInfo);
    return relayState;
  }

  public String getRelayStateInfo(WebContext webContext)
      throws HttpSamlAuthenticationException {
    Optional<String> relayState = webContext.getRequestParameter("RelayState");
    if (relayState == null || !relayState.isPresent()) {
      throw new HttpSamlAuthenticationException(
          "Could not get the RelayState from the SAML response");
    }
    return relayState.get();
  }

  public HiveSamlRelayStateInfo getRelayStateInfo(String relayState)
      throws HttpSamlAuthenticationException {
    HiveSamlRelayStateInfo relayStateInfo = relayStateCache.getIfPresent(relayState);
    if (relayStateInfo == null) {
      throw new HttpSamlAuthenticationException(
          "Invalid value of relay state received: " + relayState);
    }
    return relayStateInfo;
  }

  public synchronized boolean validateClientIdentifier(String relayStateKey,
      String clientIdentifier) {
    HiveSamlRelayStateInfo relayStateInfo = relayStateCache.getIfPresent(relayStateKey);
    if (relayStateInfo == null) {
      return false;
    }
    relayStateCache.invalidate(relayStateKey);
    LOG.debug("Validating client identifier {} with {}", clientIdentifier,
        relayStateInfo.getClientIdentifier());
    return relayStateInfo.getClientIdentifier().equals(clientIdentifier);
  }
}