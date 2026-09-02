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

/**
 * Base class for SAML relay state generators. Contains common functionality
 * for managing relay state cache and client identifier validation.
 *
 * @param <T> The type of relay state info stored in the cache
 */
public abstract class HiveSamlRelayStateCacheBase<T extends HiveSamlRelayStateInfo>
    implements ValueGenerator {

  protected final Cache<String, T> relayStateCache =
      CacheBuilder.newBuilder()
          //TODO(Vihang) make this configurable
          .expireAfterWrite(5, TimeUnit.MINUTES)
          .build();

  protected static final Random randGenerator = new SecureRandom();
  protected final Logger LOG = LoggerFactory.getLogger(getClass());

  protected HiveSamlRelayStateCacheBase() {
  }

  @Override
  public String generateValue(WebContext webContext) {
    String relayState = UUID.randomUUID().toString();
    T relayStateInfo = createRelayStateInfo(webContext);
    String clientIdentifier = getClientIdentifier(relayStateInfo);
    if (clientIdentifier != null) {
      webContext.setResponseHeader(HiveSamlUtils.SSO_CLIENT_IDENTIFIER, clientIdentifier);
    }
    relayStateCache.put(relayState, relayStateInfo);
    return relayState;
  }

  /**
   * Creates the appropriate relay state info object based on the web context.
   * This method must be implemented by subclasses to provide the specific
   * relay state info type and extraction logic.
   *
   * @param webContext the web context containing request information
   * @return the relay state info object
   */
  protected abstract T createRelayStateInfo(WebContext webContext);

  /**
   * Extracts the client identifier from the relay state info object.
   *
   * @param relayStateInfo the relay state info object
   * @return the client identifier string
   */
  protected abstract String getClientIdentifier(T relayStateInfo);

  public String getRelayStateInfo(WebContext webContext)
      throws HttpSamlAuthenticationException {
    Optional<String> relayState = webContext.getRequestParameter("RelayState");
    if (relayState == null || !relayState.isPresent()) {
      throw new HttpSamlAuthenticationException(
          "Could not get the RelayState from the SAML response");
    }
    return relayState.get();
  }

  public T getRelayStateInfo(String relayState)
      throws HttpSamlAuthenticationException {
    T relayStateInfo = relayStateCache.getIfPresent(relayState);
    if (relayStateInfo == null) {
      throw new HttpSamlAuthenticationException(
          "Invalid value of relay state received: " + relayState);
    }
    return relayStateInfo;
  }

  public synchronized boolean validateClientIdentifier(String relayStateKey,
      String clientIdentifier) {
    T relayStateInfo = relayStateCache.getIfPresent(relayStateKey);
    if (relayStateInfo == null) {
      return false;
    }
    relayStateCache.invalidate(relayStateKey);
    String storedClientIdentifier = getClientIdentifier(relayStateInfo);
    // If stored client identifier is null, this workflow doesn't use client validation
    if (storedClientIdentifier == null) {
      return true;
    }
    LOG.debug("Validating client identifier {} with {}", clientIdentifier,
        storedClientIdentifier);
    return storedClientIdentifier.equals(clientIdentifier);
  }

  /**
   * Invalidates a relay state, making it one-time use.
   */
  public void invalidateRelayState(String relayStateKey) {
    relayStateCache.invalidate(relayStateKey);
  }
}
