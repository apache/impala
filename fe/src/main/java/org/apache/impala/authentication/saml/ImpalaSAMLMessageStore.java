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
import org.opensaml.core.xml.XMLObject;
import org.pac4j.saml.store.SAMLMessageStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Custom SAML message store implementation that uses Guava cache for storing
 * SAML request IDs. This enables InResponseTo validation and replay attack
 * prevention without requiring HTTP session cookies.
 *
 * The cache stores request IDs with automatic expiration (5 minutes) to prevent
 * memory leaks while maintaining request/response correlation across separate
 * HTTP requests.
 */
public class ImpalaSAMLMessageStore implements SAMLMessageStore {
  private static final Logger LOG =
      LoggerFactory.getLogger(ImpalaSAMLMessageStore.class);

  // Shared cache for storing sent SAML request IDs
  // Key: request ID (String), Value: XMLObject (we store the request ID as marker)
  private static final Cache<String, XMLObject> requestIdCache =
      CacheBuilder.newBuilder()
          .expireAfterWrite(5, TimeUnit.MINUTES)
          .build();

  /**
   * Retrieves and removes the SAML request with the given ID.
   *
   * This implements a "single-use" pattern following pac4j's HttpSessionStore behavior:
   * the request ID is automatically removed after retrieval to prevent replay attacks
   * where an attacker reuses the same request_id with a different response.
   *
   * When pac4j validates InResponseTo, it calls get() to verify the request_id exists.
   * By removing it immediately, we ensure each request_id can only be used once.
   *
   * @param key the SAML request ID to look up
   * @return Optional containing the XMLObject if found, empty otherwise
   */
  @Override
  public Optional<XMLObject> get(String key) {
    XMLObject value = requestIdCache.getIfPresent(key);
    if (value != null) {
      LOG.debug("Looking up SAML request ID: {}, found: true - removing after "
          + "retrieval", key);
      requestIdCache.invalidate(key);
    } else {
      LOG.debug("Looking up SAML request ID: {}, found: false", key);
    }
    return Optional.ofNullable(value);
  }

  @Override
  public void set(String key, XMLObject value) {
    LOG.debug("Storing SAML request ID: {}", key);
    requestIdCache.put(key, value);
  }

  @Override
  public void remove(String key) {
    // This method is part of the Store interface but is not used by pac4j.
    // The get() method already removes entries to implement single-use semantics.
    LOG.debug("Removing SAML request ID: {}", key);
    requestIdCache.invalidate(key);
  }
}
