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

import org.pac4j.core.context.WebContext;
import org.pac4j.saml.store.SAMLMessageStore;
import org.pac4j.saml.store.SAMLMessageStoreFactory;

/**
 * Factory for creating ImpalaSAMLMessageStore instances.
 *
 * This factory provides SAML message stores backed by a Guava cache instead of
 * HTTP session cookies, enabling InResponseTo validation and replay attack
 * prevention across separate HTTP requests without session cookie dependencies.
 */
public class ImpalaSAMLMessageStoreFactory implements SAMLMessageStoreFactory {

  private static final ImpalaSAMLMessageStore INSTANCE = new ImpalaSAMLMessageStore();

  @Override
  public SAMLMessageStore getMessageStore(WebContext context) {
    // Return singleton instance since the cache is static and shared
    return INSTANCE;
  }
}
