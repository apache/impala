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

/**
 * Relay state generator for the WebServer SAML Request which includes the requested
 * resource URI.
 */
public class HiveSamlRelayStateStoreWS
    extends HiveSamlRelayStateCacheBase<HiveSamlRelayStateInfoWS> {
  private static final HiveSamlRelayStateStoreWS INSTANCE =
      new HiveSamlRelayStateStoreWS();

  private HiveSamlRelayStateStoreWS() {
  }

  public static HiveSamlRelayStateStoreWS get() {
    return INSTANCE;
  }

  @Override
  protected HiveSamlRelayStateInfoWS createRelayStateInfo(WebContext webContext) {
    String path = webContext.getPath();
    if (path.isEmpty()) {
      throw new RuntimeException("SAML RelayState path is not set ");
    }
    return new HiveSamlRelayStateInfoWS(path);
  }

  @Override
  protected String getClientIdentifier(HiveSamlRelayStateInfoWS relayStateInfo) {
    // WebServer workflow doesn't use client identifier
    return null;
  }
}