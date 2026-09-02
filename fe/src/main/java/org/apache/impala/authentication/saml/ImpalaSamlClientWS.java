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

import org.apache.impala.common.InternalException;
import org.apache.impala.service.BackendConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Simplified SAML2Client implementation for Webserver without Bearer Token.
 * Extends ImpalaSamlClientBase for shared functionality.
 */
public class ImpalaSamlClientWS extends ImpalaSamlClientBase {

  private static final Logger LOG = LoggerFactory.getLogger(ImpalaSamlClientWS.class);
  private static ImpalaSamlClientWS INSTANCE;

  private ImpalaSamlClientWS() throws Exception {
    super(BackendConfig.INSTANCE.getWSSaml2IdpMetadata(),
        ImpalaSamlClientWS.class.getSimpleName(),
        HiveSamlRelayStateStoreWS.get(),
        BackendConfig.INSTANCE.getWSSaml2SpCallbackUrl());
  }

  public static synchronized ImpalaSamlClientWS get()
      throws InternalException {
    if (INSTANCE != null) {
      return INSTANCE;
    }
    try {
      INSTANCE = new ImpalaSamlClientWS();
    } catch (Exception e) {
      throw new InternalException("Could not instantiate SAML2.0 client", e);
    }
    return INSTANCE;
  }

  /**
   * Generates a SAML request using the HTTP-Redirect Binding.
   */
  @Override
  public void setRedirect(WrappedWebContext webContext)
      throws InternalException {
    String path = webContext.getPath();
    if (path.isEmpty()) {
      throw new RuntimeException("SAML RelayState path is not set ");
    }
    LOG.debug("Request has path set as {}", path);
    setRedirectCommon(webContext);
  }

  /**
   * Validates SAML authentication response for WebServer context.
   * Uses common validation logic and handles response with HTTP redirect.
   */
  @Override
  public void validateAuthnResponse(WrappedWebContext webContext)
      throws InternalException {
    validateAuthnResponseCommon(webContext, HiveSamlRelayStateStoreWS.get(),
        new AuthnResponseHandler<HiveSamlRelayStateInfoWS>() {
          @Override
          public void handleSuccess(WrappedWebContext webContext, String nameId,
              String relayState, HiveSamlRelayStateInfoWS relayStateInfo) {
            Preconditions.checkState(nameId != null);
            String uri = relayStateInfo.getUri();
            LOG.debug("Successfully validated saml response. Forwarding to " + uri);
            // Invalidate relay state to make it one-time use
            HiveSamlRelayStateStoreWS.get().invalidateRelayState(relayState);
            webContext.setResponseStatusCode(302);
            webContext.setResponseHeader("Location", uri);
            // returning authenticated user name in response's body
            webContext.setResponseContent("", nameId);
          }

          @Override
          public void handleFailure(WrappedWebContext webContext,
              HiveSamlRelayStateInfoWS relayStateInfo, String errorMsg) {
            webContext.setResponseStatusCode(401);
            webContext.setResponseContent("", errorMsg);
          }
        });
  }
}
