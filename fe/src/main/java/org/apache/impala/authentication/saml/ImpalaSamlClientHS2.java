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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.http.HttpStatus;
import org.apache.impala.common.InternalException;
import org.apache.impala.service.BackendConfig;
import org.pac4j.core.credentials.TokenCredentials;
import org.pac4j.core.credentials.extractor.BearerAuthExtractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
// based on https://github.com/vihangk1/hive/blob/d0209a6f026106622523bd4ec7eeeae33782e7a3/service/src/java/org/apache/hive/service/auth/saml/HiveSaml2Client.java

/**
 * HiveServer2's implementation of SAML2Client with Bearer Token support.
 * Extends ImpalaSamlClientBase for shared functionality.
 */
public class ImpalaSamlClientHS2 extends ImpalaSamlClientBase {

  private static final Logger LOG = LoggerFactory.getLogger(ImpalaSamlClientHS2.class);
  private static ImpalaSamlClientHS2 INSTANCE;
  private final AuthTokenGenerator tokenGenerator;

  private ImpalaSamlClientHS2() throws Exception {
    super(BackendConfig.INSTANCE.getHS2Saml2IdpMetadata(),
        ImpalaSamlClientHS2.class.getSimpleName(),
        HiveSamlRelayStateStoreHS2.get(),
        BackendConfig.INSTANCE.getHS2Saml2SpCallbackUrl());
    tokenGenerator = HiveSamlAuthTokenGenerator.get();
  }

  public static synchronized ImpalaSamlClientHS2 get()
      throws InternalException {
    if (INSTANCE != null) {
      return INSTANCE;
    }
    try {
      INSTANCE = new ImpalaSamlClientHS2();
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
    Optional<String> responsePort =
        webContext.getRequestHeader(HiveSamlUtils.SSO_TOKEN_RESPONSE_PORT);
    if (responsePort == null || !responsePort.isPresent()) {
      throw new InternalException("No response port specified");
    }
    LOG.debug("Request has response port set as {}", responsePort);
    setRedirectCommon(webContext);
  }

  /**
   * Validates SAML authentication response for HiveServer2 context.
   * Uses common validation logic and handles response with HTML form POST.
   */
  @Override
  public void validateAuthnResponse(WrappedWebContext webContext)
      throws InternalException {
    validateAuthnResponseCommon(webContext, HiveSamlRelayStateStoreHS2.get(),
        new AuthnResponseHandler<HiveSamlRelayStateInfoHS2>() {
          @Override
          public void handleSuccess(WrappedWebContext webContext, String nameId,
              String relayState, HiveSamlRelayStateInfoHS2 relayStateInfo) {
            int port = relayStateInfo.getPort();
            LOG.debug(
                "Successfully validated saml response. Forwarding the token to port "
                    + port);
            String token = tokenGenerator.get(nameId, relayState);
            generateFormData(webContext, "http://127.0.0.1:" + port, token, true, "");
          }

          @Override
          public void handleFailure(WrappedWebContext webContext,
              HiveSamlRelayStateInfoHS2 relayStateInfo, String errorMsg) {
            int port = relayStateInfo.getPort();
            generateFormData(webContext, "http://127.0.0.1:" + port, null, false,
                errorMsg);
          }
        });
    webContext.setResponseStatusCode(HttpStatus.SC_OK);
  }

  /**
   * Generates an HTML form that auto-submits to the target URL.
   * Used to POST authentication token back to the client.
   */
  private void generateFormData(WrappedWebContext webContext, String url, String token,
      boolean success, String msg) {
    StringBuilder sb = new StringBuilder();
    sb.append("<html>");
    sb.append("<body onload='document.forms[\"form\"].submit()'>");
    sb.append(String.format("<form name='form' action='%s' method='POST'>", url));
    sb.append(String.format("<input type='hidden' name='%s' value='%s'/>",
        HiveSamlUtils.TOKEN_KEY, token));
    sb.append(String.format("<input type='hidden' name='%s' value='%s'/>",
        HiveSamlUtils.STATUS_KEY, success));
    sb.append(String.format("<input type='hidden' name='%s' value='%s'/>",
        HiveSamlUtils.MESSAGE_KEY, msg));
    sb.append("</form>");
    sb.append("</body>");
    sb.append("</html>");
    webContext.setResponseContent("text/html;charset=utf-8", sb.toString());
  }

  public String validateBearer(WrappedWebContext webContext) throws InternalException {
    LOG.info(webContext.getRequestAsJsonString());
    try {
      return doSamlAuth(webContext);
    } catch (HttpSamlAuthenticationException ex) {
      throw new InternalException("SAML2 bearer validation failed", ex);
    }
  }

  // based on
  // https://github.com/vihangk1/hive/blob/d0209a6f026106622523bd4ec7eeeae33782e7a3/service/src/java/org/apache/hive/service/cli/thrift/ThriftHttpServlet.java
  private String doSamlAuth(WrappedWebContext webContext)
      throws HttpSamlAuthenticationException {
    BearerAuthExtractor extractor = new BearerAuthExtractor();
    Optional<TokenCredentials> tokenCredentials = extractor.extract(webContext);
    String token = tokenCredentials.map(TokenCredentials::getToken).orElse(null);
    if (token == null) {
      throw new HttpSamlAuthenticationException("No token found");
    }

    Optional<String> clientIdentifier =
        webContext.getRequestHeader(HiveSamlUtils.SSO_CLIENT_IDENTIFIER);
    if (clientIdentifier == null || !clientIdentifier.isPresent()) {
      throw new HttpSamlAuthenticationException("Client identifier not found.");
    }
    String user = HiveSamlAuthTokenGenerator.get().validate(token);
    // token is valid; now confirm if the code verifier matches with the relay state.
    Map<String, String> keyValues = new HashMap<>();
    if (HiveSamlAuthTokenGenerator.parse(token, keyValues)) {
      String relayStateKey = keyValues.get(HiveSamlAuthTokenGenerator.RELAY_STATE);
      if (!HiveSamlRelayStateStoreHS2.get()
          .validateClientIdentifier(relayStateKey, clientIdentifier.get())) {
        throw new HttpSamlAuthenticationException(
            "Code verifier could not be validated");
      }
    }
    return user;
  }
}
