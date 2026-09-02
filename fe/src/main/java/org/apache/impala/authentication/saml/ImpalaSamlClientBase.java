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

import static org.opensaml.saml.common.xml.SAMLConstants.SAML2_POST_BINDING_URI;

import java.util.Optional;

import org.apache.impala.service.BackendConfig;
import org.opensaml.saml.common.xml.SAMLConstants;
import org.pac4j.core.exception.http.RedirectionAction;
import org.pac4j.core.exception.http.WithLocationAction;
import org.pac4j.saml.client.SAML2Client;
import org.pac4j.saml.config.SAML2Configuration;
import org.pac4j.core.exception.http.RedirectionActionHelper;
import org.pac4j.saml.credentials.SAML2Credentials;
import org.pac4j.saml.credentials.extractor.SAML2CredentialsExtractor;
import org.pac4j.saml.store.SAMLMessageStoreFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.impala.common.InternalException;

/**
 * Base class for Impala SAML clients containing shared functionality.
 * Extracted common code from ImpalaSamlClientHS2 and ImpalaSamlClientWS.
 */
public abstract class ImpalaSamlClientBase extends SAML2Client {

  private static final Logger LOG = LoggerFactory.getLogger(ImpalaSamlClientBase.class);
  protected final HiveSamlGroupNameFilter groupNameFilter;

  protected ImpalaSamlClientBase(String idpMetadata, String clientName,
      HiveSamlRelayStateCacheBase<?> stateGenerator, String callbackUrl)
      throws Exception {
    super(getSamlConfig(idpMetadata));
    // setUseModernHttpCodes(false) is needed to return 302 instead of 303 for
    // redirect actions on POST requests.
    RedirectionActionHelper.setUseModernHttpCodes(false);
    setName(clientName);
    setStateGenerator(stateGenerator);
    setCallbackUrl(callbackUrl);
    groupNameFilter = new HiveSamlGroupNameFilter();
    init();
    // ReplayCache is now configured via ImpalaSAMLMessageStoreFactory in getSamlConfig().
  }

  /**
   * Extracts the SAML specific configuration needed to initialize the SAML2.0 client.
   */
  protected static SAML2Configuration getSamlConfig(String idpMetadata) throws Exception {
    BackendConfig conf = BackendConfig.INSTANCE;
    LOG.info("keystore path: " + conf.getSaml2KeystorePath());
    SAML2Configuration saml2Configuration = new SAML2Configuration(
       conf.getSaml2KeystorePath(),
       conf.getSaml2KeystorePassword(),
       conf.getSaml2PrivateKeyPassword(),
       idpMetadata);

    saml2Configuration
        .setAuthnRequestBindingType(SAMLConstants.SAML2_REDIRECT_BINDING_URI);
    saml2Configuration.setResponseBindingType(SAML2_POST_BINDING_URI);
    saml2Configuration.setServiceProviderEntityId(conf.getSaml2SpEntityId());
    saml2Configuration.setWantsAssertionsSigned(conf.getSaml2WantAsserationsSigned());
    saml2Configuration.setAuthnRequestSigned(conf.getSaml2SignRequest());
    saml2Configuration.setAllSignatureValidationDisabled(conf.getSaml2EETestMode());

    // Enable replay cache for InResponseTo validation and replay attack prevention.
    // Uses custom ImpalaSAMLMessageStoreFactory with Guava cache (similar to relay
    // state) instead of HTTP session cookies. This enables:
    // 1. Validate that InResponseTo matches a request ID we actually sent
    // 2. Prevent replay attacks by tracking used responses
    // 3. Works across separate HTTP requests without requiring session cookies
    // Cache expires after 5 minutes to prevent memory leaks.
    SAMLMessageStoreFactory storeFactory = new ImpalaSAMLMessageStoreFactory();
    saml2Configuration.setSamlMessageStoreFactory(storeFactory);

    return saml2Configuration;
  }

  /**
   * Generates a SAML request using the HTTP-Redirect Binding.
   * Common implementation for both client types.
   */
  protected final void setRedirectCommon(WrappedWebContext webContext)
      throws InternalException {
    Optional<RedirectionAction> redirect = getRedirectionAction(webContext);
    if (redirect == null || !redirect.isPresent()) {
      throw new InternalException("Could not get the redirect response");
    }
    webContext.setResponseStatusCode(redirect.get().getCode());
    WithLocationAction locationAction = (WithLocationAction) redirect.get();
    webContext.setResponseHeader("Location", locationAction.getLocation());
  }

  /**
   * Core credential validation logic shared by both client types.
   * Validates SAML credentials and applies group filtering.
   */
  protected final String validateAuthnResponseInner(WrappedWebContext webContext)
      throws HttpSamlAuthenticationException {
    Optional<SAML2Credentials> credentials;
    try {
      SAML2CredentialsExtractor credentialsExtractor = new SAML2CredentialsExtractor(
          this);
      credentials = credentialsExtractor.extract(webContext);
    } catch (Exception ex) {
      throw new HttpSamlAuthenticationException("Could not validate the SAML response",
          ex);
    }
    if (!credentials.isPresent()) {
      throw new HttpSamlAuthenticationException("Credentials could not be extracted");
    }

    String nameId = credentials.get().getNameId().getValue();
    if (!groupNameFilter.apply(credentials.get().getAttributes())) {
      LOG.warn("Could not match any groups for the nameid {}", nameId);
      throw new HttpSamlNoGroupsMatchedException(
          "None of the configured groups match for the user");
    }
    return nameId;
  }

  /**
   * Common validation logic for SAML authentication responses.
   * Handles relay state retrieval, credential validation, and delegates response
   * handling to the provided callback.
   */
  protected final <T extends HiveSamlRelayStateInfo> void validateAuthnResponseCommon(
      WrappedWebContext webContext,
      HiveSamlRelayStateCacheBase<T> store,
      AuthnResponseHandler<T> handler) {
    String nameId;
    String relayState;
    T relayStateInfo;

    try {
      relayState = store.getRelayStateInfo(webContext);
      relayStateInfo = store.getRelayStateInfo(relayState);
    } catch (HttpSamlAuthenticationException e) {
      LOG.error("Invalid relay state", e);
      webContext.setResponseStatusCode(org.apache.http.HttpStatus.SC_UNAUTHORIZED);
      return;
    }

    try {
      nameId = validateAuthnResponseInner(webContext);
    } catch (HttpSamlAuthenticationException e) {
      if (e instanceof HttpSamlNoGroupsMatchedException) {
        LOG.error("Could not authenticate user since the groups didn't match", e);
      } else {
        LOG.error("SAML response could not be validated", e);
      }
      handler.handleFailure(webContext, relayStateInfo,
          "SAML assertion could not be validated. Check server logs for more details.");
      return;
    }

    handler.handleSuccess(webContext, nameId, relayState, relayStateInfo);
  }

  /**
   * Callback interface for handling successful or failed authentication responses.
   * Allows different implementations for HS2 vs WebServer response formatting.
   */
  protected interface AuthnResponseHandler<T extends HiveSamlRelayStateInfo> {
    void handleSuccess(WrappedWebContext webContext, String nameId,
        String relayState, T relayStateInfo);
    void handleFailure(WrappedWebContext webContext, T relayStateInfo, String errorMsg);
  }

  // Abstract methods to be implemented by subclasses

  public abstract void setRedirect(WrappedWebContext webContext) throws InternalException;

  public abstract void validateAuthnResponse(WrappedWebContext webContext)
      throws InternalException;
}
