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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.http.HttpStatus;
import org.apache.impala.common.InternalException;
import org.apache.impala.service.BackendConfig;
import org.opensaml.saml.common.xml.SAMLConstants;
import org.pac4j.core.credentials.TokenCredentials;
import org.pac4j.core.credentials.extractor.BearerAuthExtractor;
import org.pac4j.core.exception.http.RedirectionAction;
import org.pac4j.core.exception.http.WithLocationAction;
import org.pac4j.saml.client.SAML2Client;
import org.pac4j.saml.config.SAML2Configuration;
import org.pac4j.core.exception.http.RedirectionActionHelper;
import org.pac4j.saml.credentials.SAML2Credentials;
import org.pac4j.saml.credentials.SAML2Credentials.SAMLAttribute;
import org.pac4j.saml.credentials.extractor.SAML2CredentialsExtractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
// modified version of https://github.com/vihangk1/hive/blob/master_saml/service/src/java/org/apache/hive/service/auth/saml/HiveSaml2Client.java

/**
 * HiveServer2's implementation of SAML2Client. We mostly rely on pac4j to do most of the
 * heavy lifting. This class implements the initialization logic of the underlying {@link
 * SAML2Client} using the HiveConf. Also, implements the generation of SAML requests using
 * HTTP-Redirect binding. //TODO: Add support for HTTP-Post binding for SAML request.
 */
public class ImpalaSamlClient extends SAML2Client {

  private static final Logger LOG = LoggerFactory.getLogger(ImpalaSamlClient.class);
  private static ImpalaSamlClient INSTANCE;
  private final HiveSamlGroupNameFilter groupNameFilter;
  private final HiveSamlHttpServlet samlHttpServlet;

  private ImpalaSamlClient() throws Exception {
    super(getSamlConfig());
    // setUseModernHttpCodes(false) is needed to return 302 instead of 303 for
    // redirect actions on POST requests.
    RedirectionActionHelper.setUseModernHttpCodes(false);
    setCallbackUrl(getCallBackUrl());
    setName(ImpalaSamlClient.class.getSimpleName());
    setStateGenerator(HiveSamlRelayStateStore.get());
    groupNameFilter = new HiveSamlGroupNameFilter();
    samlHttpServlet = new HiveSamlHttpServlet(this);
    init();
    //TODO handle the replayCache as described in http://www.pac4j.org/docs/clients/saml.html
  }

  private static String getCallBackUrl() throws Exception {
    // TODO: validate the URL, e.g. it must use the hs2-http port
    BackendConfig conf = BackendConfig.INSTANCE;
    return conf.getSaml2SpCallbackUrl();
  }

  public static synchronized ImpalaSamlClient get()
      throws InternalException {
    if (INSTANCE != null) {
      return INSTANCE;
    }
    try {
      INSTANCE = new ImpalaSamlClient();
    } catch (Exception e) {
      throw new InternalException("Could not instantiate SAML2.0 client", e);
    }
    return INSTANCE;
  }

  /**
   * Extracts the SAML specific configuration needed to initialize the SAML2.0 client.
   */
  private static SAML2Configuration getSamlConfig() throws Exception {
    BackendConfig conf = BackendConfig.INSTANCE;
    LOG.info("keystore path: " + conf.getSaml2KeystorePath());
    SAML2Configuration saml2Configuration = new SAML2Configuration(
       conf.getSaml2KeystorePath(),
       conf.getSaml2KeystorePassword(),
       conf.getSaml2PrivateKeyPassword(),
       conf.getSaml2IdpMetadata());

    saml2Configuration
        .setAuthnRequestBindingType(SAMLConstants.SAML2_REDIRECT_BINDING_URI);
    saml2Configuration.setResponseBindingType(SAML2_POST_BINDING_URI);
    saml2Configuration.setServiceProviderEntityId(conf.getSaml2SpEntityId());
    saml2Configuration.setWantsAssertionsSigned(conf.getSaml2WantAsserationsSigned());
    saml2Configuration.setAuthnRequestSigned(conf.getSaml2SignRequest());
    saml2Configuration.setAllSignatureValidationDisabled(conf.getSaml2EETestMode());
    return saml2Configuration;
  }

  /**
   * Generates a SAML request using the HTTP-Redirect Binding.
   */
  public void setRedirect(WrappedWebContext webContext)
      throws InternalException {
    Optional<String> responsePort =
        webContext.getRequestHeader(HiveSamlUtils.SSO_TOKEN_RESPONSE_PORT);
    if (responsePort == null || !responsePort.isPresent()) {
      throw new InternalException("No response port specified");
    }
    LOG.debug("Request has response port set as {}", responsePort);
    Optional<RedirectionAction>  redirect = getRedirectionAction(webContext);
    if (redirect == null || !redirect.isPresent()) {
      throw new InternalException("Could not get the redirect response");
    }
    webContext.setResponseStatusCode(redirect.get().getCode());
    WithLocationAction locationAction = (WithLocationAction) redirect.get();
    webContext.setResponseHeader("Location", locationAction.getLocation());;
  }

  /**
   * Given a response which may contain a SAML Assertion, validates it. If the validation
   * is successful, it extracts the nameId from the assertion which is used as the
   * identity of the end user.
   *
   * @param request
   * @param response
   * @return the NameId as received in the assertion if the assertion was valid.
   * @throws HttpSamlAuthenticationException In case the assertition is not present or is
   *                                         invalid.
   */
  public void validateAuthnResponse(WrappedWebContext webContext)
      throws InternalException {
    // Bit weird logic - doPost calls back to validateAuthnRequestInner.
    // This is done to keep original structure by Vihang + keep ImpalaSamlClient as the only
    // class other parts of Impala should know about.
    samlHttpServlet.doPost(webContext);
    webContext.setResponseStatusCode(HttpStatus.SC_OK);
  }

  public String validateAuthnResponseInner(WrappedWebContext webContext)
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

  public String validateBearer(WrappedWebContext webContext) throws InternalException {
    LOG.info(webContext.getRequestAsJsonString());
    try {
      return doSamlAuth(webContext);
    } catch (HttpSamlAuthenticationException ex) {
      throw new InternalException("SAML2 bearer validation failed", ex);
    }
  }

  // copied from
  // https://github.com/vihangk1/hive/blob/master_saml/service/src/java/org/apache/hive/service/cli/thrift/ThriftHttpServlet.java
  private String doSamlAuth(WrappedWebContext webContext) throws HttpSamlAuthenticationException {
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
      if (!HiveSamlRelayStateStore.get()
          .validateClientIdentifier(relayStateKey, clientIdentifier.get())) {
        throw new HttpSamlAuthenticationException(
            "Code verifier could not be validated");
      }
    }
    return user;
  }
}
