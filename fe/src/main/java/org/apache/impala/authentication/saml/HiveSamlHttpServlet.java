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

import com.google.common.base.Preconditions;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveSamlHttpServlet {

  private static final Logger LOG = LoggerFactory
      .getLogger(HiveSamlHttpServlet.class);
  private final AuthTokenGenerator tokenGenerator;
  ImpalaSamlClient samlClient;

  public HiveSamlHttpServlet(ImpalaSamlClient samlClient) {
    this.samlClient = samlClient;
    tokenGenerator = HiveSamlAuthTokenGenerator.get();
  }

  public void doPost(WrappedWebContext webContext) {
    String nameId;
    String relayState;
    int port;
    try {
      relayState = HiveSamlRelayStateStore.get().getRelayStateInfo(webContext);
      port = HiveSamlRelayStateStore.get().getRelayStateInfo(relayState).getPort();
    } catch (HttpSamlAuthenticationException e) {
      LOG.error("Invalid relay state" ,e);
      webContext.setResponseStatusCode(HttpStatus.SC_UNAUTHORIZED);
      return;
    }
    try {
      LOG.debug("RelayState port is " + port);
      nameId = samlClient.validateAuthnResponseInner(webContext);
    } catch (HttpSamlAuthenticationException e) {
      if (e instanceof HttpSamlNoGroupsMatchedException) {
        LOG.error("Could not authenticate user since the groups didn't match", e);
      } else {
        LOG.error("SAML response could not be validated", e);
      }
      generateFormData(webContext, "http://127.0.0.1:" + port, null, false,
          "SAML assertion could not be validated. Check server logs for more details.");
      return;
    }
    Preconditions.checkState(nameId != null);
    LOG.debug(
        "Successfully validated saml response. Forwarding the token to port " + port);
    generateFormData(webContext, "http://127.0.0.1:" + port,
        tokenGenerator.get(nameId, relayState), true, "");
  }

  private void generateFormData(WrappedWebContext webContext, String url, String token,
      boolean sucess, String msg) {
    StringBuilder sb = new StringBuilder();
    sb.append("<html>");
    sb.append("<body onload='document.forms[\"form\"].submit()'>");
    sb.append(String.format("<form name='form' action='%s' method='POST'>", url));
    sb.append(String
        .format("<input type='hidden' name='%s' value='%s'/>", HiveSamlUtils.TOKEN_KEY,
            token));
    sb.append(String.format("<input type='hidden' name='%s' value='%s'/>",
        HiveSamlUtils.STATUS_KEY, sucess));
    sb.append(String
        .format("<input type='hidden' name='%s' value='%s'/>", HiveSamlUtils.MESSAGE_KEY,
            msg));
    sb.append("</form>");
    sb.append("</body>");
    sb.append("</html>");
    webContext.setResponseContent("text/html;charset=utf-8", sb.toString());
  }
}
