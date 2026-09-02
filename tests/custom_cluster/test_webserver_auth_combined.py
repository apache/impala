# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import absolute_import, division, print_function
import base64
import os

from tests.custom_cluster.test_saml2_sso import TestSamlBase, NoRedirection

try:
  from urllib.request import build_opener, Request
  from urllib.error import HTTPError
except ImportError:
  from urllib2 import build_opener, Request, HTTPError


class TestWebserverFallthroughBase(TestSamlBase):
  """Base class with shared helpers for fallthrough auth tests."""

  def _assert_browser_fallthrough_to_saml(self, auth_failure_metric):
    """Verify that a browser request without Authorization header falls through to SAML2.

    Sends a request without credentials and asserts:
    - Response is a 302 redirect to the SAML2 IdP
    - The given auth failure metric was not incremented (auth was not attempted)

    Args:
      auth_failure_metric: webserver metric name to verify was not incremented,
          e.g. 'impala.webserver.total-basic-auth-failure'
    """
    opener = build_opener(NoRedirection)
    req = Request("http://localhost:25000/")
    response = opener.open(req)

    assert response.getcode() == 302, \
        "Browser without auth header should redirect to SAML2, not return 401"

    location = response.info()["location"]
    assert TestSamlBase.IDP_URL in location, \
        "Should redirect to SAML2 IdP, got: {}".format(location)

    relay_state, saml_req_xml = self._parse_redirection_response_common(response)
    request_id = self._parse_authn_request(saml_req_xml)
    assert request_id is not None, "Should get valid SAML AuthnRequest"

    failures = self.cluster.impalads[0].service.get_metric_value(auth_failure_metric)
    assert (failures is None or failures == 0), \
        "Auth should not be attempted when no Authorization header present, " \
        "metric={} got={}".format(auth_failure_metric, failures)

  def _assert_explicit_auth_no_fallthrough(
      self, auth_scheme, token_bytes, expected_body_fragment, auth_failure_metric):
    """Verify that an explicit Authorization header causes auth attempt, not SAML2
    redirect.

    Sends a request with an invalid Authorization header and asserts:
    - Response is 401, not a 302 redirect to the SAML2 IdP
    - Response body contains the expected auth failure message
    - The given auth failure metric was incremented

    Args:
      auth_scheme: Authorization scheme, e.g. 'Basic' or 'Negotiate'
      token_bytes: Raw bytes to base64-encode as the auth token
      expected_body_fragment: Substring expected in the 401 response body
      auth_failure_metric: webserver metric name expected to be incremented,
          e.g. 'impala.webserver.total-basic-auth-failure'
    """
    opener = build_opener(NoRedirection)
    req = Request("http://localhost:25000/")
    token = base64.b64encode(token_bytes).decode('ascii')
    req.add_header("Authorization", "{} {}".format(auth_scheme, token))

    try:
      response = opener.open(req)
      status_code = response.getcode()
      response_body = response.read().decode('utf-8')
    except HTTPError as e:
      status_code = e.code
      response_body = e.read().decode('utf-8')

    assert status_code == 401, \
        "Explicit {} auth failure should return 401, got {}".format(
            auth_scheme, status_code)
    assert expected_body_fragment in response_body, \
        "Should get {} auth failure message, not SAML2 redirect".format(auth_scheme)

    failures = self.cluster.impalads[0].service.get_metric_value(auth_failure_metric)
    assert failures >= 1, \
        "{} failure metric should increment for explicit {} auth, got {}".format(
            auth_failure_metric, auth_scheme, failures)


class TestWebserverLdapSaml2(TestWebserverFallthroughBase):
  """
  Tests for LDAP + SAML2 fallthrough on webserver.

  This test suite validates the fallthrough behavior when both LDAP
  and SAML2 are configured on the webserver:

  1. Browser (no Authorization header): Should fallthrough from LDAP to SAML2
  2. Explicit Basic auth (Authorization: Basic header): Should NOT fallthrough,
     respects client's explicit authentication method choice
  """

  # LDAP + SAML2 configuration
  # We point LDAP to a non-existent server - this is intentional to test fallthrough
  # logic without needing actual LDAP infrastructure
  LDAP_SAML_ARGS = (
      TestSamlBase.SSO_ARGS_COMMON
      + " --webserver_require_ldap"
      " --enable_ldap_auth"
      " --ldap_uri=ldap://nonexistent:389"
      " --ldap_passwords_in_clear_ok"
      " --webserver_ldap_passwords_in_clear_ok"
      " --webserver_saml2_idp_metadata=%s/saml2_sso_metadata.xml"
      " --webserver_saml2_sp_callback_url=http://localhost:25000/SAML2/SSO/POST") % (
          TestSamlBase.CERT_DIR)

  # Apply cluster configuration at class level - all tests share same cluster
  SHARED_CLUSTER_ARGS = {'impalad_args': LDAP_SAML_ARGS, 'cluster_size': 1}

  def test_browser_fallthrough_to_saml(self):
    """
    Test that a browser without Authorization header falls through from LDAP to SAML2.

    When a browser accesses the webserver without credentials:
    - LDAP auth is skipped (no Authorization header = no explicit client choice)
    - Request falls through to SAML2
    - Client gets 302 redirect to IdP
    """
    self._assert_browser_fallthrough_to_saml(
        "impala.webserver.total-basic-auth-failure")

  def test_explicit_basic_auth_no_fallthrough(self):
    """
    Test that explicit Basic auth does NOT fallthrough to SAML2.

    When a client explicitly sends Authorization: Basic header:
    - LDAP auth is attempted (client made explicit choice)
    - On failure, returns 401 (no fallthrough to SAML2)
    - Respects client's authentication method choice
    """
    self._assert_explicit_auth_no_fallthrough(
        "Basic", b"baduser:badpass",
        "Must authenticate with Basic authentication",
        "impala.webserver.total-basic-auth-failure")

  def test_complete_saml_flow_after_fallthrough(self):
    """
    Test complete SAML2 workflow after LDAP fallthrough.

    This validates that after falling through from LDAP:
    1. Client gets redirect to IdP
    2. Client can complete SAML2 authentication
    3. Client receives authentication cookie
    """
    # Step 1: Initial request without auth header -> redirect to IdP
    relay_state, request_id = self._ws_request_resource(25000)

    # Step 2: Complete SAML2 authentication flow
    initial_success = self.cluster.impalads[0].service.get_metric_value(
        TestSamlBase.WS_SAML_SUCCESS_METRIC)
    initial_success = 0 if initial_success is None else initial_success

    attributes_xml = TestSamlBase.ATTRIBUTE_STATEMENT.format(group_name="group1")
    self._ws_send_authn_response(
        "http://localhost:25000/SAML2/SSO/POST",
        request_id, relay_state, attributes_xml, expect_success=True)

    # Step 3: Verify SAML2 authentication succeeded
    final_success = self.cluster.impalads[0].service.get_metric_value(
        TestSamlBase.WS_SAML_SUCCESS_METRIC)
    assert final_success == initial_success + 1, \
        "SAML success metric should increment after successful auth"


class TestWebserverSpnegoSaml2(TestWebserverFallthroughBase):
  """
  Tests for SPNEGO + SAML2 fallthrough on webserver.

  This test suite validates the fallthrough behavior when both SPNEGO
  and SAML2 are configured on the webserver:

  1. Browser (no Authorization header): Should fallthrough from SPNEGO to SAML2
  2. Explicit Negotiate auth (Authorization: Negotiate header): Should NOT fallthrough,
     respects client's explicit authentication method choice
  """

  # Dummy keytab file path - use fixed temp location for decorator
  KEYTAB_FILE = "/tmp/impala_test_spnego_dummy.keytab"

  # SPNEGO + SAML2 configuration
  # We use a dummy keytab file - this is intentional to test fallthrough
  # logic without requiring real Kerberos infrastructure
  SPNEGO_SAML_ARGS = (
      TestSamlBase.SSO_ARGS_COMMON
      + " --webserver_require_spnego"
      " --spnego_keytab_file=%s"
      " --webserver_saml2_idp_metadata=%s/saml2_sso_metadata.xml"
      " --webserver_saml2_sp_callback_url=http://localhost:25000/SAML2/SSO/POST") % (
          KEYTAB_FILE, TestSamlBase.CERT_DIR)

  # Apply cluster configuration at class level - all tests share same cluster
  SHARED_CLUSTER_ARGS = {'impalad_args': SPNEGO_SAML_ARGS, 'cluster_size': 1}

  @classmethod
  def setup_class(cls):
    # Create the dummy keytab file - SPNEGO startup requires the file to exist
    # The file doesn't need to be a valid keytab; authentication will fail as expected
    with open(cls.KEYTAB_FILE, 'wb') as f:
      f.write(b'')  # Empty file is sufficient
    super(TestWebserverSpnegoSaml2, cls).setup_class()

  @classmethod
  def teardown_class(cls):
    super(TestWebserverSpnegoSaml2, cls).teardown_class()
    # Clean up the dummy keytab file
    if os.path.exists(cls.KEYTAB_FILE):
      os.remove(cls.KEYTAB_FILE)

  def test_browser_fallthrough_to_saml(self):
    """
    Test that a browser without Authorization header falls through from SPNEGO to SAML2.

    When a browser accesses the webserver without credentials:
    - SPNEGO auth is skipped (no Authorization header = no explicit client choice)
    - Request falls through to SAML2
    - Client gets 302 redirect to IdP
    """
    self._assert_browser_fallthrough_to_saml(
        "impala.webserver.total-negotiate-auth-failure")

  def test_explicit_negotiate_auth_no_fallthrough(self):
    """
    Test that explicit Negotiate auth does NOT fallthrough to SAML2.

    When a client explicitly sends Authorization: Negotiate header:
    - SPNEGO auth is attempted (client made explicit choice)
    - On failure, returns 401 (no fallthrough to SAML2)
    - Respects client's authentication method choice
    """
    self._assert_explicit_auth_no_fallthrough(
        "Negotiate", b"invalid_spnego_token",
        "Not authorized",
        "impala.webserver.total-negotiate-auth-failure")

  def test_complete_saml_flow_after_fallthrough(self):
    """
    Test complete SAML2 workflow after SPNEGO fallthrough.

    This validates that after falling through from SPNEGO:
    1. Client gets redirect to IdP
    2. Client can complete SAML2 authentication
    3. Client receives authentication cookie
    """
    # Step 1: Initial request without auth header -> redirect to IdP
    relay_state, request_id = self._ws_request_resource(25000)

    # Step 2: Complete SAML2 authentication flow
    initial_success = self.cluster.impalads[0].service.get_metric_value(
        TestSamlBase.WS_SAML_SUCCESS_METRIC)
    initial_success = 0 if initial_success is None else initial_success

    attributes_xml = TestSamlBase.ATTRIBUTE_STATEMENT.format(group_name="group1")
    self._ws_send_authn_response(
        "http://localhost:25000/SAML2/SSO/POST",
        request_id, relay_state, attributes_xml, expect_success=True)

    # Step 3: Verify SAML2 authentication succeeded
    final_success = self.cluster.impalads[0].service.get_metric_value(
        TestSamlBase.WS_SAML_SUCCESS_METRIC)
    assert final_success == initial_success + 1, \
        "SAML success metric should increment after successful auth"
