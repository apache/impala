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
#

from __future__ import absolute_import, division, print_function
import base64
import datetime
import os
import pytest
import uuid
import xml.etree.ElementTree as ET
import zlib

try:
  from urllib.parse import parse_qs, urlparse
  from urllib.request import HTTPErrorProcessor, build_opener, Request
except ImportError:
  from urllib2 import HTTPErrorProcessor, build_opener, Request
  from urlparse import parse_qs, urlparse

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.test_vector import ImpalaTestVector
from tests.common.test_dimensions import create_client_protocol_dimension
from tests.shell.util import run_impala_shell_cmd


class NoRedirection(HTTPErrorProcessor):
  """Allows inspecting http redirection responses. """
  def http_response(self, request, response):
    return response


def format_time(time):
  """ Converts datetimes to the format expected in SAML2 XMLs. """
  return time.strftime("%Y-%m-%dT%H:%M:%SZ")


class TestClientSaml(CustomClusterTestSuite):
  """ Tests for a client using SAML2 browser profile.

      Most tests simulate the SAML2 browser profile workflow by sending 3 http requests
      to Impala's hs2-http port:
      1. a POST request with header X-Hive-Token-Response-Port set
         - Normally the client should listen on the port above, but this is not needed
           during the tests
         - Impala responds with a redirection to the SSO service ('IDP_URL') with
           encoded SAMLRequest and RelayState parameters.
         - SAML2 browser profile is designed for web pages that are already opened in the
           browser, which would do the redirection automatically. As in our case the
           workflow is expected to be executed from a client like JDBC, the client should
           normally open a browser tab with the the location in the redirection. During
           the tests the browser is not involved.
         - Implemented by _request_resource() in the tests.
      2. a POST request with the same path as configured in flag saml2_sp_callback_url
         containing an encoded AuthNResponse and the RelayState as content
         - Normally this comes from the browser.
         - Impala validates the AuthNResponse end responds with an HTML form that submits
           to localhost:{X-Hive-Token-Response-Port} and contains a bearer token.
         - Implemented by _send_authn_response() in the tests.
      3. a POST request with the bearer token as auth header
         - Impala validates the token and returns an auth cookie.
         - Implemented by _request_resource_with_bearer() in the tests.

      After getting the auth cookie the client should send "normal" hs2-http Thrift
      requests, but this is not included in the tests as currently there is no client
      that supports SAML auth. IMPALA-10496 tracks adding SAML support to Impyla.
  """

  CERT_DIR = "%s/testdata/authentication" % os.environ['IMPALA_HOME']
  SP_CALLBACK_URL = "http://localhost:28000/SAML2/SSO/POST"
  IDP_URL = "https://localhost:8443/simplesaml/saml2/idp/SSOService.php"
  CLIENT_PORT = 12345
  HOST_PORT = pytest.config.option.impalad_hs2_http_port
  ASSERTATION_ERROR_MESSAGE = \
      "SAML assertion could not be validated. Check server logs for more details."

  SSO_ARGS = ("--saml2_keystore_path=%s/saml2_sso.jks "
              "--saml2_keystore_password_cmd=\"echo -n storepass\" "
              "--saml2_private_key_password_cmd=\"echo -n keypass\" "
              "--saml2_idp_metadata=%s/saml2_sso_metadata.xml "
              "--saml2_sp_callback_url=%s "
              "--saml2_sp_entity_id=org.apache.impala "
              "--saml2_want_assertations_signed=false "
              "--saml2_allow_without_tls_debug_only=true "
              "--cookie_require_secure=false "
              "--saml2_ee_test_mode=true"
              % (CERT_DIR, CERT_DIR, SP_CALLBACK_URL))

  SSO_ARGS_WITH_GROUP_FILTER = (SSO_ARGS + " " +
                                "--saml2_group_filter=group1,group2 "
                                "--saml2_group_attribute_name=eduPersonAffiliation")

  @CustomClusterTestSuite.with_args(impalad_args=SSO_ARGS, cluster_size=1)
  def test_saml2_browser_profile_no_group_filter(self, vector):
    # Iterate over test vector within test function to avoid restarting cluster.
    for vector in\
        [ImpalaTestVector([value]) for value in create_client_protocol_dimension()]:
      protocol = vector.get_value("protocol")
      if protocol != "hs2-http":
        # SAML2 should not affect non http protocols.
        args = ["--protocol=%s" % protocol, "-q", "select 1 + 2"]
        run_impala_shell_cmd(vector, args, expect_success=True)
        continue

      # hs2-http connections without further arguments should be rejected.
      args = ["--protocol=hs2-http", "-q", "select 1 + 2"]
      run_impala_shell_cmd(vector, args, expect_success=False)

      # test the SAML worflow with different attributes
      self._test_saml2_browser_workflow("", True)

      attributes_xml = TestClientSaml.ATTRIBUTE_STATEMENT.format(group_name="group1")
      self._test_saml2_browser_workflow(attributes_xml, True)

      attributes_xml = TestClientSaml.ATTRIBUTE_STATEMENT.format(group_name="bad_group")
      self._test_saml2_browser_workflow(attributes_xml, True)

  @CustomClusterTestSuite.with_args(
      impalad_args=SSO_ARGS_WITH_GROUP_FILTER, cluster_size=1)
  def test_saml2_browser_profile_with_group_filter(self, vector):
      # test the SAML worflow with different attributes
      self._test_saml2_browser_workflow("", False)

      attributes_xml = TestClientSaml.ATTRIBUTE_STATEMENT.format(group_name="group1")
      self._test_saml2_browser_workflow(attributes_xml, True)

      attributes_xml = TestClientSaml.ATTRIBUTE_STATEMENT.format(group_name="bad_group")
      self._test_saml2_browser_workflow(attributes_xml, False)

  def _test_saml2_browser_workflow(self, attributes_xml, expect_success):
    """ Sends the 3 SAML releated requests to Impala and parses/validates
        their response.
        'attributes_xml': contains the attributes part of authn response
        'expect_success': if false, then the workflow is expected to fail when
                          Impala validates the assertations in authn response """
    relay_state, client_id, request_id = self._request_resource()
    bearer_token = self._send_authn_response(request_id, relay_state,
                                             attributes_xml, expect_success)
    if not expect_success: return
    self._request_resource_with_bearer(client_id, bearer_token)

  def _request_resource(self):
    """ Initial POST request to hs2-http port, response should be redirected
        to IDP and contain the authnrequest. """
    opener = build_opener(NoRedirection)
    req = Request("http://localhost:%s" % TestClientSaml.HOST_PORT, " ")
    req.add_header('X-Hive-Token-Response-Port', TestClientSaml.CLIENT_PORT)
    response = opener.open(req)
    relay_state, client_id, saml_req_xml = \
        self._parse_redirection_response(response)
    request_id = self._parse_authn_request(saml_req_xml)
    return relay_state, client_id, request_id

  def _parse_redirection_response(self, response):
    assert response.getcode() == 302
    client_id = response.info().get("X-Hive-Client-Identifier", None)
    assert client_id is not None
    new_url = response.info()["location"]
    assert new_url.startswith(TestClientSaml.IDP_URL)
    query = parse_qs(urlparse(new_url).query.encode('ASCII'))
    relay_state = query["RelayState"][0]
    assert relay_state is not None
    saml_req = query["SAMLRequest"][0]
    assert saml_req is not None
    saml_req_xml = zlib.decompress(base64.urlsafe_b64decode(saml_req), -15)
    return relay_state, client_id, saml_req_xml

  def _parse_authn_request(self, saml_req_xml):
    root = ET.fromstring(saml_req_xml)
    assert root.tag == "{urn:oasis:names:tc:SAML:2.0:protocol}AuthnRequest"
    return root.attrib["ID"]

  def _request_resource_with_bearer(self, client_id, bearer_token):
    """ Send POST request to hs2-http port again, this time with bearer tokan.
        The response should contain a security cookie if the validation succeeded """
    req = Request("http://localhost:%s" % TestClientSaml.HOST_PORT, " ")
    req.add_header('X-Hive-Client-Identifier', client_id)
    req.add_header('Authorization', "Bearer " + bearer_token)
    opener = build_opener(NoRedirection)
    response = opener.open(req)
    # saml2_ee_test_mode=true leads to returning 401 unauthorized - otherwise the
    # call would hang if there is no Thrift message.
    assert response.getcode() == 401
    cookies = response.info()['Set-Cookie']
    assert cookies.startswith("impala.auth=")

  def _send_authn_response(self, request_id, relay_state,
                           attributes_xml, expect_success):
    """ Send an authnresponse to Impala - normally the IDP would do this, but in
        this test we generate it from an xml template.
        Impala should answer with a form that submits to CLIENT_PORT and contains
        the bearer token as a hidden state. """
    authn_resp = self._generate_authn_response(request_id, attributes_xml)
    encoded_authn_resp = base64.urlsafe_b64encode(authn_resp)
    body = "SAMLResponse=%s&RelayState=%s" % (encoded_authn_resp, relay_state)
    opener = build_opener(NoRedirection)
    req = Request(TestClientSaml.SP_CALLBACK_URL, body)
    response = opener.open(req)
    bearer_token = self._parse_xhtml_form(response, expect_success)
    return bearer_token

  @staticmethod
  def _generate_authn_response(request_id, attributes_xml):
    now = datetime.datetime.utcnow()
    expire_at = now + datetime.timedelta(hours=2)
    schema = TestClientSaml.AUTHN_RESPONSE_SCHEMA
    return schema.format(request_id=request_id,
                         not_on_or_after=format_time(expire_at),
                         not_before=format_time(now),
                         issue_instant=format_time(now),
                         attribute_statement=attributes_xml,
                         msg_id=str(uuid.uuid4()),
                         assertation_id=str(uuid.uuid4()))

  @staticmethod
  def _parse_xhtml_form(response, expect_success):
    assert response.getcode() == 200
    content = response.read()
    root = ET.fromstring(content)
    assert root.tag == "html"
    assert root[0].tag == "body"
    assert root[0][0].tag == "form"
    token = ""
    message = ""
    for input in root[0][0]:
      assert input.tag == "input"
      if input.attrib["name"] == "token":
        token = input.attrib["value"]
      elif input.attrib["name"] == "message":
        message = input.attrib["value"]

    if expect_success:
      assert token.startswith("u=user1")
    else:
      assert message == TestClientSaml.ASSERTATION_ERROR_MESSAGE
    return token

  # A sample authn response (generated by simplesaml php) with placeholders for fields
  # that matter for the tests. Moved to the bottom of the class to be less obstructive.
  AUTHN_RESPONSE_SCHEMA = """
    <samlp:Response
        xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol"
        xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion"
        ID="{msg_id}"
        Version="2.0" IssueInstant="{issue_instant}"
        Destination="http://localhost:28000/SAML2/SSO/POST?client_name=ImpalaSamlClient"
        InResponseTo="{request_id}">
      <saml:Issuer>
        https://localhost:8443/simplesaml/saml2/idp/metadata.php
      </saml:Issuer>
      <samlp:Status>
        <samlp:StatusCode Value="urn:oasis:names:tc:SAML:2.0:status:Success"/>
      </samlp:Status>
      <saml:Assertion
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xmlns:xs="http://www.w3.org/2001/XMLSchema"
          ID="{assertation_id}"
          Version="2.0"
          IssueInstant="{issue_instant}">
        <saml:Issuer>
            https://localhost:8443/simplesaml/saml2/idp/metadata.php
        </saml:Issuer>
        <saml:Subject>
          <saml:NameID
              SPNameQualifier="org.apache.impala"
              Format="urn:oasis:names:tc:SAML:2.0:nameid-format:transient">
            user1
          </saml:NameID>
          <saml:SubjectConfirmation Method="urn:oasis:names:tc:SAML:2.0:cm:bearer">
          <saml:SubjectConfirmationData
              NotOnOrAfter="{not_on_or_after}"
              Recipient="http://localhost:28000/SAML2/SSO/POST?client_name=ImpalaSamlClient"
              InResponseTo="{request_id}"/>
          </saml:SubjectConfirmation>
        </saml:Subject>
        <saml:Conditions NotBefore="{not_before}" NotOnOrAfter="{not_on_or_after}">
          <saml:AudienceRestriction>
            <saml:Audience>org.apache.impala</saml:Audience>
          </saml:AudienceRestriction>
        </saml:Conditions>
        <saml:AuthnStatement
            AuthnInstant="{issue_instant}"
            SessionNotOnOrAfter="{not_on_or_after}"
            SessionIndex="_b7ac1881122cb9e24fbdef7ed40c0aafeeef1313cf">
          <saml:AuthnContext>
            <saml:AuthnContextClassRef>urn:oasis:names:tc:SAML:2.0:ac:classes:Password</saml:AuthnContextClassRef>
          </saml:AuthnContext>
        </saml:AuthnStatement>
        {attribute_statement}
      </saml:Assertion>
    </samlp:Response>"""

  # A sample 'AttributeStatement' part of authn response (generated by simplesaml php).
  ATTRIBUTE_STATEMENT = """
    <saml:AttributeStatement>
      <saml:Attribute Name="uid" NameFormat="urn:oasis:names:tc:SAML:2.0:attrname-format:basic">
        <saml:AttributeValue xsi:type="xs:string">1</saml:AttributeValue>
      </saml:Attribute>
      <saml:Attribute Name="eduPersonAffiliation" NameFormat="urn:oasis:names:tc:SAML:2.0:attrname-format:basic">
        <saml:AttributeValue xsi:type="xs:string">{group_name}</saml:AttributeValue>
      </saml:Attribute>
      <saml:Attribute Name="email" NameFormat="urn:oasis:names:tc:SAML:2.0:attrname-format:basic">
        <saml:AttributeValue xsi:type="xs:string">user1@example.com</saml:AttributeValue>
      </saml:Attribute>
    </saml:AttributeStatement>"""
