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
import json
import logging
import os
import pytest
import requests
import sys

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.network import SKIP_SSL_MSG
from tests.common.test_dimensions import create_client_protocol_dimension
from tests.shell.util import run_impala_shell_cmd, \
    create_impala_shell_executable_dimension
from tests.common.impala_connection import create_connection

LOG = logging.getLogger('impala_test_suite')

CERT_DIR = "%s/be/src/testutil" % os.environ['IMPALA_HOME']

# Use wildcard san cert to be flexible about host name.
SSL_WILDCARD_SAN_ARGS = ("--ssl_client_ca_certificate={0}/wildcardCA.pem "
                         "--ssl_server_certificate={0}/wildcard-san-cert.pem "
                         "--ssl_private_key={0}/wildcard-san-cert.key "
                         "--hostname={1} "
                         "--state_store_host={1} "
                         "--catalog_service_host={1} "
                         "--webserver_certificate_file={0}/wildcard-san-cert.pem "
                         "--webserver_private_key_file={0}/wildcard-san-cert.key "
                         ).format(CERT_DIR, "ip4.impala.test")

WILDCARD_CA_CERT_PATH = "%s/wildcardCA.pem" % CERT_DIR

IPV6_ONLY_IP_WEBSERBER_ARG = "--webserver_interface=::1 "
IPV6_DUAL_IP_WEBSERBER_ARG = "--webserver_interface=:: "
IPV6_ONLY_IP_QUERY_ARG = "--external_interface=::1 "
IPV6_DUAL_IP_QUERY_ARG = "--external_interface=:: "
IPV6_ONLY_IP_COORDINATOR_ARG = \
    "%s %s" % (IPV6_ONLY_IP_WEBSERBER_ARG, IPV6_ONLY_IP_QUERY_ARG)
IPV6_DUAL_IP_COORDINATOR_ARG = \
    "%s %s" % (IPV6_DUAL_IP_WEBSERBER_ARG, IPV6_DUAL_IP_QUERY_ARG)

IPV6_ONLY_HOSTNAME_WEBSERBER_ARG = "--webserver_interface=ip6.impala.test "
IPV6_DUAL_HOSTNAME_WEBSERBER_ARG = "--webserver_interface=ip46.impala.test "
IPV6_ONLY_HOSTNAME_QUERY_ARG = "--external_interface=::1 "
IPV6_DUAL_HOSTNAME_QUERY_ARG = "--external_interface=:: "

WEBUI_PORTS = [25000, 25010, 25020]

# Error text can depend on both protocol and python version.
CONN_ERR = ["Could not connect", "Connection refused"]
# Due to THRIFT-792, SSL errors are suppressed when using OpenSSL hostname verification.
# This is the only option on Python 3.12+, using ssl.PROTOCOL_TLS_CLIENT.
CERT_ERR = ["doesn't match", "certificate verify failed", "Could not connect"]
WEB_CERT_ERR = ("CertificateError" if sys.version_info.major < 3
                else "SSLCertVerificationError")


class TestIPv6Base(CustomClusterTestSuite):
  ca_cert = None

  @classmethod
  def setup_class(cls):
    super(TestIPv6Base, cls).setup_class()

  @classmethod
  def add_test_dimensions(cls):
    super(TestIPv6Base, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_client_protocol_dimension())
    cls.ImpalaTestMatrix.add_dimension(create_impala_shell_executable_dimension())

  def _smoke(self, host, vector, expected_errors=[]):
    proto = vector.get_value('protocol')
    try:
      port = self._get_default_port(proto)
      host_port = "%s:%d" % (host, port)
      use_ssl = self.ca_cert is not None
      with create_connection(host_port, protocol=proto, use_ssl=use_ssl) as conn:
        conn.connect()
        assert not expected_errors
        res = conn.execute("select 1")
        assert res.data == ["1"]
    except Exception as ex:
      for err in expected_errors:
        if err in str(ex): return
      raise ex

  def _webui_smoke(self, url, err=None):
    """Tests to check glibc version and locale is available"""
    try:
      if self.ca_cert:
        other_info_page = requests.get(url + "/?json", verify=self.ca_cert).text
      else:
        other_info_page = requests.get(url + "/?json", verify=False).text
      assert err is None
      other_info = json.loads(other_info_page)
      assert "glibc_version" in other_info
    except Exception as ex:
      if not err: raise ex
      assert err in str(ex)

  def _shell_smoke(self, host, vector, expected_errors=[]):
    proto = vector.get_value('protocol')
    port = self._get_default_port(proto)
    host_port = "%s:%d" % (host, port)
    try:
      shell_options = ["-i", host_port, "-q", "select 1"]
      if self.ca_cert:
        shell_options.append("--ssl")
        shell_options.append("--ca_cert=" + self.ca_cert)
      result = run_impala_shell_cmd(vector, shell_options)
      assert not expected_errors
      assert "Fetched 1 row" in result.stderr
    except Exception as ex:
      for err in expected_errors:
        if err in str(ex): return
      raise ex


@CustomClusterTestSuite.with_args(impalad_args=IPV6_DUAL_IP_WEBSERBER_ARG
                                               + IPV6_DUAL_IP_QUERY_ARG,
                                  statestored_args=IPV6_DUAL_IP_WEBSERBER_ARG,
                                  catalogd_args=IPV6_DUAL_IP_WEBSERBER_ARG)
class TestIPv6DualNoSsl(TestIPv6Base):

  def test_ipv6_dual_no_ssl(self, vector):
    for port in WEBUI_PORTS:
      self._webui_smoke("http://127.0.0.1:%d" % port)
      self._webui_smoke("http://[::1]:%d" % port)
      self._webui_smoke("http://ip4.impala.test:%d" % port)
      self._webui_smoke("http://ip6.impala.test:%d" % port)
      self._webui_smoke("http://ip46.impala.test:%d" % port)

    self._smoke("[::1]", vector)
    self._smoke("127.0.0.1", vector)
    self._smoke("ip4.impala.test", vector)
    self._smoke("ip6.impala.test", vector)
    self._smoke("ip46.impala.test", vector)

    self._shell_smoke("[::1]", vector)
    self._shell_smoke("127.0.0.1", vector)
    self._shell_smoke("ip4.impala.test", vector)
    self._shell_smoke("ip6.impala.test", vector)
    self._shell_smoke("ip46.impala.test", vector)


@CustomClusterTestSuite.with_args(impalad_args=IPV6_ONLY_IP_WEBSERBER_ARG
                                               + IPV6_ONLY_IP_QUERY_ARG,
                                  statestored_args=IPV6_ONLY_IP_WEBSERBER_ARG,
                                  catalogd_args=IPV6_ONLY_IP_WEBSERBER_ARG)
class TestIPv6OnlyNoSsl(TestIPv6Base):

  def test_ipv6_only_no_ssl(self, vector):
    self.check_connections()
    for port in WEBUI_PORTS:
      self._webui_smoke("http://127.0.0.1:%d" % port, err="Connection refused")
      self._webui_smoke("http://[::1]:%d" % port)
      self._webui_smoke("http://ip4.impala.test:%d" % port, err="Connection refused")
      self._webui_smoke("http://ip6.impala.test:%d" % port)
      self._webui_smoke("http://ip46.impala.test:%d" % port)

    self._smoke("[::1]", vector)
    self._smoke("127.0.0.1", vector, CONN_ERR)
    self._smoke("ip4.impala.test", vector, CONN_ERR)
    self._smoke("ip6.impala.test", vector)
    self._smoke("ip46.impala.test", vector)

    self._shell_smoke("[::1]", vector)
    self._shell_smoke("127.0.0.1", vector, CONN_ERR)
    self._shell_smoke("ip4.impala.test", vector, CONN_ERR)
    self._shell_smoke("ip6.impala.test", vector)
    self._shell_smoke("ip46.impala.test", vector)


@CustomClusterTestSuite.with_args(impalad_args=IPV6_DUAL_HOSTNAME_WEBSERBER_ARG
                                               + IPV6_DUAL_HOSTNAME_QUERY_ARG
                                               + SSL_WILDCARD_SAN_ARGS,
                                  statestored_args=IPV6_DUAL_HOSTNAME_WEBSERBER_ARG
                                                  + SSL_WILDCARD_SAN_ARGS,
                                  catalogd_args=IPV6_DUAL_HOSTNAME_WEBSERBER_ARG
                                                + SSL_WILDCARD_SAN_ARGS)
class TestIPv6DualSsl(TestIPv6Base):
  ca_cert = WILDCARD_CA_CERT_PATH

  @pytest.mark.skipif(SKIP_SSL_MSG != "", reason=SKIP_SSL_MSG)
  def test_ipv6_dual_ssl(self, vector):
    self.check_connections()
    for port in WEBUI_PORTS:
      self._webui_smoke("https://127.0.0.1:%d" % port, WEB_CERT_ERR)
      self._webui_smoke("https://[::1]:%d" % port, WEB_CERT_ERR)
      self._webui_smoke("https://ip4.impala.test:%d" % port)
      self._webui_smoke("https://ip6.impala.test:%d" % port)
      self._webui_smoke("https://ip46.impala.test:%d" % port)

    self._smoke("[::1]", vector, CONN_ERR)
    self._smoke("127.0.0.1", vector, CONN_ERR)
    self._smoke("ip4.impala.test", vector)
    self._smoke("ip6.impala.test", vector)
    self._smoke("ip46.impala.test", vector)

    self._shell_smoke("[::1]", vector, CERT_ERR)
    self._shell_smoke("127.0.0.1", vector, CERT_ERR)
    self._shell_smoke("ip4.impala.test", vector)
    self._shell_smoke("ip6.impala.test", vector)
    self._shell_smoke("ip46.impala.test", vector)


@CustomClusterTestSuite.with_args(impalad_args=IPV6_ONLY_HOSTNAME_WEBSERBER_ARG
                                               + IPV6_ONLY_HOSTNAME_QUERY_ARG
                                               + SSL_WILDCARD_SAN_ARGS,
                                 statestored_args=IPV6_ONLY_HOSTNAME_WEBSERBER_ARG
                                                  + SSL_WILDCARD_SAN_ARGS,
                                 catalogd_args=IPV6_ONLY_HOSTNAME_WEBSERBER_ARG
                                               + SSL_WILDCARD_SAN_ARGS)
class TestIPv6OnlySsl(TestIPv6Base):
  ca_cert = WILDCARD_CA_CERT_PATH

  @pytest.mark.skipif(SKIP_SSL_MSG != "", reason=SKIP_SSL_MSG)
  def test_ipv6_only_ssl(self, vector):
    self.check_connections()
    for port in WEBUI_PORTS:
      self._webui_smoke("https://127.0.0.1:%d" % port, WEB_CERT_ERR)
      self._webui_smoke("https://[::1]:%d" % port, WEB_CERT_ERR)
      self._webui_smoke("https://ip4.impala.test:%d" % port, "Connection refused")
      self._webui_smoke("https://ip6.impala.test:%d" % port)
      self._webui_smoke("https://ip46.impala.test:%d" % port)

    self._smoke("[::1]", vector, CONN_ERR)
    self._smoke("127.0.0.1", vector, CONN_ERR)
    self._smoke("ip4.impala.test", vector, CONN_ERR)
    self._smoke("ip6.impala.test", vector)
    self._smoke("ip46.impala.test", vector)

    self._shell_smoke("[::1]", vector, CERT_ERR)
    self._shell_smoke("127.0.0.1", vector, CONN_ERR)
    self._shell_smoke("ip4.impala.test", vector, CONN_ERR)
    self._shell_smoke("ip6.impala.test", vector)
    self._shell_smoke("ip46.impala.test", vector)
