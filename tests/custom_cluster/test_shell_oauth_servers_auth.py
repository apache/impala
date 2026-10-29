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

import base64
import hashlib
import json
import logging
import os
import re
import subprocess
import pytest

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.test_dimensions import create_client_protocol_http_transport
from tests.shell.util import get_unused_port, run_impala_shell_cmd
from tests.util.proxy_servers import HttpProxy, Socks5Proxy, StaticFileServer

JWKS_JSON_PATH = os.path.join(
    os.environ['IMPALA_HOME'], 'testdata', 'jwt', 'jwks_signing.json')
CERT_DIR = os.path.join(os.environ['IMPALA_HOME'], 'be', 'src', 'testutil')
# Self-signed certificate for 'localhost'.
SERVER_CERT = os.path.join(CERT_DIR, 'server-cert.pem')
SERVER_KEY = os.path.join(CERT_DIR, 'server-key.pem')

LOG = logging.getLogger(__name__)

# Ports of the JWKS servers and proxies started by TestImpalaShellOAuthServersAuth. They
# are picked when the module is loaded because the impalad arguments depend on them.
JWKS_HTTP_PORT = get_unused_port()
JWKS_HTTPS_PORT = get_unused_port()
HTTP_PROXY_PORT = get_unused_port()
HTTPS_PROXY_PORT = get_unused_port()
SOCKS5_PROXY_PORT = get_unused_port()
TUNNEL_PROXY_PORT = get_unused_port()
HTTPS_TUNNEL_PROXY_PORT = get_unused_port()
UNUSED_PORT = get_unused_port()

# The JWKS URL hosts are not resolvable, so the JWKS can only be downloaded through a
# proxy, which forwards every connection to the JWKS server.
UNRESOLVABLE_HOST = 'jwks.proxy-test.invalid'
JWKS_HTTP_URL = 'http://{0}:{1}/jwks.json'.format(UNRESOLVABLE_HOST, JWKS_HTTP_PORT)
JWKS_HTTPS_URL = 'https://{0}:{1}/jwks.json'.format(UNRESOLVABLE_HOST, JWKS_HTTPS_PORT)
HTTP_PROXY_URL = 'http://127.0.0.1:{0}'.format(HTTP_PROXY_PORT)
TUNNEL_PROXY_URL = 'http://127.0.0.1:{0}'.format(TUNNEL_PROXY_PORT)
HTTPS_PROXY_URL = 'https://localhost:{0}'.format(HTTPS_PROXY_PORT)
HTTPS_TUNNEL_PROXY_URL = 'https://localhost:{0}'.format(HTTPS_TUNNEL_PROXY_PORT)
# The certificate of the https JWKS server is only valid for 'localhost', so the JWKS URL
# used to verify it through an https proxy must use that host name.
JWKS_HTTPS_LOCALHOST_URL = 'https://localhost:{0}/jwks.json'.format(JWKS_HTTPS_PORT)
SOCKS5H_PROXY_URL = 'socks5h://127.0.0.1:{0}'.format(SOCKS5_PROXY_PORT)
UNREACHABLE_PROXY_URL = 'socks5h://127.0.0.1:{0}'.format(UNUSED_PORT)


def public_key_pin(cert_file):
  """Returns the SHA-256 hash of the public key of 'cert_file' in the
  'sha256//<base64 hash>' format of libcurl's CURLOPT_PROXY_PINNEDPUBLICKEY. This is
  equivalent to:
    openssl x509 -in <cert_file> -pubkey -noout | openssl pkey -pubin -outform der \
      | openssl dgst -sha256 -binary | base64"""
  public_key_pem = subprocess.check_output(
      ['openssl', 'x509', '-in', cert_file, '-pubkey', '-noout']).decode('ascii')
  public_key_der = base64.b64decode(''.join(
      line for line in public_key_pem.splitlines() if not line.startswith('-----')))
  digest = hashlib.sha256(public_key_der).digest()
  return 'sha256//' + base64.b64encode(digest).decode('ascii')


SERVER_CERT_PUBLIC_KEY_PIN = public_key_pin(SERVER_CERT)
WRONG_PUBLIC_KEY_PIN = 'sha256//AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA='


def oauth_servers_flag(oauth_servers):
  """Returns the -oauth_servers impalad flag for the given list of server configs."""
  oauth_servers_json = json.dumps(oauth_servers, separators=(',', ':'))
  # CustomClusterTestSuite applies str.format() to impalad_args; escape JSON braces.
  # Single quotes preserve JSON double quotes through shlex/gflags parsing.
  return "-oauth_servers='{0}'".format(
      oauth_servers_json.replace('{', '{{').replace('}', '}}'))


def jwks_proxy_impalad_args(jwks_url, jwks_proxy, **extra_fields):
  oauth_server = {'jwksUrl': jwks_url, 'jwksProxy': jwks_proxy, 'usernameClaim': 'sub'}
  oauth_server.update(extra_fields)
  return '-v 2 -jwt_token_auth=true -jwt_allow_without_tls=true {0}'.format(
      oauth_servers_flag([oauth_server]))


class TestImpalaShellOAuthServersAuth(CustomClusterTestSuite):
  """End-to-end tests for JWT/OAuth authentication configured via --oauth_servers."""

  JWKS_JSON_PATH = JWKS_JSON_PATH
  JWT_SIGNED_PATH = os.path.join(os.path.dirname(JWKS_JSON_PATH), 'jwt_signed')
  OAUTH_SIGNED_PATH = JWT_SIGNED_PATH

  OAUTH_SERVERS_FLAG = oauth_servers_flag(
      [{'jwksFilePath': JWKS_JSON_PATH, 'usernameClaim': 'sub'}])

  JWT_IMPALAD_ARGS = ('-v 2 -jwt_token_auth=true -jwt_allow_without_tls=true {0}'
                      .format(OAUTH_SERVERS_FLAG))

  OAUTH_IMPALAD_ARGS = ('-v 2 -oauth_token_auth=true -oauth_allow_without_tls=true {0}'
                        .format(OAUTH_SERVERS_FLAG))

  OAUTH_LEGACY_FLAGS_IMPALAD_ARGS = (
      '-v 2 -oauth_token_auth=true -oauth_allow_without_tls=true '
      '-oauth_jwks_file_path={0} -oauth_jwt_custom_claim_username=sub'
      .format(JWKS_JSON_PATH))

  @classmethod
  def add_test_dimensions(cls):
    cls.ImpalaTestMatrix.add_dimension(create_client_protocol_http_transport())

  @classmethod
  def setup_class(cls):
    super(TestImpalaShellOAuthServersAuth, cls).setup_class()
    cls.servers = []
    cls.proxies = []
    try:
      cls._start_server(StaticFileServer(JWKS_HTTP_PORT, JWKS_JSON_PATH))
      cls._start_server(StaticFileServer(
          JWKS_HTTPS_PORT, JWKS_JSON_PATH, certfile=SERVER_CERT, keyfile=SERVER_KEY))
      http_backend = ('127.0.0.1', JWKS_HTTP_PORT)
      https_backend = ('127.0.0.1', JWKS_HTTPS_PORT)
      cls.http_proxy = cls._start_proxy(HttpProxy(HTTP_PROXY_PORT, http_backend))
      cls.https_proxy = cls._start_proxy(HttpProxy(
          HTTPS_PROXY_PORT, http_backend, certfile=SERVER_CERT, keyfile=SERVER_KEY))
      cls.socks5_proxy = cls._start_proxy(Socks5Proxy(SOCKS5_PROXY_PORT, http_backend))
      cls.tunnel_proxy = cls._start_proxy(HttpProxy(TUNNEL_PROXY_PORT, https_backend))
      cls.https_tunnel_proxy = cls._start_proxy(HttpProxy(HTTPS_TUNNEL_PROXY_PORT,
          https_backend, certfile=SERVER_CERT, keyfile=SERVER_KEY))
    except Exception:
      cls._stop_servers()
      raise

  @classmethod
  def teardown_class(cls):
    cls._stop_servers()
    super(TestImpalaShellOAuthServersAuth, cls).teardown_class()

  @classmethod
  def _start_server(cls, server):
    server.start()
    cls.servers.append(server)
    return server

  @classmethod
  def _start_proxy(cls, proxy):
    cls.proxies.append(cls._start_server(proxy))
    return proxy

  @classmethod
  def _stop_servers(cls):
    for server in cls.servers:
      try:
        server.stop()
      except Exception as e:
        LOG.warning("Failed to stop %s: %s", type(server).__name__, e)
    cls.servers = []
    cls.proxies = []

  def setup_method(self, method):
    for proxy in self.proxies:
      proxy.clear_requests()
    super(TestImpalaShellOAuthServersAuth, self).setup_method(method)

  def _assert_proxied_jwt_auth_succeeds(self, vector, jwks_url, jwks_proxy):
    args = ['--protocol', vector.get_value('protocol'), '-j', '--jwt_cmd',
            'cat {0}'.format(self.JWT_SIGNED_PATH),
            '-q', 'select logged_in_user()', '--auth_creds_ok_in_clear']
    result = run_impala_shell_cmd(vector, args)
    assert 'test-user' in result.stdout
    self.assert_impalad_log_contains("INFO", re.escape(
        "Downloading JWKS from '{0}' via proxy '{1}'".format(jwks_url, jwks_proxy)))

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=JWT_IMPALAD_ARGS,
    impala_log_dir="{oauth_servers_jwt_success}",
    tmp_dir_placeholders=["oauth_servers_jwt_success"],
    disable_log_buffering=True,
    cluster_size=1)
  def test_jwt_auth_with_oauth_servers(self, vector):
    """JWT auth succeeds when JWKS is configured through --oauth_servers."""
    args = ['--protocol', vector.get_value('protocol'), '-j', '--jwt_cmd',
            'cat {0}'.format(self.JWT_SIGNED_PATH),
            '-q', 'select version()', '--auth_creds_ok_in_clear']
    result = run_impala_shell_cmd(vector, args)
    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.backend-num-queries-executed", 1, timeout=15)

    self._stop_impala_cluster()
    self.assert_impalad_log_contains("INFO",
        r'--oauth_servers=.*' + re.escape(self.JWKS_JSON_PATH), expected_count=1)
    self.assert_impalad_log_contains("INFO",
        'effective username: test-user', expected_count=1)
    assert "version()" in result.stdout
    assert "impalad version" in result.stdout

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=OAUTH_IMPALAD_ARGS,
    impala_log_dir="{oauth_servers_oauth_success}",
    tmp_dir_placeholders=["oauth_servers_oauth_success"],
    disable_log_buffering=True,
    cluster_size=1)
  def test_oauth_auth_with_oauth_servers(self, vector):
    """OAuth auth succeeds when JWKS is configured through --oauth_servers."""
    args = ['--protocol', vector.get_value('protocol'), '-a', '--oauth_cmd',
            'cat {0}'.format(self.OAUTH_SIGNED_PATH),
            '-q', 'select version()', '--auth_creds_ok_in_clear']
    result = run_impala_shell_cmd(vector, args)
    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.backend-num-queries-executed", 1, timeout=15)

    self._stop_impala_cluster()
    self.assert_impalad_log_contains("INFO",
        r'--oauth_servers=.*' + re.escape(self.JWKS_JSON_PATH), expected_count=1)
    self.assert_impalad_log_contains("INFO",
        'effective username: test-user', expected_count=1)
    assert "version()" in result.stdout
    assert "impalad version" in result.stdout

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=OAUTH_LEGACY_FLAGS_IMPALAD_ARGS,
    impala_log_dir="{oauth_legacy_flags_success}",
    tmp_dir_placeholders=["oauth_legacy_flags_success"],
    disable_log_buffering=True,
    cluster_size=1)
  def test_oauth_auth_with_legacy_oauth_flags(self, vector):
    """OAuth auth succeeds with legacy oauth_* flags."""
    args = ['--protocol', vector.get_value('protocol'), '-a', '--oauth_cmd',
            'cat {0}'.format(self.OAUTH_SIGNED_PATH),
            '-q', 'select version()', '--auth_creds_ok_in_clear']
    result = run_impala_shell_cmd(vector, args)
    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.backend-num-queries-executed", 1, timeout=15)

    self._stop_impala_cluster()
    self.assert_impalad_log_contains("INFO",
        'effective username: test-user', expected_count=1)
    assert "version()" in result.stdout
    assert "impalad version" in result.stdout

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=jwks_proxy_impalad_args(JWKS_HTTP_URL, HTTP_PROXY_URL),
    impala_log_dir="{jwks_http_proxy}",
    tmp_dir_placeholders=["jwks_http_proxy"],
    disable_log_buffering=True,
    cluster_size=1)
  def test_jwks_url_through_http_proxy(self, vector):
    """The JWKS is downloaded from an http URL through an http proxy."""
    self._assert_proxied_jwt_auth_succeeds(vector, JWKS_HTTP_URL, HTTP_PROXY_URL)
    assert ('GET', JWKS_HTTP_URL) in self.http_proxy.get_requests()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=jwks_proxy_impalad_args(
        JWKS_HTTPS_URL, TUNNEL_PROXY_URL, verifyServerCert=False),
    impala_log_dir="{jwks_http_proxy_tunnel}",
    tmp_dir_placeholders=["jwks_http_proxy_tunnel"],
    disable_log_buffering=True,
    cluster_size=1)
  def test_jwks_https_url_through_http_proxy_tunnel(self, vector):
    """The JWKS is downloaded from an https URL through a CONNECT tunnel of an http
    proxy."""
    self._assert_proxied_jwt_auth_succeeds(vector, JWKS_HTTPS_URL, TUNNEL_PROXY_URL)
    assert ('CONNECT', '{0}:{1}'.format(UNRESOLVABLE_HOST, JWKS_HTTPS_PORT)) in \
        self.tunnel_proxy.get_requests()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=jwks_proxy_impalad_args(
        JWKS_HTTP_URL, HTTPS_PROXY_URL, caCertFilePath=SERVER_CERT),
    impala_log_dir="{jwks_https_proxy}",
    tmp_dir_placeholders=["jwks_https_proxy"],
    disable_log_buffering=True,
    cluster_size=1)
  def test_jwks_url_through_https_proxy(self, vector):
    """The JWKS is downloaded through an https proxy whose TLS certificate is verified
    with caCertFilePath, which jwksProxyCaCertFilePath defaults to."""
    self._assert_proxied_jwt_auth_succeeds(vector, JWKS_HTTP_URL, HTTPS_PROXY_URL)
    assert ('GET', JWKS_HTTP_URL) in self.https_proxy.get_requests()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=jwks_proxy_impalad_args(
        JWKS_HTTPS_LOCALHOST_URL, HTTPS_TUNNEL_PROXY_URL, caCertFilePath=SERVER_CERT),
    impala_log_dir="{jwks_https_url_https_proxy}",
    tmp_dir_placeholders=["jwks_https_url_https_proxy"],
    disable_log_buffering=True,
    cluster_size=1)
  def test_jwks_https_url_through_https_proxy(self, vector):
    """The JWKS is downloaded from an https URL through a CONNECT tunnel of an https
    proxy (TLS inside TLS), and both TLS certificates are verified with
    caCertFilePath."""
    self._assert_proxied_jwt_auth_succeeds(
        vector, JWKS_HTTPS_LOCALHOST_URL, HTTPS_TUNNEL_PROXY_URL)
    assert ('CONNECT', 'localhost:{0}'.format(JWKS_HTTPS_PORT)) in \
        self.https_tunnel_proxy.get_requests()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=jwks_proxy_impalad_args(
        JWKS_HTTP_URL, HTTPS_PROXY_URL, jwksProxyCaCertFilePath=SERVER_CERT),
    impala_log_dir="{jwks_https_proxy_ca_cert}",
    tmp_dir_placeholders=["jwks_https_proxy_ca_cert"],
    disable_log_buffering=True,
    cluster_size=1)
  def test_jwks_url_through_https_proxy_with_proxy_ca_cert(self, vector):
    """The TLS certificate of the https proxy is verified with jwksProxyCaCertFilePath."""
    self._assert_proxied_jwt_auth_succeeds(vector, JWKS_HTTP_URL, HTTPS_PROXY_URL)
    assert ('GET', JWKS_HTTP_URL) in self.https_proxy.get_requests()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=jwks_proxy_impalad_args(JWKS_HTTP_URL, HTTPS_PROXY_URL,
        caCertFilePath=SERVER_CERT, jwksProxyCaCertFilePath=''),
    impala_log_dir="{jwks_https_proxy_empty_ca_cert}",
    tmp_dir_placeholders=["jwks_https_proxy_empty_ca_cert"],
    disable_log_buffering=True,
    log_symlinks=True,
    expect_startup_fail=True,
    impalad_timeout_s=60,
    cluster_size=1)
  def test_jwks_url_through_https_proxy_with_empty_proxy_ca_cert_fails(self):
    """An empty jwksProxyCaCertFilePath does not fall back to caCertFilePath, so the TLS
    certificate of the https proxy is not trusted and impalad fails to start."""
    self.wait_for_log_exists("impalad", "ERROR")
    self.assert_impalad_log_contains("ERROR", re.escape(
        "Impalad services did not start correctly, exiting.  Error: Error downloading "
        "JWKS from '{0}' via proxy '{1}'".format(JWKS_HTTP_URL, HTTPS_PROXY_URL)))
    assert not self.https_proxy.get_requests()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=jwks_proxy_impalad_args(JWKS_HTTP_URL, HTTPS_PROXY_URL),
    impala_log_dir="{jwks_https_proxy_untrusted_cert}",
    tmp_dir_placeholders=["jwks_https_proxy_untrusted_cert"],
    disable_log_buffering=True,
    log_symlinks=True,
    expect_startup_fail=True,
    impalad_timeout_s=60,
    cluster_size=1)
  def test_jwks_url_through_https_proxy_with_untrusted_cert_fails(self):
    """impalad fails to start if the TLS certificate of the https proxy is not trusted
    via caCertFilePath."""
    self.wait_for_log_exists("impalad", "ERROR")
    self.assert_impalad_log_contains("ERROR", re.escape(
        "Impalad services did not start correctly, exiting.  Error: Error downloading "
        "JWKS from '{0}' via proxy '{1}'".format(JWKS_HTTP_URL, HTTPS_PROXY_URL)))
    assert not self.https_proxy.get_requests()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=jwks_proxy_impalad_args(
        JWKS_HTTP_URL, HTTPS_PROXY_URL, caCertFilePath=SERVER_CERT,
        jwksProxyPinnedPublicKey=SERVER_CERT_PUBLIC_KEY_PIN),
    impala_log_dir="{jwks_https_proxy_pinned_key}",
    tmp_dir_placeholders=["jwks_https_proxy_pinned_key"],
    disable_log_buffering=True,
    cluster_size=1)
  def test_jwks_url_through_https_proxy_with_pinned_public_key(self, vector):
    """The JWKS is downloaded through an https proxy whose public key matches
    jwksProxyPinnedPublicKey."""
    self._assert_proxied_jwt_auth_succeeds(vector, JWKS_HTTP_URL, HTTPS_PROXY_URL)
    assert ('GET', JWKS_HTTP_URL) in self.https_proxy.get_requests()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=jwks_proxy_impalad_args(
        JWKS_HTTP_URL, HTTPS_PROXY_URL, caCertFilePath=SERVER_CERT,
        jwksProxyPinnedPublicKey=WRONG_PUBLIC_KEY_PIN),
    impala_log_dir="{jwks_https_proxy_wrong_pinned_key}",
    tmp_dir_placeholders=["jwks_https_proxy_wrong_pinned_key"],
    disable_log_buffering=True,
    log_symlinks=True,
    expect_startup_fail=True,
    impalad_timeout_s=60,
    cluster_size=1)
  def test_jwks_url_through_https_proxy_with_wrong_pinned_public_key_fails(self):
    """impalad fails to start if the public key of the https proxy does not match
    jwksProxyPinnedPublicKey."""
    self.wait_for_log_exists("impalad", "ERROR")
    self.assert_impalad_log_contains("ERROR", re.escape(
        "Impalad services did not start correctly, exiting.  Error: Error downloading "
        "JWKS from '{0}' via proxy '{1}'".format(JWKS_HTTP_URL, HTTPS_PROXY_URL)))
    assert not self.https_proxy.get_requests()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=jwks_proxy_impalad_args(JWKS_HTTP_URL, SOCKS5H_PROXY_URL),
    impala_log_dir="{jwks_socks5h_proxy}",
    tmp_dir_placeholders=["jwks_socks5h_proxy"],
    disable_log_buffering=True,
    cluster_size=1)
  def test_jwks_url_through_socks5h_proxy(self, vector):
    """The JWKS is downloaded through a SOCKS5 proxy that resolves the host name."""
    self._assert_proxied_jwt_auth_succeeds(vector, JWKS_HTTP_URL, SOCKS5H_PROXY_URL)
    assert (UNRESOLVABLE_HOST, JWKS_HTTP_PORT) in self.socks5_proxy.get_requests()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=jwks_proxy_impalad_args(JWKS_HTTP_URL, UNREACHABLE_PROXY_URL),
    impala_log_dir="{jwks_unreachable_proxy}",
    tmp_dir_placeholders=["jwks_unreachable_proxy"],
    disable_log_buffering=True,
    log_symlinks=True,
    expect_startup_fail=True,
    impalad_timeout_s=60,
    cluster_size=1)
  def test_jwks_url_through_unreachable_proxy_fails_startup(self):
    """impalad fails to start if the JWKS cannot be downloaded through the proxy."""
    self.wait_for_log_exists("impalad", "ERROR")
    self.assert_impalad_log_contains("ERROR", re.escape(
        "Impalad services did not start correctly, exiting.  Error: Error downloading "
        "JWKS from '{0}' via proxy '{1}'".format(JWKS_HTTP_URL, UNREACHABLE_PROXY_URL)))
