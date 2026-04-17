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
import os
import pytest

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.impala_cluster import (
    DEFAULT_BEESWAX_PORT, DEFAULT_HS2_PORT, DEFAULT_CATALOG1_SERVICE_PORT,
    DEFAULT_STATE_STORE_SUBSCRIBER_PORT, DEFAULT_CATALOGD_STATE_STORE_SUBSCRIBER_PORT,
    DEFAULT_CATALOGD1_STATE_STORE_SUBSCRIBER_PORT, DEFAULT_HS2_HTTP_PORT,
    DEFAULT_ADMISSIOND_STATE_STORE_SUBSCRIBER_PORT, DEFAULT_IMPALAD_WEBSERVER_PORT,
    DEFAULT_STATESTORED_WEBSERVER_PORT, DEFAULT_CATALOGD_WEBSERVER_PORT,
    DEFAULT_ADMISSIOND_WEBSERVER_PORT, DEFAULT_STATESTORE_SERVICE_PORT,
    DEFAULT_STATESTORE1_SERVICE_PORT, DEFAULT_STATESTORE_HA_SERVICE_PORT,
    DEFAULT_PEER_STATESTORE_HA_SERVICE_PORT, DEFAULT_CATALOG_SERVICE_PORT,
    DEFAULT_EXTERNAL_FE_PORT,
)
from tests.common.network import (
    OPENSSL_TLS_1_2_CIPHERSUITES,
    OPENSSL_TLS_1_3_CIPHERSUITES,
    SKIP_SSL_MSG,
    run_openssl_cmd
)
from tests.common.test_vector import ImpalaTestDimension


# TLS ports to assert.
TLS_PORTS = [
    DEFAULT_BEESWAX_PORT,
    DEFAULT_HS2_PORT,
    DEFAULT_HS2_HTTP_PORT,
    DEFAULT_STATE_STORE_SUBSCRIBER_PORT,
    DEFAULT_STATESTORE_SERVICE_PORT,
    DEFAULT_STATESTORE1_SERVICE_PORT,
    DEFAULT_STATESTORED_WEBSERVER_PORT,
    DEFAULT_PEER_STATESTORE_HA_SERVICE_PORT,
    DEFAULT_CATALOGD_STATE_STORE_SUBSCRIBER_PORT,
    DEFAULT_CATALOGD1_STATE_STORE_SUBSCRIBER_PORT,
    DEFAULT_CATALOG_SERVICE_PORT,
    DEFAULT_CATALOG1_SERVICE_PORT,
    DEFAULT_CATALOGD_WEBSERVER_PORT,
    DEFAULT_IMPALAD_WEBSERVER_PORT,
    DEFAULT_ADMISSIOND_STATE_STORE_SUBSCRIBER_PORT,
    DEFAULT_ADMISSIOND_WEBSERVER_PORT,
    DEFAULT_STATESTORE_HA_SERVICE_PORT,
    DEFAULT_EXTERNAL_FE_PORT,
]

# TLS 1.2 RSA ciphersuites allowed by the cluster configuration.
TLS_1_2_CIPHERSUITES_RSA = [
    "ECDHE-RSA-AES128-GCM-SHA256",
    "ECDHE-RSA-AES256-GCM-SHA384",
]

# TLS 1.2 ECDSA ciphersuites allowed by the cluster configuration.
TLS_1_2_CIPHERSUITES_ECDSA = [
  "ECDHE-ECDSA-AES128-GCM-SHA256",
  "ECDHE-ECDSA-AES256-SHA",
]

# TLS 1.3 ciphersuites allowed by the cluster configuration.
TLS_1_3_CIPHERSUITES = [
  "TLS_AES_128_GCM_SHA256",
  "TLS_AES_256_GCM_SHA384",
]

# Strings defining each TLS version
TLS_VER_10 = "1.0"
TLS_VER_11 = "1.1"
TLS_VER_12 = "1.2"
TLS_VER_13 = "1.3"

# Directory that contains the test TLS certificates and private keys.
CERT_DIR = "{}/be/src/testutil".format(os.environ['IMPALA_HOME'])

# Test dimension specific values. Set up here instead of in add_test_dimensions to cut
# down test output (which includes everything from the ImpalaTestDimension creating
# confusing and complicated test output).
TEST_DIMENSIONS_DATA = {
  TLS_VER_10: {
    # TLS 1.0 is not supported, thus there are no test ciphersuites which results in the
    # tests asserting that all ciphersuites are rejected.
    "openssl_ver": "-tls1",
    "openssl_ciphersuites_arg": "-cipher",
    "extended_master_secret": True,
    "all_ciphersuites": OPENSSL_TLS_1_2_CIPHERSUITES,
    "test_ciphersuites": [],
  },
  TLS_VER_11: {
    # TLS 1.1 is not supported, thus there are no test ciphersuites which results in the
    # tests asserting that all ciphersuites are rejected.
    "openssl_ver": "-tls1_1",
    "openssl_ciphersuites_arg": "-cipher",
    "extended_master_secret": True,
    "all_ciphersuites": OPENSSL_TLS_1_2_CIPHERSUITES,
    "test_ciphersuites": [],
  },
  TLS_VER_12: {
    "openssl_ver": "-tls1_2",
    "openssl_ciphersuites_arg": "-cipher",
    "extended_master_secret": True,
    "all_ciphersuites": OPENSSL_TLS_1_2_CIPHERSUITES,
    "test_ciphersuites": TLS_1_2_CIPHERSUITES_RSA + TLS_1_2_CIPHERSUITES_ECDSA,
  },
  TLS_VER_13: {
    "openssl_ver": "-tls1_3",
    "openssl_ciphersuites_arg": "-ciphersuites",
    "extended_master_secret": False,  # TLS 1.3 does not use extended master secrets.
    "all_ciphersuites": OPENSSL_TLS_1_3_CIPHERSUITES,
    "test_ciphersuites": TLS_1_3_CIPHERSUITES,
  },
}

# List of arguments to configure on each cluster daemon to enable TLS on all ports using
# the above certificates.
TLS_LOCALHOST_ARGS = (
    "--hostname=localhost "
    "--webserver_interface=localhost "
    "--ssl_cipher_list={0} "
    "--tls_ciphersuites={1} "
).format(
    ":".join(TEST_DIMENSIONS_DATA[TLS_VER_12]["test_ciphersuites"]),
    ":".join(TEST_DIMENSIONS_DATA[TLS_VER_13]["test_ciphersuites"]),
)

# Specific values for the RSA and ECDSA tests. Defined outside of test dimensions because
# these values are used directly in the cluster startup args.
RSA_CA_FILE = "{}/wildcardCA.pem".format(CERT_DIR)

TLS_RSA_LOCALHOST_ARGS = (
    "{localhost_arg} "
    "--ssl_client_ca_certificate={ca_file} "
    "--webserver_certificate_file={cert_dir}/localhost.pem "
    "--webserver_private_key_file={cert_dir}/localhost.key "
    "--ssl_server_certificate={cert_dir}/localhost.pem "
    "--ssl_private_key={cert_dir}/localhost.key "
).format(localhost_arg=TLS_LOCALHOST_ARGS, ca_file=RSA_CA_FILE, cert_dir=CERT_DIR)

ECDSA_CA_FILE = "{}/wildcardCA-ecdsa.pem".format(CERT_DIR)

TLS_ECDSA_LOCALHOST_ARGS = (
    "{localhost_arg} "
    "--ssl_client_ca_certificate={ca_file} "
    "--webserver_certificate_file={cert_dir}/localhost-ecdsa.pem "
    "--webserver_private_key_file={cert_dir}/localhost-ecdsa.key "
    "--ssl_server_certificate={cert_dir}/localhost-ecdsa.pem "
    "--ssl_private_key={cert_dir}/localhost-ecdsa.key "
).format(localhost_arg=TLS_LOCALHOST_ARGS, ca_file=ECDSA_CA_FILE, cert_dir=CERT_DIR)


class BaseTestServerTls(CustomClusterTestSuite):
  """
     Tests that the cluster properly accepts or rejects TLS connections based on the
     configured TLS versions and ciphersuites and that extended master secrets are used
     for TLS 1.2 connections.

     Checks webserver and Thrift server ports.

     The cluster is set up with a separate admissiond and both statestored and catalogd
     running in HA mode to ensure all TLS-enabled cluster ports are asserted.

     Uses the openssl s_client command to perform assertions because the Python 'ssl'
     module changed drastically with Python 3.7 and thus doesn't provide a consistent
     interface across Python versions.
  """

  @classmethod
  def setup_class(cls):
    super(BaseTestServerTls, cls).setup_class()

    unsupported_ciphersuites = []

    # Assert all test TLS 1.2 ciphersuites are supported by OS/OpenSSL.
    for ciphersuite in TEST_DIMENSIONS_DATA[TLS_VER_12]["test_ciphersuites"]:
      if ciphersuite not in OPENSSL_TLS_1_2_CIPHERSUITES:
        unsupported_ciphersuites.append(ciphersuite)

    # Assert all test TLS 1.3 ciphersuites are supported by OS/OpenSSL.
    for ciphersuite in TEST_DIMENSIONS_DATA[TLS_VER_13]["test_ciphersuites"]:
      if ciphersuite not in OPENSSL_TLS_1_3_CIPHERSUITES:
        unsupported_ciphersuites.append(ciphersuite)

    assert len(unsupported_ciphersuites) == 0, \
        "Configured TLS ciphers are not supported by OS/OpenSSL: {}".format(
            unsupported_ciphersuites)

  @classmethod
  def add_test_dimensions(cls):
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('tls_version', "1.0", "1.1", TLS_VER_12, TLS_VER_13))

  @classmethod
  def ca_file(cls):
    raise NotImplementedError("Subclasses must implement ca_file()")

  def _ciphersuite_filter(self, vector):  # noqa: U100
    raise NotImplementedError("Subclasses must implement _ciphersuite_filter()")

  def _get_dimension_data(self, vector):
    return TEST_DIMENSIONS_DATA[vector.get_value("tls_version")]

  def _get_tls_ver_str(self, vector):
    return vector.get_value("tls_version")

  def _ciphersuites_allowed(self, vector):
    return sorted(ciph for ciph in self._get_dimension_data(vector)["test_ciphersuites"]
        if self._ciphersuite_filter(vector)(ciph))

  def _ciphersuites_notallowed(self, vector):
    """
       Builds a list of ciphersuites that are not allowed by the cluster configuration and
       that should be rejected by the cluster. This is done by taking all OS/OpenSSL
       ciphersuites and removing the test ciphersuites (that should be allowed).
    """
    ret = []
    for ciph in set(self._get_dimension_data(vector)["all_ciphersuites"]) \
        - set(self._get_dimension_data(vector)["test_ciphersuites"]):
      if self._ciphersuite_filter(vector)(ciph):
        ret.append(ciph)
    return sorted(ret)

  def _openssl_handshake(self, vector, port, ciphersuites):
    """
       Uses the openssl s_client command to attempt a TLS handshake on the given port with
       the specified TLS version and ciphersuites. Returns a tuple of (success, output)
       where success is a boolean indicating whether the handshake succeeded and output is
       the combined stdout/stderr output from the openssl command as a string.
    """
    cmd = ["s_client", "-connect", "localhost:{}".format(port), "-servername",
           "localhost", "-CAfile", self.ca_file()]

    # Add the correct TLS version and ciphersuite flags based on the tls_version argument.
    cmd.extend([
        self._get_dimension_data(vector)["openssl_ver"],
        self._get_dimension_data(vector)["openssl_ciphersuites_arg"],
        ciphersuites,
    ])

    rc, out = run_openssl_cmd(cmd)
    return rc == 0, out

  @pytest.mark.execute_serially
  @pytest.mark.skipif(SKIP_SSL_MSG != "", reason=SKIP_SSL_MSG)
  def test_webserver_thrift_ciphersuites_allowed(self, vector):
    """
       Asserts the cluster accepts TLS connections with the configured ciphersuites and
       TLS version on webserver and Thrift server ports.
    """
    # Skip test if there are no allowed ciphersuites for the TLS version.
    if len(self._ciphersuites_allowed(vector)) == 0:
      pytest.skip("No configured ciphersuites for TLS version {}"
          .format(self._get_tls_ver_str(vector)))

    # Loop through all ports attempting a TLS handshake with each TLS ciphersuite.
    for port in TLS_PORTS:
      # Assert a successful TLS handshake for all TLS 1.2 ciphersuites configured on the
      # cluster.
      for cipher in self._ciphersuites_allowed(vector):
        success, out = self._openssl_handshake(vector, port, cipher)

        assert success, "Expected TLS {} handshake to succeed on 'localhost:{}' with " \
            "cipher '{}', output:\n{}" \
            .format(self._get_tls_ver_str(vector), port, cipher, out)

        assert \
            "Ciphersuite: {}".format(cipher) in out \
            or "Cipher is {}".format(cipher) in out, \
            "Expected TLS {} cipher '{}' to be negotiated on 'localhost:{}', " \
            "output:\n{}".format(self._get_tls_ver_str(vector), cipher, port, out)

        if self._get_dimension_data(vector)["extended_master_secret"]:
          assert "Extended master secret: yes" in out, \
              "Expected TLS {} on 'localhost:{}' with cipher '{}' to use extended " \
              "master secret, output:\n{}".format(self._get_tls_ver_str(vector), port,
                  cipher, out)

  @pytest.mark.execute_serially
  @pytest.mark.skipif(SKIP_SSL_MSG != "", reason=SKIP_SSL_MSG)
  def test_webserver_thrift_ciphersuites_notallowed(self, vector):
    """
       Asserts the cluster denies TLS connections with ciphersuites that are not
       configured on webserver and Thrift server ports.
    """
    # Loop through all ports attempting a TLS handshake with each disallowed cipher.
    for port in TLS_PORTS:
      for cipher in self._ciphersuites_notallowed(vector):
        success, out = self._openssl_handshake(vector, port, cipher)
        assert not success, "Unexpected TLS {} cipher '{}' accepted on 'localhost:{}', " \
            "output:\n{}".format(self._get_tls_ver_str(vector), cipher, port, out)


@CustomClusterTestSuite.with_args(cluster_size=1,
                                  impalad_args=TLS_RSA_LOCALHOST_ARGS,
                                  statestored_args=TLS_RSA_LOCALHOST_ARGS,
                                  catalogd_args=TLS_RSA_LOCALHOST_ARGS,
                                  admissiond_args=TLS_RSA_LOCALHOST_ARGS,
                                  start_args="--enable_admission_service "
                                             "--enable_statestored_ha "
                                             "--enable_catalogd_ha "
                                             "--enable_external_fe_support")
class TestServerTlsRSA(BaseTestServerTls):
  """
     Spins up a cluster configured to use TLS with RSA certificates and runs webserver
     and Thrift server TLS tests.
  """

  @classmethod
  def ca_file(cls):
    return RSA_CA_FILE

  def _ciphersuite_filter(self, vector):
    """
       Filters TLS ciphersuites to include RSA ciphersuites.
    """
    return lambda ciph: self._get_tls_ver_str(vector) == TLS_VER_13 or "ECDSA" not in ciph


@CustomClusterTestSuite.with_args(cluster_size=1,
                                  impalad_args=TLS_ECDSA_LOCALHOST_ARGS,
                                  statestored_args=TLS_ECDSA_LOCALHOST_ARGS,
                                  catalogd_args=TLS_ECDSA_LOCALHOST_ARGS,
                                  admissiond_args=TLS_ECDSA_LOCALHOST_ARGS,
                                  start_args="--enable_admission_service "
                                             "--enable_statestored_ha "
                                             "--enable_catalogd_ha "
                                             "--enable_external_fe_support")
class TestServerTlsECDSA(BaseTestServerTls):
  """
     Spins up a cluster configured to use TLS with ECDSA certificates and runs webserver
     and Thrift server TLS tests.
  """

  @classmethod
  def ca_file(cls):
    return ECDSA_CA_FILE

  def _ciphersuite_filter(self, vector):
    """
       Filters TLS ciphersuites to only include ECDSA ciphersuites.
    """
    return lambda ciph: self._get_tls_ver_str(vector) == TLS_VER_13 or "ECDSA" in ciph
