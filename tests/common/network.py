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

# Tools for identifying network characteristics.

from __future__ import absolute_import, division, print_function
import socket
import ssl
import subprocess


# Retrieves the host external IP rather than localhost/127.0.0.1 so we have an IP that
# Impala will consider distinct from storage backends to force remote scheduling.
def get_external_ip():
  with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
    s.settimeout(0)
    # This address is used to get the networking stack to identify a return IP address.
    # Timeout=0 means it doesn't need to resolve.
    s.connect(('10.254.254.254', 1))
    return s.getsockname()[0]


def split_host_port(host_port):
  """Checks if the host name also contains a port and separates the two.
  Returns either (host, None) or (host, port). Detects if host is an ipv6 address
  like "[::]" and removes the brackets from it.
  """
  is_ipv6_address = host_port[0] == "["
  if is_ipv6_address:
    parts = host_port[1:].split("]")
    if len(parts) == 1 or not parts[1]:
      return (parts[0], None)
    return (parts[0], int(parts[1][1:]))
  else:
    parts = host_port.split(":")
    if len(parts) == 1:
      return (parts[0], None)
    return (parts[0], int(parts[1]))


def to_host_port(host, port):
  is_ipv6_address = ":" in host
  fmt = "[{0}]:{1}" if is_ipv6_address else "{0}:{1}"
  return fmt.format(host, port)


def run_openssl_cmd(args, timeout_s=3):
  """
     Runs the 'openssl' command with the provided args array as a subprocess. Force closes
     STDIN without writing any data to it which avoids openssl commands hanging due to
     waiting for input.

     Args:
       args: List of strings forming the arguments to the 'openssl' command.
       timeout_s: Optional timeout in seconds. If provided and exceeded, the process is
                  killed.

     Returns: Tuple containing the return code and the combined stdout and stderr output
              as a string.
  """

  assert args[0] != "openssl", "'openssl' command must not be included in the args list."
  args = ["openssl"] + args

  proc = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
      stderr=subprocess.STDOUT, universal_newlines=True)

  try:
    if timeout_s is None:
      stdout, stderr = proc.communicate(input="")
    else:
      stdout, stderr = proc.communicate(input="", timeout=timeout_s)
  except subprocess.TimeoutExpired as e:
    # TimeoutExpired exceptions usually indicate the server port does not support TLS.
    proc.kill()
    stdout = proc.communicate()
    raise e

  out = stdout
  if not isinstance(out, str):
    out = out.decode("utf-8", "replace")

  return proc.returncode, out


def __get_openssl_supported_ciphers(tls_flag):
  """
     Note: Do not call this function directly. Use the OPENSSL_TLS_1_2_CIPHERSUITES and
     OPENSSL_TLS_1_3_CIPHERSUITES variables instead which are initialized at module load.

     Returns a sorted list of ciphersuites supported by the OS OpenSSL for the given
     TLS version flag. Allowed values for tls_flag are '-tls1_2' and '-tls1_3'.

     If querying openssl fails, returns an empty list.
  """
  rc, out = run_openssl_cmd(["ciphers", "-s", tls_flag])
  if rc != 0:
    return []
  ciphers = set([c.strip() for c in out.strip().split(":") if c.strip()])
  return sorted(ciphers)


OPENSSL_TLS_1_2_CIPHERSUITES = __get_openssl_supported_ciphers("-tls1_2")
OPENSSL_TLS_1_3_CIPHERSUITES = __get_openssl_supported_ciphers("-tls1_3")

CERT_TO_CA_MAP = {
  "wildcard-cert.pem": "wildcardCA.pem",
  "wildcard-san-cert.pem": "wildcardCA.pem",
  "localhost.pem": "wildcardCA.pem",
  "localhost-ecdsa.pem": "wildcardCA-ecdsa.pem",
}

REQUIRED_MIN_OPENSSL_VERSION = 0x10101000
_openssl_version_number = getattr(ssl, "OPENSSL_VERSION_NUMBER", None)
if _openssl_version_number is None:
  SKIP_SSL_MSG = "Legacy OpenSSL module detected"
elif _openssl_version_number < REQUIRED_MIN_OPENSSL_VERSION:
  SKIP_SSL_MSG = "Only have OpenSSL version %X, but test requires %X" % (
    ssl.OPENSSL_VERSION_NUMBER, REQUIRED_MIN_OPENSSL_VERSION)
else:
  SKIP_SSL_MSG = ""
