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

# Utilities for starting/stopping the Valkey (Redis-compatible) Docker container that
# backs the distributed HBO cache in custom-cluster tests.

import logging
import os
import socket
import subprocess
import time

IMPALA_HOME = os.environ['IMPALA_HOME']
LOG = logging.getLogger('impala_test_suite')

# Defaults mirror testdata/bin/run-valkey.sh and kill-valkey.sh.
VALKEY_CONTAINER_NAME = os.getenv('IMPALA_VALKEY_CONTAINER',
                                  'impala-minicluster-valkey')
VALKEY_IMAGE = os.getenv('IMPALA_TEST_VALKEY_IMAGE', 'valkey/valkey:8')
# The container runs with --network=host so it listens on localhost:6379
VALKEY_HOST = os.getenv('IMPALA_VALKEY_HOST', 'localhost')
VALKEY_PORT = int(os.getenv('IMPALA_VALKEY_PORT', '6379'))
# Optional password; when set the server is started with --requirepass and every probe
# authenticates first. Default '' means no authentication (the common case).
VALKEY_PASSWORD = os.getenv('IMPALA_VALKEY_PASSWORD', '')


class ValkeyUnavailable(Exception):
  """Raised when the Valkey container cannot be started or reached (e.g. Docker is not
  usable or the image cannot be pulled). Callers typically translate this into a
  pytest.skip()."""
  pass


class ValkeyCluster(object):
  """Lifecycle helper for the Valkey Docker container."""

  def __init__(self, container=VALKEY_CONTAINER_NAME, host=VALKEY_HOST,
               port=VALKEY_PORT, image=VALKEY_IMAGE, password=VALKEY_PASSWORD):
    self.container = container
    self.host = host
    self.port = port
    self.image = image
    # When non-empty, the server requires this password (--requirepass) and every probe
    # authenticates with it before issuing a command.
    self.password = password
    # True only if start() actually started the container, so teardown does not stop a
    # container that a developer had already running.
    self.started_by_us = False

  @staticmethod
  def _container_status(container):
    """Returns the docker container status string ('running', 'exited', ...) or None if
    the container does not exist / docker is unavailable."""
    try:
      out = subprocess.check_output(
          ['docker', 'inspect', '--format', '{{.State.Status}}', container],
          stderr=subprocess.STDOUT, universal_newlines=True)
      return out.strip()
    except Exception:
      return None

  @classmethod
  def is_container_running(cls, container=VALKEY_CONTAINER_NAME):
    return cls._container_status(container) == 'running'

  def start(self, timeout_s=60):
    """Ensure the Valkey container is running and answering PING. Idempotent: if the
    container is already up it is left as-is. Raises ValkeyUnavailable if the container
    cannot be started (e.g. Docker unavailable or the image cannot be pulled)."""
    status = self._container_status(self.container)
    if status == 'running':
      self.started_by_us = False
    elif status in ('exited', 'created', 'paused'):
      # A previous run (or kill-valkey.sh, which only stops the container) left it
      # around; just start it again rather than recreating it.
      try:
        subprocess.check_call(['docker', 'start', self.container], close_fds=True)
        self.started_by_us = True
      except (subprocess.CalledProcessError, OSError) as e:
        raise ValkeyUnavailable(
            "Could not 'docker start' container '{0}': {1}".format(self.container, e))
    else:
      # No such container: create it via the run script (which pulls the image). Pass
      # this instance's container/port/password so a non-default (e.g. password-auth)
      # instance can be brought up alongside the default passwordless one.
      script = os.path.join(IMPALA_HOME, 'testdata/bin/run-valkey.sh')
      env = dict(os.environ)
      env['IMPALA_VALKEY_CONTAINER'] = self.container
      env['IMPALA_VALKEY_PORT'] = str(self.port)
      env['IMPALA_VALKEY_PASSWORD'] = self.password
      try:
        subprocess.check_call([script], cwd=IMPALA_HOME, env=env, close_fds=True)
        self.started_by_us = True
      except (subprocess.CalledProcessError, OSError) as e:
        raise ValkeyUnavailable(
            "Could not start Valkey container via {0}: {1}".format(script, e))
    self._wait_until_ready(timeout_s)

  def stop(self):
    """Best-effort stop of the container. Never raises."""
    script = os.path.join(IMPALA_HOME, 'testdata/bin/kill-valkey.sh')
    # Pass this instance's container name so a non-default (e.g. password-auth) instance
    # is stopped rather than the default one.
    env = dict(os.environ)
    env['IMPALA_VALKEY_CONTAINER'] = self.container
    try:
      subprocess.call([script], cwd=IMPALA_HOME, env=env, close_fds=True)
    except Exception as e:
      LOG.warning("Error while stopping Valkey container '%s': %s", self.container, e)

  def _container_logs_tail(self, max_lines=50):
    """Best-effort tail of the container's stdout/stderr, for embedding in error
    messages."""
    try:
      out = subprocess.check_output(
          ['docker', 'logs', '--tail', str(max_lines), self.container],
          stderr=subprocess.STDOUT, universal_newlines=True)
      return out.strip() or "(container produced no log output yet)"
    except Exception as e:
      return "(could not read 'docker logs {0}': {1})".format(self.container, e)

  def _send_command(self, *args, **kwargs):
    """Send one command over a fresh RESP connection and return the raw first chunk of
    the reply, or None if the server cannot be reached. Uses the inline command form
    (space-separated tokens terminated by CRLF), which Redis/Valkey accept for the
    simple, argument-free commands used here (PING, FLUSHALL). Probing over a raw
    socket keeps the harness independent of which CLI binary (valkey-cli vs redis-cli)
    the image ships. When a password is configured, AUTH is sent first on the same
    connection; if it is rejected None is returned (the reply is not +OK)."""
    timeout_s = kwargs.pop('timeout_s', 2)
    try:
      with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(timeout_s)
        s.connect((self.host, self.port))
        if self.password:
          s.sendall(('AUTH ' + self.password + '\r\n').encode('ascii'))
          if not s.recv(256).startswith(b'+OK'):
            return None
        s.sendall((' '.join(args) + '\r\n').encode('ascii'))
        return s.recv(256)
    except OSError:
      return None

  def ping(self):
    """Return True if the server answers PING with +PONG."""
    reply = self._send_command('PING')
    return reply is not None and reply.startswith(b'+PONG')

  def flush_all(self):
    """Delete all keys on the server (RESP FLUSHALL), to isolate test phases. Raises
    ValkeyUnavailable if the server does not acknowledge."""
    reply = self._send_command('FLUSHALL')
    if reply is None or not reply.startswith(b'+OK'):
      raise ValkeyUnavailable(
          "FLUSHALL on {0}:{1} was not acknowledged (reply: {2!r})".format(
              self.host, self.port, reply))

  def _wait_until_ready(self, timeout_s):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
      if self.ping():
        LOG.info("Valkey container '%s' is ready at %s:%d.", self.container,
                 self.host, self.port)
        return
      time.sleep(0.5)
    raise ValkeyUnavailable(
        "Valkey did not answer PING on {0}:{1} within {2}s; it likely failed to "
        "start. Last {3} lines of 'docker logs {4}':\n{5}".format(
            self.host, self.port, timeout_s, 50, self.container,
            self._container_logs_tail()))
