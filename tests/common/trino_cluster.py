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

# Utilities for starting/stopping the Trino Docker container used by the
# Impala <-> Trino interop tests, and for running queries against it.
#
# Trino runs in the 'impala-minicluster-trino' container built by
# testdata/bin/build-trino-docker-image.sh and started by
# testdata/bin/run-trino.sh. It is configured against the same HMS + HDFS as the
# Impala minicluster (see testdata/bin/TRINO-README.md), so Iceberg tables created
# by either engine are visible to the other. Trino does not speak the HiveServer2
# protocol, so we drive it through the Trino CLI that ships inside the container
# via 'docker exec', which keeps the test dependencies to just Docker.

import json
import logging
import os
import socket
import subprocess
import time

IMPALA_HOME = os.environ['IMPALA_HOME']
LOG = logging.getLogger('impala_test_suite')

# Defaults mirror testdata/bin/run-trino.sh, build-trino-docker-image.sh and the
# minicluster_trino Dockerfile (the built image is tagged like the container name).
TRINO_CONTAINER_NAME = os.getenv('IMPALA_TRINO_CONTAINER', 'impala-minicluster-trino')
TRINO_IMAGE = os.getenv('IMPALA_TRINO_IMAGE', 'impala-minicluster-trino')
TRINO_SERVER = os.getenv('IMPALA_TRINO_SERVER', 'localhost:9091')
# Trino's Iceberg catalog wired to our HiveCatalog/HMS (see iceberg.properties).
# Note: default session properties (e.g. disabling the DELTA_LENGTH_BYTE_ARRAY
# Parquet encoding that Impala cannot read yet) are applied server-side via the
# image's session-property-config, not per query here.
DEFAULT_CATALOG = 'iceberg'


class TrinoUnavailable(Exception):
  """Raised when the Trino container cannot be started or reached (e.g. Docker is
  not usable or the image has not been built). Callers typically translate this
  into a pytest.skip()."""
  pass


class TrinoCluster(object):
  """Lifecycle + query helper for the Trino Docker container. Modeled on
  tests/common/iceberg_rest_server.py."""

  def __init__(self, container=TRINO_CONTAINER_NAME, server=TRINO_SERVER,
               image=TRINO_IMAGE):
    self.container = container
    self.server = server
    self.image = image
    # True only if start() actually started the container, so teardown does not
    # stop a container that a developer had already running.
    self.started_by_us = False

  @staticmethod
  def _container_status(container):
    """Returns the docker container status string ('running', 'exited', ...) or
    None if the container does not exist / docker is unavailable."""
    try:
      out = subprocess.check_output(
          ['docker', 'inspect', '--format', '{{.State.Status}}', container],
          stderr=subprocess.STDOUT, universal_newlines=True)
      return out.strip()
    except Exception:
      return None

  @classmethod
  def is_container_running(cls, container=TRINO_CONTAINER_NAME):
    return cls._container_status(container) == 'running'

  def _image_exists(self):
    try:
      subprocess.check_output(['docker', 'image', 'inspect', self.image],
          stderr=subprocess.STDOUT, universal_newlines=True)
      return True
    except Exception:
      return False

  def _build_image(self):
    """Build the Trino Docker image via build-trino-docker-image.sh. This is done
    lazily (only when the image is missing) so that non-interop workflows never
    pay the cost; layer caching makes subsequent builds cheap. Raises
    TrinoUnavailable on failure so callers can skip."""
    script = os.path.join(IMPALA_HOME, 'testdata/bin/build-trino-docker-image.sh')
    LOG.info("Building Trino Docker image '%s' via %s", self.image, script)
    try:
      subprocess.check_call([script], cwd=IMPALA_HOME, close_fds=True)
    except (subprocess.CalledProcessError, OSError) as e:
      raise TrinoUnavailable(
          "Could not build Trino image via {0}: {1}".format(script, e))

  def start(self, timeout_s=180):
    """Ensure the Trino container is running and serving queries. Idempotent: if
    the container is already up it is left as-is. Raises TrinoUnavailable if the
    container cannot be started (e.g. Docker unavailable or image not built)."""
    status = self._container_status(self.container)
    if status == 'running':
      self.started_by_us = False
    elif status in ('exited', 'created', 'paused'):
      # A previous run (or 'kill-trino.sh', which only stops the container)
      # left it around; just start it again rather than recreating it.
      try:
        subprocess.check_call(['docker', 'start', self.container], close_fds=True)
        self.started_by_us = True
      except (subprocess.CalledProcessError, OSError) as e:
        raise TrinoUnavailable(
            "Could not 'docker start' container '{0}': {1}".format(self.container, e))
    else:
      # No such container: build the image if it is missing, then create the
      # container via the run script.
      if not self._image_exists():
        self._build_image()
      script = os.path.join(IMPALA_HOME, 'testdata/bin/run-trino.sh')
      try:
        subprocess.check_call([script], cwd=IMPALA_HOME, close_fds=True)
        self.started_by_us = True
      except (subprocess.CalledProcessError, OSError) as e:
        raise TrinoUnavailable(
            "Could not start Trino container via {0}: {1}".format(script, e))
    self._wait_until_ready(timeout_s)

  def stop(self):
    """Best-effort stop of the container. Never raises."""
    script = os.path.join(IMPALA_HOME, 'testdata/bin/kill-trino.sh')
    try:
      subprocess.call([script], cwd=IMPALA_HOME, close_fds=True)
    except Exception as e:
      LOG.warning("Error while stopping Trino container '%s': %s", self.container, e)

  def _wait_until_ready(self, timeout_s):
    deadline = time.time() + timeout_s
    host, _, port = self.server.partition(':')
    port = int(port)
    # Phase 1: wait for the coordinator to accept TCP connections.
    while time.time() < deadline:
      with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        if s.connect_ex((host, port)) == 0:
          break
      time.sleep(0.5)
    # Phase 2: wait until the coordinator can actually answer a trivial query
    # (catalogs registered, workers available).
    last_err = ''
    while time.time() < deadline:
      _, stderr, rc = self.run_query("SELECT 1", catalog=None, schema=None,
                                     output_format='TSV')
      if rc == 0:
        LOG.info("Trino container '%s' is ready.", self.container)
        return
      last_err = stderr
      time.sleep(1)
    raise TrinoUnavailable(
        "Trino did not become ready within {0}s. Last error:\n{1}".format(
            timeout_s, last_err))

  def run_query(self, sql, catalog=DEFAULT_CATALOG, schema='default',
                output_format='JSON', user=None, timeout_s=120):
    """Run a single SQL statement through the containerized Trino CLI. Returns a
    (stdout, stderr, returncode) tuple; does NOT raise on query error so callers
    can decide how to report failures."""
    cmd = ['docker', 'exec', self.container, 'trino',
           '--server=' + self.server, '--output-format=' + output_format]
    if catalog:
      cmd.append('--catalog=' + catalog)
    if schema:
      cmd.append('--schema=' + schema)
    if user:
      cmd.append('--user=' + user)
    cmd += ['--execute', sql]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            stdin=subprocess.DEVNULL, universal_newlines=True)
    try:
      stdout, stderr = proc.communicate(timeout=timeout_s)
    except subprocess.TimeoutExpired:
      proc.kill()
      stdout, stderr = proc.communicate()
      return stdout, "Trino query timed out after {0}s\n{1}".format(timeout_s, stderr), 1
    return stdout, stderr, proc.returncode


def _format_trino_value(value):
  """Format a single JSON-decoded Trino value using the same textual convention as
  Impala RESULTS lines: SQL NULL -> bare 'NULL'; booleans -> 'true'/'false';
  integers -> bare; strings (and everything Trino emits as a JSON string, e.g.
  DECIMAL/DOUBLE/TIMESTAMP, plus complex values) -> single-quoted with embedded
  single quotes doubled."""
  if value is None:
    return 'NULL'
  if isinstance(value, bool):
    return 'true' if value else 'false'
  if isinstance(value, int):
    return str(value)
  if isinstance(value, float):
    return repr(value)
  if isinstance(value, str):
    return "'%s'" % value.replace("'", "''")
  # Arrays / objects: serialize compactly and quote so they compare as a string.
  return "'%s'" % json.dumps(value, sort_keys=True).replace("'", "''")


def parse_trino_json_output(stdout):
  """Parse Trino '--output-format JSON' output (one JSON object per row, keys in
  select order) into (column_labels, rows) where each row is a comma-joined string
  in the Impala RESULTS convention (see _format_trino_value)."""
  labels = []
  rows = []
  for line in stdout.splitlines():
    line = line.strip()
    if not line:
      continue
    # object_pairs_hook=list preserves column order and any duplicate names.
    pairs = json.loads(line, object_pairs_hook=list)
    if not labels:
      labels = [key for key, _ in pairs]
    rows.append(','.join(_format_trino_value(val) for _, val in pairs))
  return labels, rows


class TrinoQueryResult(object):
  """Minimal result object for a Trino query exposing the subset of attributes the
  test framework relies on (mirrors what ImpalaConnection results provide to
  run_test_case and the result verifiers): 'query', 'success', 'log',
  'column_labels' and 'data' (rows formatted in the Impala RESULTS convention)."""

  def __init__(self, query, stdout, stderr, returncode):
    self.query = query
    self.success = returncode == 0
    self.log = stderr or ''
    self.column_labels = []
    self.data = []
    if self.success and stdout.strip():
      self.column_labels, self.data = parse_trino_json_output(stdout)
