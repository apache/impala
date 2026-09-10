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

# End-to-end tests for the distributed (Redis/Valkey) HBO cache backend, selected with
# --hbo_cache_backend=redis. A Valkey container backs the cache (started/stopped by
# CustomClusterTestSuite via run_valkey=True; see tests/common/valkey_cluster.py). The
# suites here verify the properties that distinguish the distributed backend from the
# default in-memory one:
#   - parity: it yields the same HBO cardinalities as the in-memory backend;
#   - persistence: HBO stats outlive a coordinator restart (the in-memory cache would be
#     lost);
#   - sharing: a coordinator sees HBO stats written by a *different* coordinator;
#   - graceful degradation: when the backend is unreachable, queries still succeed with
#     no HBO stats (HBO is an optimization, never a correctness dependency).
#
# Like the Trino interop tests, the container is started per-test-class and an
# unavailable Valkey/Docker fails the test rather than skipping silently.

from __future__ import absolute_import, division, print_function

import time
import uuid
import pytest

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.hbo_test_util import HboTestMixin, QUERY_OPTIONS
# Aliased to a non-"Test" name so pytest does not re-collect its cases here
from tests.query_test.test_hbo import TestHBO as _InMemoryHboSuite

# impalad flags selecting the Redis/Valkey backend against the container that run_valkey
# brings up on localhost:6379 (the container uses --network=host; see run-valkey.sh).
REDIS_IMPALAD_ARGS = (
    "--hbo_cache_backend=redis "
    "--hbo_cache_redis_host=localhost "
    "--hbo_cache_redis_port=6379 "
    "--hbo_cache_redis_timeout_ms=2000")

# A scan+agg query template whose scan node records HBO stats. The string literal is made
# unique per test run so that a rerun against a still-populated cache does not match stats
# an earlier run recorded (mirrors the marker trick in tests/query_test/test_hbo.py).
_COUNT_QUERY = ("select count(*) from functional.alltypes "
                "where year=2009 and int_col=1 and string_col='{0}'")


class TestHboRedis(HboTestMixin, CustomClusterTestSuite):
  """Parity and cross-coordinator sharing for the Redis/Valkey HBO backend. Two
  coordinators share one Valkey container, so neither test restarts the cluster."""

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args=REDIS_IMPALAD_ARGS, run_valkey=True)
  def test_parity_with_in_memory_backend(self):
    """Replay the entire in-memory HBO suite against the Redis backend."""
    self.valkey.flush_all()
    # Iterate the test_* methods declared directly on TestHBO, sorted for a stable order.
    for name in sorted(_InMemoryHboSuite.__dict__):
      method = getattr(_InMemoryHboSuite, name)
      if name.startswith('test_') and callable(method):
        # The methods are bound to this Redis-backed instance, so self.client and
        # self.execute_query talk to the Redis cache.
        method(self)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      cluster_size=2, impalad_args=REDIS_IMPALAD_ARGS, run_valkey=True)
  def test_stats_shared_across_coordinators(self):
    """Stats written by one coordinator are visible to another via the shared cache: run
    a query on coordinator 0, then EXPLAIN it on coordinator 1 (which never executed it)
    and see HBO-derived cardinalities."""
    self.valkey.flush_all()
    query = _COUNT_QUERY.format('redis_shared_' + uuid.uuid4().hex[:8])

    producer = self.cluster.impalads[0].service.create_hs2_client()
    try:
      self.execute_query_expect_success(producer, query, QUERY_OPTIONS)
    finally:
      producer.close()
    # Give the write time to land in the cache before the other coordinator reads it.
    time.sleep(1)

    consumer = self.cluster.impalads[1].service.create_hs2_client()
    try:
      result = self.execute_query_expect_success(
          consumer, "explain " + query, QUERY_OPTIONS)
      assert "from HBO" in '\n'.join(result.data), '\n'.join(result.data)
    finally:
      consumer.close()


class TestHboRedisPersistence(CustomClusterTestSuite):
  """HBO stats survive a coordinator restart when stored in Redis/Valkey. Isolated in its
  own class because restarting the coordinator invalidates the shared class-level client
  used by sibling tests."""

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      cluster_size=1, impalad_args=REDIS_IMPALAD_ARGS, run_valkey=True)
  def test_stats_persist_across_coordinator_restart(self):
    self.valkey.flush_all()
    query = _COUNT_QUERY.format('redis_persist_' + uuid.uuid4().hex[:8])

    client = self.cluster.impalads[0].service.create_hs2_client()
    try:
      self.execute_query_expect_success(client, query, QUERY_OPTIONS)
    finally:
      client.close()
    # Ensure the stats are written to the cache before we drop the in-memory copy.
    time.sleep(1)

    # Restart the coordinator, discarding its in-memory HBO cache. Only Valkey retains the
    # stats now.
    impalad = self.cluster.impalads[0]
    impalad.kill_and_wait_for_exit()
    impalad.start()
    impalad.service.wait_for_num_known_live_backends(len(self.cluster.impalads))

    client = self.cluster.impalads[0].service.create_hs2_client()
    try:
      result = self.execute_query_expect_success(
          client, "explain " + query, QUERY_OPTIONS)
      assert "from HBO" in '\n'.join(result.data), '\n'.join(result.data)
    finally:
      client.close()


class TestHboRedisGracefulDegradation(CustomClusterTestSuite):
  """When the Redis/Valkey backend is unreachable, HBO must fail soft: queries still
  succeed, just without HBO-derived cardinalities. No Valkey container is started; the
  coordinator points at a port where nothing listens."""

  # A port where nothing is expected to be listening, so every Redis operation fails fast
  # with connection-refused. The short timeout keeps failing operations from slowing
  # queries.
  UNREACHABLE_IMPALAD_ARGS = (
      "--hbo_cache_backend=redis "
      "--hbo_cache_redis_host=localhost "
      "--hbo_cache_redis_port=6399 "
      "--hbo_cache_redis_timeout_ms=500")

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      cluster_size=1, impalad_args=UNREACHABLE_IMPALAD_ARGS)
  def test_queries_succeed_when_backend_unreachable(self):
    self.client.set_configuration(QUERY_OPTIONS)
    query = _COUNT_QUERY.format('redis_degraded_' + uuid.uuid4().hex[:8])
    # The query must succeed even though every HBO put fails against the dead backend.
    self.execute_query(query)
    time.sleep(1)
    # And nothing was cached, so EXPLAIN shows no HBO-derived cardinalities.
    result = self.execute_query("explain " + query)
    assert "from HBO" not in '\n'.join(result.data), '\n'.join(result.data)


class TestHboRedisAuth(CustomClusterTestSuite):
  """The Redis/Valkey backend authenticates with a password obtained from
  --hbo_cache_redis_password_cmd (a Unix command whose stdout is the password, mirroring
  --ldap_bind_password_cmd). Uses a dedicated password-protected Valkey instance on its
  own container name and port so it never clashes with the default passwordless one used
  by the sibling classes."""

  # Credentials for the password-protected instance: the container requires _PASSWORD
  # (--requirepass) and impalad obtains the same password by running the command flag.
  _PASSWORD = "hbo-secret-pw"
  _PORT = 6380
  _CONTAINER = "impala-minicluster-valkey-auth"

  # impalad points at the auth instance and gets the password from a command.
  AUTH_IMPALAD_ARGS = (
      "--hbo_cache_backend=redis "
      "--hbo_cache_redis_host=localhost "
      "--hbo_cache_redis_port=%d "
      "--hbo_cache_redis_password_cmd='echo -n %s' "
      "--hbo_cache_redis_timeout_ms=2000" % (_PORT, _PASSWORD))

  # Same instance, but impalad is handed the wrong password. Every authenticated Redis op
  # is rejected, so HBO must fail soft (queries succeed, no HBO-derived cardinalities).
  WRONG_PASSWORD_IMPALAD_ARGS = (
      "--hbo_cache_backend=redis "
      "--hbo_cache_redis_host=localhost "
      "--hbo_cache_redis_port=%d "
      "--hbo_cache_redis_password_cmd='echo -n wrong-%s' "
      "--hbo_cache_redis_timeout_ms=2000" % (_PORT, _PASSWORD))

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      cluster_size=1, impalad_args=AUTH_IMPALAD_ARGS, run_valkey=True,
      valkey_container=_CONTAINER, valkey_port=_PORT, valkey_password=_PASSWORD)
  def test_password_cmd_auth(self):
    self.valkey.flush_all()
    self.client.set_configuration(QUERY_OPTIONS)
    query = _COUNT_QUERY.format('redis_auth_' + uuid.uuid4().hex[:8])
    self.execute_query(query)
    time.sleep(1)
    result = self.execute_query("explain " + query)
    assert "from HBO" in '\n'.join(result.data), '\n'.join(result.data)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      cluster_size=1, impalad_args=WRONG_PASSWORD_IMPALAD_ARGS, run_valkey=True,
      valkey_container=_CONTAINER, valkey_port=_PORT, valkey_password=_PASSWORD)
  def test_wrong_password_degrades_gracefully(self):
    """A wrong password makes every Redis op fail authentication. HBO must fail soft: the
    query still succeeds and EXPLAIN shows no HBO-derived cardinalities."""
    self.valkey.flush_all()
    self.client.set_configuration(QUERY_OPTIONS)
    query = _COUNT_QUERY.format('redis_badauth_' + uuid.uuid4().hex[:8])
    self.execute_query(query)
    time.sleep(1)
    result = self.execute_query("explain " + query)
    assert "from HBO" not in '\n'.join(result.data), '\n'.join(result.data)
