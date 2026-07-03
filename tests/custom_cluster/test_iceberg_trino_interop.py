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

# Interop tests between Impala and Trino over Apache Iceberg V3 tables.
#
# Trino runs in the 'impala-minicluster-trino' Docker container (see
# testdata/bin/TRINO-README.md) and is configured against the same HMS + HDFS as
# the Impala minicluster, so Iceberg tables created by either engine are visible
# to the other. The tests are driven by .test files that mix Impala QUERY/RESULTS
# sections with TRINO_QUERY/RESULTS sections (see run_test_case).
#
# These tests only run in exhaustive mode and are skipped when the Trino container
# (or Docker) is not available, or when not running on HDFS.

from __future__ import absolute_import, division, print_function

import pytest

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIf


# Share a single Impala cluster (and Trino container) across all test methods in the
# class; the interop tests do not need any special impalad flags. run_trino=True makes
# CustomClusterTestSuite start the Trino container before the Impala cluster and stop it
# on teardown (only if this class started it). Any failure to start Trino (e.g. Docker
# or the image being unavailable) fails the tests rather than skipping, so the problem
# is visible instead of silently skipped.
@SkipIf.not_hdfs
@CustomClusterTestSuite.with_args(cluster_size=3, run_trino=True)
class TestIcebergTrinoInterop(CustomClusterTestSuite):
  """Impala <-> Trino interop tests for Iceberg V3 (INSERT, deletion-vector
  DELETE/UPDATE/MERGE, column default values)."""

  @pytest.mark.execute_serially
  def test_insert(self, vector, unique_database):
    """INSERT/SELECT round-trips of Iceberg V3 tables between the two engines."""
    self.run_test_case('QueryTest/iceberg-trino-interop-insert', vector,
                       use_db=unique_database)

  @pytest.mark.execute_serially
  def test_delete_update_merge_deletion_vectors(self, vector, unique_database):
    """DELETE/UPDATE/MERGE round-trips that both engines materialize as Puffin
    deletion vectors."""
    self.run_test_case('QueryTest/iceberg-trino-interop-delete-dv', vector,
                       use_db=unique_database)

  @pytest.mark.execute_serially
  def test_default_values(self, vector, unique_database):
    """Iceberg V3 column default values written by one engine and read by the
    other."""
    self.run_test_case('QueryTest/iceberg-trino-interop-default-values', vector,
                       use_db=unique_database)
