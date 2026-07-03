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

# Minimal Impala <-> Trino interop tests over legacy (non-Iceberg) Hive tables.
#
# Trino runs in the 'impala-minicluster-trino' Docker container (see
# testdata/bin/TRINO-README.md) configured against the same HMS + HDFS as the Impala
# minicluster, and exposes legacy Hive tables through its 'hive' catalog. These tests
# check both directions:
#   - Trino reads databases/tables that Impala (via HMS) exposes, including the standard
#     'functional' database.
#   - Impala reads a database + table that Trino creates and writes.
#
# The Trino container is started/stopped by CustomClusterTestSuite via run_trino=True.
# The tests are skipped on non-HDFS filesystems; if the Trino container or Docker is
# unavailable they fail (rather than skipping silently) so the problem is visible.

from __future__ import absolute_import, division, print_function

import pytest

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIf
from tests.common.trino_cluster import parse_trino_json_output
from tests.util.event_processor_utils import EventProcessorUtils


# Share a single Impala cluster + Trino container across the class (see run_trino=True).
@SkipIf.not_hdfs
@CustomClusterTestSuite.with_args(cluster_size=3, run_trino=True)
class TestTrinoInterop(CustomClusterTestSuite):
  """Impala <-> Trino interop over legacy Hive tables (Trino's 'hive' catalog)."""

  # Trino catalog wired to our HMS for legacy (non-Iceberg) Hive tables.
  HIVE_CATALOG = 'hive'

  def _trino_rows(self, stmt, schema='default'):
    """Run a statement in Trino's 'hive' catalog and return its result rows in the
    Impala RESULTS textual convention (strings single-quoted, numbers/booleans bare).
    Raises RuntimeError on a Trino error."""
    stdout = self.run_stmt_in_trino(stmt, catalog=self.HIVE_CATALOG, schema=schema)
    _labels, rows = parse_trino_json_output(stdout)
    return rows

  @pytest.mark.execute_serially
  def test_trino_reads_impala_objects(self, unique_database):
    """Trino sees databases/tables that Impala exposes via the shared HMS: the standard
    'functional' database (a small legacy text table), and a fresh table Impala just
    created -- with no explicit refresh needed on Trino's side."""
    # (a) The pre-existing 'functional' database and its legacy text table are visible,
    #     and Trino reads the same row count Impala does.
    impala_count = self.execute_scalar("SELECT count(*) FROM functional.alltypestiny")
    trino_count = self._trino_rows("SELECT count(*) FROM alltypestiny",
                                   schema="functional")[0]
    assert trino_count == impala_count, \
        "Trino count {0} != Impala count {1}".format(trino_count, impala_count)

    # (b) A table Impala creates in a fresh database is immediately visible to Trino.
    self.execute_query(
        "CREATE TABLE {0}.t (id INT, s STRING) STORED AS PARQUET".format(unique_database))
    self.execute_query(
        "INSERT INTO {0}.t VALUES (1, 'a'), (2, 'b'), (3, 'c')".format(unique_database))

    # The new schema shows up in Trino's 'hive' catalog...
    show_schemas = self.run_stmt_in_trino("SHOW SCHEMAS", catalog=self.HIVE_CATALOG)
    assert unique_database in show_schemas, \
        "Impala database {0} not visible to Trino:\n{1}".format(
            unique_database, show_schemas)

    # ...and Trino reads back exactly the rows Impala wrote.
    rows = self._trino_rows("SELECT id, s FROM t ORDER BY id", schema=unique_database)
    assert rows == ["1,'a'", "2,'b'", "3,'c'"], rows

  @pytest.mark.execute_serially
  def test_impala_reads_trino_objects(self, unique_name):
    """Trino creates its own database + legacy Hive table and writes rows; Impala picks
    them up from the HMS events (no explicit INVALIDATE), since Impala must learn about
    HMS objects created out-of-band."""
    # unique_name gives a collision-free schema name for parallel runs without creating
    # any Impala-side database (this schema is created and dropped by Trino below, so we
    # deliberately do not use the unique_database fixture).
    trino_db = unique_name
    try:
      self.run_stmt_in_trino("CREATE SCHEMA {0}.{1}".format(self.HIVE_CATALOG, trino_db),
                             catalog=self.HIVE_CATALOG)
      # Create an empty table and populate it with a separate INSERT. Our HMS metadata
      # transformer converts the MANAGED table Trino requests into an EXTERNAL one
      # (Trino does not advertise Hive ACID write capabilities); the image sets
      # hive.non-managed-table-writes-enabled=true so Trino can still INSERT into it. ORC
      # is used (not Parquet) because Trino's Parquet writer emits DELTA_LENGTH_BYTE_ARRAY
      # for string columns, which Impala cannot read yet -- and the hive connector,
      # unlike iceberg, exposes no session property to disable it.
      self.run_stmt_in_trino(
          "CREATE TABLE {0}.t (id INTEGER, s VARCHAR) WITH (format = 'ORC')".format(
              trino_db),
          catalog=self.HIVE_CATALOG)
      self.run_stmt_in_trino(
          "INSERT INTO {0}.t VALUES (1, 'x'), (2, 'y')".format(trino_db),
          catalog=self.HIVE_CATALOG)

      # Impala learns about the out-of-band HMS objects (the new schema + table) from
      # the HMS notification events, so wait for the event processor to catch up rather
      # than issuing an explicit INVALIDATE METADATA.
      EventProcessorUtils.wait_for_event_processing(self)
      result = self.execute_query(
          "SELECT id, s FROM {0}.t ORDER BY id".format(trino_db))
      assert result.data == ["1\tx", "2\ty"], result.data
    finally:
      # Clean up the Trino-created schema (unique_name only provides a name, not a DB).
      self.run_stmt_in_trino(
          "DROP SCHEMA IF EXISTS {0}.{1} CASCADE".format(self.HIVE_CATALOG, trino_db),
          catalog=self.HIVE_CATALOG)
