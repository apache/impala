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
from builtins import range
from copy import deepcopy

from kudu.schema import (
    BOOL,
    DOUBLE,
    FLOAT,
    INT16,
    INT32,
    INT64,
    INT8,
    SchemaBuilder,
    STRING,
    BINARY,
    UNIXTIME_MICROS,
    DATE)
from kudu.client import Partitioning
from kudu.util import to_unixtime_micros
import logging
import pytest
import random
import re
import textwrap
import threading
import time
from datetime import datetime
from pytz import utc

from tests.common.environ import ImpalaTestClusterProperties, HIVE_MAJOR_VERSION
from tests.common.kudu_test_suite import KuduTestSuite
from tests.common.impala_cluster import ImpalaCluster
from tests.common.skip import SkipIfNotHdfsMinicluster, SkipIfKudu, SkipIfHive2
from tests.common.test_dimensions import (
  HS2,
  add_exec_option_dimension,
  add_mandatory_exec_option,
  create_single_exec_option_dimension)
from tests.verifiers.metric_verifier import MetricVerifier

KUDU_MASTER_HOSTS = pytest.config.option.kudu_master_hosts
IMPALA_TEST_CLUSTER_PROPERTIES = ImpalaTestClusterProperties.get_instance()

LOG = logging.getLogger(__name__)


def create_external_kudu_query(db, tbl, kudu_tbl_name, other_props=None):
  props = {'kudu.table_name': kudu_tbl_name}
  if other_props is not None:
    props.update(other_props)
  props_str = ', '.join(["'{}'='{}'".format(k, v) for k, v in props.items()])
  return (
    """
    CREATE EXTERNAL TABLE {}.{}
    STORED AS KUDU
    TBLPROPERTIES ({})""").format(db, tbl, props_str)


def set_latest_observed_option(vector, kudu_client):
  vector.set_exec_option(
    'kudu_snapshot_read_timestamp_micros',
    to_unixtime_micros(kudu_client.latest_observed_timestamp()))


# TODO(IMPALA-8614): parameterize some tests to run with HMS integration enabled.
class TestKuduBasicDML(KuduTestSuite):
  """
  This suite tests the basic DML operations when using a kudu table.
  """
  @classmethod
  def add_test_dimensions(cls):
    super(TestKuduBasicDML, cls).add_test_dimensions()
    # The default read mode of READ_LATEST does not provide high enough consistency for
    # these tests.
    add_mandatory_exec_option(cls, "kudu_read_mode", "READ_AT_SNAPSHOT")
    # Run with and without multithreading to ensure Kudu DML works with both threading
    # models. E.g. see IMPALA-9782.
    add_exec_option_dimension(cls, "mt_dop", [0, 4])

  @SkipIfKudu.no_hybrid_clock()
  def test_kudu_insert(self, vector, unique_database):
    # Remove 'abort_on_error' option so we can set it at .test file.
    # Revisit this if 'abort_on_error' dimension size increase.
    vector.unset_exec_option('abort_on_error')
    self.run_test_case('QueryTest/kudu_insert', vector, use_db=unique_database)

  @SkipIfKudu.no_hybrid_clock()
  def test_kudu_update(self, vector, unique_database):
    self.run_test_case('QueryTest/kudu_update', vector, use_db=unique_database)

  @SkipIfKudu.no_hybrid_clock()
  def test_kudu_upsert(self, vector, unique_database):
    self.run_test_case('QueryTest/kudu_upsert', vector, use_db=unique_database)

  @SkipIfKudu.no_hybrid_clock()
  def test_kudu_delete(self, vector, unique_database):
    self.run_test_case('QueryTest/kudu_delete', vector, use_db=unique_database)

  @SkipIfKudu.no_hybrid_clock()
  def test_kudu_create_table_like_table(self, vector, unique_database):
    self.run_test_case(
      'QueryTest/kudu_create_table_like_table',
      vector,
      use_db=unique_database)


class TestKuduTimestampConvert(KuduTestSuite):
  """
  This suite tests converts UTC timestamps read from kudu table to local time.
  """
  @classmethod
  def add_test_dimensions(cls):
    super(TestKuduTimestampConvert, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_mandatory_exec_option('convert_kudu_utc_timestamps', 'true')
    cls.ImpalaTestMatrix.add_mandatory_exec_option('write_kudu_utc_timestamps', 'true')
    cls.ImpalaTestMatrix.add_mandatory_exec_option(
        'use_local_tz_for_unix_timestamp_conversions', 'false')
    cls.ImpalaTestMatrix.add_mandatory_exec_option('timezone', '"America/Los_Angeles"')

  @SkipIfKudu.no_hybrid_clock()
  def test_kudu_timestamp_conversion(self, vector, unique_database):
    self.run_test_case(
        'QueryTest/kudu_timestamp_conversion', vector, use_db=unique_database)

  @SkipIfKudu.no_hybrid_clock()
  def test_kudu_predicate_with_timestamp_conversion(self, vector):
    self.run_test_case('QueryTest/kudu_predicate_with_timestamp_conversion', vector)

  @SkipIfKudu.no_hybrid_clock()
  def test_kudu_runtime_filter_with_timestamp_conversion(self, vector):
    new_vector = deepcopy(vector)
    del new_vector.get_value('exec_option')['timezone']  # .test file sets timezone
    self.run_test_case('QueryTest/kudu_runtime_filter_with_timestamp_conversion',
                       new_vector)


# TODO(IMPALA-8614): parameterize some tests to run with HMS integration enabled.
class TestKuduOperations(KuduTestSuite):
  """
  This suite tests the different modification operations when using a kudu table.
  """

  @classmethod
  def add_test_dimensions(cls):
    super(TestKuduOperations, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    # The default read mode of READ_LATEST does not provide high enough consistency for
    # these tests.
    add_mandatory_exec_option(cls, "kudu_read_mode", "READ_AT_SNAPSHOT")

  @SkipIfKudu.no_hybrid_clock()
  @SkipIfKudu.hms_integration_enabled()
  def test_out_of_range_timestamps(self, vector, kudu_client, unique_database):
    """Test timestamp values that are outside of Impala's supported date range."""
    tbl = 'times'
    self.execute_query_using_vector(
      ("CREATE TABLE {}.{} (a INT PRIMARY KEY, ts TIMESTAMP)"
       "PARTITION BY HASH(a) PARTITIONS 3 STORED AS KUDU").format(unique_database, tbl),
      vector)
    assert kudu_client.table_exists(
        KuduTestSuite.to_kudu_table_name(unique_database, tbl))

    table = kudu_client.table(KuduTestSuite.to_kudu_table_name(unique_database, tbl))
    session = kudu_client.new_session()
    session.apply(table.new_insert((0, datetime(1987, 5, 19, 0, 0, tzinfo=utc))))
    # Add a date before 1400
    session.apply(table.new_insert((1, datetime(1300, 1, 1, 0, 0, tzinfo=utc))))
    # TODO: Add a date after 9999. There isn't a way to represent a date greater than
    # 9999 in Python datetime.
    # session.apply(table.new_insert((2, datetime(12000, 1, 1, 0, 0, tzinfo=utc))))
    session.flush()

    set_latest_observed_option(vector, kudu_client)
    # TODO: The test driver should have a way to specify query options in an 'options'
    # section rather than having to split abort_on_error cases into separate files.
    vector.set_exec_option('abort_on_error', 0)
    self.run_test_case('QueryTest/kudu-overflow-ts', vector,
                       use_db=unique_database)

    vector.set_exec_option('abort_on_error', 1)
    self.run_test_case('QueryTest/kudu-overflow-ts-abort-on-error', vector,
                       use_db=unique_database)

  @SkipIfKudu.no_hybrid_clock()
  def test_kudu_scan_node(self, vector, unique_database):
    self.run_test_case('QueryTest/kudu-scan-node', vector, use_db=unique_database)

  @SkipIfNotHdfsMinicluster.tuned_for_minicluster
  @SkipIfKudu.no_hybrid_clock()
  def test_kudu_insert_mem_limit(self, vector, unique_database):
    self.run_test_case('QueryTest/kudu_insert_mem_limit', vector, use_db=unique_database)

  @SkipIfKudu.no_hybrid_clock()
  def test_kudu_partition_ddl(self, vector, unique_database):
    self.run_test_case('QueryTest/kudu_partition_ddl', vector, use_db=unique_database)

  @pytest.mark.skipif(IMPALA_TEST_CLUSTER_PROPERTIES.is_remote_cluster(),
                      reason="Test references hardcoded hostnames: IMPALA-4873")
  @pytest.mark.execute_serially
  @SkipIfKudu.no_hybrid_clock()
  @SkipIfKudu.hms_integration_enabled()
  def test_kudu_alter_table(self, vector, unique_database):
    self.run_test_case('QueryTest/kudu_alter', vector, use_db=unique_database)

  @SkipIfKudu.no_hybrid_clock()
  def test_kudu_stats(self, vector, unique_database):
    self.run_test_case('QueryTest/kudu_stats', vector, use_db=unique_database)

  @SkipIfKudu.no_hybrid_clock()
  def test_kudu_describe(self, vector, unique_database):
    self.run_test_case('QueryTest/kudu_describe', vector, use_db=unique_database)

  @SkipIfKudu.no_hybrid_clock()
  def test_kudu_limit(self, vector, unique_database):
    self.run_test_case('QueryTest/kudu_limit', vector, use_db=unique_database)

  def test_kudu_column_options(self, vector, kudu_client, unique_database):
    """Test Kudu column options"""
    encodings = ["ENCODING PLAIN_ENCODING", ""]
    compressions = ["COMPRESSION SNAPPY", ""]
    nullability = ["NOT NULL", "NULL", ""]
    defaults = ["DEFAULT 1", ""]
    blocksizes = ["BLOCK_SIZE 32768", ""]
    indx = 1
    for encoding in encodings:
      for compression in compressions:
        for default in defaults:
          for blocksize in blocksizes:
            for nullable in nullability:
              impala_tbl_name = "test_column_options_%s" % str(indx)
              self.execute_query_using_vector(
                """
                CREATE TABLE {}.{} (
                  a INT PRIMARY KEY {} {} {} {},
                  b INT {} {} {} {} {}
                ) PARTITION BY HASH (a) PARTITIONS 3 STORED AS KUDU""".format(
                  unique_database, impala_tbl_name,
                  encoding, compression, default, blocksize,
                  nullable, encoding, compression, default, blocksize),
                vector)
              indx = indx + 1
              assert kudu_client.table_exists(
                  KuduTestSuite.to_kudu_table_name(unique_database, impala_tbl_name))

  def test_kudu_col_changed(self, vector, kudu_client, unique_database,
                            cluster_properties):
    """Test changing a Kudu column outside of Impala results in a failure on read with
       outdated metadata (IMPALA-4828)."""
    tbl = 'foo'
    db_tbl = unique_database + '.' + tbl
    self.execute_query_using_vector(
      """
      CREATE TABLE {}.{} (a INT PRIMARY KEY, s STRING)
      PARTITION BY HASH(a) PARTITIONS 3 STORED AS KUDU""".format(unique_database, tbl),
      vector)
    assert kudu_client.table_exists(
        KuduTestSuite.to_kudu_table_name(unique_database, tbl))

    # Force metadata to be loaded on impalads
    self.execute_query_using_vector("select * from %s" % db_tbl, vector)

    # Load the table via the Kudu client and change col 's' to be a different type.
    table = kudu_client.table(KuduTestSuite.to_kudu_table_name(unique_database, tbl))
    alterer = kudu_client.new_table_alterer(table)
    alterer.drop_column("s")
    table = alterer.alter()
    alterer = kudu_client.new_table_alterer(table)
    alterer.add_column("s", "int32")
    table = alterer.alter()

    # Add some rows
    session = kudu_client.new_session()
    for i in range(100):
      op = table.new_insert((i, i))
      session.apply(op)
    session.flush()

    set_latest_observed_option(vector, kudu_client)
    # Scanning should result in an error with Catalog V1, since the metadata is cached.
    try:
      self.execute_query_using_vector("SELECT * FROM %s" % db_tbl, vector)
      assert cluster_properties.is_catalog_v2_cluster(), \
          "Should fail with Catalog V1, which caches metadata"
    except Exception as e:
      assert not cluster_properties.is_catalog_v2_cluster(), \
          "Should succeed with Catalog V2, which does not cache metadata"
      expected_error = "Column 's' is type INT but Impala expected STRING. The table "\
          "metadata in Impala may be outdated and need to be refreshed."
      assert expected_error in str(e)

    # After a REFRESH the scan should succeed
    self.execute_query_using_vector("REFRESH %s" % db_tbl, vector)
    result = self.execute_query_using_vector("SELECT * FROM %s" % db_tbl, vector)
    assert len(result.data) == 100

  def test_kudu_col_not_null_changed(self, vector, kudu_client, unique_database,
                                     cluster_properties):
    """Test changing a NOT NULL Kudu column outside of Impala results in a failure
       on read with outdated metadata (IMPALA-4828)."""
    tbl = 'foo'
    db_tbl = unique_database + '.' + tbl
    self.execute_query_using_vector(
      """CREATE TABLE %s (a INT PRIMARY KEY, s STRING NOT NULL)
      PARTITION BY HASH(a) PARTITIONS 3 STORED AS KUDU""" % db_tbl,
      vector)
    assert kudu_client.table_exists(
        KuduTestSuite.to_kudu_table_name(unique_database, tbl))

    # Force metadata to be loaded on impalads
    self.execute_query_using_vector("select * from %s" % db_tbl, vector)

    # Load the table via the Kudu client and change col 's' to be a different type.
    table = kudu_client.table(KuduTestSuite.to_kudu_table_name(unique_database, tbl))
    alterer = kudu_client.new_table_alterer(table)
    alterer.drop_column("s")
    table = alterer.alter()
    alterer = kudu_client.new_table_alterer(table)
    alterer.add_column("s", "string", nullable=True)
    table = alterer.alter()

    # Add some rows
    session = kudu_client.new_session()
    for i in range(100):
      op = table.new_insert((i, None))
      session.apply(op)
    session.flush()

    set_latest_observed_option(vector, kudu_client)
    # Scanning should result in an error
    try:
      self.execute_query_using_vector("SELECT * FROM %s" % db_tbl, vector)
      assert cluster_properties.is_catalog_v2_cluster(), \
          "Should fail with Catalog V1, which caches metadata"
    except Exception as e:
      assert not cluster_properties.is_catalog_v2_cluster(), \
          "Should succeed with Catalog V2, which does not cache metadata"
      expected_error = "Column 's' is nullable but Impala expected it to be "\
          "not nullable. The table metadata in Impala may be outdated and need to be "\
          "refreshed."
      assert expected_error in str(e)

    # After a REFRESH the scan should succeed
    self.execute_query_using_vector("REFRESH %s" % db_tbl, vector)
    result = self.execute_query_using_vector("SELECT * FROM %s" % db_tbl, vector)
    assert len(result.data) == 100

  def test_kudu_col_null_changed(self, vector, kudu_client, unique_database,
                                 cluster_properties):
    """Test changing a NULL Kudu column outside of Impala results in a failure
       on read with outdated metadata (IMPALA-4828)."""
    tbl = 'foo'
    db_tbl = unique_database + '.' + tbl
    self.execute_query_using_vector(
      """CREATE TABLE %s (a INT PRIMARY KEY, s STRING NULL)
      PARTITION BY HASH(a) PARTITIONS 3 STORED AS KUDU""" % db_tbl, vector)
    assert kudu_client.table_exists(
        KuduTestSuite.to_kudu_table_name(unique_database, tbl))

    # Force metadata to be loaded on impalads
    self.execute_query_using_vector("select * from %s" % db_tbl, vector)

    # Load the table via the Kudu client and change col 's' to be a different type.
    table = kudu_client.table(KuduTestSuite.to_kudu_table_name(unique_database, tbl))
    alterer = kudu_client.new_table_alterer(table)
    alterer.drop_column("s")
    table = alterer.alter()
    alterer = kudu_client.new_table_alterer(table)
    alterer.add_column("s", "string", nullable=False, default="bar")
    table = alterer.alter()

    # Add some rows
    session = kudu_client.new_session()
    for i in range(100):
      op = table.new_insert((i, "bar"))
      session.apply(op)
    session.flush()

    set_latest_observed_option(vector, kudu_client)
    # Scanning should result in an error
    try:
      self.execute_query_using_vector("SELECT * FROM %s" % db_tbl, vector)
      assert cluster_properties.is_catalog_v2_cluster(), \
          "Should fail with Catalog V1, which caches metadata"
    except Exception as e:
      assert not cluster_properties.is_catalog_v2_cluster(), \
          "Should succeed with Catalog V2, which does not cache metadata"
      expected_error = "Column 's' is not nullable but Impala expected it to be "\
          "nullable. The table metadata in Impala may be outdated and need to be "\
          "refreshed."
      assert expected_error in str(e)

    # After a REFRESH the scan should succeed
    self.execute_query_using_vector("REFRESH %s" % db_tbl, vector)
    result = self.execute_query_using_vector("SELECT * FROM %s" % db_tbl, vector)
    assert len(result.data) == 100

  def test_kudu_col_added(self, vector, kudu_client, unique_database,
                          cluster_properties):
    """Test adding a Kudu column outside of Impala."""
    tbl = 'foo'
    db_tbl = unique_database + '.' + tbl
    self.execute_query_using_vector("""CREATE TABLE %s (a INT PRIMARY KEY)
        PARTITION BY HASH(a) PARTITIONS 3 STORED AS KUDU""" % db_tbl, vector)
    assert kudu_client.table_exists(
        KuduTestSuite.to_kudu_table_name(unique_database, tbl))

    # Force metadata to be loaded on impalads
    self.execute_query_using_vector("select * from %s" % db_tbl, vector)

    # Load the table via the Kudu client and add a new col
    table = kudu_client.table(KuduTestSuite.to_kudu_table_name(unique_database, tbl))
    alterer = kudu_client.new_table_alterer(table)
    alterer.add_column("b", "int32")
    table = alterer.alter()

    # Add some rows
    session = kudu_client.new_session()
    op = table.new_insert((0, 0))
    session.apply(op)
    session.flush()

    set_latest_observed_option(vector, kudu_client)
    result = self.execute_query_using_vector("SELECT * FROM %s" % db_tbl, vector)
    if cluster_properties.is_catalog_v2_cluster():
      # Changes in Kudu should be immediately visible to Impala with Catalog V2.
      assert result.data[0].split('\t') == ['0', '0']
    else:
      # Only the first col is visible to Impala. Impala will not know about the missing
      # column, so '*' is expanded to known columns. This doesn't have a separate check
      # because the query can proceed and checking would need to fetch metadata from the
      # Kudu master, which is what REFRESH is for.
      assert result.data[0].split('\t') == ['0']

    # After a REFRESH both cols should be visible
    self.execute_query_using_vector("REFRESH %s" % db_tbl, vector)
    result = self.execute_query_using_vector("SELECT * FROM %s" % db_tbl, vector)
    assert result.data[0].split('\t') == ['0', '0']

  @SkipIfKudu.no_hybrid_clock()
  @SkipIfKudu.hms_integration_enabled()
  def test_kudu_col_removed(self, vector, kudu_client, unique_database):
    """Test removing a Kudu column outside of Impala."""
    tbl = 'foo'
    db_tbl = unique_database + '.' + tbl
    self.execute_query_using_vector("""CREATE TABLE %s (a INT PRIMARY KEY, s STRING)
        PARTITION BY HASH(a) PARTITIONS 3 STORED AS KUDU""" % db_tbl, vector)
    assert kudu_client.table_exists(
        KuduTestSuite.to_kudu_table_name(unique_database, tbl))

    # Force metadata to be loaded on impalads
    self.execute_query_using_vector("select * from %s" % db_tbl, vector)
    self.execute_query_using_vector("insert into %s values (0, 'foo')" % db_tbl, vector)

    # Load the table via the Kudu client and change col 's' to be a different type.
    table = kudu_client.table(KuduTestSuite.to_kudu_table_name(unique_database, tbl))
    alterer = kudu_client.new_table_alterer(table)
    alterer.drop_column("s")
    table = alterer.alter()

    # Scanning should result in an error
    try:
      self.execute_query_using_vector("SELECT * FROM %s" % db_tbl, vector)
    except Exception as e:
      expected_error = "Column 's' not found in kudu table impala::test_kudu_col_removed"
      assert expected_error in str(e)

    # After a REFRESH the scan should succeed
    self.execute_query_using_vector("REFRESH %s" % db_tbl, vector)
    result = self.execute_query_using_vector("SELECT * FROM %s" % db_tbl, vector)
    assert result.data == ['0']

  def test_kudu_show_unbounded_range_partition(self, vector, kudu_client,
                                               unique_database):
    """Check that a single unbounded range partition gets printed correctly."""
    schema_builder = SchemaBuilder()
    column_spec = schema_builder.add_column("id", INT64)
    column_spec.nullable(False)
    schema_builder.set_primary_keys(["id"])
    schema = schema_builder.build()
    tbl = 'unbounded_range_table'
    name = unique_database + "." + tbl

    try:
      kudu_client.create_table(
        name, schema, partitioning=Partitioning().set_range_partition_columns(["id"]))
      kudu_table = kudu_client.table(name)

      self.execute_query_using_vector(
        create_external_kudu_query(unique_database, tbl, kudu_table.name), vector)
      result = self.execute_query("SHOW RANGE PARTITIONS %s" % name)
      assert result.column_labels == ['RANGE (ID)']
      assert result.column_types == ['STRING']
      assert result.data == ['UNBOUNDED']

    finally:
      if kudu_client.table_exists(name):
        kudu_client.delete_table(name)

  @SkipIfKudu.no_hybrid_clock()
  def test_column_storage_attributes(self, vector, unique_database):
    """Tests that for every valid combination of column type, encoding, and compression,
       we can insert a value and scan it back from Kudu."""
    # This test takes about 2min and is unlikely to break, so only run it in exhaustive.
    if self.exploration_strategy() != 'exhaustive':
      pytest.skip("Only runs in exhaustive to reduce core time.")
    table_name = "%s.storage_attrs" % unique_database
    types = ['boolean', 'tinyint', 'smallint', 'int', 'bigint', 'float', 'double',
             'string', 'timestamp', 'decimal', 'date', 'varchar(10)']
    create_query = "create table %s (id int primary key" % table_name
    for t in types:
      # We truncate the type attributes in the column name to keep things simple.
      create_query += ", %s_col %s" % (t.split('(')[0], t)
    create_query += ") partition by hash(id) partitions 16 stored as kudu"
    self.execute_query_using_vector(create_query, vector)

    encodings = ['AUTO_ENCODING', 'PLAIN_ENCODING', 'PREFIX_ENCODING', 'GROUP_VARINT',
                 'RLE', 'DICT_ENCODING', 'BIT_SHUFFLE']
    compressions = ['DEFAULT_COMPRESSION', 'NO_COMPRESSION', 'SNAPPY', 'LZ4', 'ZLIB']
    i = 0
    for e in encodings:
      for c in compressions:
        for t in types:
          try:
            # We truncate the type attributes in the column name to keep things simple.
            self.execute_query_using_vector(
              """
              alter table {} alter column {}_col
              set encoding {} compression {}""".format(
                table_name, t.split('(')[0], e, c),
              vector)
          except Exception as err:
            assert "encoding %s not supported for type" % e in str(err)
        self.execute_query_using_vector(
          """insert into %s values (%s, true, 0, 0, 0, 0, 0, 0, '0',
          cast('2009-01-01' as timestamp), cast(0 as decimal),
          cast('2010-01-01' as date), cast('' as varchar(10)))""" % (table_name, i),
          vector)
        result = self.execute_query_using_vector(
          "select * from %s where id = %s" % (table_name, i), vector)
        # Beeswax return 'true' while HS2 return 'True'. Lowercase the result.
        assert result.data[0].lower().split('\t') == [
          str(i), 'true', '0', '0', '0', '0', '0', '0', '0',
          '2009-01-01 00:00:00', '0', '2010-01-01', '']
        i += 1
    result = self.execute_query_using_vector(
      "select count(*) from %s" % table_name, vector)
    print(result.data == [str(i)])

  def test_concurrent_schema_change(self, vector, unique_database):
    """Tests that an insert into a Kudu table with a concurrent schema change either
    succeeds or fails gracefully."""
    table_name = "%s.test_schema_change" % unique_database
    self.execute_query_using_vector(
      """create table %s (col0 bigint primary key, col1 bigint)
      partition by hash(col0) partitions 16 stored as kudu""" % table_name, vector)

    iters = 5

    def insert_values():
      threading.current_thread().errors = []
      client = self.create_impala_client_from_vector(vector)
      for i in range(0, iters):
        time.sleep(random.random())  # sleeps for up to one second
        try:
          client.execute("insert into %s values (0, 0), (1, 1)" % table_name)
        except Exception as e:
          threading.current_thread().errors.append(e)

    insert_thread = threading.Thread(target=insert_values)
    insert_thread.start()

    for i in range(0, iters):
      time.sleep(random.random())  # sleeps for up to one second
      self.execute_query_using_vector(
        "alter table %s drop column col1" % table_name, vector)
      if i % 2 == 0:
        self.execute_query_using_vector(
          "alter table %s add columns (col1 string)" % table_name, vector)
      else:
        self.execute_query_using_vector(
          "alter table %s add columns (col1 bigint)" % table_name, vector)

    insert_thread.join()

    for error in insert_thread.errors:
      msg = str(error)
      # The first two are AnalysisExceptions, the next two come from KuduTableSink::Open()
      # if the schema has changed since analysis, the rest come from the Kudu server if
      # the schema changes between KuduTableSink::Open() and when the write ops are sent.
      possible_errors = [
        "has fewer columns (1) than the SELECT / VALUES clause returns (2)",
        "(type: TINYINT) is not compatible with column 'col1' (type: STRING)",
        "has fewer columns than expected.",
        "Column col1 has unexpected type.",
        "Client provided column col1[int64 NULLABLE] not present in tablet",
        "Client provided column col1 INT64 NULLABLE not present in tablet",
        "The column 'col1' must have type string NULLABLE found int64 NULLABLE"
      ]
      assert any(err in msg for err in possible_errors)

  def _retry_query(self, query, vector, expected):
    retries = 0
    while retries < 3:
      result = self.execute_query_using_vector(query, vector)
      if result.data == expected:
        break
      retries += 1
      time.sleep(1)
    assert retries < 3, \
        "Did not get a correct result for %s after 3 retries: %s" % (query, result)

  def test_read_modes(self, vector, unique_database):
    """Other Kudu tests are run with a scan level of READ_AT_SNAPSHOT to have predicable
    scan results. This test verifies that scans work as expected at the scan level of
    READ_LATEST by retrying the scan if the results are incorrect."""
    vector.set_exec_option('kudu_read_mode', 'READ_LATEST')
    table_name = "%s.test_read_latest" % unique_database
    self.execute_query_using_vector(
      """create table %s (a int primary key, b string)
      partition by hash(a)
      partitions 8 stored as kudu""" % table_name, vector)
    self.execute_query_using_vector(
      "insert into %s values (0, 'a'), (1, 'b'), (2, 'c')" % table_name, vector)
    self._retry_query("select * from %s order by a" % table_name, vector,
                      ['0\ta', '1\tb', '2\tc'])
    self.execute_query_using_vector(
      """insert into %s select id, string_col from functional.alltypes
      where id > 2 limit 100""" % table_name, vector)
    self._retry_query("select count(*) from %s" % table_name, vector, ['103'])

  def test_replica_selection(self, vector, unique_database):
    """This test verifies that scans work as expected with different replica selection.
    """
    table_name = "%s.replica_selection" % unique_database
    self.execute_query_using_vector(
      """create table %s (a int primary key, b string)
      partition by hash(a)
      partitions 8 stored as kudu""" % table_name, vector)
    self.execute_query_using_vector(
      """insert into %s select id, string_col from functional.alltypes
      limit 100""" % table_name, vector)

    vector.set_exec_option('kudu_replica_selection', 'LEADER_ONLY')
    result = self.execute_query_using_vector(
      "select count(*) from %s" % table_name, vector)
    assert result.data == ['100']

    vector.set_exec_option('kudu_replica_selection', 'CLOSEST_REPLICA')
    result = self.execute_query_using_vector(
      "select count(*) from %s" % table_name, vector)
    assert result.data == ['100']


class TestKuduPartitioning(KuduTestSuite):
  @classmethod
  def add_test_dimensions(cls):
    # Test both the interpreted and the codegen'd path.
    super(TestKuduPartitioning, cls).add_test_dimensions()

  def test_partitions_evenly_distributed(self, vector, kudu_client, unique_database):
    """Sanity check for KuduPartitionExpr. We insert numbers into a table and check that
    inserted elements are distributed evenly among the partitions. The assumption here is
    that the source distribution is more or less uniform and that hashing retains this
    property. This protects against some but not all errors. The number of partitions
    should be the same as the number of impalads."""

    table_name = "partitioning"
    table_full_name = unique_database + ".partitioning"
    self.execute_query("""CREATE TABLE %s (a INT PRIMARY KEY)
        PARTITION BY HASH(a) PARTITIONS 3 STORED AS KUDU""" % table_full_name,
                       vector.get_exec_option_dict())
    assert kudu_client.table_exists(KuduTestSuite.to_kudu_table_name(
        unique_database, table_name))

    query = "INSERT INTO %s SELECT id FROM functional.alltypes" % table_full_name
    result = self.execute_query(query, vector.get_exec_option_dict())

    profile = result.runtime_profile

    numbers = TestKuduPartitioning.extract_kudu_rows_from_profile(profile)
    TestKuduPartitioning.assert_rows_evenly_distributed(numbers)

  @staticmethod
  def assert_rows_evenly_distributed(rows):
    TOLERANCE_RATIO = 0.1
    avg = rows[0]  # The first result is from the averaged summary.
    values = rows[1:]

    for value in values:
      abs_diff = abs(avg - value)
      ratio = float(abs_diff) / avg
      assert ratio < TOLERANCE_RATIO

  @staticmethod
  def extract_kudu_rows_from_profile(profile):
    # First we look for a header that contains "KuduTableSink", then under that we find
    # the number of rows.
    res = []
    kudu_table_sink = "KuduTableSink"
    total_num_rows_re = re.compile(r"TotalNumRows:.*\(([0-9]+)\)")

    within_kudu_table_sink_section = False
    for line in profile.splitlines():
      if within_kudu_table_sink_section:
        match = total_num_rows_re.search(line)
        if match:
          res.append(int(match.group(1)))
          within_kudu_table_sink_section = False
      else:
        if kudu_table_sink in line:
          within_kudu_table_sink_section = True
    return res


class TestCreateExternalTable(KuduTestSuite):

  @classmethod
  def add_test_dimensions(cls):
    super(TestCreateExternalTable, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())

  @SkipIfKudu.hms_integration_enabled()
  def test_external_timestamp_default_value(self, vector, kudu_client, unique_database):
    """Checks that a Kudu table created outside Impala with a default value on a
       UNIXTIME_MICROS column can be loaded by Impala, and validates the DESCRIBE
       output is correct."""
    schema_builder = SchemaBuilder()
    column_spec = schema_builder.add_column("id", INT64)
    column_spec.nullable(False)
    column_spec = schema_builder.add_column("ts", UNIXTIME_MICROS)
    column_spec.default(datetime(2009, 1, 1, 0, 0, tzinfo=utc))
    schema_builder.set_primary_keys(["id"])
    schema = schema_builder.build()
    tbl = 'tsdefault'
    name = unique_database + "." + tbl

    try:
      kudu_client.create_table(
        name, schema, partitioning=Partitioning().set_range_partition_columns(["id"]))
      kudu_table = kudu_client.table(name)

      self.execute_query_using_vector(
        create_external_kudu_query(unique_database, tbl, kudu_table.name), vector)
      result = self.execute_query_using_vector("DESCRIBE %s" % name, vector)
      table_desc = [[col.strip() if col else col for col in row.split('\t')]
                    for row in result.data]
      # Pytest shows truncated output on failure, so print the details just in case.
      LOG.info(table_desc)
      assert ["ts", "timestamp", "", "false", "", "true", "1230768000000000",
              "AUTO_ENCODING", "DEFAULT_COMPRESSION", "0"] in table_desc
    finally:
      if kudu_client.table_exists(name):
        kudu_client.delete_table(name)

  @SkipIfKudu.hms_integration_enabled()
  def test_implicit_table_props(self, vector, kudu_client, unique_database):
    """Check that table properties added internally during table creation are as
       expected.
    """
    with self.temp_kudu_table(
      kudu_client, [STRING, INT8, BOOL], num_key_cols=2) as kudu_table:
      impala_table_name = self.get_kudu_table_base_name(kudu_table.name)

      self.execute_query_using_vector(
        create_external_kudu_query(unique_database, impala_table_name, kudu_table.name),
        vector)
      result = self.execute_query_using_vector(
        "DESCRIBE FORMATTED %s.%s" % (unique_database, impala_table_name), vector)
      table_desc = [[col.strip() if col else col for col in row.split('\t')]
                    for row in result.data]
      LOG.info(table_desc)
      # Pytest shows truncated output on failure, so print the details just in case.
      assert ["", "EXTERNAL", "TRUE"] in table_desc
      assert ["", "kudu.master_addresses", KUDU_MASTER_HOSTS] in table_desc
      assert ["", "kudu.table_name", kudu_table.name] in table_desc
      assert ["", "storage_handler", "org.apache.hadoop.hive.kudu.KuduStorageHandler"] \
          in table_desc

  @SkipIfKudu.hms_integration_enabled()
  def test_col_types(self, vector, kudu_client, unique_database):
    """Check that a table can be created using all available column types."""
    # TODO: Add DECIMAL when the Kudu python client supports decimal
    kudu_types = [STRING, BOOL, DOUBLE, FLOAT, INT16, INT32, INT64, INT8, BINARY,
                  UNIXTIME_MICROS, DATE]
    with self.temp_kudu_table(kudu_client, kudu_types) as kudu_table:
      impala_table_name = self.get_kudu_table_base_name(kudu_table.name)

      self.execute_query_using_vector(
        create_external_kudu_query(unique_database, impala_table_name, kudu_table.name),
        vector)
      result = self.execute_query_using_vector(
        "DESCRIBE %s.%s" % (unique_database, impala_table_name), vector)
      kudu_schema = kudu_table.schema
      for i, row in enumerate(result.data):
        cols = row.split('\t')
        kudu_col = kudu_schema[i]
        assert cols[0] == kudu_col.name
        assert cols[1].upper() == \
            self.kudu_col_type_to_impala_col_type(kudu_col.type.type)

  @SkipIfKudu.hms_integration_enabled()
  def test_drop_external_table(self, vector, kudu_client, unique_database):
    """Check that dropping an external table only affects the catalog and does not delete
       the table in Kudu.
    """
    with self.temp_kudu_table(kudu_client, [INT32]) as kudu_table:
      impala_table_name = self.get_kudu_table_base_name(kudu_table.name)

      self.execute_query_using_vector(
        create_external_kudu_query(unique_database, impala_table_name, kudu_table.name),
        vector)
      result = self.execute_query_using_vector(
        "SELECT COUNT(*) FROM %s.%s" % (unique_database, impala_table_name), vector)
      assert result.data == ['0']
      result = self.execute_query_using_vector(
        "DROP TABLE %s.%s" % (unique_database, impala_table_name), vector)
      assert result.success
      try:
        self.execute_query_using_vector(
          "SELECT COUNT(*) FROM %s.%s" % (unique_database, impala_table_name), vector)
        assert False
      except Exception as e:
        assert "Could not resolve table reference" in str(e)
      assert kudu_client.table_exists(kudu_table.name)

  @SkipIfKudu.hms_integration_enabled()
  def test_explicit_name(self, vector, kudu_client, unique_database):
    """Check that a Kudu table can be specified using a table property."""
    with self.temp_kudu_table(kudu_client, [INT32]) as kudu_table:
      table_name = self.random_table_name()

      self.execute_query_using_vector(
        create_external_kudu_query(unique_database, table_name, kudu_table.name),
        vector)
      result = self.execute_query_using_vector(
        "SELECT * FROM %s.%s" % (unique_database, table_name), vector)
      assert len(result.data) == 0

  @SkipIfKudu.hms_integration_enabled()
  def test_explicit_name_preference(self, vector, kudu_client, unique_database):
    """Check that the table name from a table property is used when a table of the
       implied name also exists.
    """
    with self.temp_kudu_table(kudu_client, [INT64]) as preferred_kudu_table:
      with self.temp_kudu_table(kudu_client, [INT8]) as other_kudu_table:
        impala_table_name = self.get_kudu_table_base_name(other_kudu_table.name)

        self.execute_query_using_vector(
          create_external_kudu_query(
            unique_database, impala_table_name, preferred_kudu_table.name),
          vector)
        result = self.execute_query_using_vector(
          "DESCRIBE %s.%s" % (unique_database, impala_table_name), vector)
        assert result.data[0].split('\t') == \
            ["a", "bigint", "", "true", "true", "false", "", "AUTO_ENCODING",
             "DEFAULT_COMPRESSION", "0"]

  @SkipIfKudu.hms_integration_enabled()
  def test_explicit_name_doesnt_exist(self, vector, unique_database):
    kudu_table_name = self.random_table_name()
    try:
      self.execute_query_using_vector(
        create_external_kudu_query(
          unique_database, self.random_table_name(), kudu_table_name),
        vector)
      assert False
    except Exception as e:
      assert "Table does not exist in Kudu: '%s'" % kudu_table_name in str(e)

  @SkipIfKudu.hms_integration_enabled()
  def test_explicit_name_doesnt_exist_but_implicit_does(self, vector, kudu_client,
                                                        unique_database):
    """Check that when an explicit table name is given but that table doesn't exist,
       there is no fall-through to an existing implicit table.
    """
    with self.temp_kudu_table(kudu_client, [INT32]) as kudu_table:
      table_name = self.random_table_name()
      try:
        self.execute_query_using_vector(
          create_external_kudu_query(
            unique_database, self.get_kudu_table_base_name(kudu_table.name), table_name),
          vector)
        assert False
      except Exception as e:
        assert "Table does not exist in Kudu: '%s'" % table_name in str(e)

  @SkipIfKudu.no_hybrid_clock()
  @SkipIfKudu.hms_integration_enabled()
  def test_table_without_partitioning(self, vector, kudu_client, unique_database):
    """Test a Kudu table created without partitioning (i.e. equivalent to a single
       unbounded partition). It is not possible to create such a table in Impala, but
       it can be created directly in Kudu and then loaded as an external table.
       Regression test for IMPALA-5154."""
    vector.set_exec_option('kudu_read_mode', 'READ_AT_SNAPSHOT')
    schema_builder = SchemaBuilder()
    column_spec = schema_builder.add_column("id", INT64)
    column_spec.nullable(False)
    schema_builder.set_primary_keys(["id"])
    schema = schema_builder.build()
    partitioning = Partitioning().set_range_partition_columns([])
    tbl = 'one_big_unbounded_partition'
    name = "%s.%s" % (unique_database, tbl)

    try:
      kudu_client.create_table(name, schema, partitioning=partitioning)

      self.execute_query_using_vector(
        create_external_kudu_query(unique_database, tbl, name), vector)
      self.execute_query_using_vector(
        "INSERT INTO %s VALUES (1), (2), (3)" % name, vector)
      result = self.execute_query_using_vector(
        "SELECT COUNT(*) FROM %s" % name, vector)
      assert result.data == ['3']
      try:
        self.execute_query_using_vector(
          "SHOW RANGE PARTITIONS %s" % name, vector)
        assert False
      except Exception as e:
        assert "AnalysisException: SHOW RANGE PARTITIONS requested but table does "\
            "not have range partitions" in str(e)
    finally:
      if kudu_client.table_exists(name):
        kudu_client.delete_table(name)

  @SkipIfKudu.no_hybrid_clock()
  @SkipIfKudu.hms_integration_enabled()
  def test_column_name_case(self, vector, kudu_client, unique_database):
    """IMPALA-5286: Tests that an external Kudu table that was created with a column name
       containing upper case letters is handled correctly."""
    vector.set_exec_option('kudu_read_mode', 'READ_AT_SNAPSHOT')
    tbl = 'kudu_external_test'
    table_name = '%s.%s' % (unique_database, tbl)
    if kudu_client.table_exists(table_name):
      kudu_client.delete_table(table_name)

    schema_builder = SchemaBuilder()
    key_col = 'Key'
    schema_builder.add_column(key_col, INT64).nullable(False).primary_key()
    schema = schema_builder.build()
    partitioning = Partitioning().set_range_partition_columns([key_col])\
        .add_range_partition([1], [10])

    try:
      kudu_client.create_table(table_name, schema, partitioning)

      self.execute_query_using_vector(
        create_external_kudu_query(unique_database, tbl, table_name), vector)

      # Perform a variety of operations on the table.
      self.execute_query_using_vector(
        "insert into %s (kEy) values (5), (1), (4)" % table_name, vector)
      result = self.execute_query_using_vector(
        "select keY from %s where KeY %% 2 = 0" % table_name, vector)
      assert result.data == ['4']
      result = self.execute_query_using_vector(
        "select * from %s order by kEY" % table_name, vector)
      assert result.data == ['1', '4', '5']

      # Do a join with a runtime filter targeting the column.
      result = self.execute_query_using_vector(
        "select count(*) from %s a, %s b where a.key = b.key" % (table_name, table_name),
        vector)
      assert result.data == ['3']

      self.execute_query_using_vector(
        "alter table %s add range partition 11 < values < 20" % table_name, vector)

      new_key = "KEY2"
      self.execute_query_using_vector(
        "alter table %s change KEy %s bigint" % (table_name, new_key),
        vector)
      val_col = "vaL"
      self.execute_query_using_vector(
        "alter table %s add columns (%s bigint)" % (table_name, val_col), vector)

      result = self.execute_query_using_vector("describe %s" % table_name, vector)
      # 'describe' should print the column name in lower case.
      assert new_key.lower() in result.data[0]
      assert val_col.lower() in result.data[1]

      self.execute_query_using_vector(
        "alter table %s drop column Val" % table_name, vector)
      result = self.execute_query_using_vector("describe %s" % table_name, vector)
      assert len(result.data) == 1

      self.execute_query_using_vector(
        "alter table %s drop range partition 11 < values < 20" % table_name, vector)
    finally:
      if kudu_client.table_exists(table_name):
        kudu_client.delete_table(table_name)

  @SkipIfKudu.hms_integration_enabled()
  def test_conflicting_column_name(self, vector, kudu_client, unique_database):
    """IMPALA-5283: Tests that loading an external Kudu table that was created with column
       names that differ only in case results in an error."""
    tbl = 'kudu_external_test'
    table_name = '%s.%s' % (unique_database, tbl)
    if kudu_client.table_exists(table_name):
      kudu_client.delete_table(table_name)

    schema_builder = SchemaBuilder()
    col0 = 'col'
    schema_builder.add_column(col0, INT64).nullable(False).primary_key()
    col1 = 'COL'
    schema_builder.add_column(col1, INT64)
    schema = schema_builder.build()
    partitioning = Partitioning().set_range_partition_columns([col0])\
        .add_range_partition([1], [10])

    try:
      kudu_client.create_table(table_name, schema, partitioning)

      self.execute_query_using_vector(
        create_external_kudu_query(unique_database, tbl, table_name), vector)
      assert False, 'create table should have resulted in an exception'
    except Exception as e:
      assert 'Error loading Kudu table: Impala does not support column names that ' \
          + 'differ only in casing' in str(e)
    finally:
      if kudu_client.table_exists(table_name):
        kudu_client.delete_table(table_name)


class TestShowCreateTable(KuduTestSuite):
  column_properties = "ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION"

  def assert_show_create_equals(self, db_name, create_sql, show_create_sql,
                                do_exact_match=False, extra_args=None):
    """Executes 'create_sql' to create a table, then runs "SHOW CREATE TABLE" and checks
       that the output is the same as 'show_create_sql'. 'create_sql' and
       'show_create_sql' can be templates that can be used with str.format(). format()
       will be called with 'table', 'db', 'kudu_addr', and 'extra_args' entries as
       keyword args. Also, compares HMS-3 specific output due to HMS translation.
       If do_exact_match is True does not manipulate the output and compares exactly
       with the show_create_sql parameter.
    """
    format_args = {
        "table": self.random_table_name(),
        "db": db_name,
        "kudu_addr": KUDU_MASTER_HOSTS}
    if extra_args is not None:
      format_args.update(extra_args)
    self.execute_query(create_sql.format(**format_args))
    result = self.execute_query("SHOW CREATE TABLE {db}.{table}".format(**format_args))
    output = result.data[0]
    if not do_exact_match and HIVE_MAJOR_VERSION > 2:
      # in case of HMS-3 all Kudu tables are translated to external tables with some
      # additional properties. This code below makes sure that we have the expected table
      # properties and the table is external
      # TODO we should move these tests to a query.test file so that we can have better
      # way to compare the output against different hive versions
      assert output.startswith("CREATE EXTERNAL TABLE")
      assert "'external.table.purge'='TRUE', " in output
      # We have made sure that the output starts with CREATE EXTERNAL TABLE, now we can
      # change it to "CREATE TABLE" to make it easier to compare rest of the str
      output = output.replace("CREATE EXTERNAL TABLE", "CREATE TABLE")
      # We should also remove the additional tbl property external.table.purge so that we
      # can compare the rest of output
      output = output.replace("'external.table.purge'='TRUE', ", "")
    assert output == \
        textwrap.dedent(show_create_sql.format(**format_args)).strip()

  @SkipIfKudu.hms_integration_enabled()
  def test_primary_key_and_distribution(self, unique_database):
    # TODO: Add case with BLOCK_SIZE
    self.assert_show_create_equals(
      unique_database,
      """
      CREATE TABLE {db}.{table} (c INT PRIMARY KEY)
      PARTITION BY HASH (c) PARTITIONS 3 STORED AS KUDU""",
      """
      CREATE TABLE {db}.{table} (
        c INT NOT NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION,
        PRIMARY KEY (c)
      )
      PARTITION BY HASH (c) PARTITIONS 3
      STORED AS KUDU
      TBLPROPERTIES ('TRANSLATED_TO_EXTERNAL'='TRUE', """
      """'kudu.master_addresses'='{kudu_addr}')""")
    self.assert_show_create_equals(
      unique_database,
      """
      CREATE TABLE {db}.{table} (c INT PRIMARY KEY, d STRING NULL)
      PARTITION BY HASH (c) PARTITIONS 3, RANGE (c)
      (PARTITION VALUES <= 1, PARTITION 1 < VALUES <= 2,
       PARTITION 2 < VALUES) STORED AS KUDU""",
      """
      CREATE TABLE {db}.{table} (
        c INT NOT NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION,
        d STRING NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION,
        PRIMARY KEY (c)
      )
      PARTITION BY HASH (c) PARTITIONS 3, RANGE (c) (...)
      STORED AS KUDU
      TBLPROPERTIES ('TRANSLATED_TO_EXTERNAL'='TRUE', """
      """'kudu.master_addresses'='{kudu_addr}')""")
    self.assert_show_create_equals(
      unique_database,
      """
      CREATE TABLE {db}.{table} (c INT ENCODING PLAIN_ENCODING, PRIMARY KEY (c))
      PARTITION BY HASH (c) PARTITIONS 3 STORED AS KUDU""",
      """
      CREATE TABLE {db}.{table} (
        c INT NOT NULL ENCODING PLAIN_ENCODING COMPRESSION DEFAULT_COMPRESSION,
        PRIMARY KEY (c)
      )
      PARTITION BY HASH (c) PARTITIONS 3
      STORED AS KUDU
      TBLPROPERTIES ('TRANSLATED_TO_EXTERNAL'='TRUE', """
      """'kudu.master_addresses'='{kudu_addr}')""")
    self.assert_show_create_equals(
      unique_database,
      """
      CREATE TABLE {db}.{table} (c INT COMPRESSION LZ4, d STRING, PRIMARY KEY(c, d))
      PARTITION BY HASH (c) PARTITIONS 3, HASH (d) PARTITIONS 3,
      RANGE (c, d) (PARTITION VALUE = (1, 'aaa'), PARTITION VALUE = (2, 'bbb'))
      STORED AS KUDU""",
      """
      CREATE TABLE {db}.{table} (
        c INT NOT NULL ENCODING AUTO_ENCODING COMPRESSION LZ4,
        d STRING NOT NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION,
        PRIMARY KEY (c, d)
      )
      PARTITION BY HASH (c) PARTITIONS 3, HASH (d) PARTITIONS 3, RANGE (c, d) (...)
      STORED AS KUDU
      TBLPROPERTIES ('TRANSLATED_TO_EXTERNAL'='TRUE', """
      """'kudu.master_addresses'='{kudu_addr}')""")
    self.assert_show_create_equals(
      unique_database,
      """
      CREATE TABLE {db}.{table} (c INT, d STRING, e INT NULL DEFAULT 10, """
      """PRIMARY KEY(c, d))
      PARTITION BY RANGE (c) (PARTITION VALUES <= 1, PARTITION 1 < VALUES <= 2,
      PARTITION 2 < VALUES <= 3, PARTITION 3 < VALUES) STORED AS KUDU""",
      """
      CREATE TABLE {db}.{table} (
        c INT NOT NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION,
        d STRING NOT NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION,
        e INT NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION DEFAULT 10,
        PRIMARY KEY (c, d)
      )
      PARTITION BY RANGE (c) (...)
      STORED AS KUDU
      TBLPROPERTIES ('TRANSLATED_TO_EXTERNAL'='TRUE', """
      """'kudu.master_addresses'='{kudu_addr}')""")
    self.assert_show_create_equals(
      unique_database,
      """
      CREATE TABLE {db}.{table} (c INT PRIMARY KEY) STORED AS KUDU""",
      """
      CREATE TABLE {db}.{table} (
        c INT NOT NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION,
        PRIMARY KEY (c)
      )
      STORED AS KUDU
      TBLPROPERTIES ('TRANSLATED_TO_EXTERNAL'='TRUE', """
      """'kudu.master_addresses'='{kudu_addr}')""")
    self.assert_show_create_equals(
      unique_database,
      """
      CREATE TABLE {db}.{table} (c INT COMMENT 'Ab 1@' PRIMARY KEY) STORED AS KUDU""",
      """
      CREATE TABLE {db}.{table} (
        c INT NOT NULL {p} COMMENT 'Ab 1@',
        PRIMARY KEY (c)
      )
      STORED AS KUDU
      TBLPROPERTIES ('TRANSLATED_TO_EXTERNAL'='TRUE', """
      """'kudu.master_addresses'='{kudu_addr}')""",
      extra_args={'p': self.column_properties})

  @SkipIfKudu.hms_integration_enabled()
  def test_timestamp_default_value(self, unique_database):
    create_sql_fmt = (
      """
      CREATE TABLE {db}.{table} (c INT, d TIMESTAMP,
      e TIMESTAMP NULL DEFAULT CAST('{ts_create}' AS TIMESTAMP),
      PRIMARY KEY(c, d))
      PARTITION BY HASH(c) PARTITIONS 3
      STORED AS KUDU""")
    # Long lines are unfortunate, but extra newlines will break the test.
    show_create_sql_fmt = (
      """
      CREATE TABLE {db}.{table} (
        c INT NOT NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION,
        d TIMESTAMP NOT NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION,
        e TIMESTAMP NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION """
      """DEFAULT unix_micros_to_utc_timestamp({ts_show}),
        PRIMARY KEY (c, d)
      )
      PARTITION BY HASH (c) PARTITIONS 3
      STORED AS KUDU
      TBLPROPERTIES ('TRANSLATED_TO_EXTERNAL'='TRUE', """
      """'kudu.master_addresses'='{kudu_addr}')""")
    self.assert_show_create_equals(
      unique_database,
      create_sql_fmt, show_create_sql_fmt,
      extra_args={
        "ts_create": "2009-01-01 00:00:00.000001000",
        "ts_show": "1230768000000001"})
    self.assert_show_create_equals(
      unique_database,
      create_sql_fmt, show_create_sql_fmt,
      extra_args={
        "ts_create": "2009-01-01 00:00:00.000001001",
        "ts_show": "1230768000000001"})
    self.assert_show_create_equals(
      unique_database,
      create_sql_fmt, show_create_sql_fmt,
      extra_args={
        "ts_create": "2009-01-01 00:00:00.000000999",
        "ts_show": "1230768000000001"})

  @SkipIfKudu.hms_integration_enabled()
  def test_external_kudu_table_name_with_show_create(
    self, kudu_client, unique_database):
    """Check that the generated kudu.table_name tblproperty is present with
       show create table with external Kudu tables.
    """
    schema_builder = SchemaBuilder()
    column_spec = schema_builder.add_column("id", INT64)
    column_spec.nullable(False)
    schema_builder.set_primary_keys(["id"])
    partitioning = Partitioning().set_range_partition_columns(["id"])
    schema = schema_builder.build()

    kudu_table_name = self.random_table_name()
    try:
      kudu_client.create_table(kudu_table_name, schema, partitioning)
      kudu_table = kudu_client.table(kudu_table_name)

      table_name_prop = "'kudu.table_name'='%s'" % kudu_table.name
      self.assert_show_create_equals(
        unique_database,
        """
        CREATE EXTERNAL TABLE {db}.{table} STORED AS KUDU
        TBLPROPERTIES({props})""",
        """
        CREATE EXTERNAL TABLE {db}.{table}
        STORED AS KUDU
        TBLPROPERTIES ('kudu.master_addresses'='{kudu_addr}', {kudu_table})""",
        do_exact_match=True,
        extra_args={
          "props": table_name_prop,
          "kudu_table": table_name_prop})
    finally:
      if kudu_client.table_exists(kudu_table_name):
        kudu_client.delete_table(kudu_table_name)

  @SkipIfKudu.hms_integration_enabled()
  def test_managed_kudu_table_name_with_show_create(self, unique_database):
    """Check that the generated kudu.table_name tblproperty is not present with
       show create table with managed Kudu tables.
    """
    self.assert_show_create_equals(
      unique_database,
      """
      CREATE TABLE {db}.{table} (c INT PRIMARY KEY)
      PARTITION BY HASH (c) PARTITIONS 3
      STORED AS KUDU""",
      """
      CREATE TABLE {db}.{table} (
        c INT NOT NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION,
        PRIMARY KEY (c)
      )
      PARTITION BY HASH (c) PARTITIONS 3
      STORED AS KUDU
      TBLPROPERTIES ('TRANSLATED_TO_EXTERNAL'='TRUE', """
      """'kudu.master_addresses'='{kudu_addr}')""")

  def test_synchronized_kudu_table_with_show_create(self, unique_database):
    # in this case we do exact match with the provided input since this is specifically
    # creating a synchronized table
    self.assert_show_create_equals(
      unique_database,
      """
      CREATE EXTERNAL TABLE {db}.{table} (
        id BIGINT,
        name STRING,
        PRIMARY KEY(id))
      PARTITION BY HASH PARTITIONS 16
      STORED AS KUDU
      TBLPROPERTIES('external.table.purge'='true')""",
      """
      CREATE EXTERNAL TABLE {db}.{table} (
        id BIGINT NOT NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION,
        name STRING NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION,
        PRIMARY KEY (id)
      )
      PARTITION BY HASH (id) PARTITIONS 16
      STORED AS KUDU
      TBLPROPERTIES ('external.table.purge'='true', """
      """'kudu.master_addresses'='{kudu_addr}')""",
      do_exact_match=True)

    self.assert_show_create_equals(
      unique_database,
      """
      CREATE EXTERNAL TABLE {db}.{table} (
        id BIGINT PRIMARY KEY,
        name STRING)
      PARTITION BY HASH(id) PARTITIONS 16
      STORED AS KUDU
      TBLPROPERTIES('external.table.purge'='true')""",
      """
      CREATE EXTERNAL TABLE {db}.{table} (
        id BIGINT NOT NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION,
        name STRING NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION,
        PRIMARY KEY (id)
      )
      PARTITION BY HASH (id) PARTITIONS 16
      STORED AS KUDU
      TBLPROPERTIES ('external.table.purge'='true', """
      """'kudu.master_addresses'='{kudu_addr}')""",
      do_exact_match=True)


class TestDropDb(KuduTestSuite):

  @SkipIfKudu.hms_integration_enabled()
  def test_drop_non_empty_db(self, kudu_client, unique_database):
    """Check that an attempt to drop a database will fail if Kudu tables are present
       and that the tables remain.
    """
    with self.temp_kudu_table(
      kudu_client, [INT32], db_name=unique_database) as kudu_table:
      impala_table_name = self.get_kudu_table_base_name(kudu_table.name)
      self.execute_query(
        create_external_kudu_query(unique_database, impala_table_name, kudu_table.name))
      self.execute_query("USE DEFAULT")
      try:
        self.execute_query("DROP DATABASE %s" % unique_database)
        assert False
      except Exception as e:
        assert "One or more tables exist" in str(e)
      result = self.execute_query("SELECT COUNT(*) FROM %s.%s" % (
        unique_database, impala_table_name))
      assert result.data == ['0']

  @SkipIfKudu.hms_integration_enabled()
  def test_drop_db_cascade(self, kudu_client, unique_name):
    """Check that an attempt to drop a database will succeed even if Kudu tables are
       present and that the managed tables are removed.
    """
    self.execute_query("CREATE DATABASE " + unique_name)
    with self.temp_kudu_table(kudu_client, [INT32], db_name=unique_name) as kudu_table:
      # Create an external Kudu table
      impala_table_name = self.get_kudu_table_base_name(kudu_table.name)
      self.execute_query(
        create_external_kudu_query(unique_name, impala_table_name, kudu_table.name))

      # Create a managed Kudu table
      managed_table_name = self.random_table_name()
      self.execute_query("""
          CREATE TABLE %s.%s (a INT PRIMARY KEY) PARTITION BY HASH (a) PARTITIONS 3
          STORED AS KUDU""" % (unique_name, managed_table_name))
      kudu_table_name = "impala::" + unique_name + "." + managed_table_name
      assert kudu_client.table_exists(kudu_table_name)

      # Create a table in HDFS
      hdfs_table_name = self.random_table_name()
      self.execute_query("""CREATE TABLE %s.%s (a INT) PARTITIONED BY (x INT)""" % (
        unique_name, hdfs_table_name))

      self.execute_query("USE DEFAULT")
      self.execute_query("DROP DATABASE %s CASCADE" % unique_name)
      result = self.execute_query("SHOW DATABASES")
      assert unique_name not in result.data
      assert kudu_client.table_exists(kudu_table.name)
      assert not kudu_client.table_exists(managed_table_name)

  @SkipIfKudu.hms_integration_enabled()
  def test_soft_drop_db_cascade(self, kudu_client, unique_name):
    """Check that an attempt to drop a database will succeed but the managed Kudu tables
       are not removed immediately if 'kudu_table_reserve_seconds' is greater than 0.
       These Kudu tables are in 'soft_deleted' state and can be recalled during the
       reservation period.
    """
    options = dict()
    self.execute_query("CREATE DATABASE " + unique_name, options)
    table_name_pattern = "managed_kudu_table_"
    for i in range(10):
      managed_table_name = table_name_pattern + str(i)
      kudu_tbl_name = KuduTestSuite.to_kudu_table_name(unique_name, managed_table_name)
      if kudu_client.table_exists(kudu_tbl_name):
        kudu_client.delete_table(kudu_tbl_name)
      self.execute_query("""
          CREATE TABLE %s.%s (a INT PRIMARY KEY) PARTITION BY HASH (a) PARTITIONS 3
          STORED AS KUDU""" % (unique_name, managed_table_name), options)
      assert kudu_client.table_exists(kudu_tbl_name)

    options['kudu_table_reserve_seconds'] = 300
    self.execute_query("USE DEFAULT", options)
    self.execute_query("DROP DATABASE %s CASCADE" % unique_name, options)
    result = self.execute_query("SHOW DATABASES", options)
    assert unique_name not in result.data

    for i in range(10):
      kudu_tbl_name = \
          KuduTestSuite.to_kudu_table_name(unique_name, table_name_pattern + str(i))
      assert kudu_client.table_exists(kudu_tbl_name)
      assert kudu_tbl_name not in kudu_client.list_tables()
      assert kudu_tbl_name in kudu_client.list_soft_deleted_tables()
      table = kudu_client.table(kudu_tbl_name)
      kudu_client.recall_table(table.id)
      assert kudu_tbl_name in kudu_client.list_tables()
      assert kudu_tbl_name not in kudu_client.list_soft_deleted_tables()


class TestImpalaKuduIntegration(KuduTestSuite):

  @classmethod
  def default_test_protocol(cls):
    # Some tests here inspect result through ImpylaHS2ResultSet.tuples().
    # Make sure to test only with 'hs2' protocol and client.
    return HS2

  @SkipIfKudu.hms_integration_enabled()
  def test_replace_kudu_table(self, kudu_client, unique_database):
    """Check that an external Kudu table is accessible if the underlying Kudu table is
        modified using the Kudu client.
    """
    # Create an external Kudu table
    col_names = ['a']
    with self.temp_kudu_table(kudu_client, [INT32], col_names=col_names,
                              db_name=unique_database) as kudu_table:
      impala_table_name = self.get_kudu_table_base_name(kudu_table.name)
      self.execute_query(
        create_external_kudu_query(unique_database, impala_table_name, kudu_table.name))
      result = self.execute_query("DESCRIBE %s.%s" % (
        unique_database, impala_table_name))
      assert result.tuples() == \
          [("a", "int", "", "true", "true", "false", "", "AUTO_ENCODING",
            "DEFAULT_COMPRESSION", "0")]

      # Drop the underlying Kudu table and replace it with another Kudu table that has
      # the same name but different schema
      kudu_client.delete_table(kudu_table.name)
      assert not kudu_client.table_exists(kudu_table.name)
      new_col_names = ['b', 'c']
      name_parts = kudu_table.name.split(".")
      assert len(name_parts) == 2
      with self.temp_kudu_table(
        kudu_client, [STRING, STRING], col_names=new_col_names, db_name=name_parts[0],
        name=name_parts[1]) as new_kudu_table:
        assert kudu_client.table_exists(new_kudu_table.name)
        # Refresh the external table and verify that the new schema is loaded from
        # Kudu.
        self.execute_query("REFRESH %s.%s" % (
          unique_database, impala_table_name))
        result = self.execute_query("DESCRIBE %s.%s" % (
          unique_database, impala_table_name))
        assert result.tuples() == \
            [("b", "string", "", "true", "true", "false", "", "AUTO_ENCODING",
              "DEFAULT_COMPRESSION", "0"),
             ("c", "string", "", "false", "", "true", "", "AUTO_ENCODING",
              "DEFAULT_COMPRESSION", "0")]

  @SkipIfKudu.hms_integration_enabled()
  def test_delete_external_kudu_table(self, kudu_client, unique_database):
    """Check that Impala can recover from the case where the underlying Kudu table of
        an external table is dropped using the Kudu client.
    """
    with self.temp_kudu_table(kudu_client, [INT32]) as kudu_table:
      # Create an external Kudu table
      impala_table_name = self.get_kudu_table_base_name(kudu_table.name)
      self.execute_query(
        create_external_kudu_query(unique_database, impala_table_name, kudu_table.name))
      result = self.execute_query("DESCRIBE %s.%s" % (
        unique_database, impala_table_name))
      assert result.tuples() == \
          [("a", "int", "", "true", "true", "false", "", "AUTO_ENCODING",
            "DEFAULT_COMPRESSION", "0")]
      # Drop the underlying Kudu table
      kudu_client.delete_table(kudu_table.name)
      assert not kudu_client.table_exists(kudu_table.name)
      err_msg = 'the table does not exist: table_name: "%s"' % (kudu_table.name)
      try:
        self.execute_query("REFRESH %s.%s" % (unique_database, impala_table_name))
      except Exception as e:
        assert err_msg in str(e)
      self.execute_query("DROP TABLE %s.%s" % (unique_database, impala_table_name))
      result = self.execute_query("SHOW TABLES IN " + unique_database)
      assert (impala_table_name,) not in result.tuples()

  @SkipIfKudu.hms_integration_enabled()
  def test_delete_managed_kudu_table(self, kudu_client, unique_database):
    """Check that dropping a managed Kudu table works even if the underlying Kudu table
        has been dropped externally."""
    impala_tbl_name = "foo"
    self.execute_query("""CREATE TABLE %s.%s (a INT PRIMARY KEY) PARTITION BY HASH (a)
        PARTITIONS 3 STORED AS KUDU""" % (unique_database, impala_tbl_name))
    kudu_tbl_name = KuduTestSuite.to_kudu_table_name(unique_database, impala_tbl_name)
    assert kudu_client.table_exists(kudu_tbl_name)
    kudu_client.delete_table(kudu_tbl_name)
    assert not kudu_client.table_exists(kudu_tbl_name)
    self.execute_query("DROP TABLE %s.%s" % (unique_database, impala_tbl_name))
    result = self.execute_query("SHOW TABLES IN %s" % unique_database)
    assert (impala_tbl_name,) not in result.tuples()

  @SkipIfKudu.hms_integration_enabled()
  def test_soft_delete_kudu_table(self, kudu_client, unique_database):
    """Check that the query option 'kudu_table_reserve_seconds' works for managed Kudu
    table. If it is greater than 0, the underlying Kudu will not be deleted immediately.
    During the reservation period, the Kudu table can be recalled."""
    options = dict()
    tbl = "foo"
    db_tbl = unique_database + "." + tbl
    kudu_tbl_name = KuduTestSuite.to_kudu_table_name(unique_database, tbl)
    if kudu_client.table_exists(kudu_tbl_name):
      kudu_client.delete_table(kudu_tbl_name)
    self.execute_query("""CREATE TABLE %s (a INT PRIMARY KEY) PARTITION BY HASH (a)
        PARTITIONS 3 STORED AS KUDU""" % db_tbl, options)
    assert kudu_client.table_exists(kudu_tbl_name)

    options["kudu_table_reserve_seconds"] = 300
    self.execute_query("DROP TABLE %s" % db_tbl, options)
    result = self.execute_query("SHOW TABLES IN %s" % unique_database, options)
    assert (db_tbl,) not in result.tuples()

    assert kudu_client.table_exists(kudu_tbl_name)
    assert kudu_tbl_name not in kudu_client.list_tables()
    assert kudu_tbl_name in kudu_client.list_soft_deleted_tables()

    table = kudu_client.table(kudu_tbl_name)
    kudu_client.recall_table(table.id)
    assert kudu_tbl_name in kudu_client.list_tables()
    assert kudu_tbl_name not in kudu_client.list_soft_deleted_tables()


@SkipIfNotHdfsMinicluster.tuned_for_minicluster
class TestKuduMemLimits(KuduTestSuite):

  QUERIES = ["select * from tpch_kudu.lineitem where l_orderkey = -1",
             "select * from tpch_kudu.lineitem where l_commitdate like '%cheese'",
             "select * from tpch_kudu.lineitem limit 90"]

  # The value indicates the minimum memory requirements for the queries above, the first
  # memory limit corresponds to the first query
  MB = 1024 * 1024
  QUERY_MEM_LIMITS = [MB, MB, 10 * MB]

  @classmethod
  def add_test_dimensions(cls):
    super(TestKuduMemLimits, cls).add_test_dimensions()
    # The default read mode of READ_LATEST does not provide high enough consistency for
    # these tests.
    add_exec_option_dimension(cls, "mem_limit", [cls.MB, 10 * cls.MB, 0])
    # IMPALA-9856: We disable query result spooling so that this test can run queries
    # with low mem_limit.
    add_mandatory_exec_option(cls, 'spool_query_results', False)

  @pytest.mark.execute_serially
  def test_low_mem_limit_low_selectivity_scan(self, vector):
    """Tests that the queries specified in this test suite run under the given
    memory limits."""
    for i, q in enumerate(self.QUERIES):
      try:
        self.execute_query(q, vector.get_exec_option_dict())
      except Exception as e:
        if (vector.get_exec_option('mem_limit') > self.QUERY_MEM_LIMITS[i]):
          raise
        assert "Memory limit exceeded" in str(e)

    # IMPALA-4654: Validate the fix for a bug where LimitReached() wasn't respected in
    # the KuduScanner and the limit query above would result in a fragment running an
    # additional minute. This ensures that the num fragments 'in flight' reaches 0 in
    # less time than IMPALA-4654 was reproducing (~60sec) but yet still enough time that
    # this test won't be flaky.
    verifiers = [MetricVerifier(i.service)
                 for i in ImpalaCluster.get_e2e_test_cluster().impalads]
    for v in verifiers:
      v.wait_for_metric("impala-server.num-fragments-in-flight", 0, timeout=30)


@SkipIfHive2.create_external_kudu_table
class TestCreateSynchronizedTable(KuduTestSuite):

  def test_create_synchronized_table(self, kudu_client, unique_database):
    """
    Creates a synchronized Kudu table and makes sure that the statement does not fail.
    """
    table_name = self.random_table_name()
    # create a external kudu table with external.table.purge=true
    self.execute_query("""
      CREATE EXTERNAL TABLE {}.{} (
        id int PRIMARY KEY,
        name string)
      PARTITION BY HASH PARTITIONS 8
      STORED AS KUDU
      TBLPROPERTIES ('external.table.purge'='true')
    """.format(unique_database, table_name))
    # make sure that the table was created
    result = self.execute_query("SHOW TABLES IN {}".format(unique_database))
    assert table_name in result.data
    # make sure that the kudu table was created with default name
    assert kudu_client.table_exists(self.to_kudu_table_name(unique_database, table_name))
    # make sure that the external.table.purge property can be changed
    self.execute_query(
      "ALTER TABLE {}.{} set TBLPROPERTIES ('external.table.purge'='FALSE')".format(
        unique_database, table_name))
    result = self.execute_query("SHOW TABLES IN {}".format(unique_database))
    assert table_name in result.data
    self.execute_query(
      "ALTER TABLE {}.{} set TBLPROPERTIES ('external.table.purge'='TRUE')".format(
        unique_database, table_name))
    result = self.execute_query("SHOW TABLES IN {}".format(unique_database))
    assert table_name in result.data
    # make sure that table can be renamed
    new_table_name = self.random_table_name()
    self.execute_query(
      "ALTER TABLE {}.{} rename to {}.{}".format(
        unique_database, table_name, unique_database, new_table_name))
    result = self.execute_query("SHOW TABLES IN {}".format(unique_database))
    assert new_table_name in result.data
    # make sure that the kudu table was created with default name
    assert kudu_client.table_exists(
      self.to_kudu_table_name(unique_database, new_table_name))
    # now make sure that table disappears after we remove it
    self.execute_query("DROP TABLE {}.{}".format(unique_database, new_table_name))
    result = self.execute_query("SHOW TABLES IN {}".format(unique_database))
    assert new_table_name not in result.data
    assert not kudu_client.table_exists(
      self.to_kudu_table_name(unique_database, new_table_name))

  def test_invalid_sync_table_stmts(self, unique_database):
    """
    Test makes sure that a invalid way to create a synchronized table is erroring out
    """
    table_name = self.random_table_name()
    try:
      self.execute_query("""
        CREATE EXTERNAL TABLE %s.%s (
          a int PRIMARY KEY)
        PARTITION BY HASH PARTITIONS 8
        STORED AS KUDU
        TBLPROPERTIES ('external.table.purge'='false')
      """ % (unique_database, table_name))
      assert False, \
          "Create table statement with external.table.purge=False should error out"
    except Exception as e:
      # We throw this exception since the analyzer checks for properties one by one.
      # This is the first property that it checks for an external table
      assert "Table property kudu.table_name must be specified when " \
             "creating an external Kudu table" in str(e)

    try:
      # missing external.table.purge in TBLPROPERTIES
      self.execute_query("""
        CREATE EXTERNAL TABLE %s.%s (
          a int PRIMARY KEY)
        PARTITION BY HASH PARTITIONS 8
        STORED AS KUDU
        TBLPROPERTIES ('FOO'='BAR')
        """ % (unique_database, table_name))
      assert False, \
          "Create external table statement must include external.table.purge property"
    except Exception as e:
      # We throw this exception since the analyzer checks for properties one by one.
      # This is the first property that it checks for an external table
      assert "Table property kudu.table_name must be specified when " \
             "creating an external Kudu table" in str(e)

    try:
      # Trying to create a managed table with external.purge.table property in it
      self.execute_query("""
        CREATE TABLE %s.%s (
          a int PRIMARY KEY)
        PARTITION BY HASH PARTITIONS 8
        STORED AS KUDU
        TBLPROPERTIES ('external.table.purge'='true')
              """ % (unique_database, table_name))
      assert False, \
          "Managed table creation with external.table.purge property must be disallowed"
    except Exception as e:
      assert "Table property 'external.table.purge' cannot be set to true " \
             "with an managed Kudu table." in str(e)

    # TODO should we block this?
    self.execute_query("""
      CREATE TABLE %s.%s (
        a int PRIMARY KEY)
      PARTITION BY HASH PARTITIONS 8
      STORED AS KUDU
      TBLPROPERTIES ('external.table.purge'='False')""" % (unique_database, table_name))
    result = self.execute_query("SHOW TABLES IN %s" % unique_database)
    assert table_name in result.data

  def test_sync_tbl_with_kudu_table(self, kudu_client, unique_database):
    """
    Test tries to create a synchronized table with an existing Kudu table name and
    makes sure it fails.
    """
    create_query = (
      """
      CREATE EXTERNAL TABLE {}.{} (
        a int PRIMARY KEY)
      PARTITION BY HASH PARTITIONS 8
      STORED AS KUDU
      TBLPROPERTIES('external.table.purge'='true', 'kudu.table_name' = '{}')"""
    )
    with self.temp_kudu_table(kudu_client, [INT32]) as kudu_table:
      table_name = self.random_table_name()
      try:
        self.execute_query(
          create_query.format(
            unique_database, table_name, self.get_kudu_table_base_name(kudu_table.name)))
        assert False, ("External tables with external.purge.table property must fail "
                       "if the kudu table already exists")
      except Exception as e:
        assert ("Not allowed to set 'kudu.table_name' manually for"
                " synchronized Kudu tables") in str(e)


class TestKuduReadTokenSplit(KuduTestSuite):
  """
  This suite verifies impala's integration of Kudu's split token API.
  """

  @classmethod
  def add_test_dimensions(cls):
    super(TestKuduReadTokenSplit, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())

  @SkipIfKudu.no_hybrid_clock()
  @SkipIfNotHdfsMinicluster.tuned_for_minicluster
  def test_kudu_scanner(self, vector):
    """This runs explain query with variations of mt_dop and
    targeted_kudu_scan_range_length to verify targeted_kudu_scan_range_length's
    functionality.
    Test disabled for EC since the erasure coded files when loaded in kudu
    during data load cause the expected behaviour to change"""
    explain_query = "explain select * from tpch_kudu.lineitem "
    plans = []

    regular_num_inst = self.__get_num_scanner_instances(
      explain_query, vector, mt_dop=None, targeted_kudu_scan_range_length=None,
      plans=plans)

    mt_dop_1_num_inst = self.__get_num_scanner_instances(
      explain_query, vector, mt_dop=1, targeted_kudu_scan_range_length=None,
      plans=plans)

    # targeted_kudu_scan_range_length should be disabled by default and num instances
    # will be equal to the number of partitions
    with_mt_dop_num_inst = self.__get_num_scanner_instances(
      explain_query, vector, mt_dop=10, targeted_kudu_scan_range_length=None,
      plans=plans)

    # This will result is more splits
    with_mt_dop_and_low_range_len_num_inst = self.__get_num_scanner_instances(
      explain_query, vector, mt_dop=10, targeted_kudu_scan_range_length="8mb",
      plans=plans)

    assert mt_dop_1_num_inst == regular_num_inst, str(plans)
    assert regular_num_inst < with_mt_dop_num_inst, str(plans)
    assert with_mt_dop_num_inst < with_mt_dop_and_low_range_len_num_inst, str(plans)

  def __get_num_scanner_instances(self, explain_query, vector, mt_dop,
                                  targeted_kudu_scan_range_length, plans):
    """This is a helper method that runs the explain query with the provided query
    options (mt_dop and targeted_kudu_scan_range_length). Appends the generated plan to
    'plans' and returns the num of kudu scanner instances """
    regex = r'F00:PLAN FRAGMENT \[RANDOM\] hosts=3 instances=([0-9]+)'
    # The default read mode of READ_LATEST does not provide high enough consistency for
    # these tests.
    client = self.create_impala_client(protocol=vector.get_value('protocol'))
    client.set_configuration_option("kudu_read_mode", "READ_AT_SNAPSHOT")
    client.set_configuration_option("explain_level", 3)
    if targeted_kudu_scan_range_length:
      client.set_configuration_option(
        "targeted_kudu_scan_range_length", targeted_kudu_scan_range_length)
    if mt_dop:
      client.set_configuration_option("mt_dop", mt_dop)
    result = client.execute(explain_query)
    plan = "\n".join(result.data)
    plans.append(plan)
    matches = re.search(regex, plan)
    assert len(matches.groups()) == 1, plan
    client.clear_configuration()
    return int(matches.group(1))


@SkipIfHive2.create_external_kudu_table
class TestKuduInsertWithBufferedTupleDesc(KuduTestSuite):
  """
  This test verifies bug fixing for IMPALA-11029.
  """

  # queries to create Kudu tables.
  _create_kudu_table_1_query = "CREATE TABLE {0} (id1 INT NOT NULL, " \
      "agrmt INT NOT NULL, big_id BIGINT NOT NULL, outdated_flag STRING NOT NULL, " \
      "mod_ts TIMESTAMP NOT NULL, PRIMARY KEY (id1, agrmt)) " \
      "PARTITION BY HASH (id1) PARTITIONS 2 STORED AS KUDU"

  _create_kudu_table_2_query = "CREATE TABLE {0} (cl_id INT NOT NULL, " \
      "cl_agrmt INT NOT NULL, outdat STRING NULL, mod_dat TIMESTAMP NULL, " \
      "PRIMARY KEY (cl_id, cl_agrmt)) " \
      "PARTITION BY HASH (cl_id) PARTITIONS 2 STORED AS KUDU"

  # query to insert rows to Kudu table.
  _insert_query = "INSERT INTO {0} (id1, agrmt, big_id, outdated_flag, mod_ts) " \
      "SELECT i.cl_id, cast(row_number() over(order by null) as int), i.cl_agrmt, 'Y', " \
      "case when outdat='Y' and i.mod_dat is not null then i.mod_dat else now() end " \
      "from {1} i left join {0} u on u.big_id=i.cl_agrmt " \
      "left join (select id1, big_id from {0} group by id1, big_id) uu " \
      "on uu.big_id=i.cl_agrmt " \
      "where u.big_id is null"

  @SkipIfKudu.no_hybrid_clock()
  def test_kudu_insert_with_buffered_tuple_desc(self, kudu_client, unique_database):
    # Create Kudu tables.
    table_1_name = "%s.tab1" % unique_database
    self.execute_query(self._create_kudu_table_1_query.format(table_1_name))
    assert kudu_client.table_exists(
        KuduTestSuite.to_kudu_table_name(unique_database, "tab1"))
    table_2_name = "%s.tab2" % unique_database
    self.execute_query(self._create_kudu_table_2_query.format(table_2_name))
    assert kudu_client.table_exists(
        KuduTestSuite.to_kudu_table_name(unique_database, "tab2"))

    # Insert rows
    try:
      self.execute_query(self._insert_query.format(table_1_name, table_2_name))
      result = self.execute_query("SELECT * FROM %s" % table_1_name)
      assert len(result.data) == 0
    except Exception as e:
      # Not expect to throw exception like "IllegalStateException: null"
      assert False, str(e)
