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

# Targeted Impala insert tests

import os
import pytest

from testdata.common import widetable
from tests.common.impala_cluster import ImpalaCluster
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.parametrize import UniqueDatabase
from tests.common.skip import SkipIfABFS, SkipIfEC, SkipIfLocal, \
    SkipIfNotHdfsMinicluster, SkipIfS3, SkipIfDockerizedCluster
from tests.common.test_dimensions import (
    create_exec_option_dimension,
    create_uncompressed_text_dimension)
from tests.common.test_result_verifier import (
    QueryTestResult,
    parse_result_rows)
from tests.common.test_vector import ImpalaTestDimension
from tests.verifiers.metric_verifier import MetricVerifier

PARQUET_CODECS = ['none', 'snappy', 'gzip']

class TestInsertQueries(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestInsertQueries, cls).add_test_dimensions()
    # Fix the exec_option vector to have a single value. This is needed should we decide
    # to run the insert tests in parallel (otherwise there will be two tests inserting
    # into the same table at the same time for the same file format).
    # TODO: When we do decide to run these tests in parallel we could create unique temp
    # tables for each test case to resolve the concurrency problems.
    if cls.exploration_strategy() == 'core':
      cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension(
          cluster_sizes=[0], disable_codegen_options=[True, False], batch_sizes=[0],
          sync_ddl=[0]))
      cls.ImpalaTestMatrix.add_dimension(
          create_uncompressed_text_dimension(cls.get_workload()))
    else:
      cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension(
          cluster_sizes=[0], disable_codegen_options=[True, False], batch_sizes=[0, 1, 16],
          sync_ddl=[0, 1]))
      cls.ImpalaTestMatrix.add_dimension(
          ImpalaTestDimension("compression_codec", *PARQUET_CODECS));
      # Insert is currently only supported for text and parquet
      # For parquet, we want to iterate through all the compression codecs
      # TODO: each column in parquet can have a different codec.  We could
      # test all the codecs in one table/file with some additional flags.
      cls.ImpalaTestMatrix.add_constraint(lambda v:\
          v.get_value('table_format').file_format == 'parquet' or \
            (v.get_value('table_format').file_format == 'text' and \
            v.get_value('compression_codec') == 'none'))
      cls.ImpalaTestMatrix.add_constraint(lambda v:\
          v.get_value('table_format').compression_codec == 'none')
      # Only test other batch sizes for uncompressed parquet to keep the execution time
      # within reasonable bounds.
      cls.ImpalaTestMatrix.add_constraint(lambda v:\
          v.get_value('exec_option')['batch_size'] == 0 or \
            (v.get_value('table_format').file_format == 'parquet' and \
            v.get_value('compression_codec') == 'none'))

  @pytest.mark.execute_serially
  def test_insert_large_string(self, vector, unique_database):
    """Test handling of large strings in inserter and scanner."""
    if "-Xcheck:jni" in os.environ.get("LIBHDFS_OPTS", ""):
      pytest.skip("Test unreasonably slow with JNI checking.")
    table_name = unique_database + ".insert_largestring"

    file_format = vector.get_value('table_format').file_format
    if file_format == "parquet":
      stored_as = file_format
    else:
      assert file_format == "text"
      stored_as = "textfile"
    self.client.execute("""
        create table {0}
        stored as {1} as
        select repeat('AZ', 128 * 1024 * 1024) as s""".format(table_name, stored_as))

    # Make sure it produces correct result when materializing no tuples.
    result = self.client.execute("select count(*) from {0}".format(table_name))
    assert result.data == ["1"]

    # Make sure it got the length right.
    result = self.client.execute("select length(s) from {0}".format(table_name))
    assert result.data == [str(2 * 128 * 1024 * 1024)]

    # Spot-check the data.
    result = self.client.execute(
        "select substr(s, 200 * 1024 * 1024, 5) from {0}".format(table_name))
    assert result.data == ["ZAZAZ"]

    # IMPALA-7648: test that we gracefully fail when there is not enough memory
    # to fit the scanned string in memory.
    self.client.set_configuration_option("mem_limit", "50M")
    try:
      self.client.execute("select s from {0}".format(table_name))
      assert False, "Expected query to fail"
    except Exception, e:
      assert "Memory limit exceeded" in str(e)


  @classmethod
  def setup_class(cls):
    super(TestInsertQueries, cls).setup_class()

  @pytest.mark.execute_serially
  # Erasure coding doesn't respect memory limit
  @SkipIfEC.fix_later
  # ABFS partition names cannot end in periods
  @SkipIfABFS.jira(reason="HADOOP-15860")
  def test_insert(self, vector):
    if (vector.get_value('table_format').file_format == 'parquet'):
      vector.get_value('exec_option')['COMPRESSION_CODEC'] = \
          vector.get_value('compression_codec')
    self.run_test_case('QueryTest/insert', vector,
        multiple_impalad=vector.get_value('exec_option')['sync_ddl'] == 1)

  @pytest.mark.execute_serially
  @SkipIfNotHdfsMinicluster.tuned_for_minicluster
  def test_insert_mem_limit(self, vector):
    if (vector.get_value('table_format').file_format == 'parquet'):
      vector.get_value('exec_option')['COMPRESSION_CODEC'] = \
          vector.get_value('compression_codec')
    self.run_test_case('QueryTest/insert-mem-limit', vector,
        multiple_impalad=vector.get_value('exec_option')['sync_ddl'] == 1)
    # IMPALA-7023: These queries can linger and use up memory, causing subsequent
    # tests to hit memory limits. Wait for some time to allow the query to
    # be reclaimed.
    verifiers = [MetricVerifier(i.service)
                 for i in ImpalaCluster.get_e2e_test_cluster().impalads]
    for v in verifiers:
      v.wait_for_metric("impala-server.num-fragments-in-flight", 0, timeout=60)

  @pytest.mark.execute_serially
  @SkipIfS3.eventually_consistent
  def test_insert_overwrite(self, vector):
    self.run_test_case('QueryTest/insert_overwrite', vector,
        multiple_impalad=vector.get_value('exec_option')['sync_ddl'] == 1)

  @pytest.mark.execute_serially
  def test_insert_bad_expr(self, vector):
    # The test currently relies on codegen being disabled to trigger an error in
    # the output expression of the table sink.
    if vector.get_value('exec_option')['disable_codegen']:
      self.run_test_case('QueryTest/insert_bad_expr', vector,
          multiple_impalad=vector.get_value('exec_option')['sync_ddl'] == 1)

  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_insert_random_partition(self, vector, unique_database):
    """Regression test for IMPALA-402: partitioning by rand() leads to strange behaviour
    or crashes."""
    self.run_test_case('QueryTest/insert-random-partition', vector, unique_database,
        multiple_impalad=vector.get_value('exec_option')['sync_ddl'] == 1)

class TestInsertWideTable(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestInsertWideTable, cls).add_test_dimensions()

    # Only vary codegen
    cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension(
        cluster_sizes=[0], disable_codegen_options=[True, False], batch_sizes=[0]))

    # Inserts only supported on text and parquet
    # TODO: Enable 'text'/codec once the compressed text writers are in.
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'parquet' or \
        v.get_value('table_format').file_format == 'text')
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').compression_codec == 'none')

    # Don't run on core. This test is very slow (IMPALA-864) and we are unlikely to
    # regress here.
    if cls.exploration_strategy() == 'core':
      cls.ImpalaTestMatrix.add_constraint(lambda v: False);

  @SkipIfLocal.parquet_file_size
  def test_insert_wide_table(self, vector, unique_database):
    table_format = vector.get_value('table_format')

    # Text can't handle as many columns as Parquet (codegen takes forever)
    num_cols = 1000 if table_format.file_format == 'text' else 2000

    table_name = unique_database + ".insert_widetable"
    if vector.get_value('exec_option')['disable_codegen']:
      table_name += "_codegen_disabled"

    col_descs = widetable.get_columns(num_cols)
    create_stmt = "CREATE TABLE " + table_name + "(" + ','.join(col_descs) + ")"
    if vector.get_value('table_format').file_format == 'parquet':
      create_stmt += " stored as parquet"
    self.client.execute(create_stmt)

    # Get a single row of data
    col_vals = widetable.get_data(num_cols, 1, quote_strings=True)[0]
    insert_stmt = "INSERT INTO " + table_name + " VALUES(" + col_vals + ")"
    self.client.execute(insert_stmt)

    result = self.client.execute("select count(*) from " + table_name)
    assert result.data == ["1"]

    result = self.client.execute("select * from " + table_name)
    types = result.column_types
    labels = result.column_labels
    expected = QueryTestResult([col_vals], types, labels, order_matters=False)
    actual = QueryTestResult(parse_result_rows(result), types, labels, order_matters=False)
    assert expected == actual

class TestInsertPartKey(ImpalaTestSuite):
  """Regression test for IMPALA-875"""
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestInsertPartKey, cls).add_test_dimensions()
    # Only run for a single table type
    cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension(
        cluster_sizes=[0], disable_codegen_options=[False], batch_sizes=[0],
        sync_ddl=[1]))

    cls.ImpalaTestMatrix.add_constraint(lambda v:
        (v.get_value('table_format').file_format == 'text'))
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').compression_codec == 'none')

  @pytest.mark.execute_serially
  def test_insert_part_key(self, vector):
    """Test that partition column exprs are cast to the correct type. See IMPALA-875."""
    self.run_test_case('QueryTest/insert_part_key', vector,
        multiple_impalad=vector.get_value('exec_option')['sync_ddl'] == 1)

class TestInsertNullQueries(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestInsertNullQueries, cls).add_test_dimensions()
    # Fix the exec_option vector to have a single value. This is needed should we decide
    # to run the insert tests in parallel (otherwise there will be two tests inserting
    # into the same table at the same time for the same file format).
    # TODO: When we do decide to run these tests in parallel we could create unique temp
    # tables for each test case to resolve the concurrency problems.
    cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension(
        cluster_sizes=[0], disable_codegen_options=[False], batch_sizes=[0]))

    # These tests only make sense for inserting into a text table with special
    # logic to handle all the possible ways NULL needs to be written as ascii
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
          (v.get_value('table_format').file_format == 'text' and \
           v.get_value('table_format').compression_codec == 'none'))

  @classmethod
  def setup_class(cls):
    super(TestInsertNullQueries, cls).setup_class()

  @pytest.mark.execute_serially
  def test_insert_null(self, vector):
    self.run_test_case('QueryTest/insert_null', vector)
