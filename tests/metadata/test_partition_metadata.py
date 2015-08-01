# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import pytest
import shlex
import time
from subprocess import call
from tests.common.test_result_verifier import *
from tests.util.shell_util import exec_process
from tests.common.test_vector import *
from tests.common.test_dimensions import ALL_NODES_ONLY
from tests.common.impala_test_suite import *
from tests.common.skip import SkipIfS3, SkipIfIsilon
from tests.util.filesystem_utils import WAREHOUSE


# Tests specific to partition metadata.
# TODO: Split up the DDL tests and move some of the partition-specific tests
# here.
@pytest.mark.execute_serially
class TestPartitionMetadata(ImpalaTestSuite):
  TEST_DB = 'partition_md'
  TEST_TBL = 'bulk_part'

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestPartitionMetadata, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(create_single_exec_option_dimension())

    # There is no reason to run these tests using all dimensions.
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text' and\
        v.get_value('table_format').compression_codec == 'none')

  def setup_method(self, method):
    self.cleanup_db(self.TEST_DB)
    ddl = "create database {0} location '{1}/{0}.db'".format(self.TEST_DB, WAREHOUSE)
    self.client.execute(ddl)

  def teardown_method(self, method):
    self.cleanup_db(self.TEST_DB)

  @SkipIfS3.insert # S3: missing coverage: partition DDL
  def test_multiple_partitions_same_location(self, vector):
    """Regression test for IMPALA-597. Verifies Impala is able to properly read
    tables that have multiple partitions pointing to the same location.
    """
    self.client.execute("use %s" % self.TEST_DB)
    impala_location = '%s/%s.db/%s' % (WAREHOUSE, self.TEST_DB, self.TEST_TBL)
    hdfs_client_location = impala_location.split("/")[-1]
    # Cleanup any existing data in the table directory.
    self.hdfs_client.delete_file_dir(hdfs_client_location, recursive=True)
    # Create the table
    self.client.execute("create table {0}(i int) partitioned by(j int)"
        "location '{1}/{2}.db/{0}'".format(self.TEST_TBL, WAREHOUSE, self.TEST_DB))

    # Point multiple partitions to the same location and use partition locations that
    # do not contain a key=value path.
    self.hdfs_client.make_dir(hdfs_client_location + '/p')

    # Point both partitions to the same location.
    self.client.execute("alter table %s add partition (j=1) location '%s/p'" %
        (self.TEST_TBL, impala_location))
    self.client.execute("alter table %s add partition (j=2) location '%s/p'" %
        (self.TEST_TBL, impala_location))

    # Insert some data.
    self.client.execute("insert into table %s partition(j=1) select 1" % self.TEST_TBL)

    # The data will be read twice because each partition points to the same location.
    data = self.execute_scalar("select sum(i), sum(j) from %s" % self.TEST_TBL)
    assert data.split('\t') == ['2', '3']

    self.client.execute("insert into %s partition(j) select 1, 1" % self.TEST_TBL)
    self.client.execute("insert into %s partition(j) select 1, 2" % self.TEST_TBL)
    data = self.execute_scalar("select sum(i), sum(j) from %s" % self.TEST_TBL)
    assert data.split('\t') == ['6', '9']

  @SkipIfS3.hive
  @SkipIfIsilon.hive
  def test_partition_metadata_compatibility(self, vector):
    """Regression test for IMPALA-2048. For partitioned tables, test that when Impala
    updates the partition metadata (e.g. by doing a compute stats), the tables are
    accessible in Hive."""
    TEST_TBL_HIVE = "part_parquet_tbl_hive"
    TEST_TBL_IMP = "part_parquet_tbl_impala"
    # First case, the table is created in HIVE.
    self.run_stmt_in_hive("create table %s.%s(a int) partitioned by (x int) "\
        "stored as parquet" % (self.TEST_DB, TEST_TBL_HIVE))
    self.run_stmt_in_hive("set hive.exec.dynamic.partition.mode=nostrict;"\
        "insert into %s.%s partition (x) values(1,1)" % (self.TEST_DB, TEST_TBL_HIVE))
    self.run_stmt_in_hive("select * from %s.%s" % (self.TEST_DB, TEST_TBL_HIVE))
    # Load the table in Impala and modify its partition metadata by computing table
    # statistics.
    self.client.execute("invalidate metadata %s.%s" % (self.TEST_DB, TEST_TBL_HIVE))
    self.client.execute("compute stats %s.%s" % (self.TEST_DB, TEST_TBL_HIVE))
    self.client.execute("select * from %s.%s" % (self.TEST_DB, TEST_TBL_HIVE))
    # Make sure the table is accessible in Hive
    self.run_stmt_in_hive("select * from %s.%s" % (self.TEST_DB, TEST_TBL_HIVE))

    # Second case, the table is created in Impala
    self.client.execute("create table %s.%s(a int) partitioned by (x int) "\
        "stored as parquet" % (self.TEST_DB, TEST_TBL_IMP))
    self.client.execute("insert into %s.%s partition(x) values(1,1)" % (self.TEST_DB,
        TEST_TBL_IMP))
    # Make sure the table is accessible in HIVE
    self.run_stmt_in_hive("select * from %s.%s" % (self.TEST_DB, TEST_TBL_IMP))
    # Compute table statistics
    self.client.execute("compute stats %s.%s" % (self.TEST_DB, TEST_TBL_IMP))
    self.client.execute("select * from %s.%s" % (self.TEST_DB, TEST_TBL_IMP))
    # Make sure the table remains accessible in HIVE
    self.run_stmt_in_hive("select * from %s.%s" % (self.TEST_DB, TEST_TBL_IMP))

  def run_stmt_in_hive(self, stmt):
    hive_ret = call(['hive', '-e', stmt])
    assert hive_ret == 0, 'Error executing statement %s in Hive' % stmt

