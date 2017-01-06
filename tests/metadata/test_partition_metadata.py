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

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfS3, SkipIfIsilon, SkipIfLocal
from tests.common.test_dimensions import create_single_exec_option_dimension
from tests.util.filesystem_utils import WAREHOUSE

# Tests specific to partition metadata.
# TODO: Split up the DDL tests and move some of the partition-specific tests
# here.
class TestPartitionMetadata(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestPartitionMetadata, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())

    # There is no reason to run these tests using all dimensions.
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text' and\
        v.get_value('table_format').compression_codec == 'none')

  @SkipIfLocal.hdfs_client
  def test_multiple_partitions_same_location(self, vector, unique_database):
    """Regression test for IMPALA-597. Verifies Impala is able to properly read
    tables that have multiple partitions pointing to the same location.
    """
    TBL_NAME = "same_loc_test"
    FQ_TBL_NAME = unique_database + "." + TBL_NAME
    TBL_LOCATION = '%s/%s.db/%s' % (WAREHOUSE, unique_database, TBL_NAME)
    # Cleanup any existing data in the table directory.
    self.filesystem_client.delete_file_dir(TBL_NAME, recursive=True)
    # Create the table
    self.client.execute("create table %s (i int) partitioned by(j int) location '%s'"
        % (FQ_TBL_NAME, TBL_LOCATION))

    # Point multiple partitions to the same location and use partition locations that
    # do not contain a key=value path.
    self.filesystem_client.make_dir(TBL_NAME + '/p')

    # Point both partitions to the same location.
    self.client.execute("alter table %s add partition (j=1) location '%s/p'"
        % (FQ_TBL_NAME, TBL_LOCATION))
    self.client.execute("alter table %s add partition (j=2) location '%s/p'"
        % (FQ_TBL_NAME, TBL_LOCATION))

    # Insert some data. This will only update partition j=1 (IMPALA-1480).
    self.client.execute("insert into table %s partition(j=1) select 1" % FQ_TBL_NAME)
    # Refresh to update file metadata of both partitions.
    self.client.execute("refresh %s" % FQ_TBL_NAME)

    # The data will be read twice because each partition points to the same location.
    data = self.execute_scalar("select sum(i), sum(j) from %s" % FQ_TBL_NAME)
    assert data.split('\t') == ['2', '3']

    self.client.execute("insert into %s partition(j) select 1, 1" % FQ_TBL_NAME)
    self.client.execute("insert into %s partition(j) select 1, 2" % FQ_TBL_NAME)
    self.client.execute("refresh %s" % FQ_TBL_NAME)
    data = self.execute_scalar("select sum(i), sum(j) from %s" % FQ_TBL_NAME)
    assert data.split('\t') == ['6', '9']

  @SkipIfS3.hive
  @SkipIfIsilon.hive
  @SkipIfLocal.hive
  def test_partition_metadata_compatibility(self, vector, unique_database):
    """Regression test for IMPALA-2048. For partitioned tables, test that when Impala
    updates the partition metadata (e.g. by doing a compute stats), the tables are
    accessible in Hive."""
    FQ_TBL_HIVE = unique_database + ".part_parquet_tbl_hive"
    FQ_TBL_IMP = unique_database + ".part_parquet_tbl_impala"
    # First case, the table is created in HIVE.
    self.run_stmt_in_hive("create table %s (a int) partitioned by (x int) "\
        "stored as parquet" % FQ_TBL_HIVE)
    self.run_stmt_in_hive("set hive.exec.dynamic.partition.mode=nostrict;"\
        "insert into %s partition (x) values(1,1)" % FQ_TBL_HIVE)
    self.run_stmt_in_hive("select * from %s" % FQ_TBL_HIVE)
    # Load the table in Impala and modify its partition metadata by computing table
    # statistics.
    self.client.execute("invalidate metadata %s" % FQ_TBL_HIVE)
    self.client.execute("compute stats %s" % FQ_TBL_HIVE)
    self.client.execute("select * from %s" % FQ_TBL_HIVE)
    # Make sure the table is accessible in Hive
    self.run_stmt_in_hive("select * from %s" % FQ_TBL_HIVE)

    # Second case, the table is created in Impala
    self.client.execute("create table %s (a int) partitioned by (x int) "\
        "stored as parquet" % FQ_TBL_IMP)
    self.client.execute("insert into %s partition(x) values(1,1)" % FQ_TBL_IMP)
    # Make sure the table is accessible in HIVE
    self.run_stmt_in_hive("select * from %s" % FQ_TBL_IMP)
    # Compute table statistics
    self.client.execute("compute stats %s" % FQ_TBL_IMP)
    self.client.execute("select * from %s" % FQ_TBL_IMP)
    # Make sure the table remains accessible in HIVE
    self.run_stmt_in_hive("select * from %s" % FQ_TBL_IMP)
