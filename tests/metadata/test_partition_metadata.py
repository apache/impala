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
import pytest
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfFS, SkipIfLocal
from tests.common.test_dimensions import (create_single_exec_option_dimension,
    create_uncompressed_text_dimension, FILE_FORMAT_TO_STORED_AS_MAP)
from tests.util.filesystem_utils import WAREHOUSE, FILESYSTEM_PREFIX

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

    # Run one variation of the test with each file formats that we support writing.
    # The compression shouldn't affect the partition handling so restrict to the core
    # compression codecs.
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        (v.get_value('table_format').file_format in ('text', 'parquet') and
         v.get_value('table_format').compression_codec == 'none'))

  @SkipIfLocal.hdfs_client # TODO: this dependency might not exist anymore
  def test_multiple_partitions_same_location(self, vector, unique_database):
    """Regression test for IMPALA-597. Verifies Impala is able to properly read
    tables that have multiple partitions pointing to the same location.
    """
    TBL_NAME = "same_loc_test"
    FQ_TBL_NAME = unique_database + "." + TBL_NAME
    TBL_LOCATION = '%s/%s.db/%s' % (WAREHOUSE, unique_database, TBL_NAME)
    file_format = vector.get_value('table_format').file_format
    # Create the table
    self.client.execute(
        "create table %s (i int) partitioned by(j int) stored as %s location '%s'"
        % (FQ_TBL_NAME, FILE_FORMAT_TO_STORED_AS_MAP[file_format], TBL_LOCATION))

    # Point both partitions to the same location.
    self.client.execute("alter table %s add partition (j=1) location '%s/p'"
        % (FQ_TBL_NAME, TBL_LOCATION))
    self.client.execute("alter table %s add partition (j=2) location '%s/p'"
        % (FQ_TBL_NAME, TBL_LOCATION))

    # Insert some data. This will only update partition j=1 (IMPALA-1480).
    self.client.execute("insert into table %s partition(j=1) select 1" % FQ_TBL_NAME)
    # Refresh to update file metadata of both partitions
    self.client.execute("refresh %s" % FQ_TBL_NAME)

    # The data will be read twice because each partition points to the same location.
    data = self.execute_scalar("select sum(i), sum(j) from %s" % FQ_TBL_NAME)
    assert data.split('\t') == ['2', '3']

    self.client.execute("insert into %s partition(j) select 1, 1" % FQ_TBL_NAME)
    self.client.execute("insert into %s partition(j) select 1, 2" % FQ_TBL_NAME)
    self.client.execute("refresh %s" % FQ_TBL_NAME)
    data = self.execute_scalar("select sum(i), sum(j) from %s" % FQ_TBL_NAME)
    assert data.split('\t') == ['6', '9']

    # Force all scan ranges to be on the same node. It should produce the same
    # result as above. See IMPALA-5412.
    self.client.execute("set num_nodes=1")
    data = self.execute_scalar("select sum(i), sum(j) from %s" % FQ_TBL_NAME)
    assert data.split('\t') == ['6', '9']

  @SkipIfFS.hive
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


class TestMixedPartitions(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestMixedPartitions, cls).add_test_dimensions()
    # This test only needs to be run once.
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))

  @pytest.mark.parametrize('main_table_format', ['parquetfile', 'textfile'])
  def test_incompatible_avro_partition_in_non_avro_table(
      self, vector, unique_database, main_table_format):
    if main_table_format == 'parquetfile' and \
        not pytest.config.option.use_local_catalog:
      pytest.xfail("IMPALA-7309: adding an avro partition to a parquet table "
                   "changes its schema")
    self.run_test_case("QueryTest/incompatible_avro_partition", vector,
                       unique_database,
                       test_file_vars={'$MAIN_TABLE_FORMAT': main_table_format})


class TestPartitionMetadataUncompressedTextOnly(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestPartitionMetadataUncompressedTextOnly, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))

  @SkipIfLocal.hdfs_client
  def test_unsupported_text_compression(self, vector, unique_database):
    """Test querying tables with a mix of supported and unsupported compression codecs.
    Should be able to query partitions with supported codecs."""
    if FILESYSTEM_PREFIX:
      pytest.xfail("IMPALA-7099: this test's filesystem prefix handling is broken")
    TBL_NAME = "multi_text_compression"
    FQ_TBL_NAME = unique_database + "." + TBL_NAME
    TBL_LOCATION = '%s/%s.db/%s' % (WAREHOUSE, unique_database, TBL_NAME)

    file_format = vector.get_value('table_format').file_format
    # Clean up any existing data in the table directory.
    self.filesystem_client.delete_file_dir(TBL_NAME, recursive=True)
    # Create the table
    self.client.execute(
        "create external table {0} like functional.alltypes location '{1}'".format(
        FQ_TBL_NAME, TBL_LOCATION))

    self.__add_alltypes_partition(vector, FQ_TBL_NAME, "functional", 2009, 1)
    self.__add_alltypes_partition(vector, FQ_TBL_NAME, "functional_text_gzip", 2009, 2)

    # Create a new partition with a bogus file with the unsupported LZ4 suffix.
    lz4_year = 2009
    lz4_month = 3
    lz4_ym_partition_loc = self.__make_ym_partition_dir(TBL_LOCATION, lz4_year, lz4_month)
    self.filesystem_client.create_file("{0}/fake.lz4".format(lz4_ym_partition_loc),
        "some test data")
    self.client.execute(
        "alter table {0} add partition (year={1}, month={2}) location '{3}'".format(
        FQ_TBL_NAME, lz4_year, lz4_month, lz4_ym_partition_loc))

    # Create a new partition with a bogus compression codec.
    fake_comp_year = 2009
    fake_comp_month = 4
    fake_comp_ym_partition_loc = self.__make_ym_partition_dir(
        TBL_LOCATION, fake_comp_year, fake_comp_month)
    self.filesystem_client.create_file(
        "{0}/fake.fake_comp".format(fake_comp_ym_partition_loc), "fake compression")
    self.client.execute(
        "alter table {0} add partition (year={1}, month={2}) location '{3}'".format(
        FQ_TBL_NAME, fake_comp_year, fake_comp_month, fake_comp_ym_partition_loc))

    # Create a new partition with a bogus file with the now-unsupported LZO suffix
    lzo_year = 2009
    lzo_month = 5
    lzo_ym_partition_loc = self.__make_ym_partition_dir(TBL_LOCATION, lzo_year, lzo_month)
    self.filesystem_client.create_file("{0}/fake.lzo".format(lzo_ym_partition_loc),
        "some test data")
    self.client.execute(
        "alter table {0} add partition (year={1}, month={2}) location '{3}'".format(
            FQ_TBL_NAME, lzo_year, lzo_month, lzo_ym_partition_loc))

    show_files_result = self.client.execute("show files in {0}".format(FQ_TBL_NAME))
    assert len(show_files_result.data) == 5, "Expected one file per partition dir"

    self.run_test_case('QueryTest/unsupported-compression-partitions', vector,
        unique_database)

  def __add_alltypes_partition(self, vector, dst_tbl, src_db, year, month):
    """Add the (year, month) partition from ${db_name}.alltypes to dst_tbl."""
    tbl_location = self._get_table_location("{0}.alltypes".format(src_db), vector)
    part_location = "{0}/year={1}/month={2}".format(tbl_location, year, month)
    self.client.execute(
        "alter table {0} add partition (year={1}, month={2}) location '{3}'".format(
        dst_tbl, year, month, part_location))

  def __make_ym_partition_dir(self, tbl_location, year, month):
    """Create the year/month partition directory and return the path."""
    y_partition_loc = "{0}/year={1}".format(tbl_location, year)
    ym_partition_loc = "{0}/month={1}".format(y_partition_loc, month)
    if not self.filesystem_client.exists(tbl_location):
      self.filesystem_client.make_dir(tbl_location)
    if not self.filesystem_client.exists(y_partition_loc):
      self.filesystem_client.make_dir(y_partition_loc)
    if self.filesystem_client.exists(ym_partition_loc):
      self.filesystem_client.delete_file_dir(ym_partition_loc, recursive=True)
    self.filesystem_client.make_dir(ym_partition_loc)
    return ym_partition_loc
