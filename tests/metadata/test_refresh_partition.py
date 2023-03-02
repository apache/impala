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


from __future__ import absolute_import, division, print_function
from subprocess import check_call

from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import create_single_exec_option_dimension
from tests.common.test_dimensions import create_uncompressed_text_dimension
from tests.common.skip import SkipIfFS
from tests.util.filesystem_utils import get_fs_path


@SkipIfFS.hive
class TestRefreshPartition(ImpalaTestSuite):
  """
  This class tests the functionality to refresh a partition individually
  for a table in HDFS
  """

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestRefreshPartition, cls).add_test_dimensions()

    # There is no reason to run these tests using all dimensions.
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))

  def test_refresh_invalid_partition(self, vector, unique_database):
    """
    Trying to refresh a partition that does not exist does not modify anything
    either in impala or hive.
    """
    table_name = unique_database + '.' + "partition_test_table"
    self.client.execute(
        'create table %s (x int) partitioned by (y int, z int)' %
        table_name)
    self.client.execute(
        'alter table %s add partition (y=333, z=5309)' % table_name)
    assert [('333', '5309')] == self.get_impala_partition_info(table_name, 'y', 'z')
    assert ['y=333/z=5309'] == self.hive_partition_names(table_name)
    self.client.execute('refresh %s partition (y=71, z=8857)' % table_name)
    assert [('333', '5309')] == self.get_impala_partition_info(table_name, 'y', 'z')
    assert ['y=333/z=5309'] == self.hive_partition_names(table_name)

  def test_remove_data_and_refresh(self, vector, unique_database):
    """
    Data removed through hive is visible in impala after refresh of partition.
    """
    expected_error = 'Error(2): No such file or directory'
    table_name = unique_database + '.' + "partition_test_table"
    self.client.execute(
        'create table %s (x int) partitioned by (y int, z int)' %
        table_name)
    self.client.execute(
        'alter table %s add partition (y=333, z=5309)' % table_name)
    self.client.execute(
        'insert into table %s partition (y=333, z=5309) values (2)' % table_name)
    assert '2\t333\t5309' == self.client.execute(
        'select * from %s' % table_name).get_data()

    self.run_stmt_in_hive(
        'alter table %s drop partition (y=333, z=5309)' % table_name)

    # Query the table. With file handle caching, this may not produce an error,
    # because the file handles are still open in the cache. If the system does
    # produce an error, it should be the expected error.
    try:
      self.client.execute("select * from %s" % table_name)
    except ImpalaBeeswaxException as e:
      assert expected_error in str(e)

    self.client.execute('refresh %s partition (y=333, z=5309)' % table_name)
    result = self.client.execute("select count(*) from %s" % table_name)
    assert result.data == [str('0')]

  def test_add_delete_data_to_hdfs_and_refresh(self, vector, unique_database):
    """
    Data added/deleted directly in HDFS is visible in impala after refresh of
    partition.
    """
    table_name = unique_database + '.' + "partition_test_table"
    table_location = get_fs_path("/test-warehouse/%s" % unique_database)
    file_name = "alltypes.parq"
    src_file = get_fs_path("/test-warehouse/alltypesagg_parquet/year=2010/month=1/"
      "day=9/*.parq")
    file_num_rows = 1000
    self.client.execute("""
      create table %s like functional.alltypes stored as parquet
      location '%s'
    """ % (table_name, table_location))
    self.client.execute("alter table %s add partition (year=2010, month=1)" %
        table_name)
    self.client.execute("refresh %s" % table_name)
    # Check that there is no data in table
    result = self.client.execute("select count(*) from %s" % table_name)
    assert result.data == [str(0)]
    dst_path = "%s/year=2010/month=1/%s" % (table_location, file_name)
    self.filesystem_client.copy(src_file, dst_path, overwrite=True)
    # Check that data added is not visible before refresh
    result = self.client.execute("select count(*) from %s" % table_name)
    assert result.data == [str(0)]
    # Chech that data is visible after refresh
    self.client.execute("refresh %s partition (year=2010, month=1)" % table_name)
    result = self.client.execute("select count(*) from %s" % table_name)
    assert result.data == [str(file_num_rows)]
    # Check that after deleting the file and refreshing, it returns zero rows
    check_call(["hadoop", "fs", "-rm", dst_path], shell=False)
    self.client.execute("refresh %s partition (year=2010, month=1)" % table_name)
    result = self.client.execute("select count(*) from %s" % table_name)
    assert result.data == [str(0)]

  def test_confirm_individual_refresh(self, vector, unique_database):
    """
    Data added directly to HDFS is only visible for the partition refreshed
    """
    table_name = unique_database + '.' + "partition_test_table"
    table_location = get_fs_path("/test-warehouse/%s" % unique_database)
    file_name = "alltypes.parq"
    src_file = get_fs_path("/test-warehouse/alltypesagg_parquet/year=2010/month=1/"
      "day=9/*.parq")
    file_num_rows = 1000
    self.client.execute("""
      create table %s like functional.alltypes stored as parquet
      location '%s'
    """ % (table_name, table_location))
    for month in [1, 2]:
        self.client.execute("alter table %s add partition (year=2010, month=%s)" %
        (table_name, month))
    self.client.execute("refresh %s" % table_name)
    # Check that there is no data in table
    result = self.client.execute("select count(*) from %s" % table_name)
    assert result.data == [str(0)]
    dst_path = table_location + "/year=2010/month=%s/" + file_name
    for month in [1, 2]:
        self.filesystem_client.copy(src_file, dst_path % month, overwrite=True)
    # Check that data added is not visible before refresh
    result = self.client.execute("select count(*) from %s" % table_name)
    assert result.data == [str(0)]
    # Check that data is visible after refresh on the first partition only
    self.client.execute("refresh %s partition (year=2010, month=1)" %
        table_name)
    result = self.client.execute("select count(*) from %s" % table_name)
    assert result.data == [str(file_num_rows)]
    # Check that the data is not yet visible for the second partition
    # that was not refreshed
    result = self.client.execute(
        "select count(*) from %s where year=2010 and month=2" % table_name)
    assert result.data == [str(0)]
    # Check that data is visible for the second partition after refresh
    self.client.execute("refresh %s partition (year=2010, month=2)" % table_name)
    result = self.client.execute("select count(*) from %s" % table_name)
    assert result.data == [str(file_num_rows*2)]
