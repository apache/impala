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
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import create_uncompressed_text_dimension
from tests.common.skip import SkipIfLocal
from tests.util.filesystem_utils import WAREHOUSE


@SkipIfLocal.hdfs_client
class TestRecursiveListing(ImpalaTestSuite):
  """
  This class tests that files are recursively listed within directories
  and partitions, and that REFRESH picks up changes within them.
  """
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestRecursiveListing, cls).add_test_dimensions()
    # don't use any exec options, running exactly once is fine
    cls.ImpalaTestMatrix.clear_dimension('exec_option')
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        (v.get_value('table_format').file_format == 'text' and
         v.get_value('table_format').compression_codec == 'none'))

  def _show_files(self, table):
    files = self.client.execute("show files in {0}".format(table))
    return files.data

  def _get_rows(self, table):
    result = self.client.execute("select * from {0}".format(table))
    return result.data

  def test_unpartitioned(self, vector, unique_database):
    self._do_test(vector, unique_database, partitioned=False)

  def test_partitioned(self, vector, unique_database):
    self._do_test(vector, unique_database, partitioned=True)

  def _do_test(self, vector, unique_database, partitioned):
    tbl_name = "t"
    fq_tbl_name = unique_database + "." + tbl_name
    tbl_path = '%s/%s.db/%s' % (WAREHOUSE, unique_database, tbl_name)

    # Create the table
    self.execute_query_expect_success(self.client,
        ("create table {tbl} (a string) {partclause} " +
         "stored as textfile location '{loc}'").format(
            tbl=fq_tbl_name,
            partclause=(partitioned and "partitioned by (p int)" or ""),
            loc=tbl_path))

    if partitioned:
      self.execute_query_expect_success(self.client,
        "alter table {0} add partition (p=1)".format(fq_tbl_name))
      part_path = tbl_path + "/p=1"
    else:
      part_path = tbl_path

    # Add a file inside a nested directory and refresh.
    self.filesystem_client.make_dir("{0}/dir1".format(part_path[1:]))
    self.filesystem_client.create_file("{0}/dir1/file1.txt".format(part_path[1:]),
        "file1")
    self.execute_query_expect_success(self.client, "refresh {0}".format(fq_tbl_name))
    assert len(self._show_files(fq_tbl_name)) == 1
    assert len(self._get_rows(fq_tbl_name)) == 1

    # Add another file inside the same directory, make sure it shows up.
    self.filesystem_client.create_file("{0}/dir1/file2.txt".format(part_path[1:]),
        "file2")
    self.execute_query_expect_success(self.client, "refresh {0}".format(fq_tbl_name))
    assert len(self._show_files(fq_tbl_name)) == 2
    assert len(self._get_rows(fq_tbl_name)) == 2

    # Add a file at the top level, make sure it shows up.
    self.filesystem_client.create_file("{0}/file3.txt".format(part_path[1:]),
        "file3")
    self.execute_query_expect_success(self.client, "refresh {0}".format(fq_tbl_name))
    assert len(self._show_files(fq_tbl_name)) == 3
    assert len(self._get_rows(fq_tbl_name)) == 3

    # Test that disabling recursive listings makes the nested files disappear.
    self.execute_query_expect_success(self.client, ("alter table {0} set tblproperties(" +
        "'impala.disable.recursive.listing'='true')").format(fq_tbl_name))
    self.execute_query_expect_success(self.client, "refresh {0}".format(fq_tbl_name))
    assert len(self._show_files(fq_tbl_name)) == 1
    assert len(self._get_rows(fq_tbl_name)) == 1
    # Re-enable.
    self.execute_query_expect_success(self.client, ("alter table {0} set tblproperties(" +
        "'impala.disable.recursive.listing'='false')").format(fq_tbl_name))
    self.execute_query_expect_success(self.client, "refresh {0}".format(fq_tbl_name))
    assert len(self._show_files(fq_tbl_name)) == 3
    assert len(self._get_rows(fq_tbl_name)) == 3

    # Remove the dir with two files. One should remain.
    self.filesystem_client.delete_file_dir("{0}/dir1".format(part_path[1:]),
        recursive=True)
    self.execute_query_expect_success(self.client, "refresh {0}".format(fq_tbl_name))
    assert len(self._show_files(fq_tbl_name)) == 1
    assert len(self._get_rows(fq_tbl_name)) == 1
