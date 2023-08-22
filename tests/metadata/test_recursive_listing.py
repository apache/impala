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
from builtins import range
import pytest
import requests
import time

from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_test_suite import ImpalaTestSuite, LOG
from tests.common.test_dimensions import create_uncompressed_text_dimension
from tests.common.skip import SkipIfLocal, SkipIfFS
from tests.util.filesystem_utils import WAREHOUSE


@SkipIfLocal.hdfs_client
class TestRecursiveListing(ImpalaTestSuite):
  """
  This class tests that files are recursively listed within directories
  and partitions, and that REFRESH picks up changes within them.
  """
  enable_fs_tracing_url = "http://localhost:25020/set_java_loglevel?" \
                          "class=org.apache.impala.common.FileSystemUtil&level=trace"
  reset_log_level_url = "http://localhost:25020/reset_java_loglevel"

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

  def test_unpartitioned(self, unique_database):
    self._do_test(unique_database, partitioned=False)

  def test_partitioned(self, unique_database):
    self._do_test(unique_database, partitioned=True)

  def _init_test_table(self, unique_database, partitioned):
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

    return fq_tbl_name, part_path

  def _do_test(self, unique_database, partitioned):
    fq_tbl_name, part_path = self._init_test_table(unique_database, partitioned)

    # Add a file inside a nested directory and refresh.
    self.filesystem_client.make_dir("{0}/dir1".format(part_path))
    self.filesystem_client.create_file("{0}/dir1/file1.txt".format(part_path), "file1")
    self.execute_query_expect_success(self.client, "refresh {0}".format(fq_tbl_name))
    assert len(self._show_files(fq_tbl_name)) == 1
    assert len(self._get_rows(fq_tbl_name)) == 1

    # Add another file inside the same directory, make sure it shows up.
    self.filesystem_client.create_file("{0}/dir1/file2.txt".format(part_path), "file2")
    self.execute_query_expect_success(self.client, "refresh {0}".format(fq_tbl_name))
    assert len(self._show_files(fq_tbl_name)) == 2
    assert len(self._get_rows(fq_tbl_name)) == 2

    # Add a file at the top level, make sure it shows up.
    self.filesystem_client.create_file("{0}/file3.txt".format(part_path), "file3")
    self.execute_query_expect_success(self.client, "refresh {0}".format(fq_tbl_name))
    assert len(self._show_files(fq_tbl_name)) == 3
    assert len(self._get_rows(fq_tbl_name)) == 3

    # Create files in the nested hidden directories and refresh. Make sure it does not
    # show up
    self.filesystem_client.make_dir("{0}/.hive-staging".format(part_path))
    self.filesystem_client.create_file(
        "{0}/.hive-staging/file3.txt".format(part_path),
        "data-should-be-ignored-by-impala")
    self.filesystem_client.make_dir("{0}/_tmp.base_000000_1".format(part_path))
    self.filesystem_client.create_file(
        "{0}/_tmp.base_000000_1/000000_0.manifest".format(part_path),
        "manifest-file_contents")
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
    self.filesystem_client.delete_file_dir("{0}/dir1".format(part_path), recursive=True)
    self.execute_query_expect_success(self.client, "refresh {0}".format(fq_tbl_name))
    assert len(self._show_files(fq_tbl_name)) == 1
    assert len(self._get_rows(fq_tbl_name)) == 1

  @SkipIfFS.no_partial_listing
  @pytest.mark.stress
  def test_large_staging_dirs(self, unique_database):
    """Regression test for IMPALA-11464:
    Test REFRESH survives with concurrent add/remove ops on large staging/tmp dirs
    which contain more than 1000 files. Execute this serially since the sleep intervals
    might not work with concurrent workloads. Test this only on HDFS since other FS might
    not have partial listing (configured by dfs.ls.limit)."""
    fq_tbl_name, part_path = self._init_test_table(unique_database, partitioned=True)
    staging_dir = "{0}/.hive-staging".format(part_path)
    # Expected timeline (before the fix of IMPALA-11464):
    # 0ms:   catalogd list the partition dir and wait 200ms.
    # 200ms: catalogd get the staging dir and list it partially, then wait 200ms.
    # 300ms: remove the staging dir.
    # 400ms: catalogd consume the partial list (1000 items). Start listing the remaining
    #        items and get FileNotFoundException due to the dir is removed.
    # After the fix of IMPALA-11464, catalogd won't list the staging dir, so avoids
    # hitting the exception.
    self._test_listing_large_dir(fq_tbl_name, large_dir=staging_dir,
                                 pause_ms_after_remote_iterator_creation=200,
                                 pause_ms_before_file_cleanup=300,
                                 refresh_should_fail=False)

  @SkipIfFS.no_partial_listing
  @pytest.mark.stress
  def test_partition_dir_removed_inflight(self, unique_database):
    """Test REFRESH with concurrent add/remove ops on large partition dirs
    which contain more than 1000 files. Execute this serially since the sleep
    intervals might not work with concurrent workloads. Test this only on HDFS
    since other FS might not have partial listing (configured by dfs.ls.limit)"""
    fq_tbl_name, part_path = self._init_test_table(unique_database, partitioned=True)
    # Expected timeline:
    # 0ms:   catalogd start listing the partition dir. Get 1000 items in the first
    #        partial listing. Then wait for 300ms.
    # 200ms: The partition dir is removed.
    # 300ms: catalogd processed the 1000 items and start listing the remaining items.
    #        Then get FileNotFoundException since the partition dir disappears.
    self._test_listing_large_dir(fq_tbl_name, large_dir=part_path + '/',
                                 pause_ms_after_remote_iterator_creation=300,
                                 pause_ms_before_file_cleanup=200,
                                 refresh_should_fail=True)

  def _test_listing_large_dir(self, fq_tbl_name, large_dir,
                              pause_ms_after_remote_iterator_creation,
                              pause_ms_before_file_cleanup,
                              refresh_should_fail):
    # We need data files more than 1000 (default of dfs.ls.limit) so the initial
    # file listing can't list all of them.
    files = [large_dir + '/' + str(i) for i in range(1024)]
    refresh_stmt = "refresh " + fq_tbl_name
    self.client.set_configuration({
      "debug_action": "catalogd_pause_after_hdfs_remote_iterator_creation:SLEEP@"
                      + str(pause_ms_after_remote_iterator_creation)
    })
    # Enable TRACE logging in FileSystemUtil for better debugging
    response = requests.get(self.enable_fs_tracing_url)
    assert response.status_code == requests.codes.ok
    try:
      self.filesystem_client.delete_file_dir(large_dir, recursive=True)
      self.filesystem_client.make_dir(large_dir)
      self.filesystem_client.touch(files)
      LOG.info("created staging files under " + large_dir)
      handle = self.execute_query_async(refresh_stmt)
      # Wait a moment to let REFRESH finish expected partial listing on the dir.
      time.sleep(pause_ms_before_file_cleanup / 1000.0)
      LOG.info("removing staging dir " + large_dir)
      self.filesystem_client.delete_file_dir(large_dir, recursive=True)
      LOG.info("removed staging dir " + large_dir)
      try:
        self.client.fetch(refresh_stmt, handle)
        assert not refresh_should_fail, "REFRESH should fail"
      except ImpalaBeeswaxException as e:
        assert refresh_should_fail, "unexpected exception " + str(e)
    finally:
      requests.get(self.reset_log_level_url)
