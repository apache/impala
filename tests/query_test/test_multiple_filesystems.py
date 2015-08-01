# Copyright (c) 2015 Cloudera, Inc. All rights reserved.
# Validates table stored on the LocalFileSystem.
#
import pytest
from subprocess import check_call
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import create_single_exec_option_dimension
from tests.common.skip import SkipIf, SkipIfIsilon
from tests.util.filesystem_utils import get_fs_path

@SkipIf.default_fs # Run only when a non-default filesystem is available.
@SkipIfIsilon.untriaged # Missing coverage: Find out why this is failing.
class TestMultipleFilesystems(ImpalaTestSuite):
  """
  Tests that tables and queries can span multiple filesystems.
  """

  TEST_DB = 'multi_fs_db'

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestMultipleFilesystems, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(create_single_exec_option_dimension())

    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text' and \
        v.get_value('table_format').compression_codec == 'none')

  def setup_method(self, method):
    self.cleanup_db(self.TEST_DB)
    # Note: Purposely creates database on the default filesystem. Do not specify location.
    self.client.execute("create database %s" % self.TEST_DB)
    self._populate_hdfs_partitions()

  def teardown_method(self, method):
    self.cleanup_db(self.TEST_DB)

  def _populate_hdfs_partitions(self):
    """ Copy some data to defaultFS HDFS filesystem so that the test can verify tables
    that span the default (HDFS) and secondary filesystem (e.g. S3A)."""
    check_call(["hadoop", "fs", "-cp",
                get_fs_path("/test-warehouse/alltypes_parquet"),
                "/test-warehouse/%s.db/" % self.TEST_DB], shell=False)

  def test_local_filesystem(self, vector):
    self.run_test_case('QueryTest/multiple-filesystems', vector, use_db=self.TEST_DB)
