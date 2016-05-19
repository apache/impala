# Copyright (c) 2015 Cloudera, Inc. All rights reserved.
# Validates table stored on the LocalFileSystem.
#
import pytest
import os
from subprocess import check_call, call
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import create_single_exec_option_dimension
from tests.common.skip import SkipIf
from tests.util.filesystem_utils import get_secondary_fs_path, S3_BUCKET_NAME, ISILON_NAMENODE

@SkipIf.no_secondary_fs
class TestMultipleFilesystems(ImpalaTestSuite):
  """
  Tests that tables and queries can span multiple filesystems.
  """

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

  def _populate_secondary_fs_partitions(self, db_name):
    # This directory may already exist. So we needn't mind if this call fails.
    call(["hadoop", "fs", "-mkdir", get_secondary_fs_path("/multi_fs_tests/")], shell=False)
    check_call(["hadoop", "fs", "-mkdir",
                get_secondary_fs_path("/multi_fs_tests/%s.db/" % db_name)], shell=False)
    check_call(["hadoop", "fs", "-cp", "/test-warehouse/alltypes_parquet/",
                get_secondary_fs_path("/multi_fs_tests/%s.db/" % db_name)], shell=False)
    check_call(["hadoop", "fs", "-cp", "/test-warehouse/tinytable/",
                get_secondary_fs_path("/multi_fs_tests/%s.db/" % db_name)], shell=False)

  @pytest.mark.execute_serially
  def test_multiple_filesystems(self, vector, unique_database):
    try:
      self._populate_secondary_fs_partitions(unique_database)
      self.run_test_case('QueryTest/multiple-filesystems', vector, use_db=unique_database)
    finally:
      # We delete this from the secondary filesystem here because the database was created
      # in HDFS but the queries will create this path in the secondary FS as well. So
      # dropping the database will not delete the directory in the secondary FS.
      check_call(["hadoop", "fs", "-rm", "-r",
          get_secondary_fs_path("/multi_fs_tests/%s.db/" % unique_database)], shell=False)
