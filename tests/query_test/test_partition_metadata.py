#!/usr/bin/env python
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
from tests.common.test_result_verifier import *
from tests.util.shell_util import exec_process
from tests.common.test_vector import *
from tests.common.test_dimensions import ALL_NODES_ONLY
from tests.common.impala_test_suite import *

# Tests specific to partition metadata.
# TODO: Split up the DDL tests and move some of the partition-specific tests
# here.
class TestPartitionMetadata(ImpalaTestSuite):
  TEST_DB = 'partition_md'

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
    self.client.refresh()
    self.client.execute("create database %s" % self.TEST_DB)

  @pytest.mark.execute_serially
  def test_hive_bulk_partition(self, vector):
    """Regression test for IMPALA-597. Verifies Impala is able to properly read
    tables that were altered using Hive's bulk partition statements that result
    in multiple partitions pointing to the same location.
    TODO: Once IMPALA-624 is resolved re-write this test using Impala instead of Hive.
    """
    self.client.execute("use %s" % self.TEST_DB)
    location = '/test-warehouse/hive_bulk_part'
    # Cleanup any existing data in the table directory.
    self.hdfs_client.delete_file_dir(location[1:], recursive=True)
    # Create the table
    self.client.execute("create table hive_bulk_part(i int) partitioned by(j int)"\
        "location '%s'" % location)

    # Point multiple partitions to the same location and use partition locations that
    # do not contain a key=value path.
    self.hdfs_client.make_dir(location[1:] + '/p')

    hive_cmd = "use %s; alter table hive_bulk_part add partition (j=1) location '%s/p'"\
        " partition(j=2) location '%s/p'" % (self.TEST_DB, location, location)
    print "Executing: %s" % hive_cmd
    rc, stdout, stderr = exec_process("hive -e \"%s\"" % hive_cmd)
    assert rc == 0, stdout + '\n' + stderr

    # Insert some data.
    hive_cmd = "insert into table %s.hive_bulk_part partition(j=1) select 1 from "\
               "functional.alltypes limit 1" % self.TEST_DB
    print "Executing: %s" % hive_cmd
    rc, stdout, stderr = exec_process("hive -e \"%s\"" % hive_cmd)
    assert rc == 0, stdout + '\n' + stderr

    # Reload the table metadata and ensure Impala detects this properly.
    self.client.execute("invalidate metadata hive_bulk_part")

    # The data will be read twice because each partition points to the same location.
    data = self.execute_scalar("select sum(i), sum(j) from hive_bulk_part")
    assert data.split('\t') == ['2', '3']

    self.client.execute("insert into hive_bulk_part partition(j) select 1, 1")
    self.client.execute("insert into hive_bulk_part partition(j) select 1, 2")
    data = self.execute_scalar("select sum(i), sum(j) from hive_bulk_part")
    try:
      assert data.split('\t') == ['6', '6']
    except AssertionError:
      pytest.xfail('IMPALA 624: Impala does not use a partition location for INSERT')
