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
from tests.common.skip import SkipIfS3
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
    self.client.execute("create database %s" % self.TEST_DB)

  def teardown_method(self, method):
    self.cleanup_db(self.TEST_DB)

  @SkipIfS3.insert # S3: missing coverage: partition DDL
  def test_multiple_partitions_same_location(self, vector):
    """Regression test for IMPALA-597. Verifies Impala is able to properly read
    tables that have multiple partitions pointing to the same location.
    """
    self.client.execute("use %s" % self.TEST_DB)
    location = '%s/%s' % (WAREHOUSE, self.TEST_TBL)
    # Cleanup any existing data in the table directory.
    self.hdfs_client.delete_file_dir(location[1:], recursive=True)
    # Create the table
    self.client.execute("create table %s(i int) partitioned by(j int)"\
        "location '%s'" % (self.TEST_TBL, location))

    # Point multiple partitions to the same location and use partition locations that
    # do not contain a key=value path.
    self.hdfs_client.make_dir(location[1:] + '/p')

    # Point both partitions to the same location.
    self.client.execute("alter table %s add partition (j=1) location '%s/p'" %
        (self.TEST_TBL, location))
    self.client.execute("alter table %s add partition (j=2) location '%s/p'" %
        (self.TEST_TBL, location))

    # Insert some data.
    self.client.execute("insert into table %s partition(j=1) select 1" % self.TEST_TBL)

    # The data will be read twice because each partition points to the same location.
    data = self.execute_scalar("select sum(i), sum(j) from %s" % self.TEST_TBL)
    assert data.split('\t') == ['2', '3']

    self.client.execute("insert into %s partition(j) select 1, 1" % self.TEST_TBL)
    self.client.execute("insert into %s partition(j) select 1, 2" % self.TEST_TBL)
    data = self.execute_scalar("select sum(i), sum(j) from %s" % self.TEST_TBL)
    assert data.split('\t') == ['6', '9']
